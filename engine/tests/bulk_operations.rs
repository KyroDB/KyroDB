use kyrodb_engine::{
    cache_strategy::LruCacheStrategy, persistence::FsyncPolicy, HnswBackend, HotTier,
    QueryHashCache, TieredEngine, TieredEngineConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_hot_tier_bulk_fetch() {
    let hot_tier = HotTier::new(
        100,
        Duration::from_secs(60),
        kyrodb_engine::config::DistanceMetric::Cosine,
    );

    // Insert docs
    for i in 0..10 {
        let mut meta = HashMap::new();
        meta.insert("id".to_string(), i.to_string());
        hot_tier.insert(i, vec![i as f32], meta);
    }

    // Bulk fetch
    let ids = vec![0, 5, 9, 99]; // 99 is missing
    let results = hot_tier.bulk_fetch(&ids);

    assert_eq!(results.len(), 4);

    // Check 0
    assert!(results[0].is_some());
    let (emb0, meta0) = results[0].as_ref().unwrap();
    assert_eq!(emb0[0], 0.0);
    assert_eq!(meta0.get("id").unwrap(), "0");

    // Check 5
    assert!(results[1].is_some());

    // Check 9
    assert!(results[2].is_some());

    // Check 99 (missing)
    assert!(results[3].is_none());
}

#[test]
fn test_hnsw_backend_bulk_fetch() {
    let embeddings = vec![vec![1.0], vec![-1.0], vec![1.0]];
    let metadata = vec![
        HashMap::from([("id".to_string(), "0".to_string())]),
        HashMap::from([("id".to_string(), "1".to_string())]),
        HashMap::from([("id".to_string(), "2".to_string())]),
    ];

    let backend = HnswBackend::new(
        1,
        kyrodb_engine::config::DistanceMetric::Cosine,
        embeddings,
        metadata,
        100,
    )
    .unwrap();

    // Bulk fetch
    let ids = vec![0, 2, 5]; // 5 is missing
    let results = backend.bulk_fetch(&ids);

    assert_eq!(results.len(), 3);

    // Check 0
    assert!(results[0].is_some());
    let (emb0, _meta0) = results[0].as_ref().unwrap();
    assert_eq!(emb0[0], 1.0);

    // Check 2
    assert!(results[1].is_some());

    // Check 5 (missing)
    assert!(results[2].is_none());
}

#[test]
fn test_tiered_engine_bulk_query() {
    let temp_dir = TempDir::new().unwrap();
    let config = TieredEngineConfig {
        data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
        fsync_policy: FsyncPolicy::Never,
        embedding_dimension: 1,
        ..Default::default()
    };

    // Initial cold tier data
    let initial_embeddings = vec![vec![1.0]];
    let initial_metadata = vec![HashMap::from([("loc".to_string(), "cold".to_string())])];

    let cache_strategy = Box::new(LruCacheStrategy::new(100));
    let query_cache = Arc::new(QueryHashCache::new(100, 0.9));

    let engine = TieredEngine::new(
        cache_strategy,
        query_cache,
        initial_embeddings,
        initial_metadata,
        config,
    )
    .unwrap();

    // Insert into hot tier
    let mut meta_hot = HashMap::new();
    meta_hot.insert("loc".to_string(), "hot".to_string());
    engine.insert(1, vec![-1.0], meta_hot).unwrap();

    // Bulk query (0 is in cold, 1 is in hot, 2 is missing)
    let ids = vec![0, 1, 2];

    // Test with embeddings
    let results = engine.bulk_query(&ids, true);
    assert_eq!(results.len(), 3);

    // 0 (cold)
    assert!(results[0].is_some());
    let (emb0, meta0) = results[0].as_ref().unwrap();
    assert_eq!(emb0[0], 1.0);
    assert_eq!(meta0.get("loc").unwrap(), "cold");

    // 1 (hot)
    assert!(results[1].is_some());
    let (emb1, meta1) = results[1].as_ref().unwrap();
    assert_eq!(emb1[0], -1.0);
    assert_eq!(meta1.get("loc").unwrap(), "hot");

    // 2 (missing)
    assert!(results[2].is_none());

    // Test without embeddings
    let results_no_emb = engine.bulk_query(&ids, false);
    assert_eq!(results_no_emb.len(), 3);

    // 0
    assert!(results_no_emb[0].is_some());
    assert!(results_no_emb[0].as_ref().unwrap().0.is_empty());
    assert_eq!(
        results_no_emb[0].as_ref().unwrap().1.get("loc").unwrap(),
        "cold"
    );

    // 1
    assert!(results_no_emb[1].is_some());
    assert!(results_no_emb[1].as_ref().unwrap().0.is_empty());

    // 2
    assert!(results_no_emb[2].is_none());
}
