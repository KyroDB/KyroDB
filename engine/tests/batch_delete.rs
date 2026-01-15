use kyrodb_engine::{
    cache_strategy::LruCacheStrategy, persistence::FsyncPolicy, HnswBackend, HotTier,
    QueryHashCache, TieredEngine, TieredEngineConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_hot_tier_batch_delete() {
    let hot_tier = HotTier::new(100, Duration::from_secs(60));

    // Insert docs
    for i in 0..10 {
        hot_tier.insert(i, vec![i as f32], HashMap::new());
    }

    assert_eq!(hot_tier.len(), 10);

    // Batch delete
    let ids = vec![0, 5, 9, 99]; // 99 is missing
    let count = hot_tier.batch_delete(&ids);

    assert_eq!(count, 3); // 0, 5, 9 deleted
    assert_eq!(hot_tier.len(), 7);

    assert!(hot_tier.get(0).is_none());
    assert!(hot_tier.get(5).is_none());
    assert!(hot_tier.get(9).is_none());
    assert!(hot_tier.get(1).is_some());
}

#[test]
fn test_hnsw_backend_batch_delete() {
    let temp_dir = TempDir::new().unwrap();
    let embeddings = vec![vec![0.1], vec![1.0], vec![2.0], vec![3.0]];
    let metadata = vec![HashMap::new(); 4];

    let backend = HnswBackend::with_persistence(
        1,
        embeddings,
        metadata,
        100,
        temp_dir.path(),
        FsyncPolicy::Never,
        100,
    )
    .unwrap();

    assert_eq!(backend.len(), 4);

    // Batch delete
    let ids = vec![0, 2, 99]; // 99 missing
    let count = backend.batch_delete(&ids).unwrap();

    assert_eq!(count, 2);

    // Check existence (tombstones)
    assert!(!backend.exists(0));
    assert!(backend.exists(1));
    assert!(!backend.exists(2));
    assert!(backend.exists(3));

    // Verify tombstones are all zeros
    let emb0 = backend.fetch_document(0).unwrap();
    assert!(emb0.iter().all(|&x| x == 0.0));
}

#[test]
fn test_tiered_engine_batch_delete() {
    let temp_dir = TempDir::new().unwrap();
    let config = TieredEngineConfig {
        data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
        fsync_policy: FsyncPolicy::Never,
        embedding_dimension: 1,
        ..Default::default()
    };

    // Initial cold tier data: 0, 1
    let initial_embeddings = vec![vec![0.1], vec![1.0]];
    let initial_metadata = vec![HashMap::new(), HashMap::new()];

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

    // Insert into hot tier: 2, 3
    engine.insert(2, vec![2.0], HashMap::new()).unwrap();
    engine.insert(3, vec![3.0], HashMap::new()).unwrap();

    // Batch delete: 0 (cold), 2 (hot), 99 (missing)
    let ids = vec![0, 2, 99];
    let count = engine.batch_delete(&ids).unwrap();

    assert_eq!(count, 2);

    assert!(!engine.exists(0));
    assert!(engine.exists(1));
    assert!(!engine.exists(2));
    assert!(engine.exists(3));
}

#[test]
fn test_tiered_engine_batch_delete_by_filter() {
    let temp_dir = TempDir::new().unwrap();
    let config = TieredEngineConfig {
        data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
        fsync_policy: FsyncPolicy::Never,
        embedding_dimension: 1,
        ..Default::default()
    };

    // Cold tier: 0 (cat=A), 1 (cat=B)
    let mut m0 = HashMap::new();
    m0.insert("cat".to_string(), "A".to_string());
    let mut m1 = HashMap::new();
    m1.insert("cat".to_string(), "B".to_string());

    let initial_embeddings = vec![vec![0.1], vec![1.0]];
    let initial_metadata = vec![m0, m1];

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

    // Hot tier: 2 (cat=A), 3 (cat=C)
    let mut m2 = HashMap::new();
    m2.insert("cat".to_string(), "A".to_string());
    let mut m3 = HashMap::new();
    m3.insert("cat".to_string(), "C".to_string());

    engine.insert(2, vec![2.0], m2).unwrap();
    engine.insert(3, vec![3.0], m3).unwrap();

    // Delete where cat=A (should delete 0 and 2)
    let count = engine
        .batch_delete_by_filter(|meta| meta.get("cat").map(|v| v == "A").unwrap_or(false))
        .unwrap();

    assert_eq!(count, 2);

    assert!(!engine.exists(0));
    assert!(engine.exists(1));
    assert!(!engine.exists(2));
    assert!(engine.exists(3));
}
