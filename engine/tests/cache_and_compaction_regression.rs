use kyrodb_engine::{
    cache_strategy::LruCacheStrategy, DistanceMetric, HnswBackend, QueryHashCache, TieredEngine,
    TieredEngineConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

#[test]
fn search_cache_invalidated_on_insert() {
    let config = TieredEngineConfig {
        embedding_dimension: 2,
        ..Default::default()
    };

    let doc1 = normalize(vec![0.9848, 0.1736]); // ~10 degrees off
    let embeddings = vec![doc1];
    let metadata = vec![HashMap::new()];

    let cache_strategy = Box::new(LruCacheStrategy::new(16));
    let query_cache = Arc::new(QueryHashCache::new(16, 0.85));

    let engine = TieredEngine::new(cache_strategy, query_cache, embeddings, metadata, config)
        .expect("engine init");

    let query = normalize(vec![1.0, 0.0]);

    // First search caches the result (doc_id=0 from cold tier).
    let first = engine.knn_search(&query, 1).expect("search 1");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].doc_id, 0);

    // Insert a closer vector. This must invalidate the search cache so the next search
    // sees read-your-writes.
    engine
        .insert(1, normalize(vec![1.0, 0.0]), HashMap::new())
        .expect("insert");

    let second = engine.knn_search(&query, 1).expect("search 2");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].doc_id, 1);
}

#[test]
fn hnsw_capacity_reclaimed_via_tombstone_compaction() {
    let embeddings = vec![normalize(vec![1.0, 0.0]), normalize(vec![0.0, 1.0])];
    let metadata = vec![HashMap::new(); embeddings.len()];

    // Capacity is 3; fill it, delete one, then insert again.
    let backend =
        HnswBackend::new(2, DistanceMetric::Cosine, embeddings, metadata, 3).expect("backend init");

    backend
        .insert(2, normalize(vec![0.7, 0.3]), HashMap::new())
        .expect("insert to fill");
    assert_eq!(backend.len(), 3);

    assert!(backend.delete(0).expect("delete"));
    assert_eq!(backend.len(), 2);

    // This insert would fail without compaction because the HNSW graph is "full"
    // even though a tombstone exists.
    backend
        .insert(3, normalize(vec![0.6, 0.4]), HashMap::new())
        .expect("insert after compaction");

    assert_eq!(backend.len(), 3);
    assert!(backend.fetch_document(3).is_some());
}

#[test]
fn snapshots_use_unique_filenames() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    let embeddings = vec![normalize(vec![1.0, 0.0]), normalize(vec![0.0, 1.0])];
    let metadata = vec![HashMap::new(); embeddings.len()];

    let backend = HnswBackend::with_persistence(
        2,
        DistanceMetric::Cosine,
        embeddings,
        metadata,
        10,
        data_dir,
        kyrodb_engine::persistence::FsyncPolicy::Never,
        10_000,
        1024 * 1024,
    )
    .unwrap();

    backend.create_snapshot().unwrap();
    backend.create_snapshot().unwrap();

    let snapshot_count = std::fs::read_dir(data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.starts_with("snapshot_") && s.ends_with(".snap"))
                .unwrap_or(false)
        })
        .count();

    assert!(
        snapshot_count >= 2,
        "expected at least 2 snapshot files, found {}",
        snapshot_count
    );
}
