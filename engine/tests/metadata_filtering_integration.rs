//! Integration tests for metadata filtering

use kyrodb_engine::metadata_filter::matches;
use kyrodb_engine::proto::{
    AndFilter, ExactMatch, InMatch, MetadataFilter, NotFilter, OrFilter, RangeMatch,
};
use kyrodb_engine::{TieredEngine, TieredEngineConfig};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::RwLock;

fn unit_vec_128(idx: usize) -> Vec<f32> {
    let mut v = vec![0.0; 128];
    v[idx % 128] = 1.0;
    v
}

struct TestEngineGuard {
    engine: TieredEngine,
    _temp_dir: TempDir,
}

impl Deref for TestEngineGuard {
    type Target = TieredEngine;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

impl DerefMut for TestEngineGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.engine
    }
}

fn create_test_engine() -> TestEngineGuard {
    let temp_dir = TempDir::new().unwrap();
    let config = TieredEngineConfig {
        hot_tier_max_size: 100,
        hot_tier_max_age: Duration::from_secs(3600),
        hot_tier_hard_limit: 150,
        hnsw_max_elements: 1000,
        embedding_dimension: 128,
        hnsw_distance: kyrodb_engine::config::DistanceMetric::Cosine,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_search: 50,
        hnsw_disable_normalization_check: false,
        hnsw_ann_search_mode: kyrodb_engine::config::AnnSearchMode::Fp32Strict,
        hnsw_quantized_rerank_multiplier: 8,
        data_dir: Some(temp_dir.path().to_str().unwrap().to_string()),
        fsync_policy: kyrodb_engine::FsyncPolicy::Never,
        snapshot_interval: 1000,
        max_wal_size_bytes: 1024 * 1024,
        flush_interval: Duration::from_secs(60),
        cache_timeout_ms: 10,
        hot_tier_timeout_ms: 50,
        cold_tier_timeout_ms: 100,
        max_concurrent_queries: 1000,
    };

    let cache_strategy = Box::new(kyrodb_engine::cache_strategy::LruCacheStrategy::new(10));
    let query_cache = Arc::new(kyrodb_engine::QueryHashCache::new(10, 0.8));
    let initial_embeddings = vec![unit_vec_128(0)];
    let initial_metadata = vec![HashMap::new()];

    let engine = TieredEngine::new(
        cache_strategy,
        query_cache,
        initial_embeddings,
        initial_metadata,
        config,
    )
    .unwrap();

    TestEngineGuard {
        engine,
        _temp_dir: temp_dir,
    }
}

#[tokio::test]
async fn test_exact_match_filtering() {
    let engine = create_test_engine();

    let mut meta1 = HashMap::new();
    meta1.insert("category".to_string(), "A".to_string());
    engine.insert(1, unit_vec_128(1), meta1).unwrap();

    let mut meta2 = HashMap::new();
    meta2.insert("category".to_string(), "B".to_string());
    engine.insert(2, unit_vec_128(2), meta2).unwrap();

    let filter = MetadataFilter {
        filter_type: Some(kyrodb_engine::proto::metadata_filter::FilterType::Exact(
            ExactMatch {
                key: "category".to_string(),
                value: "A".to_string(),
            },
        )),
    };

    let metadata1 = engine.get_metadata(1).unwrap();
    assert!(matches(&filter, &metadata1));

    let metadata2 = engine.get_metadata(2).unwrap();
    assert!(!matches(&filter, &metadata2));
}

#[tokio::test]
async fn test_range_filtering_gte() {
    let engine = create_test_engine();

    let mut meta1 = HashMap::new();
    meta1.insert("score".to_string(), "50".to_string());
    engine.insert(1, unit_vec_128(1), meta1).unwrap();

    let mut meta2 = HashMap::new();
    meta2.insert("score".to_string(), "75".to_string());
    engine.insert(2, unit_vec_128(2), meta2).unwrap();

    let filter = MetadataFilter {
        filter_type: Some(kyrodb_engine::proto::metadata_filter::FilterType::Range(
            RangeMatch {
                key: "score".to_string(),
                bound: Some(kyrodb_engine::proto::range_match::Bound::Gte(
                    "70".to_string(),
                )),
            },
        )),
    };

    let metadata1 = engine.get_metadata(1).unwrap();
    assert!(!matches(&filter, &metadata1));

    let metadata2 = engine.get_metadata(2).unwrap();
    assert!(matches(&filter, &metadata2));
}

#[tokio::test]
async fn test_and_filtering() {
    let engine = create_test_engine();

    let mut meta1 = HashMap::new();
    meta1.insert("category".to_string(), "A".to_string());
    meta1.insert("status".to_string(), "active".to_string());
    engine.insert(1, unit_vec_128(1), meta1).unwrap();

    let mut meta2 = HashMap::new();
    meta2.insert("category".to_string(), "A".to_string());
    meta2.insert("status".to_string(), "inactive".to_string());
    engine.insert(2, unit_vec_128(2), meta2).unwrap();

    let filter = MetadataFilter {
        filter_type: Some(
            kyrodb_engine::proto::metadata_filter::FilterType::AndFilter(AndFilter {
                filters: vec![
                    MetadataFilter {
                        filter_type: Some(
                            kyrodb_engine::proto::metadata_filter::FilterType::Exact(ExactMatch {
                                key: "category".to_string(),
                                value: "A".to_string(),
                            }),
                        ),
                    },
                    MetadataFilter {
                        filter_type: Some(
                            kyrodb_engine::proto::metadata_filter::FilterType::Exact(ExactMatch {
                                key: "status".to_string(),
                                value: "active".to_string(),
                            }),
                        ),
                    },
                ],
            }),
        ),
    };

    let metadata1 = engine.get_metadata(1).unwrap();
    assert!(matches(&filter, &metadata1));

    let metadata2 = engine.get_metadata(2).unwrap();
    assert!(!matches(&filter, &metadata2));
}

#[tokio::test]
async fn test_or_filtering() {
    let engine = create_test_engine();

    let mut meta1 = HashMap::new();
    meta1.insert("category".to_string(), "A".to_string());
    engine.insert(1, unit_vec_128(1), meta1).unwrap();

    let mut meta2 = HashMap::new();
    meta2.insert("category".to_string(), "B".to_string());
    engine.insert(2, unit_vec_128(2), meta2).unwrap();

    let mut meta3 = HashMap::new();
    meta3.insert("category".to_string(), "C".to_string());
    engine.insert(3, unit_vec_128(3), meta3).unwrap();

    let filter = MetadataFilter {
        filter_type: Some(kyrodb_engine::proto::metadata_filter::FilterType::OrFilter(
            OrFilter {
                filters: vec![
                    MetadataFilter {
                        filter_type: Some(
                            kyrodb_engine::proto::metadata_filter::FilterType::Exact(ExactMatch {
                                key: "category".to_string(),
                                value: "A".to_string(),
                            }),
                        ),
                    },
                    MetadataFilter {
                        filter_type: Some(
                            kyrodb_engine::proto::metadata_filter::FilterType::Exact(ExactMatch {
                                key: "category".to_string(),
                                value: "B".to_string(),
                            }),
                        ),
                    },
                ],
            },
        )),
    };

    let metadata1 = engine.get_metadata(1).unwrap();
    assert!(matches(&filter, &metadata1));

    let metadata2 = engine.get_metadata(2).unwrap();
    assert!(matches(&filter, &metadata2));

    let metadata3 = engine.get_metadata(3).unwrap();
    assert!(!matches(&filter, &metadata3));
}

#[tokio::test]
async fn test_not_filter() {
    let engine = create_test_engine();

    let mut meta1 = HashMap::new();
    meta1.insert("type".to_string(), "foo".to_string());
    engine.insert(1, unit_vec_128(1), meta1).unwrap();

    let mut meta2 = HashMap::new();
    meta2.insert("type".to_string(), "bar".to_string());
    engine.insert(2, unit_vec_128(2), meta2).unwrap();

    let filter = MetadataFilter {
        filter_type: Some(
            kyrodb_engine::proto::metadata_filter::FilterType::NotFilter(Box::new(NotFilter {
                filter: Some(Box::new(MetadataFilter {
                    filter_type: Some(kyrodb_engine::proto::metadata_filter::FilterType::Exact(
                        ExactMatch {
                            key: "type".to_string(),
                            value: "foo".to_string(),
                        },
                    )),
                })),
            })),
        ),
    };

    let metadata1 = engine.get_metadata(1).unwrap();
    assert!(!matches(&filter, &metadata1));

    let metadata2 = engine.get_metadata(2).unwrap();
    assert!(matches(&filter, &metadata2));
}

#[tokio::test]
async fn test_in_match_filtering() {
    let engine = create_test_engine();

    let mut meta1 = HashMap::new();
    meta1.insert("status".to_string(), "pending".to_string());
    engine.insert(1, unit_vec_128(1), meta1).unwrap();

    let mut meta2 = HashMap::new();
    meta2.insert("status".to_string(), "completed".to_string());
    engine.insert(2, unit_vec_128(2), meta2).unwrap();

    let mut meta3 = HashMap::new();
    meta3.insert("status".to_string(), "failed".to_string());
    engine.insert(3, unit_vec_128(3), meta3).unwrap();

    let filter = MetadataFilter {
        filter_type: Some(kyrodb_engine::proto::metadata_filter::FilterType::InMatch(
            InMatch {
                key: "status".to_string(),
                values: vec!["pending".to_string(), "completed".to_string()],
            },
        )),
    };

    let metadata1 = engine.get_metadata(1).unwrap();
    assert!(matches(&filter, &metadata1));

    let metadata2 = engine.get_metadata(2).unwrap();
    assert!(matches(&filter, &metadata2));

    let metadata3 = engine.get_metadata(3).unwrap();
    assert!(!matches(&filter, &metadata3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_filtered_search_no_race_conditions() {
    let engine = Arc::new(RwLock::new(create_test_engine()));

    // Insert 100 documents
    for i in 0..100 {
        let mut meta = HashMap::new();
        meta.insert("category".to_string(), format!("cat_{}", i % 10));
        meta.insert("score".to_string(), i.to_string());
        let embedding = unit_vec_128(i as usize);
        engine.write().await.insert(i, embedding, meta).unwrap();
    }

    // Spawn 100 concurrent reads with different filters
    let mut handles = vec![];
    for i in 0..100 {
        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            let filter = MetadataFilter {
                filter_type: Some(kyrodb_engine::proto::metadata_filter::FilterType::Exact(
                    ExactMatch {
                        key: "category".to_string(),
                        value: format!("cat_{}", i % 10),
                    },
                )),
            };

            for doc_id in 0..100 {
                if let Some(metadata) = engine_clone.read().await.get_metadata(doc_id) {
                    let _ = matches(&filter, &metadata);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_filter_with_missing_metadata_fields() {
    let engine = create_test_engine();

    let mut meta1 = HashMap::new();
    meta1.insert("category".to_string(), "A".to_string());
    engine.insert(1, unit_vec_128(1), meta1).unwrap();

    let filter = MetadataFilter {
        filter_type: Some(kyrodb_engine::proto::metadata_filter::FilterType::Exact(
            ExactMatch {
                key: "score".to_string(),
                value: "100".to_string(),
            },
        )),
    };

    let metadata1 = engine.get_metadata(1).unwrap();
    assert!(!matches(&filter, &metadata1));
}

#[tokio::test]
async fn test_empty_metadata() {
    let engine = create_test_engine();

    let meta1 = HashMap::new();
    engine.insert(1, unit_vec_128(1), meta1).unwrap();

    let filter = MetadataFilter {
        filter_type: Some(kyrodb_engine::proto::metadata_filter::FilterType::Exact(
            ExactMatch {
                key: "any_key".to_string(),
                value: "any_value".to_string(),
            },
        )),
    };

    let metadata1 = engine.get_metadata(1).unwrap();
    assert!(!matches(&filter, &metadata1));
}
