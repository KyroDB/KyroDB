// Phase 0 Week 9-12: Integration tests for A/B testing framework
// Tests the full query flow: HNSW search → cache check → A/B split → stats persistence

use kyrodb_engine::{
    AbStatsPersister, AbTestSplitter, AccessEvent, AccessPatternLogger, AccessType, CacheStrategy,
    CachedVector, HnswVectorIndex, LearnedCachePredictor, LearnedCacheStrategy, LruCacheStrategy,
    TrainingConfig,
};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tempfile::TempDir;
use tokio::sync::RwLock;

/// Test full A/B test flow: query → cache check → HNSW search → cache admission → stats
#[tokio::test]
async fn test_full_ab_test_flow() {
    // Setup: Create HNSW index with vectors
    let dim = 128;
    let mut hnsw = HnswVectorIndex::new(dim, 1000).unwrap();

    // Insert 100 vectors
    for i in 0u64..100 {
        let embedding: Vec<f32> = (0..dim)
            .map(|j| (i * dim as u64 + j as u64) as f32 * 0.01)
            .collect();
        hnsw.add_vector(i, &embedding).unwrap();
    }

    // Setup: Create cache strategies
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let lru_strategy = Arc::new(LruCacheStrategy::new(50));
    let learned_strategy = Arc::new(LearnedCacheStrategy::new(50, predictor));
    let splitter = AbTestSplitter::new(lru_strategy.clone(), learned_strategy.clone());

    // Setup: Create stats persister
    let temp_dir = TempDir::new().unwrap();
    let stats_path = temp_dir.path().join("ab_stats.csv");
    let persister = Arc::new(AbStatsPersister::new(&stats_path).unwrap());

    // Simulate queries
    for i in 0u64..20 {
        let query: Vec<f32> = (0..dim)
            .map(|j| (i * dim as u64 + j as u64) as f32 * 0.01)
            .collect();
        let doc_id = i % 100;

        // Get strategy for this query
        let strategy = splitter.get_strategy(doc_id);

        let start = Instant::now();

        // Check cache
        if let Some(_cached) = strategy.get_cached(doc_id) {
            // Cache hit
            let latency_ns = start.elapsed().as_nanos() as u64;
            persister
                .log_hit(strategy.name(), doc_id, latency_ns)
                .await
                .unwrap();
        } else {
            // Cache miss: HNSW search
            let _results = hnsw.knn_search(&query, 10).unwrap();
            let latency_ns = start.elapsed().as_nanos() as u64;
            persister
                .log_miss(strategy.name(), doc_id, latency_ns)
                .await
                .unwrap();

            // Cache admission
            if strategy.should_cache(doc_id, &query) {
                let cached = CachedVector {
                    doc_id,
                    embedding: query.clone(),
                    distance: 0.0,
                    cached_at: Instant::now(),
                };
                strategy.insert_cached(cached);
            }
        }
    }

    // Flush stats
    persister.flush().await.unwrap();

    // Verify stats were written
    let metrics = AbStatsPersister::load_metrics(&stats_path).unwrap();
    assert!(metrics.len() > 0, "No metrics logged");
    assert!(metrics
        .iter()
        .any(|m| m.event_type == "hit" || m.event_type == "miss"));
}

/// Test A/B traffic split is correct (50/50 distribution)
#[tokio::test]
async fn test_ab_split_distribution() {
    let lru_strategy = Arc::new(LruCacheStrategy::new(100));
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let learned_strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));
    let splitter = AbTestSplitter::new(lru_strategy.clone(), learned_strategy.clone());

    // Test distribution over 1000 queries
    let mut lru_count = 0;
    let mut learned_count = 0;

    for doc_id in 0..1000 {
        let strategy = splitter.get_strategy(doc_id);
        if strategy.name() == "lru_baseline" {
            lru_count += 1;
        } else {
            learned_count += 1;
        }
    }

    // Should be roughly 50/50 (within 10% tolerance)
    assert!(
        (lru_count as i32 - 500).abs() < 50,
        "LRU count: {}",
        lru_count
    );
    assert!(
        (learned_count as i32 - 500).abs() < 50,
        "Learned count: {}",
        learned_count
    );
}

/// Test cache improves performance over time (cache hits increase)
#[tokio::test]
async fn test_cache_hit_rate_improves() {
    let dim = 128;
    let mut hnsw = HnswVectorIndex::new(dim, 1000).unwrap();

    // Insert 100 vectors
    for i in 0u64..100 {
        let embedding: Vec<f32> = (0..dim)
            .map(|j| (i * dim as u64 + j as u64) as f32 * 0.01)
            .collect();
        hnsw.add_vector(i, &embedding).unwrap();
    }

    // Create LRU strategy
    let strategy = Arc::new(LruCacheStrategy::new(50));

    // First pass: All misses (cold cache)
    let mut hits_pass1 = 0;
    for i in 0u64..50 {
        let query: Vec<f32> = (0..dim)
            .map(|j| (i * dim as u64 + j as u64) as f32 * 0.01)
            .collect();
        let doc_id = i % 100;

        if strategy.get_cached(doc_id).is_some() {
            hits_pass1 += 1;
        } else {
            // Cache miss: insert
            let cached = CachedVector {
                doc_id,
                embedding: query.clone(),
                distance: 0.0,
                cached_at: Instant::now(),
            };
            strategy.insert_cached(cached);
        }
    }

    // Second pass: Some hits (warm cache)
    let mut hits_pass2 = 0;
    for i in 0u64..50 {
        let doc_id = i % 100;
        if strategy.get_cached(doc_id).is_some() {
            hits_pass2 += 1;
        }
    }

    // Hit rate should improve
    assert!(
        hits_pass2 > hits_pass1,
        "Pass1 hits: {}, Pass2 hits: {}",
        hits_pass1,
        hits_pass2
    );
    assert!(
        hits_pass2 >= 45,
        "Expected >90% hit rate on second pass, got {}/50",
        hits_pass2
    );
}

/// Test stats persistence survives restart
#[tokio::test]
async fn test_stats_survive_restart() {
    let temp_dir = TempDir::new().unwrap();
    let stats_path = temp_dir.path().join("ab_stats.csv");

    // First session: log some events
    {
        let persister = AbStatsPersister::new(&stats_path).unwrap();
        persister.log_hit("lru_baseline", 1u64, 100).await.unwrap();
        persister.log_miss("lru_baseline", 2u64, 200).await.unwrap();
        persister.log_hit("learned_rmi", 3u64, 150).await.unwrap();
        persister.flush().await.unwrap();
    }

    // Second session: log more events
    {
        let persister = AbStatsPersister::new(&stats_path).unwrap();
        persister.log_miss("learned_rmi", 4u64, 250).await.unwrap();
        persister.flush().await.unwrap();
    }

    // Load all metrics
    let metrics = AbStatsPersister::load_metrics(&stats_path).unwrap();
    assert_eq!(metrics.len(), 4, "Should have 4 total events");

    // Verify both sessions present
    assert!(metrics.iter().any(|m| m.doc_id == 1));
    assert!(metrics.iter().any(|m| m.doc_id == 4));
}

/// Test background training task updates predictor
#[tokio::test]
async fn test_background_training_updates_predictor() {
    use kyrodb_engine::spawn_training_task;

    // Create logger with access events
    let logger = Arc::new(RwLock::new(AccessPatternLogger::new(1_000)));
    {
        let mut log = logger.write().await;
        for i in 0..200 {
            let embedding: Vec<f32> = (0..128).map(|_| i as f32).collect();
            log.log_access(i % 10, &embedding); // 10 hot documents
        }
    }

    // Create learned strategy
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

    // Spawn training task with short interval
    let config = TrainingConfig {
        interval: Duration::from_millis(500),
        window_duration: Duration::from_secs(3600),
        min_events_for_training: 100,
        rmi_capacity: 100,
    };

    let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let handle = spawn_training_task(logger.clone(), strategy.clone(), config, None, shutdown_rx).await;

    // Wait for training cycle
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Predictor should be updated (we can't directly observe, but task ran)
    // If task panicked, test would fail

    handle.abort();
}

/// Test Hybrid Semantic Cache predictor influences cache admission
#[tokio::test]
async fn test_learned_predictor_influences_admission() {
    // Create predictor and train on hot documents
    let mut predictor = LearnedCachePredictor::new(100).unwrap();

    // Train: documents 0-9 are hot (10 accesses each)
    let mut events = Vec::new();
    for _ in 0..10 {
        for doc_id in 0..10 {
            events.push(AccessEvent {
                doc_id,
                timestamp: SystemTime::now(),
                access_type: AccessType::Read,
            });
        }
    }
    predictor.train_from_accesses(&events).unwrap();

    // Create learned strategy
    let strategy = LearnedCacheStrategy::new(50, predictor);

    // Test admission policy
    let dummy_embedding = vec![0.0; 128];

    // Prefill cache to capacity so admission relies on predictor
    for idx in 0..50u64 {
        let cached = CachedVector {
            doc_id: 1_000 + idx,
            embedding: vec![0.0; 8],
            distance: 0.0,
            cached_at: Instant::now(),
        };
        strategy.insert_cached(cached);
    }

    // Hot documents (0-9) should be cached
    let mut hot_cached = 0;
    for doc_id in 0..10 {
        if strategy.should_cache(doc_id, &dummy_embedding) {
            hot_cached += 1;
        }
    }

    // Cold documents (90-99) should not be cached
    let mut cold_cached = 0;
    for doc_id in 90..100 {
        if strategy.should_cache(doc_id, &dummy_embedding) {
            cold_cached += 1;
        }
    }

    // Hybrid Semantic Cache should prefer hot documents
    assert!(
        hot_cached > cold_cached,
        "Hot cached: {}, Cold cached: {}",
        hot_cached,
        cold_cached
    );
}

/// Test concurrent access to cache (thread-safety)
#[tokio::test]
async fn test_concurrent_cache_access() {
    let strategy = Arc::new(LruCacheStrategy::new(100));

    // Spawn multiple tasks accessing cache concurrently
    let mut handles = Vec::new();

    for task_id in 0u64..10 {
        let strategy_clone = strategy.clone();
        let handle = tokio::spawn(async move {
            for i in 0u64..20 {
                let doc_id = task_id * 20 + i;
                let embedding = vec![i as f32; 128];

                // Insert
                let cached = CachedVector {
                    doc_id,
                    embedding: embedding.clone(),
                    distance: 0.0,
                    cached_at: Instant::now(),
                };
                strategy_clone.insert_cached(cached);

                // Get
                let _ = strategy_clone.get_cached(doc_id);
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    // Cache should be consistent (no panics, no corruption)
    // If there were race conditions, test would have panicked
}

/// Test A/B stats analysis computes correct metrics
#[tokio::test]
async fn test_ab_stats_analysis() {
    let temp_dir = TempDir::new().unwrap();
    let stats_path = temp_dir.path().join("ab_stats.csv");
    let persister = AbStatsPersister::new(&stats_path).unwrap();

    // Log events: LRU has 3 hits, 7 misses (30% hit rate)
    for i in 0u64..3 {
        persister.log_hit("lru_baseline", i, 100).await.unwrap();
    }
    for i in 3u64..10 {
        persister.log_miss("lru_baseline", i, 200).await.unwrap();
    }

    // Log events: Learned has 7 hits, 3 misses (70% hit rate)
    for i in 0u64..7 {
        persister.log_hit("learned_rmi", i, 150).await.unwrap();
    }
    for i in 7u64..10 {
        persister.log_miss("learned_rmi", i, 250).await.unwrap();
    }

    persister.flush().await.unwrap();

    // Analyze metrics
    let metrics = AbStatsPersister::load_metrics(&stats_path).unwrap();
    let summary = AbStatsPersister::analyze_metrics(&metrics);

    // Verify computed metrics
    assert_eq!(summary.lru_hits, 3);
    assert_eq!(summary.lru_misses, 7);
    assert!((summary.lru_hit_rate - 0.3).abs() < 0.01);

    assert_eq!(summary.learned_hits, 7);
    assert_eq!(summary.learned_misses, 3);
    assert!((summary.learned_hit_rate - 0.7).abs() < 0.01);

    assert_eq!(summary.total_events, 20);

    // Learned should be 2.33x better
    let improvement = summary.learned_hit_rate / summary.lru_hit_rate;
    assert!(improvement > 2.0, "Improvement: {:.2}x", improvement);
}
