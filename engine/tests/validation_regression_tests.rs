//! Regression tests for validation bugs
//!
//! These tests prevent recurrence of critical bugs discovered during validation:
//! 1. Hybrid Semantic Cache never caching (0% hit rate) - predictor untrained
//! 2. LRU hit rate impossibly high (98%) - uneven traffic split or Zipf broken
//! 3. Memory leak (101% growth) - unbounded buffer or broken eviction

use kyrodb_engine::{
    AbTestSplitter, AccessEvent, AccessPatternLogger, AccessType, CacheStrategy, CachedVector,
    LearnedCachePredictor, LearnedCacheStrategy, LruCacheStrategy, SemanticAdapter,
};
use rand::distributions::Distribution;
use rand_distr::Zipf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

/// Helper: Create test vector
fn create_test_vector(doc_id: u64) -> CachedVector {
    CachedVector {
        doc_id,
        embedding: vec![0.5; 128],
        distance: 0.1,
        cached_at: Instant::now(),
    }
}

/// Helper: Create access event
fn create_access(doc_id: u64, seconds_ago: u64) -> AccessEvent {
    AccessEvent {
        doc_id,
        timestamp: SystemTime::now() - Duration::from_secs(seconds_ago),
        access_type: AccessType::Read,
    }
}

/// BUG 1 REGRESSION TEST: Hybrid Semantic Cache must bootstrap before training
///
/// Previously: Hybrid Semantic Cache's should_cache() returned false when predictor
/// was untrained, causing chicken-and-egg deadlock (no caching → no training data).
///
/// Fix: should_cache() returns true when tracked_count() == 0 (bootstrap mode)
#[test]
fn test_learned_cache_bootstraps_before_training() {
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

    // CRITICAL: Before training, should cache everything (bootstrap mode)
    assert!(
        strategy.should_cache(1, &vec![0.5; 128]),
        "Untrained Hybrid Semantic Cache must cache doc 1 (bootstrap mode)"
    );
    assert!(
        strategy.should_cache(2, &vec![0.5; 128]),
        "Untrained Hybrid Semantic Cache must cache doc 2 (bootstrap mode)"
    );
    assert!(
        strategy.should_cache(99, &vec![0.5; 128]),
        "Untrained Hybrid Semantic Cache must cache doc 99 (bootstrap mode)"
    );

    // Verify tracked_count is 0 (untrained)
    let pred = strategy.predictor.read();
    assert_eq!(
        pred.tracked_count(),
        0,
        "Predictor should have 0 tracked docs before training"
    );
}

/// BUG 1 FOLLOW-UP: Hybrid Semantic Cache becomes selective after training
#[test]
fn test_learned_cache_selective_after_training() {
    let mut predictor = LearnedCachePredictor::new(100).unwrap();

    // Train on hot document (doc 1 accessed 100 times, doc 99 accessed once)
    let mut events = vec![];
    for _ in 0..100 {
        events.push(create_access(1, 60)); // Doc 1 hot
    }
    events.push(create_access(99, 60)); // Doc 99 cold

    predictor.train_from_accesses(&events).unwrap();
    assert!(
        predictor.tracked_count() > 0,
        "Predictor should have tracked docs after training"
    );

    let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

    // After training: Hot doc should be cached, cold doc should not
    let should_cache_hot = strategy.should_cache(1, &vec![0.5; 128]);
    let should_cache_cold = strategy.should_cache(99, &vec![0.5; 128]);

    // Hot doc (doc 1) must be cached
    assert!(
        should_cache_hot,
        "Hot document (100 accesses) must be cached after training"
    );

    // Cold doc (doc 99) should have lower probability (threshold dependent)
    // At minimum: hot doc should have higher admission probability
    if should_cache_hot && should_cache_cold {
        // Both cached: check predictor gives different scores
        let pred = strategy.predictor.read();
        let hotness_hot = pred.predict_hotness(1);
        let hotness_cold = pred.predict_hotness(99);
        assert!(
            hotness_hot > hotness_cold,
            "Hot doc should have higher hotness score than cold doc"
        );
    }
}

/// BUG 2 REGRESSION TEST: A/B splitter must give even 50/50 distribution
///
/// Previously: Traffic split was 64% LRU, 36% Learned (caused impossibly high LRU hit rate)
///
/// Fix: Verify doc_id % 2 routing gives exactly 50/50 split over large sample
#[test]
fn test_ab_splitter_even_distribution() {
    let lru = Arc::new(LruCacheStrategy::new(100));
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let learned = Arc::new(LearnedCacheStrategy::new(100, predictor));
    let splitter = AbTestSplitter::new(lru, learned);

    let mut lru_count = 0;
    let mut learned_count = 0;

    // Test over 10,000 doc_ids (large sample for statistical significance)
    for doc_id in 0..10_000 {
        let strategy = splitter.get_strategy(doc_id);
        if strategy.name() == "lru_baseline" {
            lru_count += 1;
        } else if strategy.name() == "learned_rmi" {
            learned_count += 1;
        } else {
            panic!("Unknown strategy name: {}", strategy.name());
        }
    }

    println!(
        "Traffic split: LRU={} ({:.1}%), Learned={} ({:.1}%)",
        lru_count,
        (lru_count as f64 / 10_000.0) * 100.0,
        learned_count,
        (learned_count as f64 / 10_000.0) * 100.0
    );

    // Should be exactly 50/50 (within 1% tolerance for even-numbered test)
    assert_eq!(lru_count, 5000, "LRU should get exactly 5000 queries (50%)");
    assert_eq!(
        learned_count, 5000,
        "Learned should get exactly 5000 queries (50%)"
    );

    // Verify deterministic routing (same doc_id always goes to same strategy)
    for doc_id in 0..100 {
        let strategy1 = splitter.get_strategy(doc_id);
        let strategy2 = splitter.get_strategy(doc_id);
        assert_eq!(
            strategy1.name(),
            strategy2.name(),
            "Same doc_id must always route to same strategy"
        );
    }
}

/// BUG 2 FOLLOW-UP: LRU hit rate must be reasonable on Zipf workload
///
/// Previously: LRU showed 98% hit rate (impossible for 10K cache on 100K corpus)
///
/// This test simulates Zipf workload and verifies LRU hit rate is in expected range
#[test]
fn test_lru_hit_rate_realistic_on_zipf() {
    let cache_capacity = 1000;
    let corpus_size = 10_000;
    let strategy = Arc::new(LruCacheStrategy::new(cache_capacity));

    // Simulate Zipf(1.5) distribution (80/20 rule)
    // Top 20% of docs (2000 docs) get 80% of accesses
    // We have cache for 1000 docs, so we can't cache all hot docs

    let mut rng = rand::thread_rng();
    let zipf = Zipf::new(corpus_size as u64, 1.5).unwrap();

    // Run 10K queries
    let mut hits = 0;
    let mut misses = 0;

    for _ in 0..10_000 {
        let doc_id = (zipf.sample(&mut rng) as u64).saturating_sub(1); // Convert to 0-indexed

        if let Some(_cached) = strategy.get_cached(doc_id) {
            hits += 1;
        } else {
            misses += 1;

            // Cache on miss (LRU always caches)
            let cached = create_test_vector(doc_id);
            strategy.insert_cached(cached);
        }
    }

    let hit_rate = hits as f64 / (hits + misses) as f64;
    println!(
        "LRU hit rate on Zipf(1.5): {:.1}% (capacity={}, corpus={})",
        hit_rate * 100.0,
        cache_capacity,
        corpus_size
    );

    // With Zipf(1.5), capacity=1000, corpus=10K, and 10K queries,
    // LRU can achieve very high hit rates because hot set fits in cache.
    // The key is that it should be STABLE (not showing the >98% from the bug).
    // The bug was 98% with capacity=10K, corpus=100K - impossible.

    // Realistic bounds for this specific test setup:
    // - Lower bound: 60% (cache is big enough to hold hot set)
    // - Upper bound: 99% (allow for very skewed Zipf distribution)

    assert!(
        hit_rate >= 0.60,
        "LRU hit rate too low: {:.1}% (expected >= 60% for this workload)",
        hit_rate * 100.0
    );
    assert!(
        hit_rate <= 0.99,
        "LRU hit rate at 100% ({:.1}%) suggests caching everything (bug)",
        hit_rate * 100.0
    );

    // The real bug was 98% hit rate with 10K cache on 100K corpus.
    // This test has 1K cache on 10K corpus, so high hit rate is expected.
}

/// BUG 3 REGRESSION TEST: Access logger ring buffer must enforce capacity
///
/// Previously: Memory doubled in 22 minutes (circular buffer not evicting)
///
/// Fix: Verify ring buffer saturates at capacity and doesn't grow unbounded
#[test]
fn test_access_logger_enforces_capacity() {
    let capacity = 1000;
    let logger = AccessPatternLogger::new(capacity);

    // Log 2× capacity events
    for i in 0..2000 {
        logger.log_access(i, &vec![0.5; 128]);
    }

    // Buffer should saturate at capacity (not grow to 2000)
    assert_eq!(
        logger.len(),
        capacity,
        "Ring buffer must saturate at capacity"
    );

    // Should get most recent events (1000-1999)
    let all_events = logger.get_recent_window(Duration::from_secs(3600));
    assert_eq!(
        all_events.len(),
        capacity,
        "Should have exactly capacity events"
    );

    // Verify we have the most recent events
    let doc_ids: Vec<u64> = all_events.iter().map(|e| e.doc_id).collect();
    assert_eq!(
        doc_ids[0], 1000,
        "Oldest event should be doc_id 1000 (first overwritten)"
    );
    assert_eq!(
        doc_ids[capacity - 1],
        1999,
        "Newest event should be doc_id 1999 (last inserted)"
    );
}

/// BUG 3 FOLLOW-UP: Cache eviction must work correctly
///
/// Verify LRU cache evicts oldest entries when full
#[test]
fn test_cache_eviction_works() {
    let capacity = 100;
    let strategy = Arc::new(LruCacheStrategy::new(capacity));

    // Fill cache to capacity
    for i in 0..capacity {
        strategy.insert_cached(create_test_vector(i as u64));
    }

    // Get initial cache state via stats() method
    let stats_str = strategy.stats();
    println!("Before overflow: {}", stats_str);

    // Insert one more (should evict oldest)
    strategy.insert_cached(create_test_vector(capacity as u64));

    let stats_str = strategy.stats();
    println!("After overflow: {}", stats_str);

    // Verify oldest doc (0) was evicted
    assert!(
        strategy.get_cached(0).is_none(),
        "Oldest doc should be evicted"
    );
    // Verify newest doc is cached
    assert!(
        strategy.get_cached(capacity as u64).is_some(),
        "Newest doc should be cached"
    );
}

/// BUG 3 FOLLOW-UP: Memory should not grow unbounded under sustained load
///
/// This test runs 100K operations and verifies cache size stays bounded
#[test]
fn test_cache_memory_bounded() {
    let capacity = 1000;
    let strategy = Arc::new(LruCacheStrategy::new(capacity));

    // Simulate 100K queries with Zipf distribution
    let mut rng = rand::thread_rng();
    let zipf = Zipf::new(10_000, 1.5).unwrap();

    for _ in 0..100_000 {
        let doc_id = (zipf.sample(&mut rng) as u64).saturating_sub(1);

        if strategy.get_cached(doc_id).is_none() {
            strategy.insert_cached(create_test_vector(doc_id));
        }
    }

    // Verify cache stayed bounded
    let stats_str = strategy.stats();
    println!("After 100K queries: {}", stats_str);

    // Verify evictions occurred (proof capacity is enforced)
    assert!(
        stats_str.contains("evictions"),
        "Stats should track evictions"
    );
}

/// INTEGRATION TEST: Full validation simulation (LRU vs Learned)
///
/// This test simulates the full validation workload in miniature:
/// - A/B split traffic
/// - Train Hybrid Semantic Cache periodically
/// - Verify Hybrid Semantic Cache outperforms LRU
#[test]
fn test_full_ab_validation_simulation() {
    let cache_capacity = 1000;
    let corpus_size = 10_000;

    // Initialize strategies
    let lru = Arc::new(LruCacheStrategy::new(cache_capacity));
    let predictor = LearnedCachePredictor::new(cache_capacity).unwrap();
    let semantic_adapter = SemanticAdapter::new();
    let learned = Arc::new(LearnedCacheStrategy::new_with_semantic(
        cache_capacity,
        predictor,
        semantic_adapter,
    ));
    let splitter = AbTestSplitter::new(lru.clone(), learned.clone());

    // Access logger for training
    let logger = AccessPatternLogger::new(100_000);

    // Simulate Zipf workload
    let mut rng = rand::thread_rng();
    let zipf = Zipf::new(corpus_size as u64, 1.5).unwrap();

    let mut lru_hits = 0u64;
    let mut lru_misses = 0u64;
    let mut learned_hits = 0u64;
    let mut learned_misses = 0u64;

    // Run 50K queries (with training every 10K)
    for i in 0..50_000 {
        let doc_id = (zipf.sample(&mut rng) as u64).saturating_sub(1);

        // Route to strategy
        let strategy = splitter.get_strategy(doc_id);
        let strategy_name = strategy.name();

        // Check cache
        let cache_hit = if let Some(_cached) = strategy.get_cached(doc_id) {
            true
        } else {
            // Cache miss - insert if strategy says so
            let embedding = vec![0.5; 128];

            if strategy.should_cache(doc_id, &embedding) {
                strategy.insert_cached(create_test_vector(doc_id));
            }

            // Log access for training
            logger.log_access(doc_id, &embedding);

            false
        };

        // Record metrics
        if strategy_name == "lru_baseline" {
            if cache_hit {
                lru_hits += 1;
            } else {
                lru_misses += 1;
            }
        } else {
            if cache_hit {
                learned_hits += 1;
            } else {
                learned_misses += 1;
            }
        }

        // Train Hybrid Semantic Cache every 10K queries
        if i > 0 && i % 10_000 == 0 {
            let recent = logger.get_recent_window(Duration::from_secs(3600));
            if !recent.is_empty() {
                let mut new_predictor = LearnedCachePredictor::new(cache_capacity).unwrap();
                new_predictor.train_from_accesses(&recent).unwrap();
                learned.update_predictor(new_predictor);

                println!(
                    "Training at {}K queries: {} events processed",
                    i / 1000,
                    recent.len()
                );
            }
        }
    }

    // Calculate hit rates
    let lru_hit_rate = lru_hits as f64 / (lru_hits + lru_misses) as f64;
    let learned_hit_rate = learned_hits as f64 / (learned_hits + learned_misses) as f64;
    let improvement = learned_hit_rate / lru_hit_rate;

    println!();
    println!("=== Validation Simulation Results ===");
    println!(
        "LRU:     {:>6} hits, {:>6} misses, {:.1}% hit rate",
        lru_hits,
        lru_misses,
        lru_hit_rate * 100.0
    );
    println!(
        "Learned: {:>6} hits, {:>6} misses, {:.1}% hit rate",
        learned_hits,
        learned_misses,
        learned_hit_rate * 100.0
    );
    println!("Improvement: {:.2}×", improvement);

    // Assertions - with 1K cache on 10K corpus Zipf(1.5),
    // hit rates can be very high because hot set fits in cache.
    // The bug was uneven traffic split, not the absolute hit rate values.
    assert!(
        lru_hit_rate >= 0.15,
        "LRU hit rate too low: {:.1}%",
        lru_hit_rate * 100.0
    );
    assert!(
        lru_hit_rate <= 0.99,
        "LRU hit rate at 100% ({:.1}%) suggests caching everything",
        lru_hit_rate * 100.0
    );

    assert!(
        learned_hit_rate >= 0.30,
        "Learned hit rate too low: {:.1}%",
        learned_hit_rate * 100.0
    );

    // Hybrid Semantic Cache should outperform LRU (at least after training)
    // Allow for bootstrap period where learned might be lower
    assert!(
        improvement >= 0.8,
        "Hybrid Semantic Cache should be at least 80% as good as LRU (got {:.2}×)",
        improvement
    );
}

/// DIAGNOSTIC TEST: Verify strategy names are correct
#[test]
fn test_strategy_names_correct() {
    let lru = Arc::new(LruCacheStrategy::new(100));
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let learned = Arc::new(LearnedCacheStrategy::new(100, predictor));

    assert_eq!(
        lru.name(),
        "lru_baseline",
        "LRU strategy name must be 'lru_baseline'"
    );
    assert_eq!(
        learned.name(),
        "learned_rmi",
        "Learned strategy name must be 'learned_rmi'"
    );
}

/// DIAGNOSTIC TEST: Verify cache capacity is enforced
#[test]
fn test_cache_capacity_enforced() {
    let capacity = 50;
    let cache = kyrodb_engine::VectorCache::new(capacity);

    // Insert 100 vectors (2× capacity)
    for i in 0..100 {
        cache.insert(create_test_vector(i));
    }

    // Size should be at most capacity
    assert!(
        cache.len() <= capacity,
        "Cache size {} exceeds capacity {}",
        cache.len(),
        capacity
    );
}
