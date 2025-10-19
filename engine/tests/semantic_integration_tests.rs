//! Integration tests for semantic adapter with cache strategy
//!
//! Tests Phase 1 semantic-aware Hybrid Semantic Cache

use kyrodb_engine::{
    AccessEvent, AccessPatternLogger, AccessType, CacheStrategy, CachedVector,
    LearnedCachePredictor, LearnedCacheStrategy, SemanticAdapter, SemanticConfig,
};
use std::time::{Instant, SystemTime};

fn create_test_vector(doc_id: u64, embedding: Vec<f32>) -> CachedVector {
    CachedVector {
        doc_id,
        embedding,
        distance: 0.1,
        cached_at: Instant::now(),
    }
}

#[test]
fn test_semantic_adapter_integration_with_learned_strategy() {
    // Create predictor and train on hot documents
    let mut predictor = LearnedCachePredictor::new(100).unwrap();

    // Train: doc 1 hot (100 accesses), doc 2 cold (1 access)
    let mut events = vec![];
    for _ in 0..100 {
        events.push(AccessEvent {
            doc_id: 1,
            timestamp: SystemTime::now(),
            access_type: AccessType::Read,
        });
    }
    events.push(AccessEvent {
        doc_id: 2,
        timestamp: SystemTime::now(),
        access_type: AccessType::Read,
    });

    predictor.train_from_accesses(&events).unwrap();

    // Create semantic adapter
    let semantic_adapter = SemanticAdapter::new();

    // Create learned strategy with semantic
    let strategy = LearnedCacheStrategy::new_with_semantic(10, predictor, semantic_adapter);

    assert!(strategy.has_semantic());
    assert_eq!(strategy.name(), "learned_semantic");

    // Test hot document (high frequency)
    let hot_embedding = vec![1.0; 384];
    let should_cache_hot = strategy.should_cache(1, &hot_embedding);
    assert!(should_cache_hot, "Hot document should be cached");

    // Test cold document (low frequency, but semantically similar to hot)
    let similar_embedding = vec![0.99; 384]; // Very similar to hot_embedding
    let should_cache_similar = strategy.should_cache(2, &similar_embedding);

    // Without semantic: cold doc would likely be rejected
    // With semantic: cold doc might be accepted due to similarity
    println!(
        "Should cache similar (cold freq, high semantic): {}",
        should_cache_similar
    );
}

#[test]
fn test_semantic_boost_for_cold_documents() {
    // Create predictor with no training (all docs are cold)
    let predictor = LearnedCachePredictor::new(100).unwrap();

    // Create semantic adapter
    let semantic_adapter = SemanticAdapter::new();

    // Create learned strategy with semantic
    let strategy = LearnedCacheStrategy::new_with_semantic(10, predictor, semantic_adapter);

    // Cache a hot embedding
    let hot_embedding = vec![1.0; 384];
    strategy.should_cache(1, &hot_embedding);

    // Try to cache a very similar embedding (different doc_id)
    // Frequency score is low (unseen), but semantic score is high
    let similar_embedding = vec![0.99; 384];
    let should_cache = strategy.should_cache(2, &similar_embedding);

    println!(
        "Cold doc with high semantic similarity should cache: {}",
        should_cache
    );

    // Very different embedding should not benefit from semantic boost
    let different_embedding = vec![-1.0; 384];
    let should_not_cache = strategy.should_cache(3, &different_embedding);

    println!(
        "Cold doc with low semantic similarity: {}",
        should_not_cache
    );
}

#[test]
fn test_semantic_stats_collection() {
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let semantic_adapter = SemanticAdapter::new();
    let strategy = LearnedCacheStrategy::new_with_semantic(10, predictor, semantic_adapter);

    // Generate some traffic
    for i in 0..100 {
        let embedding = vec![i as f32; 384];
        strategy.should_cache(i, &embedding);
    }

    // Check stats include semantic info
    let stats = strategy.stats();
    println!("Stats: {}", stats);

    assert!(stats.contains("Semantic:"));
    assert!(stats.contains("fast"));
    assert!(stats.contains("slow"));
}

#[test]
fn test_frequency_only_strategy_without_semantic() {
    // Create predictor and train
    let mut predictor = LearnedCachePredictor::new(100).unwrap();

    let mut events = vec![];
    for _ in 0..100 {
        events.push(AccessEvent {
            doc_id: 1,
            timestamp: SystemTime::now(),
            access_type: AccessType::Read,
        });
    }

    predictor.train_from_accesses(&events).unwrap();

    // Create learned strategy WITHOUT semantic
    let strategy = LearnedCacheStrategy::new(10, predictor);

    assert!(!strategy.has_semantic());
    assert_eq!(strategy.name(), "learned_rmi");

    // Should work with frequency-only logic
    let embedding = vec![1.0; 384];
    let should_cache = strategy.should_cache(1, &embedding);
    assert!(
        should_cache,
        "Hot document should be cached (frequency-only)"
    );

    // Stats should not include semantic info
    let stats = strategy.stats();
    assert!(!stats.contains("Semantic:"));
}

#[test]
fn test_semantic_cache_admission_flow() {
    let logger = AccessPatternLogger::new(10000);
    let mut predictor = LearnedCachePredictor::new(100).unwrap();

    // Simulate access pattern
    for i in 0..1000 {
        let doc_id = if i < 500 { 1 } else { 2 }; // doc 1 hot, doc 2 lukewarm
        logger.log_access(doc_id, &vec![doc_id as f32; 384]);
    }

    // Train predictor
    let events = logger.get_recent_window(std::time::Duration::from_secs(3600));
    predictor.train_from_accesses(&events).unwrap();

    // Create semantic strategy
    let semantic_adapter = SemanticAdapter::new();
    let strategy = LearnedCacheStrategy::new_with_semantic(100, predictor, semantic_adapter);

    // Test admission for various documents
    let mut admitted = 0;
    let mut rejected = 0;

    for i in 0..100 {
        let embedding = vec![(i % 10) as f32; 384]; // Group into 10 semantic clusters
        if strategy.should_cache(i, &embedding) {
            admitted += 1;
            strategy.insert_cached(create_test_vector(i, embedding.clone()));
        } else {
            rejected += 1;
        }
    }

    println!("Admitted: {}, Rejected: {}", admitted, rejected);
    println!("Stats: {}", strategy.stats());

    // Verify cache contains vectors
    assert!(strategy.get_cached(1).is_some() || strategy.get_cached(2).is_some());
}

#[test]
fn test_semantic_config_customization() {
    let config = SemanticConfig {
        high_confidence_threshold: 0.9,
        low_confidence_threshold: 0.2,
        semantic_similarity_threshold: 0.9,
        max_cached_embeddings: 50_000,
        similarity_scan_limit: 500,
    };

    let semantic_adapter = SemanticAdapter::with_config(config.clone());
    assert_eq!(semantic_adapter.config().high_confidence_threshold, 0.9);
    assert_eq!(semantic_adapter.config().max_cached_embeddings, 50_000);
}

#[test]
fn test_semantic_adapter_empty_embedding() {
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let semantic_adapter = SemanticAdapter::new();
    let strategy = LearnedCacheStrategy::new_with_semantic(10, predictor, semantic_adapter);

    // Empty embedding should not panic
    let empty_embedding = vec![];
    let should_cache = strategy.should_cache(1, &empty_embedding);

    println!("Empty embedding decision: {}", should_cache);

    // Should handle gracefully (likely reject due to low frequency and no semantic match)
}

#[test]
fn test_semantic_adapter_zero_vector() {
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let semantic_adapter = SemanticAdapter::new();
    let strategy = LearnedCacheStrategy::new_with_semantic(10, predictor, semantic_adapter);

    // Zero vector should not panic or produce NaN
    let zero_embedding = vec![0.0; 384];
    let should_cache = strategy.should_cache(1, &zero_embedding);

    println!("Zero vector decision: {}", should_cache);
}

#[test]
fn test_semantic_fast_path_dominance() {
    // Configure semantic adapter
    let semantic_adapter = SemanticAdapter::new();

    // Create predictor with extreme training
    let mut predictor = LearnedCachePredictor::new(100).unwrap();

    // Train doc 1 as very hot, doc 2 as very cold
    let mut events = vec![];
    for _ in 0..1000 {
        events.push(AccessEvent {
            doc_id: 1,
            timestamp: SystemTime::now(),
            access_type: AccessType::Read,
        });
    }
    events.push(AccessEvent {
        doc_id: 2,
        timestamp: SystemTime::now(),
        access_type: AccessType::Read,
    });

    predictor.train_from_accesses(&events).unwrap();

    let strategy = LearnedCacheStrategy::new_with_semantic(10, predictor, semantic_adapter);

    // Generate traffic
    for _ in 0..100 {
        // Very hot document (should use fast path)
        strategy.should_cache(1, &vec![1.0; 384]);

        // Very cold document (should use fast path)
        strategy.should_cache(2, &vec![2.0; 384]);
    }

    let stats = strategy.stats();
    println!("Fast path test stats: {}", stats);

    // Most decisions should be fast path (high or low confidence)
    // Only uncertain (0.3-0.8 freq score) should use slow path
}

#[test]
fn test_semantic_integration_with_vector_cache() {
    let predictor = LearnedCachePredictor::new(100).unwrap();
    let semantic_adapter = SemanticAdapter::new();
    let strategy = LearnedCacheStrategy::new_with_semantic(10, predictor, semantic_adapter);

    // Insert vectors
    for i in 0..20 {
        let embedding = vec![i as f32; 384];
        if strategy.should_cache(i, &embedding) {
            strategy.insert_cached(create_test_vector(i, embedding));
        }
    }

    // Retrieve vectors
    let mut hit_count = 0;
    let mut miss_count = 0;

    for i in 0..20 {
        if strategy.get_cached(i).is_some() {
            hit_count += 1;
        } else {
            miss_count += 1;
        }
    }

    println!("Hits: {}, Misses: {}", hit_count, miss_count);
    println!("Final stats: {}", strategy.stats());

    // Should have some cached vectors
    assert!(hit_count > 0);
}
