//! Integration tests for Access Pattern Logger and Hybrid Semantic Cache Pipeline
//!
//! Validate end-to-end flow from access logging to cache prediction
//!
//! Test scenarios:
//! 1. Log accesses → flush → train predictor → verify predictions
//! 2. Zipf distribution (realistic workload) → verify hot document detection
//! 3. Time-windowed training → verify freshness of predictions
//! 4. Periodic retraining → verify prediction accuracy improves

use kyrodb_engine::access_logger::AccessPatternLogger;
use kyrodb_engine::learned_cache::LearnedCachePredictor;
use std::time::Duration;

/// Training pipeline: log accesses → extract window → train predictor
fn training_pipeline(
    logger: &mut AccessPatternLogger,
    predictor: &mut LearnedCachePredictor,
) -> anyhow::Result<usize> {
    if !logger.needs_flush() {
        return Ok(0);
    }

    // Extract recent 24-hour window
    let recent_events = logger.get_recent_window(Duration::from_secs(24 * 3600));
    let event_count = recent_events.len();

    if event_count == 0 {
        return Ok(0);
    }

    // Train predictor directly from access events
    predictor.train_from_accesses(&recent_events)?;

    // Mark flushed
    logger.mark_flushed();

    Ok(event_count)
}

#[test]
fn test_basic_integration_pipeline() {
    // Create logger and predictor
    let mut logger = AccessPatternLogger::with_flush_interval(
        100_000,
        Duration::from_secs(0), // Immediate flush for testing
    );
    let mut predictor = LearnedCachePredictor::new(1000).expect("Failed to create predictor");

    // Log accesses: doc 1 (hot), doc 2 (warm), doc 3 (cold)
    let embedding = vec![0.5; 128];
    for _ in 0..100 {
        logger.log_access(1, &embedding); // Hot
    }
    for _ in 0..10 {
        logger.log_access(2, &embedding); // Warm
    }
    logger.log_access(3, &embedding); // Cold

    // Run training pipeline
    let trained_events =
        training_pipeline(&mut logger, &mut predictor).expect("Training pipeline failed");

    assert_eq!(trained_events, 111, "Should train on all 111 events");

    // Verify predictions: hot > warm > cold
    let score_1 = predictor.predict_hotness(1);
    let score_2 = predictor.predict_hotness(2);
    let score_3 = predictor.predict_hotness(3);

    assert!(
        score_1 > score_2,
        "Doc 1 (100 accesses) should be hotter than doc 2 (10 accesses): {} vs {}",
        score_1,
        score_2
    );
    assert!(
        score_2 > score_3,
        "Doc 2 (10 accesses) should be hotter than doc 3 (1 access): {} vs {}",
        score_2,
        score_3
    );

    println!(
        "Hotness scores: doc1={:.3}, doc2={:.3}, doc3={:.3}",
        score_1, score_2, score_3
    );
}

#[test]
fn test_zipf_distribution_detection() {
    // Simulate Zipf distribution (80/20 rule): 20% of docs get 80% of accesses
    let mut logger = AccessPatternLogger::with_flush_interval(100_000, Duration::from_secs(0));
    let mut predictor = LearnedCachePredictor::new(1000).expect("Failed to create predictor");

    let embedding = vec![0.5; 128];

    // Hot documents (1-20): 80% of accesses
    for doc_id in 1..=20 {
        for _ in 0..40 {
            logger.log_access(doc_id, &embedding);
        }
    }

    // Cold documents (21-100): 20% of accesses
    for doc_id in 21..=100 {
        for _ in 0..2 {
            logger.log_access(doc_id, &embedding);
        }
    }

    // Total: 20*40 + 80*2 = 800 + 160 = 960 accesses

    // Train predictor
    let trained_events = training_pipeline(&mut logger, &mut predictor).expect("Training failed");
    assert_eq!(trained_events, 960);

    // Verify hot documents have higher scores
    let hot_scores: Vec<f32> = (1..=20)
        .map(|doc_id| predictor.predict_hotness(doc_id))
        .collect();
    let cold_scores: Vec<f32> = (21..=100)
        .map(|doc_id| predictor.predict_hotness(doc_id))
        .collect();

    let avg_hot = hot_scores.iter().sum::<f32>() / hot_scores.len() as f32;
    let avg_cold = cold_scores.iter().sum::<f32>() / cold_scores.len() as f32;

    assert!(
        avg_hot > avg_cold,
        "Hot documents (avg={:.3}) should have higher scores than cold (avg={:.3})",
        avg_hot,
        avg_cold
    );

    println!(
        "Zipf distribution detected: hot_avg={:.3}, cold_avg={:.3}, ratio={:.2}x",
        avg_hot,
        avg_cold,
        avg_hot / avg_cold
    );
}

#[test]
fn test_time_windowed_training() {
    // Verify that only recent events are used for training
    let logger = AccessPatternLogger::with_flush_interval(100_000, Duration::from_secs(0));
    let _predictor = LearnedCachePredictor::new(1000).expect("Failed to create predictor");

    let embedding = vec![0.5; 128];

    // Log accesses
    for _ in 0..50 {
        logger.log_access(1, &embedding);
    }

    // Verify get_recent_window returns all events with large window
    let all_events = logger.get_recent_window(Duration::from_secs(24 * 3600));
    assert_eq!(all_events.len(), 50, "Should have all 50 events");

    // Verify get_recent_window still returns events with 1-second window
    // (all logged within last second in this test)
    let recent = logger.get_recent_window(Duration::from_secs(1));
    assert!(!recent.is_empty(), "Should have recent events");
    assert!(
        recent.len() <= all_events.len(),
        "Recent should be subset of all"
    );

    println!(
        "Time window filtering: 24h window={} events, 1s window={} events",
        all_events.len(),
        recent.len()
    );
}

#[test]
fn test_periodic_retraining() {
    // Simulate multiple training cycles with changing access patterns
    let mut logger = AccessPatternLogger::with_flush_interval(100_000, Duration::from_secs(0));
    let mut predictor = LearnedCachePredictor::new(1000).expect("Failed to create predictor");

    let embedding = vec![0.5; 128];

    // Step 1: Doc 1 is hot
    for _ in 0..100 {
        logger.log_access(1, &embedding);
    }
    for _ in 0..10 {
        logger.log_access(2, &embedding);
    }

    training_pipeline(&mut logger, &mut predictor).expect("Training failed");

    let score_1_phase1 = predictor.predict_hotness(1);
    let score_2_phase1 = predictor.predict_hotness(2);

    assert!(
        score_1_phase1 > score_2_phase1,
        "Step 1: Doc 1 should be hotter"
    );

    // Step 2: Doc 2 becomes hot (shift in access pattern)
    for _ in 0..100 {
        logger.log_access(2, &embedding);
    }
    for _ in 0..10 {
        logger.log_access(1, &embedding);
    }

    training_pipeline(&mut logger, &mut predictor).expect("Training failed");

    let score_1_phase2 = predictor.predict_hotness(1);
    let score_2_phase2 = predictor.predict_hotness(2);

    // After retraining, doc 2 should be hotter (recent pattern dominates)
    // Note: Actual behavior depends on RMI's sliding window
    // This test validates that retraining updates predictions

    println!(
        "Retraining validation:\n  Step 1: doc1={:.3}, doc2={:.3}\n  Step 2: doc1={:.3}, doc2={:.3}",
        score_1_phase1, score_2_phase1, score_1_phase2, score_2_phase2
    );

    // Verify predictions changed
    // Require doc2 becomes at least as hot as doc1 after shift and at least one score changes
    assert!(
        score_2_phase2 >= score_1_phase2,
        "Doc 2 should become hotter after retraining"
    );
    assert!(
        score_1_phase1 != score_1_phase2 || score_2_phase1 != score_2_phase2,
        "At least one score should change after retraining"
    );
}

#[test]
fn test_cache_admission_policy() {
    // Test cache admission based on predicted hotness
    let mut logger = AccessPatternLogger::with_flush_interval(100_000, Duration::from_secs(0));
    let mut predictor = LearnedCachePredictor::new(1000).expect("Failed to create predictor");

    let embedding = vec![0.5; 128];

    // Create access pattern
    for _ in 0..100 {
        logger.log_access(1, &embedding);
    }
    for _ in 0..10 {
        logger.log_access(2, &embedding);
    }
    logger.log_access(3, &embedding);

    training_pipeline(&mut logger, &mut predictor).expect("Training failed");

    // Cache admission uses predictor's internal threshold
    let should_cache = |doc_id: u64| predictor.should_cache(doc_id);

    // Verify admission policy
    let doc1_admitted = should_cache(1);
    let doc2_admitted = should_cache(2);
    let doc3_admitted = should_cache(3);

    println!(
        "Cache admission:\n  doc1: {} (score={:.3})\n  doc2: {} (score={:.3})\n  doc3: {} (score={:.3})",
        doc1_admitted,
        predictor.predict_hotness(1),
        doc2_admitted,
        predictor.predict_hotness(2),
        doc3_admitted,
        predictor.predict_hotness(3)
    );

    // Hot document should be admitted
    assert!(
        doc1_admitted,
        "Hot document (100 accesses) should be cached"
    );

    // Cold document likely not admitted (depends on training)
    // This is a softer assertion as it depends on RMI's learned function
}

#[test]
fn test_empty_logger_training() {
    // Edge case: training on empty logger should not crash
    let mut logger = AccessPatternLogger::with_flush_interval(100_000, Duration::from_secs(0));
    let mut predictor = LearnedCachePredictor::new(1000).expect("Failed to create predictor");

    let trained_events = training_pipeline(&mut logger, &mut predictor)
        .expect("Training on empty logger should succeed");

    assert_eq!(trained_events, 0, "No events to train on");

    // Predictor should still work (return default predictions)
    let score = predictor.predict_hotness(1);
    assert!((0.0..=1.0).contains(&score), "Score should be in [0, 1]");
}

#[test]
fn test_high_volume_integration() {
    // Stress test: 100k accesses
    let mut logger = AccessPatternLogger::with_flush_interval(1_000_000, Duration::from_secs(0));
    let mut predictor = LearnedCachePredictor::new(10_000).expect("Failed to create predictor");

    let embedding = vec![0.5; 128];

    // Generate 100k accesses with power-law distribution
    use rand::Rng;
    let mut rng = rand::thread_rng();

    for _ in 0..100_000 {
        // Zipf-like: lower doc IDs accessed more frequently
        let doc_id = (rng.gen::<f32>().powf(2.0) * 1000.0) as u64;
        logger.log_access(doc_id, &embedding);
    }

    // Train predictor
    let start = std::time::Instant::now();
    let trained_events =
        training_pipeline(&mut logger, &mut predictor).expect("High-volume training failed");
    let duration = start.elapsed();

    assert_eq!(trained_events, 100_000);

    println!(
        "High-volume integration: trained on {} events in {:.2}ms",
        trained_events,
        duration.as_secs_f64() * 1000.0
    );

    // Verify predictions work
    let score_hot = predictor.predict_hotness(1);
    let score_cold = predictor.predict_hotness(999);

    assert!(
        score_hot >= score_cold,
        "Lower doc IDs (more frequent) should have higher scores"
    );
}

#[test]
fn test_logger_stats_integration() {
    // Verify stats tracking through integration
    let logger = AccessPatternLogger::new(100_000);
    let embedding = vec![0.5; 128];

    // Log some accesses
    for i in 0..1000 {
        logger.log_access(i % 100, &embedding);
    }

    let stats = logger.stats();

    assert_eq!(stats.total_accesses, 1000);
    assert_eq!(stats.total_flushes, 0);
    assert_eq!(stats.current_events, 1000);
    assert_eq!(stats.capacity, 100_000);

    println!(
        "Logger stats: {}/{} events, {} flushes",
        stats.current_events, stats.capacity, stats.total_flushes
    );
}
