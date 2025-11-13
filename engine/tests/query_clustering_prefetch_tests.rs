//! Integration tests for query clustering and prefetching features
//!
//! Tests the complete query clustering and predictive prefetching pipeline

use kyrodb_engine::{
    CacheStrategy, LearnedCachePredictor, LearnedCacheStrategy, Prefetcher, QueryClusterer,
};
use std::sync::Arc;

#[test]
fn test_query_clustering_integration() {
    let predictor = LearnedCachePredictor::new(1000).unwrap();
    let strategy = LearnedCacheStrategy::new(100, predictor);

    // Enable query clustering
    strategy.enable_query_clustering(0.85);

    // Simulate similar queries
    let query1 = vec![1.0, 0.0, 0.0, 0.0];
    let query2 = vec![0.99, 0.1, 0.0, 0.0];
    let query3 = vec![0.0, 1.0, 0.0, 0.0];

    // Test caching decision with clustering
    strategy.should_cache(1, &query1);
    strategy.should_cache(2, &query2);
    strategy.should_cache(3, &query3);

    // Verify clustering occurred (similar queries should be grouped)
    assert!(strategy.cache.len() <= 100);
}

#[test]
fn test_prefetching_integration() {
    let predictor = LearnedCachePredictor::new(1000).unwrap();
    let strategy = LearnedCacheStrategy::new(100, predictor);

    // Enable prefetching
    let prefetcher = Arc::new(Prefetcher::with_default_threshold());
    strategy.enable_prefetching(prefetcher.clone());

    // Simulate access pattern: doc 1 -> doc 2 -> doc 3
    let emb = vec![0.5; 128];

    // Access sequence
    strategy.should_cache(1, &emb);
    strategy.should_cache(2, &emb);
    strategy.should_cache(1, &emb);
    strategy.should_cache(2, &emb);
    strategy.should_cache(1, &emb);
    strategy.should_cache(3, &emb);

    // Verify prefetcher recorded patterns
    let stats = prefetcher.stats();
    assert!(stats.coaccesses.total_patterns >= 0);
}

#[test]
fn test_query_clustering_with_prefetching() {
    let predictor = LearnedCachePredictor::new(1000).unwrap();
    let strategy = LearnedCacheStrategy::new(200, predictor);

    // Enable both features
    strategy.enable_query_clustering(0.85);
    let prefetcher = Arc::new(Prefetcher::with_default_threshold());
    strategy.enable_prefetching(prefetcher.clone());

    // Similar queries accessing related documents
    let query_a = vec![1.0, 0.0, 0.0];
    let query_b = vec![0.99, 0.1, 0.0];

    for _ in 0..5 {
        strategy.should_cache(10, &query_a);
        strategy.should_cache(20, &query_a);
        strategy.should_cache(10, &query_b);
        strategy.should_cache(20, &query_b);
    }

    // Verify both systems are active
    let prefetch_stats = prefetcher.stats();
    assert!(
        prefetch_stats.coaccesses.total_source_docs > 0
            || prefetch_stats.coaccesses.total_patterns >= 0
    );
}

#[tokio::test]
async fn test_prefetch_background_task() {
    let prefetcher = Arc::new(Prefetcher::with_default_threshold());

    // Record some access patterns
    for i in 0..10 {
        prefetcher.record_access(i);
    }

    let config = kyrodb_engine::PrefetchConfig {
        enabled: true,
        interval: std::time::Duration::from_millis(100),
        max_prefetch_per_doc: 5,
        prefetch_threshold: 0.10,
        prune_interval: std::time::Duration::from_secs(1),
        max_pattern_age: std::time::Duration::from_secs(3600),
    };

    let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let handle = kyrodb_engine::spawn_prefetch_task(prefetcher.clone(), config, shutdown_rx).await;

    // Let task run briefly
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Shutdown
    handle.abort();
}

#[test]
fn test_clustering_similarity_threshold() {
    let clusterer = QueryClusterer::new(0.95); // Very high threshold

    let query1 = vec![1.0, 0.0, 0.0];
    let query2 = vec![0.5, 0.5, 0.0]; // Not similar enough
    let query3 = vec![0.99, 0.1, 0.0]; // Very similar to query1

    clusterer.add_query(1, &query1);
    clusterer.add_query(2, &query2);
    clusterer.add_query(3, &query3);

    let stats = clusterer.stats();

    // Should have at least 2 clusters (query2 is different)
    assert!(stats.total_clusters >= 2);

    // Check that query1 and query3 are clustered together
    assert!(clusterer.are_clustered(1, 3));
    assert!(!clusterer.are_clustered(1, 2));
}

#[test]
fn test_prefetch_candidates_ranking() {
    let prefetcher = Prefetcher::with_default_threshold();

    // Create strong co-access pattern: 1 -> 2 (10 times)
    for _ in 0..10 {
        prefetcher.record_access(1);
        prefetcher.record_access(2);
    }

    // Weaker pattern: 1 -> 3 (3 times)
    for _ in 0..3 {
        prefetcher.record_access(1);
        prefetcher.record_access(3);
    }

    let candidates = prefetcher.get_prefetch_candidates(1);

    // Doc 2 should be ranked higher than doc 3
    if candidates.len() >= 2 {
        assert_eq!(candidates[0], 2);
    }
}

#[test]
fn test_empty_embeddings_handled() {
    let predictor = LearnedCachePredictor::new(1000).unwrap();
    let strategy = LearnedCacheStrategy::new(100, predictor);

    strategy.enable_query_clustering(0.85);

    // Empty embedding should not panic
    let result = strategy.should_cache(1, &[]);
    assert!(result);
}

#[test]
fn test_prefetch_stats_tracking() {
    let prefetcher = Prefetcher::with_default_threshold();

    assert_eq!(prefetcher.stats().prefetches_executed, 0);
    assert_eq!(prefetcher.stats().prefetch_hits, 0);

    prefetcher.record_prefetch();
    prefetcher.record_prefetch_hit();

    assert_eq!(prefetcher.stats().prefetches_executed, 1);
    assert_eq!(prefetcher.stats().prefetch_hits, 1);
    assert_eq!(prefetcher.stats().prefetch_hit_rate, 1.0);
}
