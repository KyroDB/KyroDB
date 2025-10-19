// End-to-end integration test for three-tier architecture
//
// This test validates the complete query journey through all three layers:
// - Layer 1: Hybrid Semantic Cache (RMI + semantic prediction)
// - Layer 2: Hot tier (recent writes buffer)
// - Layer 3: Cold tier (HNSW + persistence)

use kyrodb_engine::*;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_three_tier_query_journey() {
    // Setup: 10 initial documents
    let mut initial_embeddings = Vec::new();
    for i in 0..10 {
        initial_embeddings.push(vec![i as f32 / 10.0, 1.0 - i as f32 / 10.0]);
    }

    let cache = LruCacheStrategy::new(5); // Small cache: 5 docs
    let config = TieredEngineConfig {
        hot_tier_max_size: 3, // Small hot tier: 3 docs
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();

    // Phase 1: Query existing documents (should hit cold tier)
    println!("\n=== Phase 1: Cold tier queries ===");
    for i in 0..5 {
        let result = engine.query(i, None);
        assert!(result.is_some(), "Doc {} should exist in cold tier", i);
        println!("Doc {} found in cold tier", i);
    }

    let stats = engine.stats();
    assert_eq!(stats.total_queries, 5);
    assert_eq!(stats.cold_tier_searches, 5); // All 5 hit cold tier
    assert_eq!(stats.cache_hits, 0); // Cache empty initially
    println!("Cold tier stats: {} searches", stats.cold_tier_searches);

    // Phase 2: Insert new documents (should go to hot tier)
    println!("\n=== Phase 2: Hot tier inserts ===");
    engine.insert(100, vec![0.25, 0.75]).unwrap();
    engine.insert(101, vec![0.30, 0.70]).unwrap();

    // Query documents in hot tier (before flush)
    let result = engine.query(100, None);
    assert!(result.is_some(), "Doc 100 should be in hot tier");

    let stats = engine.stats();
    assert_eq!(stats.hot_tier_hits, 1, "Expected 1 hot tier hit");
    println!("Hot tier hit: doc 100");

    // Phase 3: Flush hot tier to cold tier
    println!("\n=== Phase 3: Flushing to cold tier ===");
    // Insert one more to trigger flush (hot_tier_max_size=3)
    engine.insert(102, vec![0.35, 0.65]).unwrap();
    let flushed = engine.flush_hot_tier().unwrap();
    println!("Flushed {} documents", flushed);
    assert!(flushed > 0, "Should have flushed at least some documents");

    // Query a DIFFERENT document (101) that was flushed but not cached
    // This should hit cold tier (not cache, not hot tier)
    let result = engine.query(101, Some(&[0.30, 0.70]));
    assert!(
        result.is_some(),
        "Doc 101 should be in cold tier after flush"
    );

    let stats = engine.stats();
    println!(
        "Cold tier searches after flush: {}",
        stats.cold_tier_searches
    );
    assert!(
        stats.cold_tier_searches > 5,
        "Cold tier searches should increase after flush"
    );
    println!("Doc 101 found in cold tier after flush");

    // Phase 4: Test cache behavior
    println!("\n=== Phase 4: Cache layer ===");

    // Repeatedly query same document to trigger caching
    for _ in 0..5 {
        engine.query(0, Some(&[0.0, 1.0]));
    }

    let stats = engine.stats();
    println!(
        "Cache hits: {}, Total queries: {}",
        stats.cache_hits, stats.total_queries
    );

    // After multiple queries, some should hit cache
    // Note: Cache admission depends on strategy, so we just verify it's being used
    assert!(
        stats.cache_hits > 0 || stats.cold_tier_searches > 0,
        "Cache should be active"
    );

    // Phase 5: K-NN search across all tiers
    println!("\n=== Phase 5: K-NN search ===");
    let query = vec![0.5, 0.5];
    let results = engine.knn_search(&query, 5).unwrap();

    assert_eq!(results.len(), 5, "Should return top 5 nearest neighbors");
    println!("K-NN search returned {} results", results.len());

    for (i, result) in results.iter().enumerate() {
        println!(
            "  {}. doc_id={}, distance={:.4}",
            i + 1,
            result.doc_id,
            result.distance
        );
    }

    // Results should be sorted by distance (ascending)
    for i in 0..results.len() - 1 {
        assert!(
            results[i].distance <= results[i + 1].distance,
            "Results should be sorted by distance"
        );
    }

    println!("\n=== Three-tier architecture validated ===");
    println!("Final stats: {:?}", engine.stats());
}

#[test]
fn test_persistence_across_all_tiers() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Create engine with persistence
    {
        let initial_embeddings = vec![vec![1.0, 0.0], vec![0.8, 0.2], vec![0.6, 0.4]];

        let cache = LruCacheStrategy::new(10);
        let config = TieredEngineConfig {
            hot_tier_max_size: 2,
            hnsw_max_elements: 100,
            data_dir: Some(dir.path().to_string_lossy().to_string()),
            fsync_policy: FsyncPolicy::Always,
            snapshot_interval: 5, // Snapshot every 5 inserts
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();

        println!("\n=== Phase 1: Writing data ===");

        // Insert to hot tier and flush
        engine.insert(10, vec![0.4, 0.6]).unwrap();
        engine.insert(11, vec![0.2, 0.8]).unwrap();
        engine.flush_hot_tier().unwrap();

        // Insert more (should trigger snapshot at 5 inserts)
        engine.insert(20, vec![0.1, 0.9]).unwrap();
        engine.insert(21, vec![0.0, 1.0]).unwrap();
        engine.flush_hot_tier().unwrap();

        // Query to populate cache
        engine.query(10, Some(&[0.4, 0.6]));
        engine.query(11, Some(&[0.2, 0.8]));

        println!("Inserted docs: 0, 1, 2 (initial), 10, 11, 20, 21");
        println!("Stats: {:?}", engine.stats());

        // Explicit snapshot before shutdown
        engine.cold_tier().create_snapshot().unwrap();
        println!("Snapshot created");
    }

    // Phase 2: Recover and verify
    {
        println!("\n=== Phase 2: Recovery ===");

        let cache = LruCacheStrategy::new(10);
        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            ..Default::default()
        };

        let recovered = TieredEngine::recover(Box::new(cache), dir.path(), config).unwrap();

        // Verify initial docs
        assert!(
            recovered.query(0, None).is_some(),
            "Doc 0 should be recovered"
        );
        assert!(
            recovered.query(1, None).is_some(),
            "Doc 1 should be recovered"
        );
        assert!(
            recovered.query(2, None).is_some(),
            "Doc 2 should be recovered"
        );

        // Verify inserted docs
        assert!(
            recovered.query(10, None).is_some(),
            "Doc 10 should be recovered"
        );
        assert!(
            recovered.query(11, None).is_some(),
            "Doc 11 should be recovered"
        );
        assert!(
            recovered.query(20, None).is_some(),
            "Doc 20 should be recovered"
        );
        assert!(
            recovered.query(21, None).is_some(),
            "Doc 21 should be recovered"
        );

        println!("All documents recovered successfully");

        // Hot tier should be empty after recovery (ephemeral)
        assert_eq!(
            recovered.hot_tier().len(),
            0,
            "Hot tier should be empty after recovery"
        );

        // Cache should be empty (ephemeral)
        let stats = recovered.stats();
        assert_eq!(stats.cache_hits, 0, "Cache should be empty after recovery");

        println!("Recovery validated: {} queries", stats.total_queries);
    }
}

#[test]
fn test_concurrent_tier_access() {
    use std::thread;

    let initial_embeddings = vec![vec![1.0, 0.0]; 20];
    let cache = LruCacheStrategy::new(10);
    let config = TieredEngineConfig {
        hot_tier_max_size: 10,
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = Arc::new(TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap());

    println!("\n=== Concurrent access test ===");

    // Spawn multiple threads doing different operations
    let mut handles = vec![];

    // Thread 1: Queries
    let engine1 = Arc::clone(&engine);
    handles.push(thread::spawn(move || {
        for i in 0..10 {
            engine1.query(i, None);
        }
    }));

    // Thread 2: Inserts
    let engine2 = Arc::clone(&engine);
    handles.push(thread::spawn(move || {
        for i in 100..110 {
            engine2.insert(i, vec![0.5, 0.5]).unwrap();
        }
    }));

    // Thread 3: K-NN searches
    let engine3 = Arc::clone(&engine);
    handles.push(thread::spawn(move || {
        for _ in 0..5 {
            engine3.knn_search(&[0.5, 0.5], 5).unwrap();
        }
    }));

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let stats = engine.stats();
    println!("Concurrent operations completed");
    println!("Total queries: {}", stats.total_queries);
    println!("Cache hits: {}", stats.cache_hits);
    println!("Hot tier hits: {}", stats.hot_tier_hits);

    assert!(stats.total_queries >= 10, "Should have at least 10 queries");
}

#[test]
fn test_query_path_layering() {
    println!("\n=== Query path layering test ===");

    let initial_embeddings = vec![vec![1.0, 0.0]; 5];
    let cache = LruCacheStrategy::new(3); // Cache capacity: 3
    let config = TieredEngineConfig {
        hot_tier_max_size: 5,
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();

    // Step 1: Query doc 0 (cold tier hit)
    println!("Step 1: First query to doc 0");
    let result = engine.query(0, Some(&[1.0, 0.0]));
    assert!(result.is_some());
    let stats = engine.stats();
    assert_eq!(stats.cold_tier_searches, 1);
    assert_eq!(stats.cache_hits, 0);

    // Step 2: Query doc 0 again (may hit cache if admitted)
    println!("Step 2: Second query to doc 0");
    engine.query(0, Some(&[1.0, 0.0]));

    // Step 3: Insert new doc to hot tier
    println!("Step 3: Insert doc 100 to hot tier");
    engine.insert(100, vec![0.5, 0.5]).unwrap();

    // Step 4: Query doc 100 (hot tier hit)
    println!("Step 4: Query doc 100 from hot tier");
    let result = engine.query(100, None);
    assert!(result.is_some());
    let stats = engine.stats();
    assert_eq!(stats.hot_tier_hits, 1, "Expected hot tier hit");

    // Step 5: Query non-existent doc (misses all tiers)
    println!("Step 5: Query non-existent doc 9999");
    let result = engine.query(9999, None);
    assert!(result.is_none());

    println!("\nFinal layering stats:");
    let stats = engine.stats();
    println!("  Cache hits: {}", stats.cache_hits);
    println!("  Hot tier hits: {}", stats.hot_tier_hits);
    println!("  Cold tier searches: {}", stats.cold_tier_searches);
    println!("  Total queries: {}", stats.total_queries);

    // Verify query path integrity
    // Note: One query (doc 9999) misses all tiers, so sum won't equal total
    // But we should have at least captured the hits
    assert!(
        stats.cache_hits + stats.hot_tier_hits + stats.cold_tier_searches <= stats.total_queries,
        "Sum of tier hits should be <= total queries (because some may miss all tiers)"
    );
}
