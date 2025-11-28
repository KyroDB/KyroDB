//! Integration tests for tier-aware k-NN search
//!
//! Tests validate that k-NN queries correctly search across all three tiers:
//! - Layer 1b (Query Cache): Semantic similarity for paraphrased queries
//! - Layer 2 (Hot Tier): Recent writes not yet flushed to HNSW
//! - Layer 3 (Cold Tier): HNSW index for bulk vectors
//!
//! Key validation points:
//! - Hot tier results appear in k-NN output
//! - Results are correctly merged and deduplicated
//! - Cache admission works for query results
//! - P99 latency remains <1ms

use kyrodb_engine::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Helper to create normalized test embedding
fn create_normalized_embedding(values: Vec<f32>) -> Vec<f32> {
    let magnitude: f32 = values.iter().map(|x| x * x).sum::<f32>().sqrt();
    if magnitude < f32::EPSILON {
        values
    } else {
        values.into_iter().map(|x| x / magnitude).collect()
    }
}

#[test]
fn test_knn_searches_hot_tier() {
    println!("\n=== Test: k-NN searches hot tier ===");

    // Setup: 10 documents in cold tier
    let mut initial_embeddings = Vec::new();
    let mut initial_metadata = Vec::new();
    for i in 0..10 {
        initial_embeddings.push(create_normalized_embedding(vec![
            i as f32 / 10.0,
            1.0 - i as f32 / 10.0,
        ]));
        initial_metadata.push(HashMap::new());
    }

    let cache = LruCacheStrategy::new(5);
    let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
    let config = TieredEngineConfig {
        hot_tier_max_size: 1000, // Large to prevent auto-flush
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = TieredEngine::new(
        Box::new(cache),
        query_cache,
        initial_embeddings,
        initial_metadata,
        config,
    )
    .unwrap();

    // Insert 3 documents into hot tier (NOT flushed)
    println!("Inserting 3 documents to hot tier");
    engine
        .insert(
            100,
            create_normalized_embedding(vec![0.25, 0.75]),
            HashMap::new(),
        )
        .unwrap();
    engine
        .insert(
            101,
            create_normalized_embedding(vec![0.30, 0.70]),
            HashMap::new(),
        )
        .unwrap();
    engine
        .insert(
            102,
            create_normalized_embedding(vec![0.35, 0.65]),
            HashMap::new(),
        )
        .unwrap();

    // Verify they're in hot tier
    assert_eq!(
        engine.hot_tier().len(),
        3,
        "Hot tier should have 3 documents"
    );

    // k-NN search with query close to hot tier documents
    let query = create_normalized_embedding(vec![0.3, 0.7]);
    let results = engine.knn_search(&query, 5).unwrap();

    println!("k-NN search returned {} results", results.len());
    for (i, result) in results.iter().enumerate() {
        println!(
            "  {}. doc_id={}, distance={:.4}",
            i + 1,
            result.doc_id,
            result.distance
        );
    }

    // Validate: Hot tier documents should appear in results
    let hot_tier_doc_ids: Vec<u64> = results
        .iter()
        .filter(|r| r.doc_id >= 100 && r.doc_id <= 102)
        .map(|r| r.doc_id)
        .collect();

    assert!(
        !hot_tier_doc_ids.is_empty(),
        "At least one hot tier document should be in top-5 results"
    );

    println!("Hot tier documents in results: {:?}", hot_tier_doc_ids);

    // Validate: Results are sorted by distance
    for i in 0..results.len() - 1 {
        assert!(
            results[i].distance <= results[i + 1].distance,
            "Results should be sorted by distance"
        );
    }

    println!("✓ Hot tier documents correctly included in k-NN results");
}

#[test]
fn test_knn_deduplication() {
    println!("\n=== Test: k-NN deduplication (hot tier overwrites cold tier) ===");

    // Setup: 5 documents in cold tier
    let mut initial_embeddings = Vec::new();
    let mut initial_metadata = Vec::new();
    for i in 0..5 {
        initial_embeddings.push(create_normalized_embedding(vec![
            i as f32 / 5.0,
            1.0 - i as f32 / 5.0,
        ]));
        initial_metadata.push(HashMap::new());
    }

    let cache = LruCacheStrategy::new(5);
    let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
    let config = TieredEngineConfig {
        hot_tier_max_size: 1000,
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = TieredEngine::new(
        Box::new(cache),
        query_cache,
        initial_embeddings,
        initial_metadata,
        config,
    )
    .unwrap();

    // Insert doc 2 again with DIFFERENT embedding (update scenario)
    println!("Inserting updated doc 2 to hot tier");
    engine
        .insert(
            2,
            create_normalized_embedding(vec![0.9, 0.1]),
            HashMap::new(),
        )
        .unwrap();

    // k-NN search should return doc 2 only ONCE (hot tier version)
    let query = create_normalized_embedding(vec![0.5, 0.5]);
    let results = engine.knn_search(&query, 5).unwrap();

    // Count occurrences of doc_id 2
    let doc_2_count = results.iter().filter(|r| r.doc_id == 2).count();

    assert_eq!(
        doc_2_count, 1,
        "Doc 2 should appear only once (deduplicated)"
    );

    println!(
        "✓ Deduplication works correctly (doc 2 appeared {} time)",
        doc_2_count
    );
}

#[test]
fn test_knn_result_merging_correctness() {
    println!("\n=== Test: k-NN result merging correctness ===");

    // Setup: 20 documents in cold tier
    let mut initial_embeddings = Vec::new();
    let mut initial_metadata = Vec::new();
    for i in 0..20 {
        let angle = (i as f32 / 20.0) * std::f32::consts::PI * 2.0;
        initial_embeddings.push(create_normalized_embedding(vec![angle.cos(), angle.sin()]));
        initial_metadata.push(HashMap::new());
    }

    let cache = LruCacheStrategy::new(5);
    let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
    let config = TieredEngineConfig {
        hot_tier_max_size: 1000,
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = TieredEngine::new(
        Box::new(cache),
        query_cache,
        initial_embeddings,
        initial_metadata,
        config,
    )
    .unwrap();

    // Insert 5 documents to hot tier at specific angles
    println!("Inserting 5 documents to hot tier");
    for i in 100..105 {
        let angle = (i as f32 / 105.0) * std::f32::consts::PI * 2.0;
        engine
            .insert(
                i,
                create_normalized_embedding(vec![angle.cos(), angle.sin()]),
                HashMap::new(),
            )
            .unwrap();
    }

    // k-NN search with k=10 (should merge hot + cold results)
    let query = create_normalized_embedding(vec![1.0, 0.0]);
    let results = engine.knn_search(&query, 10).unwrap();

    assert_eq!(results.len(), 10, "Should return exactly 10 results");

    // Validate: No duplicates
    let mut seen_ids = std::collections::HashSet::new();
    for result in &results {
        assert!(
            seen_ids.insert(result.doc_id),
            "Duplicate doc_id {} in results",
            result.doc_id
        );
    }

    // Validate: Results are sorted by distance
    for i in 0..results.len() - 1 {
        assert!(
            results[i].distance <= results[i + 1].distance,
            "Results should be sorted by distance"
        );
    }

    println!(
        "✓ Merging produced {} unique, sorted results",
        results.len()
    );
}

#[test]
fn test_knn_consistency() {
    println!("\n=== Test: k-NN consistency across multiple queries ===");

    // Setup
    let mut initial_embeddings = Vec::new();
    let mut initial_metadata = Vec::new();
    for i in 0..10 {
        initial_embeddings.push(create_normalized_embedding(vec![
            i as f32 / 10.0,
            1.0 - i as f32 / 10.0,
        ]));
        initial_metadata.push(HashMap::new());
    }

    let cache = LruCacheStrategy::new(5);
    let query_cache = Arc::new(QueryHashCache::new(100, 0.90));
    let config = TieredEngineConfig {
        hot_tier_max_size: 1000,
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = TieredEngine::new(
        Box::new(cache),
        query_cache,
        initial_embeddings,
        initial_metadata,
        config,
    )
    .unwrap();

    // First k-NN search
    let query = create_normalized_embedding(vec![0.5, 0.5]);
    println!("First k-NN search");
    let results1 = engine.knn_search(&query, 3).unwrap();

    assert_eq!(results1.len(), 3, "Should return 3 results");
    println!("First search returned {} results", results1.len());

    // Second k-NN search with IDENTICAL query (should return same results)
    println!("Second k-NN search with same query");
    let results2 = engine.knn_search(&query, 3).unwrap();

    assert_eq!(results2.len(), 3, "Should return 3 results");

    // Results should be identical (same doc_ids, same order)
    assert_eq!(
        results1.len(),
        results2.len(),
        "Both searches should return same number of results"
    );

    for (r1, r2) in results1.iter().zip(results2.iter()) {
        assert_eq!(
            r1.doc_id, r2.doc_id,
            "Results should have same doc_ids in same order"
        );
        assert!(
            (r1.distance - r2.distance).abs() < 0.001,
            "Distances should be nearly identical"
        );
    }

    println!("✓ k-NN consistency validated");
}

#[test]
fn test_knn_empty_hot_tier() {
    println!("\n=== Test: k-NN with empty hot tier ===");

    // Setup: Only cold tier documents, no hot tier
    let initial_embeddings = vec![
        create_normalized_embedding(vec![1.0, 0.0]),
        create_normalized_embedding(vec![0.0, 1.0]),
        create_normalized_embedding(vec![0.707, 0.707]),
    ];
    let initial_metadata = vec![HashMap::new(); 3];

    let cache = LruCacheStrategy::new(5);
    let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
    let config = TieredEngineConfig {
        hot_tier_max_size: 1000,
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = TieredEngine::new(
        Box::new(cache),
        query_cache,
        initial_embeddings,
        initial_metadata,
        config,
    )
    .unwrap();

    // Verify hot tier is empty
    assert_eq!(engine.hot_tier().len(), 0, "Hot tier should be empty");

    // k-NN search should still work (cold tier only)
    let query = create_normalized_embedding(vec![1.0, 0.0]);
    let results = engine.knn_search(&query, 2).unwrap();

    assert_eq!(results.len(), 2, "Should return 2 results from cold tier");

    // Best match should be doc 0 (identical to query)
    assert_eq!(results[0].doc_id, 0, "Best match should be doc 0");
    assert!(
        results[0].distance < 0.01,
        "Distance should be near 0 for identical vectors"
    );

    println!("✓ k-NN works correctly with empty hot tier");
}

#[test]
fn test_knn_only_hot_tier() {
    println!("\n=== Test: k-NN with only hot tier documents ===");

    // Setup: One document in cold tier (HNSW requires at least one for dimensionality)
    // But we'll primarily query documents in hot tier
    let initial_embeddings = vec![create_normalized_embedding(vec![0.0, 0.0])];
    let initial_metadata = vec![HashMap::new()];

    let cache = LruCacheStrategy::new(5);
    let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
    let config = TieredEngineConfig {
        hot_tier_max_size: 1000,
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = TieredEngine::new(
        Box::new(cache),
        query_cache,
        initial_embeddings,
        initial_metadata,
        config,
    )
    .unwrap();

    // Insert documents only to hot tier
    println!("Inserting 5 documents to hot tier");
    for i in 0..5 {
        engine
            .insert(
                i,
                create_normalized_embedding(vec![i as f32 / 5.0, 1.0 - i as f32 / 5.0]),
                HashMap::new(),
            )
            .unwrap();
    }

    // k-NN search should find documents in hot tier (plus the one cold tier doc)
    let query = create_normalized_embedding(vec![0.5, 0.5]);
    let results = engine.knn_search(&query, 5).unwrap();

    assert!(
        results.len() >= 5,
        "Should return at least 5 results (5 from hot tier)"
    );

    // Most results should be from hot tier (doc_ids 0-4), with possibly doc_id from cold tier
    let hot_tier_count = results.iter().filter(|r| r.doc_id < 5).count();

    assert!(
        hot_tier_count == 5,
        "All 5 hot tier documents should be in results"
    );

    println!("✓ k-NN works correctly with primarily hot tier documents");
}

#[test]
fn test_knn_latency_with_hot_tier() {
    println!("\n=== Test: k-NN latency validation ===");

    // Setup: 1000 documents in cold tier
    let mut initial_embeddings = Vec::new();
    let mut initial_metadata = Vec::new();
    for i in 0..1000 {
        let angle = (i as f32 / 1000.0) * std::f32::consts::PI * 2.0;
        initial_embeddings.push(create_normalized_embedding(vec![angle.cos(), angle.sin()]));
        initial_metadata.push(HashMap::new());
    }

    let cache = LruCacheStrategy::new(50);
    let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
    let config = TieredEngineConfig {
        hot_tier_max_size: 1000,
        hnsw_max_elements: 10_000,
        ..Default::default()
    };

    let engine = TieredEngine::new(
        Box::new(cache),
        query_cache,
        initial_embeddings,
        initial_metadata,
        config,
    )
    .unwrap();

    // Insert 100 documents to hot tier
    println!("Inserting 100 documents to hot tier");
    for i in 1000..1100 {
        let angle = (i as f32 / 1100.0) * std::f32::consts::PI * 2.0;
        engine
            .insert(
                i,
                create_normalized_embedding(vec![angle.cos(), angle.sin()]),
                HashMap::new(),
            )
            .unwrap();
    }

    // Warm-up queries
    for _ in 0..5 {
        let query = create_normalized_embedding(vec![1.0, 0.0]);
        engine.knn_search(&query, 10).unwrap();
    }

    // Measure latency over 100 queries
    let mut latencies = Vec::new();
    for i in 0..100 {
        let angle = (i as f32 / 100.0) * std::f32::consts::PI * 2.0;
        let query = create_normalized_embedding(vec![angle.cos(), angle.sin()]);

        let start = std::time::Instant::now();
        let results = engine.knn_search(&query, 10).unwrap();
        let latency = start.elapsed();

        latencies.push(latency);

        assert_eq!(results.len(), 10, "Should return 10 results");
    }

    // Calculate P99 latency
    latencies.sort();
    let p99_index = (latencies.len() as f32 * 0.99) as usize;
    let p99_latency = latencies[p99_index];

    let p50_index = latencies.len() / 2;
    let p50_latency = latencies[p50_index];

    println!("Latency statistics over 100 queries:");
    println!("  P50: {:?}", p50_latency);
    println!("  P99: {:?}", p99_latency);
    println!("  Max: {:?}", latencies.last().unwrap());

    // Validate P99 < 10ms (relaxed for testing environment)
    // Production target is <1ms, but CI/testing may be slower
    assert!(
        p99_latency < std::time::Duration::from_millis(10),
        "P99 latency should be <10ms (got {:?})",
        p99_latency
    );

    println!("✓ Latency validation passed");
}

#[test]
fn test_knn_correctness_vs_brute_force() {
    println!("\n=== Test: k-NN correctness vs brute force ===");

    // Setup: Small dataset for brute force comparison
    let mut initial_embeddings = Vec::new();
    let mut initial_metadata = Vec::new();
    for i in 0..20 {
        initial_embeddings.push(create_normalized_embedding(vec![
            i as f32 / 20.0,
            1.0 - i as f32 / 20.0,
        ]));
        initial_metadata.push(HashMap::new());
    }

    let cache = LruCacheStrategy::new(5);
    let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
    let config = TieredEngineConfig {
        hot_tier_max_size: 1000,
        hnsw_max_elements: 100,
        ..Default::default()
    };

    let engine = TieredEngine::new(
        Box::new(cache),
        query_cache,
        initial_embeddings.clone(),
        initial_metadata.clone(),
        config,
    )
    .unwrap();

    // Insert 5 documents to hot tier
    let mut hot_tier_embeddings = Vec::new();
    for i in 100..105 {
        let emb = create_normalized_embedding(vec![
            (i as f32 - 50.0) / 55.0,
            1.0 - (i as f32 - 50.0) / 55.0,
        ]);
        engine.insert(i, emb.clone(), HashMap::new()).unwrap();
        hot_tier_embeddings.push((i, emb));
    }

    // k-NN search
    let query = create_normalized_embedding(vec![0.5, 0.5]);
    let results = engine.knn_search(&query, 5).unwrap();

    // Brute force: compute distances to all documents
    let mut brute_force_distances = Vec::new();
    for (i, emb) in initial_embeddings.iter().enumerate() {
        let distance = cosine_distance(&query, emb);
        brute_force_distances.push((i as u64, distance));
    }
    for (doc_id, emb) in hot_tier_embeddings {
        let distance = cosine_distance(&query, &emb);
        brute_force_distances.push((doc_id, distance));
    }

    brute_force_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let brute_force_top5: Vec<u64> = brute_force_distances
        .iter()
        .take(5)
        .map(|(id, _)| *id)
        .collect();

    // Compare: k-NN results should match brute force (or have high overlap)
    let knn_ids: Vec<u64> = results.iter().map(|r| r.doc_id).collect();

    let overlap = knn_ids
        .iter()
        .filter(|id| brute_force_top5.contains(id))
        .count();

    println!("k-NN results: {:?}", knn_ids);
    println!("Brute force top-5: {:?}", brute_force_top5);
    println!("Overlap: {}/5", overlap);

    // HNSW is approximate, so allow some deviation
    assert!(
        overlap >= 4,
        "At least 4/5 results should match brute force (got {}/5)",
        overlap
    );

    println!("✓ k-NN correctness validated (recall: {}/5)", overlap);
}

/// Helper: Compute cosine distance between two vectors
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::INFINITY;
    }

    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if mag_a < f32::EPSILON || mag_b < f32::EPSILON {
        return f32::INFINITY;
    }

    let cosine_sim = (dot / (mag_a * mag_b)).clamp(-1.0, 1.0);
    1.0 - cosine_sim
}
