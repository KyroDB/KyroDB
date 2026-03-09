//! Regression and property tests for the HNSW vector index.
//!
//! Coverage in this file is intentionally split:
//! 1. deterministic recall/order checks on structured cosine datasets
//! 2. crash-resistance checks on randomized inputs
//! 3. small edge-case behavior (empty index, duplicate vectors, etc.)

use kyrodb_engine::hnsw_index::{HnswVectorIndex, SearchResult};
use proptest::prelude::*;

/// Calculate cosine distance between two vectors
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    // Match KyroDB's cosine-distance semantics: clamp at 0 for FP stability.
    // - accumulate in f64
    // - clamp negative distances to 0 (numerical noise)
    // - return 0 for degenerate vectors
    let mut dot: f64 = 0.0;
    let mut norm_a_sq: f64 = 0.0;
    let mut norm_b_sq: f64 = 0.0;

    for (&x, &y) in a.iter().zip(b.iter()) {
        let xf = x as f64;
        let yf = y as f64;
        dot += xf * yf;
        norm_a_sq += xf * xf;
        norm_b_sq += yf * yf;
    }

    if norm_a_sq > 0.0 && norm_b_sq > 0.0 {
        let dist_unchecked = 1.0 - dot / (norm_a_sq * norm_b_sq).sqrt();
        (dist_unchecked.max(0.0)) as f32
    } else {
        0.0
    }
}

/// Brute force k-NN search for ground truth
fn brute_force_knn(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<SearchResult> {
    let mut results: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(id, vec)| (id, cosine_distance(query, vec)))
        .collect();

    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    results
        .into_iter()
        .take(k)
        .map(|(id, distance)| SearchResult {
            doc_id: id as u64,
            distance,
        })
        .collect()
}

/// Calculate recall@k: fraction of brute force top-k found by HNSW
fn calculate_recall(hnsw_results: &[SearchResult], brute_force_results: &[SearchResult]) -> f32 {
    let brute_force_ids: Vec<u64> = brute_force_results.iter().map(|r| r.doc_id).collect();
    let found = hnsw_results
        .iter()
        .filter(|r| brute_force_ids.contains(&r.doc_id))
        .count();

    found as f32 / brute_force_results.len() as f32
}

fn normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
    v
}

fn pseudo_random_vec(seed: usize, dim: usize) -> Vec<f32> {
    let mut out = Vec::with_capacity(dim);
    let mut state = (seed as u64) ^ 0xD0E1_F2A3_B4C5_9697;
    for _ in 0..dim {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        out.push(((state >> 16) & 0xFFFF) as f32 / 32768.0 - 1.0);
    }
    out
}

fn structured_cosine_dataset(
    count: usize,
    dimension: usize,
    cluster_count: usize,
) -> Vec<Vec<f32>> {
    let cluster_count = cluster_count.max(2);
    (0..count)
        .map(|idx| {
            let cluster = idx % cluster_count;
            let center = pseudo_random_vec(cluster ^ 0xC0DE, dimension);
            let local = pseudo_random_vec(idx ^ 0xBEEF, dimension);
            let mut point = Vec::with_capacity(dimension);
            for d in 0..dimension {
                point.push(0.94 * center[d] + 0.06 * local[d]);
            }
            normalize(point)
        })
        .collect()
}

#[test]
fn test_hnsw_recall_small_dataset() {
    // Small dataset: HNSW should keep strong overlap with brute-force neighbors.
    // With only 5 vectors and k=3, missing even 1 neighbor drops recall to 67%
    // Run multiple queries and check average recall to reduce flakiness
    let mut index = HnswVectorIndex::new(4, 100).unwrap();

    let vectors = vec![
        normalize(vec![1.0, 0.0, 0.0, 0.0]),
        normalize(vec![0.0, 1.0, 0.0, 0.0]),
        normalize(vec![0.0, 0.0, 1.0, 0.0]),
        normalize(vec![0.0, 0.0, 0.0, 1.0]),
        normalize(vec![0.5, 0.5, 0.0, 0.0]),
    ];

    for (id, vec) in vectors.iter().enumerate() {
        index.add_vector(id as u64, vec).unwrap();
    }

    // Test multiple queries to reduce flakiness from HNSW graph randomness
    let queries = vec![
        normalize(vec![0.9, 0.1, 0.0, 0.0]),
        normalize(vec![0.1, 0.9, 0.0, 0.0]),
        normalize(vec![0.5, 0.5, 0.0, 0.0]),
        normalize(vec![0.0, 0.0, 0.9, 0.1]),
    ];

    let mut total_recall = 0.0;
    for query in &queries {
        let hnsw_results = index.knn_search(query, 3).unwrap();
        let brute_force_results = brute_force_knn(&vectors, query, 3);
        total_recall += calculate_recall(&hnsw_results, &brute_force_results);
    }

    let avg_recall = total_recall / queries.len() as f32;
    assert!(
        avg_recall >= 0.80,
        "HNSW average recall too low: {:.2} (expected >= 0.80)",
        avg_recall
    );
}

#[test]
fn test_hnsw_recall_medium_dataset() {
    // Structured cosine dataset: public API should keep strong recall at k=10.
    let dimension = 128;
    let count = 1000;
    let k = 10;

    let vectors = structured_cosine_dataset(count, dimension, 32);

    let mut index = HnswVectorIndex::new(dimension, count).unwrap();
    for (id, vec) in vectors.iter().enumerate() {
        index.add_vector(id as u64, vec).unwrap();
    }

    // Test multiple queries
    let mut recalls = Vec::new();
    for i in 0..10 {
        let query = &vectors[i];
        let hnsw_results = index.knn_search(query, k).unwrap();
        let brute_force_results = brute_force_knn(&vectors, query, k);
        let recall = calculate_recall(&hnsw_results, &brute_force_results);
        recalls.push(recall);
    }

    let avg_recall = recalls.iter().sum::<f32>() / recalls.len() as f32;
    assert!(
        avg_recall >= 0.95,
        "HNSW recall@{} too low: {:.2}% (expected >= 95%)",
        k,
        avg_recall * 100.0
    );
}

#[test]
fn test_hnsw_results_ordered() {
    // Results should be ordered by distance (closest first)
    let mut index = HnswVectorIndex::new(4, 100).unwrap();

    let vectors = [
        normalize(vec![1.0, 0.0, 0.0, 0.0]),
        normalize(vec![0.5, 0.5, 0.0, 0.0]),
        normalize(vec![0.0, 1.0, 0.0, 0.0]),
        normalize(vec![0.0, 0.0, 1.0, 0.0]), // Add a 4th vector to ensure we get multiple results
        normalize(vec![0.0, 0.0, 0.0, 1.0]), // Add a 5th vector
    ];

    for (id, vec) in vectors.iter().enumerate() {
        index.add_vector(id as u64, vec).unwrap();
    }

    let query = normalize(vec![0.9, 0.1, 0.0, 0.0]);
    let results = index.knn_search(&query, 3).unwrap();

    // Should return results ordered by distance (closest first)
    assert!(!results.is_empty(), "Expected at least 1 result");
    for window in results.windows(2) {
        assert!(window[0].distance <= window[1].distance);
    }
}

#[test]
fn test_hnsw_identical_vectors() {
    // Query identical to inserted vector should have distance ~0
    let mut index = HnswVectorIndex::new(128, 100).unwrap();

    let vector = normalize(vec![0.1; 128]);
    index.add_vector(42, &vector).unwrap();

    let results = index.knn_search(&vector, 1).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, 42);
    assert!(results[0].distance < 0.01); // Nearly zero distance
}

// Property-based tests with proptest

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 16,
        max_shrink_iters: 0,
        .. ProptestConfig::default()
    })]
    // NOTE: This test validates robustness only.
    // Random vectors are intentionally used here as adversarial/noisy inputs, not as a
    // recall target. Recall assertions live in the deterministic structured-data tests.
    #[test]
    fn prop_hnsw_no_crash_on_random_data(
        vectors in prop::collection::vec(
            prop::collection::vec(-1.0f32..1.0f32, 32),
            50..200
        )
    ) {
        // Normalize vectors and filter out degenerate cases (zero vectors)
        let normalized: Vec<Vec<f32>> = vectors
            .iter()
            .filter_map(|v| {
                let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm > 1e-6 {
                    Some(v.iter().map(|x| x / norm).collect())
                } else {
                    None
                }
            })
            .collect();

        // Skip if we filtered out too many vectors
        if normalized.len() < 50 {
            return Ok(());
        }

        // Property 1: Index creation should not crash
        let mut index = HnswVectorIndex::new(32, normalized.len()).unwrap();

        // Property 2: Inserting vectors should not crash
        for (id, vec) in normalized.iter().enumerate() {
            index.add_vector(id as u64, vec).unwrap();
        }

        // Property 3: Searching should not crash and should return results
        let query = &normalized[0];
        let k = 10.min(normalized.len());
        let hnsw_results = index.knn_search(query, k).unwrap();

        // Property 4: Should return k results (or fewer if index size < k)
        prop_assert_eq!(hnsw_results.len(), k);

        // Property 5: Results should be ordered by distance (closest first)
        for i in 1..hnsw_results.len() {
            prop_assert!(
                hnsw_results[i-1].distance <= hnsw_results[i].distance,
                "Results not sorted by distance: [{:.4}, {:.4}]",
                hnsw_results[i-1].distance,
                hnsw_results[i].distance
            );
        }
    }

    #[test]
    fn prop_hnsw_no_crash_on_edge_cases(
        dimension in 10usize..256,
        count in 10usize..1000,
        k in 1usize..20             // Reasonable k values
    ) {
        // Should not crash on any valid inputs
        let index = HnswVectorIndex::new(dimension, count);
        prop_assert!(index.is_ok());

        let mut idx = index.unwrap();

        // Insert multiple vectors to exercise graph traversal on non-trivial topology.
        let insert_count = 5.min(count);
        for i in 0..insert_count {
            let mut raw = Vec::with_capacity(dimension);
            for j in 0..dimension {
                raw.push(0.5 + (i as f32) * 0.01 + (j as f32) * 0.001);
            }
            let vector = normalize(raw);
            let result = idx.add_vector(i as u64, &vector);
            prop_assert!(result.is_ok());
        }

        let mut qraw = Vec::with_capacity(dimension);
        for j in 0..dimension {
            qraw.push(0.3 + (j as f32) * 0.002);
        }
        let query = normalize(qraw);
        let search_k = k.min(insert_count);
        let results = idx.knn_search(&query, search_k);
        prop_assert!(results.is_ok());

        // HNSW may return fewer than k results (especially with small datasets)
        let result_vec = results.unwrap();
        prop_assert!(result_vec.len() <= search_k);
        prop_assert!(!result_vec.is_empty());
    }
}
