use kyrodb_engine::config::DistanceMetric;
use kyrodb_engine::hnsw_index::HnswVectorIndex;
use kyrodb_engine::HotTier;
use std::collections::HashMap;
use std::time::Duration;

fn assert_close(a: f32, b: f32, tol: f32) {
    let diff = (a - b).abs();
    assert!(
        diff <= tol,
        "distance mismatch: {a} vs {b} (diff={diff}, tol={tol})"
    );
}

#[test]
fn test_metric_distance_consistency_cosine() {
    let metric = DistanceMetric::Cosine;
    let dim = 3;

    // Normalized vectors.
    let embedding = vec![1.0, 0.0, 0.0];
    let query = vec![1.0, 0.0, 0.0];

    let hot_tier = HotTier::new(100, Duration::from_secs(60), metric);
    hot_tier.insert(1, embedding.clone(), HashMap::new());
    let hot = hot_tier.knn_search(&query, 1);
    assert_eq!(hot.len(), 1);
    assert_eq!(hot[0].0, 1);
    let hot_distance = hot[0].1;

    let mut hnsw = HnswVectorIndex::new_with_params(dim, 10, metric, 16, 200, false).unwrap();
    hnsw.add_vector(1, &embedding).unwrap();
    let cold = hnsw.knn_search(&query, 1).unwrap();
    assert_eq!(cold.len(), 1);
    assert_eq!(cold[0].doc_id, 1);

    assert_close(hot_distance, cold[0].distance, 1e-4);
}

#[test]
fn test_metric_distance_consistency_euclidean() {
    let metric = DistanceMetric::Euclidean;
    let dim = 2;

    let embedding = vec![0.0, 0.0];
    let query = vec![3.0, 4.0];

    let hot_tier = HotTier::new(100, Duration::from_secs(60), metric);
    hot_tier.insert(1, embedding.clone(), HashMap::new());
    let hot = hot_tier.knn_search(&query, 1);
    assert_eq!(hot.len(), 1);
    assert_eq!(hot[0].0, 1);
    let hot_distance = hot[0].1;

    let mut hnsw = HnswVectorIndex::new_with_params(dim, 10, metric, 16, 200, false).unwrap();
    hnsw.add_vector(1, &embedding).unwrap();
    let cold = hnsw.knn_search(&query, 1).unwrap();
    assert_eq!(cold.len(), 1);
    assert_eq!(cold[0].doc_id, 1);

    assert_close(hot_distance, cold[0].distance, 1e-4);
}

#[test]
fn test_metric_distance_consistency_inner_product() {
    let metric = DistanceMetric::InnerProduct;
    let dim = 2;

    // L2-normalized.
    let embedding = vec![1.0, 0.0];
    let query = vec![1.0, 0.0];

    let hot_tier = HotTier::new(100, Duration::from_secs(60), metric);
    hot_tier.insert(1, embedding.clone(), HashMap::new());
    let hot = hot_tier.knn_search(&query, 1);
    assert_eq!(hot.len(), 1);
    assert_eq!(hot[0].0, 1);
    let hot_distance = hot[0].1;

    let mut hnsw = HnswVectorIndex::new_with_params(dim, 10, metric, 16, 200, false).unwrap();
    hnsw.add_vector(1, &embedding).unwrap();
    let cold = hnsw.knn_search(&query, 1).unwrap();
    assert_eq!(cold.len(), 1);
    assert_eq!(cold[0].doc_id, 1);

    assert_close(hot_distance, cold[0].distance, 1e-4);
}
