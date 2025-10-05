//! RMI Core Tests
//!
//! Phase 0 Week 3-8: Tests for learned cache RMI components

use kyrodb_engine::rmi_core::{LocalLinearModel, RmiIndex, RmiSegment};

#[test]
fn test_local_linear_model_basic() {
    let data = vec![(10, 100), (20, 200), (30, 300), (40, 400)];
    let model = LocalLinearModel::new(&data);

    // Model should predict positions accurately
    assert_eq!(model.predict(10), 0);
    assert_eq!(model.predict(40), 3);

    // Error bound should be reasonable
    assert!(model.error_bound() <= 8);

    // Key range checks
    assert!(model.contains_key(25));
    assert!(!model.contains_key(5));
    assert!(!model.contains_key(50));
}

#[test]
fn test_rmi_segment_bounded_search() {
    let data = vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)];
    let segment = RmiSegment::new(data);

    // Exact matches
    assert_eq!(segment.bounded_search(1), Some(10));
    assert_eq!(segment.bounded_search(3), Some(30));
    assert_eq!(segment.bounded_search(5), Some(50));

    // No match
    assert_eq!(segment.bounded_search(99), None);
    assert_eq!(segment.bounded_search(0), None);
}

#[test]
fn test_rmi_index_multi_segment() {
    // Create 1000 entries
    let data: Vec<(u64, u64)> = (0..1000).map(|i| (i, i * 10)).collect();

    // Build index with 100 entries per segment
    let index = RmiIndex::build(data, 100);

    // Should have ~10 segments
    assert!(index.segment_count() >= 9 && index.segment_count() <= 11);

    // Test lookups
    assert_eq!(index.get(42), Some(420));
    assert_eq!(index.get(999), Some(9990));
    assert_eq!(index.get(1000), None);
}

#[test]
fn test_rmi_index_empty() {
    let index = RmiIndex::build(vec![], 100);
    assert_eq!(index.segment_count(), 0);
    assert_eq!(index.get(0), None);
}

#[test]
fn test_rmi_index_single_element() {
    let data = vec![(42, 100)];
    let index = RmiIndex::build(data, 100);

    assert_eq!(index.get(42), Some(100));
    assert_eq!(index.get(0), None);
    assert_eq!(index.get(100), None);
}

#[test]
fn test_rmi_with_zipfian_like_data() {
    // Simulate Zipfian access pattern: 20% of keys get 80% of accesses
    // In learned cache, this would be (doc_id, hotness_score)
    let mut data: Vec<(u64, u64)> = Vec::new();

    // Hot documents (high hotness scores)
    for i in 0..200 {
        data.push((i, 80 + (i % 20))); // Hotness 80-100
    }

    // Cold documents (low hotness scores)
    for i in 200..1000 {
        data.push((i, i % 20)); // Hotness 0-20
    }

    let index = RmiIndex::build(data, 100);

    // Verify hot docs
    let hot_score = index.get(50).unwrap();
    assert!(hot_score >= 80, "Hot doc should have high score");

    // Verify cold docs
    let cold_score = index.get(500).unwrap();
    assert!(cold_score < 20, "Cold doc should have low score");
}
