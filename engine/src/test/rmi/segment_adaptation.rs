//! RMI Segment Adaptation Tests
//!
//! Tests for segment split/merge/retrain triggers and adaptive behavior

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_segment_creation_from_data() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write enough data to create multiple segments (TARGET_SEGMENT_SIZE = 8192)
    for i in 0..25000 {
        append_kv(&log, i, format!("segment_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI (should create ~3 segments)
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all keys accessible across all segments
    for i in (0..25000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Multi-segment: key {} not found", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_single_segment_handling() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write less than TARGET_SEGMENT_SIZE
    for i in 0..1000 {
        append_kv(&log, i, format!("single_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI (should create single segment)
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all keys accessible
    for i in (0..1000).step_by(10) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Single segment: key {} not found", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_large_segment_count() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write enough for many segments
    for i in 0..100000 {
        append_kv(&log, i, format!("many_segments_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI (should create ~12 segments)
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify keys across all segments
    for i in (0..100000).step_by(1000) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Many segments: key {} not found", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_segment_boundaries_accuracy() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write data spanning multiple segments
    for i in 0..30000 {
        append_kv(&log, i, format!("boundaries_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Test keys at likely segment boundaries (multiples of 8192)
    for segment_start in (0..30000).step_by(8192) {
        // Test start of segment
        let value = lookup_kv(&log, segment_start)
            .await
            .expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Segment boundary: start key {} not found",
            segment_start
        );

        // Test near boundary
        if segment_start > 0 {
            let value = lookup_kv(&log, segment_start - 1)
                .await
                .expect("Failed to lookup");
            assert!(
                value.is_some(),
                "Segment boundary: pre-boundary key {} not found",
                segment_start - 1
            );
        }
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_adaptation_after_updates() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Initial data
    for i in 0..5000 {
        append_kv(&log, i, format!("v1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build initial RMI");

    // Large update that changes distribution
    for i in 5000..20000 {
        append_kv(&log, i, format!("v2_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild (should adapt to new distribution)
    log.build_rmi()
        .await
        .expect("Failed to rebuild after updates");

    // Verify all keys accessible with adapted segments
    for i in (0..20000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Adaptation: key {} not found after updates",
            i
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_segment_with_skewed_distribution() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Skewed distribution: dense sequential region followed by sparse region WITH OVERLAP
    // Dense region: every key from 0-10000
    for i in 0..10000 {
        append_kv(&log, i, format!("dense_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sparse region: every 10th key from 10000-20000 (continues but sparser)
    for i in (10000..20000).step_by(10) {
        append_kv(&log, i, format!("sparse_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI (should handle skew)
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify dense region
    for i in &[0, 500, 1000, 3000, 5000, 7000, 9000, 9800] {
        let value = lookup_kv(&log, *i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Skewed dense: key {} not found", i);
    }

    // Verify sparse region (checking keys that exist with step_by(10))
    for i in &[10000, 10500, 12000, 14000, 16000, 18000, 19900] {
        let value = lookup_kv(&log, *i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Skewed sparse: key {} not found", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_segment_split_threshold_behavior() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Start with data slightly under segment size
    for i in 0..8000 {
        append_kv(&log, i, format!("pre_split_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI (1 segment)
    log.build_rmi().await.expect("Failed to build RMI");

    // Add more data to exceed threshold
    for i in 8000..18000 {
        append_kv(&log, i, format!("post_split_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild (should create more segments)
    log.build_rmi()
        .await
        .expect("Failed to rebuild after split threshold");

    // Verify all keys accessible
    for i in (0..18000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Split threshold: key {} not found", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_model_quality_with_sequential_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Perfect sequential keys (ideal for linear model)
    for i in 0..20000 {
        append_kv(&log, i, format!("sequential_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI (should have excellent model quality)
    log.build_rmi().await.expect("Failed to build RMI");

    // All lookups should be very fast with perfect predictions
    for i in (0..20000).step_by(50) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Sequential model: key {} not found", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_model_quality_with_random_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Random keys (challenging for linear model)
    let mut rng_state = 42u64;
    let mut keys = Vec::new();

    for _ in 0..10000 {
        rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        let key = rng_state % 1000000;
        keys.push(key);
    }

    keys.sort_unstable();
    keys.dedup();

    for &key in &keys {
        append_kv(&log, key, format!("random_{}", key).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI (should handle random distribution)
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all random keys accessible
    for &key in keys.iter().step_by(100) {
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(value.is_some(), "Random model: key {} not found", key);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_segment_with_power_of_two_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Sequential keys as primary data (ensures enough density)
    for i in 0..5000 {
        append_kv(&log, i, format!("seq_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Add power of 2 keys in range
    for i in 0..13 {
        // Only up to 2^12 = 4096 to stay in range
        let key = 2u64.pow(i);
        if key < 5000 {
            append_kv(&log, key, format!("pow2_{}", key).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify sequential keys
    for i in (0..5000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Power of 2: sequential key {} not found",
            i
        );
    }

    // Verify power of 2 keys
    for i in 0..13 {
        let key = 2u64.pow(i);
        if key < 5000 {
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            assert!(value.is_some(), "Power of 2: key {} not found", key);
        }
    }
}
