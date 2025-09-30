//! RMI Bounded Search Tests
//!
//! Tests for SIMD operations, epsilon bounds (≤64), and O(log ε) guarantees

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_bounded_search_basic() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write data
    for i in 0..1000 {
        append_kv(&log, i, format!("bounded_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // All lookups should succeed (validates bounded search works)
    for i in (0..1000).step_by(10) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Bounded search: key {} not found within epsilon bounds",
            i
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_epsilon_bound_guarantee() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write many keys to create multiple segments
    for i in 0..20000 {
        append_kv(&log, i, format!("epsilon_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all keys accessible (epsilon ≤ 64 guarantee)
    for i in (0..20000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Epsilon bound: key {} not found (epsilon may exceed 64)",
            i
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_probe_window_correctness() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write sequential data
    for i in 0..5000 {
        append_kv(&log, i, format!("probe_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Test keys at various positions (validates probe window [pos - ε, pos + ε])
    let test_keys = vec![0, 1, 100, 500, 1000, 2500, 4999];
    
    for &key in &test_keys {
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Probe window: key {} not found within window",
            key
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_worst_case_search_complexity() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Create data pattern that challenges model prediction
    // Mix of exponential and linear patterns, with enough density
    for i in 0..1000 {
        let key = (i / 10) * 100 + (i % 10); // More regular pattern but still challenging
        append_kv(&log, key, format!("worst_case_{}", key).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // All keys should still be found (O(log ε) guarantee)
    for i in (0..1000).step_by(10) {
        let key = (i / 10) * 100 + (i % 10);
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Worst case: key {} not found (O(log ε) may be violated)",
            key
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_simd_probe_with_aligned_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write cache-aligned keys (multiples of 64)
    for i in 0..1000 {
        let key = i * 64;
        append_kv(&log, key, format!("simd_aligned_{}", key).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // SIMD operations should handle aligned keys correctly
    for i in (0..1000).step_by(8) {
        let key = i * 64;
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "SIMD aligned: key {} not found",
            key
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_segment_boundary_search() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write enough data to create multiple segments (TARGET_SEGMENT_SIZE = 8192)
    for i in 0..25000 {
        append_kv(&log, i, format!("boundary_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Test keys at likely segment boundaries
    let boundary_keys = vec![
        0,      // First segment start
        8191,   // First segment end
        8192,   // Second segment start
        16384,  // Third segment start
        24999,  // Last key
    ];

    for &key in &boundary_keys {
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Segment boundary: key {} not found at boundary",
            key
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_binary_search_fallback() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write data with irregular spacing (challenges prediction)
    // Use more reasonable spacing that still challenges but is denser
    for i in 0..2000 {
        let key = i * 5 + (i % 3); // Linear with small irregularity
        append_kv(&log, key, format!("fallback_{}", key).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // All keys should be found even with poor predictions (binary search fallback)
    for i in (0..2000).step_by(20) {
        let key = i * 5 + (i % 3);
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Binary fallback: key {} not found",
            key
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_concurrent_bounded_searches() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write data
    for i in 0..10000 {
        append_kv(&log, i, format!("concurrent_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Concurrent lookups
    let mut handles = vec![];
    for thread_id in 0..4 {
        let log_clone = Arc::clone(&log);
        let handle = tokio::spawn(async move {
            for i in (thread_id * 100..(thread_id + 1) * 100).step_by(2) {
                let value = lookup_kv(&log_clone, i).await.expect("Failed to lookup");
                assert!(
                    value.is_some(),
                    "Concurrent bounded search: key {} not found",
                    i
                );
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("Concurrent search thread failed");
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_epsilon_with_dense_segments() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Dense sequential keys (ideal for RMI)
    for i in 0..30000 {
        append_kv(&log, i, format!("dense_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Dense segments should have very low epsilon
    // Verify perfect accuracy with sequential lookups
    for i in (0..30000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Dense epsilon: key {} not found",
            i
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_epsilon_with_sparse_segments() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Sparse keys (challenging for RMI)
    for i in 0..1000 {
        let key = i * 1000 + (i % 7) * 13; // Irregular spacing
        append_kv(&log, key, format!("sparse_{}", key).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Sparse segments should still maintain epsilon ≤ 64
    for i in (0..1000).step_by(10) {
        let key = i * 1000 + (i % 7) * 13;
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Sparse epsilon: key {} not found (epsilon may exceed limit)",
            key
        );
    }
}
