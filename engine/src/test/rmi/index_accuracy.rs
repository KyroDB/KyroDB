//! RMI Index Accuracy Tests
//!
//! Tests for model prediction accuracy, error bounds, and segment boundaries

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_basic_accuracy() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write sequential keys
    for i in 0..1000 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all keys accessible with correct values
    for i in 0..1000 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "RMI accuracy: key {} not found", i);
        let expected = format!("value_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "RMI accuracy: incorrect value for key {}",
            i
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_with_sparse_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write sparse keys (every 100)
    for i in 0..100 {
        let key = i * 100;
        append_kv(&log, key, format!("sparse_{}", key).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all sparse keys accessible
    for i in 0..100 {
        let key = i * 100;
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(value.is_some(), "Sparse RMI: key {} not found", key);
        let expected = format!("sparse_{}", key);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Sparse RMI: incorrect value for key {}",
            key
        );
    }

    // Verify non-existent keys return None
    for i in 0..100 {
        let key = i * 100 + 50; // Between sparse keys
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(
            value.is_none(),
            "Sparse RMI: non-existent key {} found",
            key
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_with_clustered_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write clustered keys - closer together for better RMI handling
    // Cluster 1: 0-999, Cluster 2: 5000-5999, Cluster 3: 10000-10999
    for cluster in 0..3 {
        let base = cluster * 5000;
        for i in 0..1000 {
            let key = base + i;
            append_kv(
                &log,
                key,
                format!("cluster_{}_{}", cluster, i).as_bytes().to_vec(),
            )
            .await
            .expect("Failed to append");
        }
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all clustered keys accessible
    for cluster in 0..3 {
        let base = cluster * 5000;
        for i in (0..1000).step_by(50) {
            let key = base + i;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            assert!(value.is_some(), "Clustered RMI: key {} not found", key);
            let expected = format!("cluster_{}_{}", cluster, i);
            assert_eq!(
                String::from_utf8_lossy(&value.unwrap()),
                expected,
                "Clustered RMI: incorrect value for key {}",
                key
            );
        }
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_prediction_with_skewed_distribution() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Skewed distribution: dense sequential region followed by sparse region
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

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify dense region
    for i in &[0, 500, 1000, 3000, 5000, 7000, 9000, 9800] {
        let value = lookup_kv(&log, *i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Skewed RMI: dense key {} not found", i);
    }

    // Verify sparse region (checking keys that exist with step_by(10))
    for i in &[10000, 10500, 12000, 14000, 16000, 18000, 19900] {
        let value = lookup_kv(&log, *i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Skewed RMI: sparse key {} not found", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_with_updates() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Initial values
    for i in 0..500 {
        append_kv(&log, i, format!("v1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Update values
    for i in 0..500 {
        append_kv(&log, i, format!("v2_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild RMI
    log.build_rmi().await.expect("Failed to rebuild RMI");

    // Verify updated values
    for i in (0..500).step_by(10) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("v2_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "RMI updates: incorrect value for key {}",
            i
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_large_key_range() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write keys across large range
    let keys = vec![
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        u64::MAX / 2,
    ];

    for &key in &keys {
        append_kv(
            &log,
            key,
            format!("large_range_{}", key).as_bytes().to_vec(),
        )
        .await
        .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all keys accessible despite large range
    for &key in &keys {
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(value.is_some(), "Large range RMI: key {} not found", key);
        let expected = format!("large_range_{}", key);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Large range RMI: incorrect value for key {}",
            key
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_boundary_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write boundary keys
    let boundaries = vec![
        0,
        1,
        127,
        128,
        255,
        256,
        65535,
        65536,
        u32::MAX as u64,
        u32::MAX as u64 + 1,
    ];

    for &key in &boundaries {
        append_kv(&log, key, format!("boundary_{}", key).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all boundary keys
    for &key in &boundaries {
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(value.is_some(), "Boundary RMI: key {} not found", key);
        let expected = format!("boundary_{}", key);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Boundary RMI: incorrect value for key {}",
            key
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_accuracy_after_multiple_rebuilds() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write initial data
    for i in 0..300 {
        append_kv(&log, i, format!("r0_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Multiple rebuild cycles
    for round in 0..5 {
        // Build RMI
        log.build_rmi().await.expect("Failed to build RMI");

        // Verify accuracy
        for i in (0..300).step_by(30) {
            let value = lookup_kv(&log, i).await.expect("Failed to lookup");
            assert!(value.is_some(), "Round {} RMI: key {} not found", round, i);
        }

        // Add more data
        for i in 300..400 {
            let key = i + round * 100;
            append_kv(&log, key, format!("r{}_{}", round, key).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
    }

    // Final verification
    log.build_rmi().await.expect("Failed to final build");

    for i in (0..300).step_by(30) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Final RMI: key {} not found", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_with_random_access_pattern() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write sequential keys
    for i in 0..1000 {
        append_kv(&log, i, format!("seq_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Random access pattern
    let mut rng_state = 12345u64; // Simple LCG
    for _ in 0..500 {
        rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        let key = (rng_state % 1000) as u64;

        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(value.is_some(), "Random access RMI: key {} not found", key);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_error_bound_validation() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write data that challenges error bounds
    for i in 0..10000 {
        append_kv(&log, i, format!("bound_test_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify all keys accessible (validates error bounds are sufficient)
    for i in (0..10000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Error bound validation: key {} not found (bounds may be incorrect)",
            i
        );
    }
}
