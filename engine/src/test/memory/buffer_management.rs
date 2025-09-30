//! Buffer Management Tests
//!
//! Tests for hot buffer allocation, cleanup, and edge cases

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_hot_buffer_allocation() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write data that should trigger hot buffer allocation
    for i in 0..1000 {
        append_kv(&log, i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
    }

    // Build RMI after writes
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Access same keys multiple times to make them "hot"
    let hot_keys = vec![0, 1, 2, 3, 4];
    for _ in 0..100 {
        for &key in &hot_keys {
            let _ = lookup_kv(&log, key).await;
        }
    }

    // Hot keys should be quickly accessible
    let start = std::time::Instant::now();
    for &key in &hot_keys {
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(value.is_some(), "Hot key {} not found", key);
    }
    let elapsed = start.elapsed();

    // Hot buffer access should be very fast (< 10ms for 5 keys with I/O)
    assert!(
        elapsed.as_millis() < 10,
        "Hot buffer access too slow: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_hot_buffer_cleanup() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Fill hot buffer
    for i in 0..100 {
        append_kv(&log, i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
        // Access immediately to make hot
        let _ = lookup_kv(&log, i).await;
    }

    // Build RMI after initial writes
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Update same keys multiple times
    for _ in 0..10 {
        for i in 0..100 {
            append_kv(&log, i, fixtures::random_value(512, 1024))
                .await
                .expect("Failed to append");
        }
    }

    // Rebuild RMI after updates
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify latest values are accessible
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found after updates", i);
    }
}

#[tokio::test]
async fn test_concurrent_buffer_access() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Initial data
    for i in 0..100 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI after initial data
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    let mut tasks = JoinSet::new();

    // Concurrent buffer updates
    for _ in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let key = rand::random::<u64>() % 100;
                let value = fixtures::random_value(256, 1024);
                let _ = append_kv(&log_clone, key, value).await;
            }
        });
    }

    // Concurrent buffer reads
    for _ in 0..30 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let key = rand::random::<u64>() % 100;
                let _ = lookup_kv(&log_clone, key).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Rebuild RMI after concurrent operations
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify data integrity
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
    }
}

#[tokio::test]
async fn test_buffer_with_large_values() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write large values (64KB each)
    for i in 0..10 {
        append_kv(&log, i, fixtures::value_of_size(fixtures::LARGE_VALUE))
            .await
            .expect("Failed to append large value");
    }

    // Build RMI after large writes
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify large values are retrievable
    for i in 0..10 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Large value key {} not found", i);
        assert_eq!(
            value.unwrap().len(),
            fixtures::LARGE_VALUE,
            "Large value size mismatch"
        );
    }
}

#[tokio::test]
async fn test_buffer_eviction() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write enough data to trigger buffer eviction (simulate memory pressure)
    for i in 0..10000 {
        append_kv(&log, i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
    }

    // Build RMI after large dataset
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Access random keys to test eviction didn't corrupt data
    for _ in 0..1000 {
        let key = rand::random::<u64>() % 10000;
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Key {} not found after buffer eviction",
            key
        );
    }
}

#[tokio::test]
async fn test_buffer_pool_reuse() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write and update same keys repeatedly to test buffer pool reuse
    for cycle in 0..50 {
        for i in 0..100 {
            append_kv(&log, i, format!("cycle_{}_value_{}", cycle, i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
    }

    // Build RMI after all cycles
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify latest values
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found after reuse cycles", i);
        let value_bytes = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_bytes);
        assert!(value_str.starts_with("cycle_49_"), "Expected cycle_49 value, got: {}", value_str);
    }
}

#[tokio::test]
async fn test_mixed_size_buffer_allocation() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write values of different sizes to test buffer pool categorization
    // Small values (< 256 bytes)
    for i in 0..100 {
        append_kv(&log, i, fixtures::value_of_size(100))
            .await
            .expect("Failed to append small value");
    }

    // Medium values (256-1024 bytes)
    for i in 100..200 {
        append_kv(&log, i, fixtures::value_of_size(512))
            .await
            .expect("Failed to append medium value");
    }

    // Large values (1024-4096 bytes)
    for i in 200..300 {
        append_kv(&log, i, fixtures::value_of_size(2048))
            .await
            .expect("Failed to append large value");
    }

    // Build RMI after mixed writes
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all sizes are retrievable
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup small");
        assert!(value.is_some() && value.unwrap().len() == 100, "Small value mismatch");
    }
    for i in 100..200 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup medium");
        assert!(value.is_some() && value.unwrap().len() == 512, "Medium value mismatch");
    }
    for i in 200..300 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup large");
        assert!(value.is_some() && value.unwrap().len() == 2048, "Large value mismatch");
    }
}

#[tokio::test]
async fn test_buffer_under_snapshot() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write initial data
    for i in 0..500 {
        append_kv(&log, i, fixtures::value_of_size(512))
            .await
            .expect("Failed to append");
    }

    // Build RMI before snapshot
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Trigger snapshot
    log.snapshot().await.expect("Failed to create snapshot");

    // Continue writing after snapshot
    for i in 500..1000 {
        append_kv(&log, i, fixtures::value_of_size(512))
            .await
            .expect("Failed to append");
    }

    // Build RMI after more writes
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all data is accessible (before and after snapshot)
    for i in 0..1000 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found after snapshot", i);
    }
}
