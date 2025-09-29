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
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Write data that should trigger hot buffer allocation
    for i in 0..1000 {
        log.append(i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
    }

    // Access same keys multiple times to make them "hot"
    let hot_keys = vec![0, 1, 2, 3, 4];
    for _ in 0..100 {
        for &key in &hot_keys {
            let _ = log.lookup(key).await;
        }
    }

    // Hot keys should be quickly accessible
    let start = std::time::Instant::now();
    for &key in &hot_keys {
        let value = log.lookup(key).await.expect("Failed to lookup");
        assert!(value.is_some(), "Hot key {} not found", key);
    }
    let elapsed = start.elapsed();

    // Hot buffer access should be very fast (< 1ms for 5 keys)
    assert!(
        elapsed.as_millis() < 1,
        "Hot buffer access too slow: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_hot_buffer_cleanup() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Fill hot buffer
    for i in 0..100 {
        log.append(i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
        // Access immediately to make hot
        let _ = log.lookup(i).await;
    }

    // Update same keys multiple times
    for _ in 0..10 {
        for i in 0..100 {
            log.append(i, fixtures::random_value(512, 1024))
                .await
                .expect("Failed to append");
        }
    }

    // Verify latest values are accessible
    for i in 0..100 {
        let value = log.lookup(i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found after updates", i);
    }
}

#[tokio::test]
async fn test_concurrent_buffer_access() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Initial data
    for i in 0..100 {
        log.append(i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    let mut tasks = JoinSet::new();

    // Concurrent buffer updates
    for _ in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let key = rand::random::<u64>() % 100;
                let value = fixtures::random_value(256, 1024);
                let _ = log_clone.append(key, value).await;
            }
        });
    }

    // Concurrent buffer reads
    for _ in 0..30 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let key = rand::random::<u64>() % 100;
                let _ = log_clone.lookup(key).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Verify data integrity
    for i in 0..100 {
        let value = log.lookup(i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
    }
}

#[tokio::test]
async fn test_buffer_with_large_values() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Write large values (64KB each)
    for i in 0..10 {
        log.append(i, fixtures::value_of_size(fixtures::LARGE_VALUE))
            .await
            .expect("Failed to append large value");
    }

    // Verify large values are retrievable
    for i in 0..10 {
        let value = log.lookup(i).await.expect("Failed to lookup");
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
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Write enough data to trigger buffer eviction (simulate memory pressure)
    for i in 0..10000 {
        log.append(i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
    }

    // Access random keys to test eviction didn't corrupt data
    for _ in 0..1000 {
        let key = rand::random::<u64>() % 10000;
        let value = log.lookup(key).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Key {} not found after buffer eviction",
            key
        );
    }
}
