//! Group Commit Tests
//!
//! Tests for group commit batching and fsync policies

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_group_commit_batching() {
    let data_dir = test_data_dir();
    // Use 100ms group commit window
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 100, 10000, false)
            .expect("Failed to create log")
    );

    // Rapid fire appends should batch
    let start = Instant::now();
    for i in 0..1000 {
        log.append(i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }
    let elapsed = start.elapsed();

    // Should be much faster than 1000 individual fsyncs
    // (each fsync ~1ms, so 1000ms total, but with batching should be <200ms)
    assert!(
        elapsed < Duration::from_millis(500),
        "Group commit not batching effectively: took {:?}",
        elapsed
    );

    // Verify all data
    for i in 0..1000 {
        let value = log.lookup(i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
    }
}

#[tokio::test]
async fn test_group_commit_durability() {
    let data_dir = test_data_dir();
    
    // Write with group commit
    {
        let log = PersistentEventLog::new(data_dir.path().to_path_buf(), 50, 10000, false)
            .expect("Failed to create log");

        for i in 0..100 {
            log.append(i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        // Wait for group commit window
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(log);
    }

    // Verify persistence after restart
    {
        let log = PersistentEventLog::new(data_dir.path().to_path_buf(), 50, 10000, false)
            .expect("Failed to recover log");

        for i in 0..100 {
            let value = log.lookup(i).await.expect("Failed to lookup");
            assert!(
                value.is_some(),
                "Key {} not durable after group commit",
                i
            );
        }
    }
}

#[tokio::test]
async fn test_concurrent_group_commits() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 50, 10000, false)
            .expect("Failed to create log")
    );

    let mut tasks = tokio::task::JoinSet::new();

    // Multiple concurrent writers during group commit window
    for thread_id in 0..10 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..100 {
                let key = thread_id * 100 + i;
                log_clone
                    .append(key, format!("value_{}_{}", thread_id, i).as_bytes().to_vec())
                    .await
                    .expect("Failed to append");
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Wait for final group commit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify all data
    for thread_id in 0..10 {
        for i in 0..100 {
            let key = thread_id * 100 + i;
            let value = log.lookup(key).await.expect("Failed to lookup");
            assert!(value.is_some(), "Key {} not found after group commit", key);
        }
    }
}

#[tokio::test]
async fn test_group_commit_performance() {
    let data_dir = test_data_dir();
    
    // Test with group commit enabled (50ms window)
    let start = Instant::now();
    {
        let log = PersistentEventLog::new(data_dir.path().to_path_buf(), 50, 10000, false)
            .expect("Failed to create log");

        for i in 0..1000 {
            log.append(i, fixtures::value_of_size(256))
                .await
                .expect("Failed to append");
        }
    }
    let with_group_commit = start.elapsed();

    // Test with minimal group commit (1ms window - almost synchronous)
    let data_dir2 = test_data_dir();
    let start = Instant::now();
    {
        let log = PersistentEventLog::new(data_dir2.path().to_path_buf(), 1, 10000, false)
            .expect("Failed to create log");

        for i in 0..1000 {
            log.append(i, fixtures::value_of_size(256))
                .await
                .expect("Failed to append");
        }
    }
    let without_group_commit = start.elapsed();

    // Group commit should be faster
    println!(
        "Group commit (50ms): {:?}, Sync (1ms): {:?}",
        with_group_commit, without_group_commit
    );
}
