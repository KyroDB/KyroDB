//! WAL Append Correctness Tests
//!
//! Tests for Write-Ahead Log append operations and durability

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;

#[tokio::test]
async fn test_basic_wal_append() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Append data
    for i in 0..100 {
        log.append(i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Verify all data exists
    for i in 0..100 {
        let value = log.lookup(i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            format!("value_{}", i)
        );
    }
}

#[tokio::test]
async fn test_wal_append_ordering() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Append updates to same key
    for i in 0..10 {
        log.append(42, format!("version_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Should return latest version
    let value = log.lookup(42).await.expect("Failed to lookup");
    assert_eq!(
        String::from_utf8_lossy(&value.unwrap()),
        "version_9"
    );
}

#[tokio::test]
async fn test_wal_durability_after_restart() {
    let data_dir = test_data_dir();
    
    // Write data
    {
        let log = PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log");

        for i in 0..100 {
            log.append(i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
        
        // Explicitly drop to ensure flush
        drop(log);
    }

    // Restart and verify data persisted
    {
        let log = PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to recover log");

        for i in 0..100 {
            let value = log.lookup(i).await.expect("Failed to lookup");
            assert!(
                value.is_some(),
                "Key {} not found after restart",
                i
            );
            assert_eq!(
                String::from_utf8_lossy(&value.unwrap()),
                format!("value_{}", i)
            );
        }
    }
}

#[tokio::test]
async fn test_concurrent_wal_appends() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    let mut tasks = tokio::task::JoinSet::new();

    // 20 concurrent writers
    for thread_id in 0..20 {
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

    // Verify all data
    for thread_id in 0..20 {
        for i in 0..100 {
            let key = thread_id * 100 + i;
            let value = log.lookup(key).await.expect("Failed to lookup");
            assert!(value.is_some(), "Key {} not found", key);
        }
    }
}

#[tokio::test]
async fn test_wal_with_large_entries() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Append large entries (64KB each)
    for i in 0..10 {
        log.append(i, fixtures::value_of_size(fixtures::LARGE_VALUE))
            .await
            .expect("Failed to append large entry");
    }

    // Verify large entries
    for i in 0..10 {
        let value = log.lookup(i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Large entry {} not found", i);
        assert_eq!(
            value.unwrap().len(),
            fixtures::LARGE_VALUE,
            "Large entry size mismatch"
        );
    }
}
