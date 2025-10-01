//! WAL Append Correctness Tests
//!
//! Tests for Write-Ahead Log append operations and durability

use crate::test::utils::*;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_wal_append() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Append data
    for i in 0..100 {
        append_kv(&log, i, format!("value_{}", i).into_bytes())
            .await
            .expect("Failed to append");
    }

    // Snapshot for persistence
    log.snapshot().await.unwrap();

    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }

    // Verify all data exists
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            format!("value_{}", i)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_append_ordering() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Append updates to same key
    for i in 0..10 {
        append_kv(&log, 42, format!("version_{}", i).into_bytes())
            .await
            .expect("Failed to append");
    }

    log.snapshot().await.unwrap();

    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }

    // Should return latest version
    let value = lookup_kv(&log, 42).await.expect("Failed to lookup");
    assert_eq!(String::from_utf8_lossy(&value.unwrap()), "version_9");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_durability_after_restart() {
    let data_dir = test_data_dir();

    // Write data
    {
        let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

        for i in 0..100 {
            append_kv(&log, i, format!("value_{}", i).into_bytes())
                .await
                .expect("Failed to append");
        }

        // Explicitly drop to ensure flush
        drop(log);
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Restart and verify data persisted
    {
        let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        for i in 0..100 {
            let value = lookup_kv(&log, i).await.expect("Failed to lookup");
            assert!(value.is_some(), "Key {} not found after restart", i);
            assert_eq!(
                String::from_utf8_lossy(&value.unwrap()),
                format!("value_{}", i)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_wal_appends() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    let mut tasks = tokio::task::JoinSet::new();

    // 20 concurrent writers
    for thread_id in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..100 {
                let key = thread_id * 100 + i;
                append_kv(
                    &log_clone,
                    key,
                    format!("value_{}_{}", thread_id, i).into_bytes(),
                )
                .await
                .expect("Failed to append");
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    log.snapshot().await.unwrap();

    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }

    // Verify all data
    for thread_id in 0..20 {
        for i in 0..100 {
            let key = thread_id * 100 + i;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            assert!(value.is_some(), "Key {} not found", key);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_with_large_entries() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Append large entries (64KB each)
    for i in 0..10 {
        append_kv(&log, i, fixtures::value_of_size(fixtures::LARGE_VALUE))
            .await
            .expect("Failed to append large entry");
    }

    log.snapshot().await.unwrap();

    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }

    // Verify large entries
    for i in 0..10 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Large entry {} not found", i);
        assert_eq!(
            value.unwrap().len(),
            fixtures::LARGE_VALUE,
            "Large entry size mismatch"
        );
    }
}
