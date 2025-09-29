//! Deadlock Detection Tests
//!
//! Tests for detecting and preventing deadlocks

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::timeout;

#[tokio::test]
async fn test_no_deadlock_under_heavy_load() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .expect("Failed to create log")
    );

    let mut tasks = JoinSet::new();

    // 50 concurrent writers and readers
    for _ in 0..50 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..100 {
                let _ = append_kv(&log_clone, i, format!("value_{}", i).as_bytes().to_vec()).await;
                let _ = lookup_kv(&log_clone, i).await;
            }
        });
    }

    // All tasks should complete within 10 seconds
    let result = timeout(Duration::from_secs(10), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;

    assert!(result.is_ok(), "Operations deadlocked - did not complete within timeout");
}

#[tokio::test]
async fn test_lock_ordering_consistency() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .expect("Failed to create log")
    );

    // Write data
    for i in 0..100 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI before concurrent operations
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    let mut tasks = JoinSet::new();

    // Multiple operations that acquire locks in different orders
    for _ in 0..30 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..50 {
                // Lookup requires read lock
                let key1 = rand::random::<u64>() % 100;
                let _ = lookup_kv(&log_clone, key1).await;

                // Append requires write lock
                let key2 = rand::random::<u64>() % 100;
                let _ = append_kv(&log_clone, key2, b"test".to_vec()).await;

                // Another lookup
                let _ = lookup_kv(&log_clone, key1).await;
            }
        });
    }

    // Should complete without deadlock
    let result = timeout(Duration::from_secs(5), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;

    assert!(result.is_ok(), "Lock ordering caused deadlock");
}

#[tokio::test]
async fn test_snapshot_during_operations() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .expect("Failed to create log")
    );

    let mut tasks = JoinSet::new();

    // Continuous writes (triggers snapshots)
    let log_clone = log.clone();
    tasks.spawn(async move {
        for i in 0..500 {
            let _ = append_kv(&log_clone, i, format!("value_{}", i).as_bytes().to_vec()).await;
        }
    });

    // Concurrent reads
    for _ in 0..10 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..200 {
                let key = rand::random::<u64>() % 500;
                let _ = lookup_kv(&log_clone, key).await;
            }
        });
    }

    // Should complete without deadlock
    let result = timeout(Duration::from_secs(10), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;

    assert!(result.is_ok(), "Snapshot operations caused deadlock");
}

#[tokio::test]
async fn test_graceful_shutdown_no_deadlock() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .expect("Failed to create log")
    );

    let mut tasks = JoinSet::new();

    // Start background operations
    for _ in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..100 {
                let _ = append_kv(&log_clone, i, format!("value_{}", i).as_bytes().to_vec()).await;
            }
        });
    }

    // Let some operations start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Shutdown should complete within reasonable time
    let result = timeout(Duration::from_secs(5), async {
        drop(log);
        while tasks.join_next().await.is_some() {}
    })
    .await;

    assert!(result.is_ok(), "Graceful shutdown deadlocked");
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_rmi_rebuild_no_deadlock() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .expect("Failed to create log")
    );

    // Initial data
    for i in 0..1000 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI initially
    log.build_rmi().await.ok();

    let mut tasks = JoinSet::new();

    // Trigger RMI rebuilds
    let log_clone = log.clone();
    tasks.spawn(async move {
        for _ in 0..3 {
            let _ = log_clone.build_rmi().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Concurrent operations during rebuild
    for _ in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let key = rand::random::<u64>() % 1000;
                let _ = lookup_kv(&log_clone, key).await;
            }
        });
    }

    // Should complete without deadlock
    let result = timeout(Duration::from_secs(10), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;

    assert!(result.is_ok(), "RMI rebuild caused deadlock");
}
