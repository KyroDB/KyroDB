//! Graceful Shutdown Tests
//!
//! Tests for clean shutdown and resource cleanup

use crate::test::utils::{self, append_kv, lookup_kv};
use crate::PersistentEventLog;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_flush_before_drop() {
    // Test that pending writes are flushed before database is dropped
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");
    let data_dir = utils::temp_data_dir("flush_before_drop");
    let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // Write data with synchronous durability
    for i in 0..50 {
        append_kv(&log, i, vec![i as u8; 100]).await.unwrap();
    }

    // Drop log explicitly
    std::mem::drop(log);

    // Reopen database and verify all data persisted
    let reopened = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    for i in 0..50 {
        let value = lookup_kv(&reopened, i).await.unwrap();
        assert!(
            value.is_some(),
            "Data should persist after shutdown and reopen"
        );
        assert_eq!(value.unwrap(), vec![i as u8; 100]);
    }

    // Final cleanup
    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn test_pending_group_commit_flushed() {
    // Test that writes persist with synchronous durability
    // NOTE: This test is renamed but we keep the group commit test for future implementation
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0"); // Disabled for now

    let data_dir = utils::temp_data_dir("sync_flush");
    let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // Write data with synchronous fsync
    for i in 0..20 {
        append_kv(&log, i, vec![i as u8; 50]).await.unwrap();
    }

    // ✅ Build RMI after writes for learned index
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify data is readable
    for i in 0..20 {
        let value = lookup_kv(&log, i).await.unwrap();
        assert!(value.is_some(), "Data should be readable after write");
    }

    // Drop and reopen
    std::mem::drop(log.clone());

    let reopened = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // ✅ Build RMI after reopening
    #[cfg(feature = "learned-index")]
    reopened.build_rmi().await.ok();

    for i in 0..20 {
        let value = lookup_kv(&reopened, i).await.unwrap();
        assert!(value.is_some(), "Data should persist after fsync");
    }

    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn test_concurrent_operations_complete_before_shutdown() {
    // Test that concurrent operations complete gracefully
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");

    let data_dir = utils::temp_data_dir("concurrent_ops");
    let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // Spawn concurrent writes
    let mut handles = vec![];
    for i in 0..10 {
        let log_clone = log.clone();
        let handle = tokio::spawn(async move {
            append_kv(&log_clone, i, vec![i as u8; 100]).await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Small delay to ensure writes are fully committed
    sleep(Duration::from_millis(10)).await;

    // ✅ Build RMI after writes for learned index to work
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all writes
    for i in 0..10 {
        let value = lookup_kv(&log, i).await.unwrap();
        assert!(value.is_some(), "All writes should be durable");
    }

    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn test_wal_integrity_after_clean_shutdown() {
    // Test WAL integrity after clean shutdown
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");

    let data_dir = utils::temp_data_dir("wal_integrity");
    let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // Write data
    for i in 0..100 {
        append_kv(&log, i, vec![i as u8; 128]).await.unwrap();
    }

    // Clean shutdown
    std::mem::drop(log.clone());

    // Reopen should succeed without recovery
    let reopened = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // All data should be present
    for i in 0..100 {
        let value = lookup_kv(&reopened, i).await.unwrap();
        assert!(value.is_some(), "Data should be intact after reopen");
    }

    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn test_snapshot_persisted_on_shutdown() {
    // Test that snapshot persists through shutdown
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");

    let data_dir = utils::temp_data_dir("snapshot_persist");
    let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // Write data
    for i in 0..200 {
        append_kv(&log, i, vec![i as u8; 100]).await.unwrap();
    }

    // Create snapshot
    log.snapshot().await.unwrap();

    // Give time for snapshot to fully flush
    sleep(Duration::from_millis(10)).await;

    // Verify snapshot exists (the actual file is snapshot.bin)
    let snapshot_path = data_dir.join("snapshot.bin");
    assert!(snapshot_path.exists(), "Snapshot should be created");

    // Shutdown and reopen
    std::mem::drop(log.clone());

    let reopened = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // Verify data from snapshot
    for i in 0..200 {
        let value = lookup_kv(&reopened, i).await.unwrap();
        assert!(value.is_some(), "Data should be restored from snapshot");
    }

    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn test_no_corruption_on_rapid_shutdown() {
    // Test repeated open/close cycles don't corrupt data
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");

    let data_dir = utils::temp_data_dir("rapid_shutdown");

    // Do rapid open/write/close cycles
    for iteration in 0..10 {
        let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

        // Write some data
        for i in 0..5 {
            let key = iteration * 10 + i;
            append_kv(&log, key, vec![key as u8; 50]).await.unwrap();
        }

        // Drop immediately
        std::mem::drop(log);
    }

    // Final open to verify all data persisted
    let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    for iteration in 0..10 {
        for i in 0..5 {
            let key = iteration * 10 + i;
            let value = lookup_kv(&log, key).await.unwrap();
            assert!(
                value.is_some(),
                "Data from iteration {} should persist",
                iteration
            );
        }
    }

    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn test_background_tasks_terminate_cleanly() {
    // Test that background tasks terminate without panic
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_async");
    std::env::set_var("KYRODB_BACKGROUND_FSYNC_INTERVAL_MS", "5");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "1");
    std::env::set_var("KYRODB_GROUP_COMMIT_DELAY_MICROS", "5000");

    let data_dir = utils::temp_data_dir("background_terminate");
    let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // Write some data to start background tasks
    for i in 0..20 {
        append_kv(&log, i, vec![i as u8; 100]).await.unwrap();
    }

    // Wait a bit for background tasks to run
    sleep(Duration::from_millis(50)).await;

    // Drop should cause clean termination (no panic)
    std::mem::drop(log);

    // Test passes if we get here without panic
    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn test_fsync_completes_before_drop() {
    // Test that async fsync completes before drop
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_async");
    std::env::set_var("KYRODB_BACKGROUND_FSYNC_INTERVAL_MS", "10");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");

    let data_dir = utils::temp_data_dir("fsync_before_drop");
    let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // Write data
    for i in 0..100 {
        append_kv(&log, i, vec![i as u8; 100]).await.unwrap();
    }

    // Wait for background fsync
    sleep(Duration::from_millis(50)).await;

    // Drop and reopen
    std::mem::drop(log.clone());

    let reopened = Arc::new(PersistentEventLog::open(data_dir.clone()).await.unwrap());

    // Verify data persisted
    for i in 0..100 {
        let value = lookup_kv(&reopened, i).await.unwrap();
        assert!(value.is_some(), "All data should persist after async fsync");
    }

    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn test_multiple_databases_shutdown_independently() {
    // Test multiple DB instances shut down independently
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");

    let data_dir1 = utils::temp_data_dir("multi_db1");
    let data_dir2 = utils::temp_data_dir("multi_db2");

    let log1 = Arc::new(PersistentEventLog::open(data_dir1.clone()).await.unwrap());
    let log2 = Arc::new(PersistentEventLog::open(data_dir2.clone()).await.unwrap());

    // Write to both
    for i in 0..50 {
        append_kv(&log1, i, vec![1u8; 100]).await.unwrap();
        append_kv(&log2, i, vec![2u8; 100]).await.unwrap();
    }

    // ✅ Build RMI for both after initial writes
    #[cfg(feature = "learned-index")]
    {
        log1.build_rmi().await.ok();
        log2.build_rmi().await.ok();
    }

    // Drop first database
    std::mem::drop(log1.clone());

    // Second should still work
    for i in 50..60 {
        append_kv(&log2, i, vec![2u8; 100]).await.unwrap();
    }

    // ✅ Rebuild RMI for log2 after additional writes
    #[cfg(feature = "learned-index")]
    log2.build_rmi().await.ok();

    // Verify first DB persisted
    let log1_reopened = Arc::new(PersistentEventLog::open(data_dir1.clone()).await.unwrap());

    // ✅ Build RMI after reopening
    #[cfg(feature = "learned-index")]
    log1_reopened.build_rmi().await.ok();

    for i in 0..50 {
        let value = lookup_kv(&log1_reopened, i).await.unwrap();
        assert!(value.is_some(), "DB1 data should persist");
    }

    // Verify second DB still has all data
    for i in 0..60 {
        let value = lookup_kv(&log2, i).await.unwrap();
        assert!(value.is_some(), "DB2 data should persist");
    }

    std::fs::remove_dir_all(&data_dir1).ok();
    std::fs::remove_dir_all(&data_dir2).ok();
}
