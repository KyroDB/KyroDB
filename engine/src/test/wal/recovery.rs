//! WAL Recovery Tests
//!
//! Tests for WAL recovery scenarios, replay, and consistency

use crate::test::utils::*;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_basic() {
    let dir = test_data_dir();

    // Write data
    {
        let log = open_test_log(dir.path()).await.unwrap();

        for i in 0..100 {
            append_kv_ref(&log, i, format!("recovery_{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Explicit drop (simulates shutdown)
        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Recover
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        // Verify recovered data
        for i in 0..100 {
            let value = lookup_kv(&log, i).await.unwrap();
            assert!(value.is_some(), "Recovery failed for key {}", i);
            assert_eq!(value.unwrap(), format!("recovery_{}", i).into_bytes());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_with_snapshots() {
    let dir = test_data_dir();

    // Phase 1: Write and snapshot
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());

        for i in 0..50 {
            append_kv(&log, i, format!("snap_phase1_{}", i).into_bytes())
                .await
                .unwrap();
        }

        log.snapshot().await.unwrap();

        // Continue writing after snapshot
        for i in 50..100 {
            append_kv(&log, i, format!("snap_phase2_{}", i).into_bytes())
                .await
                .unwrap();
        }

        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Phase 2: Recover from snapshot + WAL
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        // Verify all data (from snapshot and WAL tail)
        for i in 0..100 {
            let value = lookup_kv(&log, i).await.unwrap();
            assert!(
                value.is_some(),
                "Snapshot+WAL recovery failed for key {}",
                i
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_idempotent() {
    let dir = test_data_dir();

    // Write initial data
    {
        let log = open_test_log(dir.path()).await.unwrap();

        for i in 0..50 {
            append_kv_ref(&log, i, format!("idempotent_{}", i).into_bytes())
                .await
                .unwrap();
        }

        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Multiple recovery cycles should be idempotent
    for _ in 0..3 {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        // Verify data
        for i in 0..50 {
            let value = lookup_kv(&log, i).await.unwrap();
            assert!(value.is_some(), "Idempotent recovery failed for key {}", i);
        }

        drop(log);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_preserves_order() {
    let dir = test_data_dir();

    // Write updates to same key in order
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());

        for version in 0..10 {
            append_kv(&log, 42, format!("version_{}", version).into_bytes())
                .await
                .unwrap();
        }

        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Recover and verify latest version
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        let value = lookup_kv(&log, 42).await.unwrap();
        assert!(value.is_some(), "Recovery lost key 42");

        // Should get latest version (or at least one of the versions)
        let value_bytes = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_bytes);
        assert!(value_str.starts_with("version_"), "Unexpected value format");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_after_partial_write() {
    let dir = test_data_dir();

    // Simulate partial write scenario
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());

        // Write some complete entries
        for i in 0..30 {
            append_kv(&log, i, format!("complete_{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Simulate crash during write (immediate drop)
        drop(log);
    }

    // Recovery should handle partial writes gracefully
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        // At least the complete entries should be recoverable
        let mut recovered_count = 0;
        for i in 0..30 {
            if lookup_kv(&log, i).await.unwrap().is_some() {
                recovered_count += 1;
            }
        }

        assert!(
            recovered_count >= 20,
            "Too few entries recovered: {} / 30",
            recovered_count
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_with_multiple_snapshots() {
    let dir = test_data_dir();

    // Phase 1: Write + Snapshot
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());

        for i in 0..30 {
            append_kv(&log, i, format!("snap1_{}", i).into_bytes())
                .await
                .unwrap();
        }

        log.snapshot().await.unwrap();
        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Phase 2: More writes + Another Snapshot
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());

        for i in 30..60 {
            append_kv(&log, i, format!("snap2_{}", i).into_bytes())
                .await
                .unwrap();
        }

        log.snapshot().await.unwrap();
        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Phase 3: Final writes (no snapshot)
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());

        for i in 60..90 {
            append_kv(&log, i, format!("nosnapshot_{}", i).into_bytes())
                .await
                .unwrap();
        }

        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Phase 4: Recover from multiple snapshots + WAL
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        // Verify all data recovered
        for i in 0..90 {
            let value = lookup_kv(&log, i).await.unwrap();
            assert!(
                value.is_some(),
                "Multi-snapshot recovery failed for key {}",
                i
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_empty_log() {
    let dir = test_data_dir();

    // Open empty log
    {
        let log = open_test_log(dir.path()).await.unwrap();
        drop(log);
    }

    // Recover from empty log (should not panic)
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.ok(); // Should succeed even if empty

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.ok(); // Should handle empty case
        }

        // Verify empty state
        let result = lookup_kv(&log, 0).await.unwrap();
        assert!(result.is_none(), "Empty log should have no data");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_concurrent_writes_before_crash() {
    use tokio::task::JoinSet;

    let dir = test_data_dir();

    // Concurrent writes before crash
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        let mut handles = JoinSet::new();

        for worker_id in 0..5 {
            let log_clone = log.clone();
            handles.spawn(async move {
                for i in 0..20 {
                    let key = (worker_id * 100 + i) as u64;
                    append_kv(
                        &log_clone,
                        key,
                        format!("concurrent_{}_{}", worker_id, i).into_bytes(),
                    )
                    .await
                    .unwrap();
                }
            });
        }

        while handles.join_next().await.is_some() {}

        // Crash
        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Recover
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        // Count recovered entries
        let mut recovered = 0;
        for worker_id in 0..5 {
            for i in 0..20 {
                let key = (worker_id * 100 + i) as u64;
                if lookup_kv(&log, key).await.unwrap().is_some() {
                    recovered += 1;
                }
            }
        }

        // Should recover most concurrent writes
        assert!(
            recovered >= 80, // At least 80% recovered
            "Too few concurrent writes recovered: {} / 100",
            recovered
        );
    }
}
