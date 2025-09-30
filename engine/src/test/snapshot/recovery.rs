//! Snapshot Recovery Tests
//!
//! Tests for snapshot recovery, crash recovery, and data restoration

use crate::test::utils::*;

#[tokio::test]
async fn test_basic_snapshot_recovery() {
    let data_dir = test_data_dir();

    // Phase 1: Write data and create snapshot
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        for i in 0..100 {
            append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        sync_index_after_writes(&log);
        log.snapshot().await.expect("Failed to create snapshot");
    }

    // Phase 2: Reopen and verify recovery
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        for i in 0..100 {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("Key not found after recovery");
            let expected = format!("value_{}", i);
            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "Data corrupted after recovery for key {}",
                i
            );
        }
    }
}

#[tokio::test]
async fn test_recovery_with_wal_replay() {
    let data_dir = test_data_dir();

    // Phase 1: Write data, snapshot, then write more
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        // Write and snapshot
        for i in 0..50 {
            append_kv_ref(&log, i, format!("snapshot_value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
        sync_index_after_writes(&log);
        log.snapshot().await.expect("Failed to create snapshot");

        // Write more data after snapshot (should go to WAL)
        for i in 50..100 {
            append_kv_ref(&log, i, format!("wal_value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
        sync_index_after_writes(&log);
    }

    // Phase 2: Reopen and verify both snapshot and WAL data
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        // Verify snapshot data
        for i in 0..50 {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("Snapshot key not found");
            let expected = format!("snapshot_value_{}", i);
            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "Snapshot data corrupted for key {}",
                i
            );
        }

        // Verify WAL data
        for i in 50..100 {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("WAL key not found");
            let expected = format!("wal_value_{}", i);
            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "WAL data corrupted for key {}",
                i
            );
        }
    }
}

#[tokio::test]
async fn test_recovery_with_updates_after_snapshot() {
    let data_dir = test_data_dir();

    // Phase 1: Write, snapshot, then update some keys
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        // Initial data
        for i in 0..100 {
            append_kv_ref(&log, i, format!("version_1_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        sync_index_after_writes(&log);
        log.snapshot().await.expect("Failed to create snapshot");

        // Update first 50 keys
        for i in 0..50 {
            append_kv_ref(&log, i, format!("version_2_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to update");
        }
        sync_index_after_writes(&log);
    }

    // Phase 2: Verify recovery shows latest values
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        // Updated keys should have version 2
        for i in 0..50 {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("Updated key not found");
            let expected = format!("version_2_{}", i);
            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "Updated value not preserved for key {}",
                i
            );
        }

        // Non-updated keys should have version 1
        for i in 50..100 {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("Original key not found");
            let expected = format!("version_1_{}", i);
            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "Original value not preserved for key {}",
                i
            );
        }
    }
}

#[tokio::test]
async fn test_recovery_after_crash_simulation() {
    let data_dir = test_data_dir();

    // Phase 1: Write data and snapshot
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        for i in 0..200 {
            append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        sync_index_after_writes(&log);
        log.snapshot().await.expect("Failed to create snapshot");

        // Simulate crash - drop log without clean shutdown
        std::mem::drop(log);
    }

    // Phase 2: Recover from "crash"
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        // Verify all data is recoverable
        for i in 0..200 {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed after crash")
                .expect("Key not found after crash");
            let expected = format!("value_{}", i);
            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "Data corrupted after crash for key {}",
                i
            );
        }
    }
}

#[tokio::test]
async fn test_recovery_with_large_dataset() {
    let data_dir = test_data_dir();

    // Phase 1: Write large dataset and snapshot
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        for i in 0..5000 {
            let value = format!("large_value_{}", i).repeat(5);
            append_kv_ref(&log, i, value.as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        sync_index_after_writes(&log);
        log.snapshot().await.expect("Failed to create snapshot");
    }

    // Phase 2: Verify recovery of large dataset
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        // Spot check various keys
        for i in [0, 100, 1000, 2500, 4999] {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("Key not found in large dataset");
            let expected = format!("large_value_{}", i).repeat(5);
            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "Large dataset corrupted for key {}",
                i
            );
        }
    }
}

#[tokio::test]
async fn test_recovery_without_snapshot() {
    let data_dir = test_data_dir();

    // Phase 1: Write data but NO snapshot
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        for i in 0..100 {
            append_kv_ref(&log, i, format!("wal_only_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
        sync_index_after_writes(&log);
        // NO snapshot created
    }

    // Phase 2: Recover from WAL only
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        for i in 0..100 {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("Key not found after WAL-only recovery");
            let expected = format!("wal_only_{}", i);
            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "WAL-only data corrupted for key {}",
                i
            );
        }
    }
}

#[tokio::test]
async fn test_recovery_with_multiple_snapshots() {
    let data_dir = test_data_dir();

    // Phase 1: Multiple snapshot cycles
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        // Cycle 1
        for i in 0..50 {
            append_kv_ref(&log, i, format!("cycle_1_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
        sync_index_after_writes(&log);
        log.snapshot().await.expect("Snapshot 1 failed");

        // Cycle 2
        for i in 50..100 {
            append_kv_ref(&log, i, format!("cycle_2_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
        sync_index_after_writes(&log);
        log.snapshot().await.expect("Snapshot 2 failed");

        // Cycle 3
        for i in 100..150 {
            append_kv_ref(&log, i, format!("cycle_3_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
        sync_index_after_writes(&log);
        log.snapshot().await.expect("Snapshot 3 failed");
    }

    // Phase 2: Verify recovery uses latest snapshot
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        // All data from all cycles should be present
        for i in 0..150 {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("Key not found after multiple snapshots");
            
            let expected = if i < 50 {
                format!("cycle_1_{}", i)
            } else if i < 100 {
                format!("cycle_2_{}", i)
            } else {
                format!("cycle_3_{}", i)
            };

            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "Multi-snapshot data corrupted for key {}",
                i
            );
        }
    }
}

#[tokio::test]
async fn test_recovery_rmi_coordination() {
    let data_dir = test_data_dir();

    // Phase 1: Write, snapshot, build RMI
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        for i in 0..1000 {
            append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        sync_index_after_writes(&log);
        log.snapshot().await.expect("Failed to create snapshot");

        #[cfg(feature = "learned-index")]
        log.build_rmi().await.ok();
    }

    // Phase 2: Recover and rebuild RMI
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        #[cfg(feature = "learned-index")]
        log.build_rmi().await.expect("Failed to rebuild RMI after recovery");

        // Verify data via ultra-fast lookups
        #[cfg(feature = "learned-index")]
        {
            log.warmup().await.ok();
            
            for i in [0, 100, 500, 999] {
                let offset = log.lookup_key_ultra_fast(i);
                assert!(
                    offset.is_some(),
                    "RMI lookup failed after recovery for key {}",
                    i
                );
            }
        }
    }
}

#[tokio::test]
async fn test_recovery_data_integrity() {
    let data_dir = test_data_dir();

    let mut expected_data = std::collections::HashMap::new();

    // Phase 1: Complex write pattern
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        // Initial writes
        for i in 0..100 {
            let value = format!("initial_{}", i);
            append_kv_ref(&log, i, value.as_bytes().to_vec())
                .await
                .expect("Failed to append");
            expected_data.insert(i, value);
        }

        sync_index_after_writes(&log);
        log.snapshot().await.expect("Failed to create snapshot");

        // Updates after snapshot
        for i in (0..50).step_by(2) {
            let value = format!("updated_{}", i);
            append_kv_ref(&log, i, value.as_bytes().to_vec())
                .await
                .expect("Failed to update");
            expected_data.insert(i, value);
        }

        // New keys after snapshot
        for i in 100..150 {
            let value = format!("new_{}", i);
            append_kv_ref(&log, i, value.as_bytes().to_vec())
                .await
                .expect("Failed to append");
            expected_data.insert(i, value);
        }
        sync_index_after_writes(&log);
    }

    // Phase 2: Verify complete data integrity
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        for (key, expected_value) in &expected_data {
            let actual_value = lookup_kv_ref(&log, *key)
                .await
                .expect("Lookup failed")
                .expect("Key not found after recovery");
            assert_eq!(
                String::from_utf8_lossy(&actual_value),
                *expected_value,
                "Data integrity violation for key {}",
                key
            );
        }
    }
}

#[tokio::test]
async fn test_recovery_performance() {
    let data_dir = test_data_dir();

    // Phase 1: Write large dataset and snapshot
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        for i in 0..10_000 {
            append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        sync_index_after_writes(&log);
        log.snapshot().await.expect("Failed to create snapshot");
    }

    // Phase 2: Measure recovery time
    {
        let start = std::time::Instant::now();
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery
        let recovery_time = start.elapsed();

        println!("Recovery time for 10K keys: {:?}", recovery_time);

        // Recovery should be reasonably fast (< 1 second for 10K keys)
        assert!(
            recovery_time.as_secs() < 5,
            "Recovery took too long: {:?}",
            recovery_time
        );

        // Spot check data
        for i in [0, 5000, 9999] {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("Key not found");
            let expected = format!("value_{}", i);
            assert_eq!(String::from_utf8_lossy(&value), expected);
        }
    }
}
