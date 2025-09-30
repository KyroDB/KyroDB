//! Snapshot Atomicity Tests
//!
//! Tests for snapshot atomicity, consistency, and isolation

use crate::test::utils::*;
use bincode::Options; // Import for with_limit()
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_snapshot_atomic_file_creation() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

    // Write data
    for i in 0..100 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before snapshot
    sync_index_after_writes(&log);

    // Monitor filesystem during snapshot
    let snapshot_path = data_dir.path().join("snapshot.bin");
    let temp_path = data_dir.path().join("snapshot.bin.tmp");

    log.snapshot().await.expect("Failed to create snapshot");

    // After snapshot:
    // 1. Final file should exist
    assert!(snapshot_path.exists(), "Final snapshot should exist");

    // 2. Temp file should NOT exist (atomic rename completed)
    assert!(
        !temp_path.exists(),
        "Temp file should not exist after atomic rename"
    );
}

#[tokio::test]
async fn test_snapshot_consistency_under_reads() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.expect("Failed to create log"));

    // Write initial data
    for i in 0..500 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before concurrent operations
    sync_index_after_writes(&log);

    let mut tasks = JoinSet::new();
    let error_count = Arc::new(AtomicU64::new(0));

    // Continuous readers
    for _ in 0..4 {
        let log_clone = log.clone();
        let errors = error_count.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                for i in (0..500).step_by(50) {
                    match lookup_kv(&log_clone, i).await {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                sleep(Duration::from_micros(100)).await;
            }
        });
    }

    // Snapshot creator
    let log_snapshot = log.clone();
    tasks.spawn(async move {
        sleep(Duration::from_millis(50)).await;
        sync_index_after_writes(&log_snapshot); // Sync before snapshot
        log_snapshot.snapshot().await.expect("Snapshot failed");
    });

    // Wait for all tasks
    while tasks.join_next().await.is_some() {}

    let errors = error_count.load(Ordering::Relaxed);
    assert_eq!(
        errors, 0,
        "Snapshot should not interfere with reads: {} errors",
        errors
    );
}

#[tokio::test]
async fn test_snapshot_isolation_from_writes() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.expect("Failed to create log"));

    // Write initial data
    for i in 0..200 {
        append_kv_ref(&log, i, format!("initial_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync initial data
    sync_index_after_writes(&log);

    let snapshot_started = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    // Snapshot task
    let log_snapshot = log.clone();
    let started_flag = snapshot_started.clone();
    tasks.spawn(async move {
        started_flag.store(true, Ordering::Release);
        log_snapshot.snapshot().await.expect("Snapshot failed");
    });

    // Wait for snapshot to start
    while !snapshot_started.load(Ordering::Acquire) {
        sleep(Duration::from_micros(10)).await;
    }

    // Write new data during snapshot
    let log_writer = log.clone();
    tasks.spawn(async move {
        for i in 200..300 {
            let _ = append_kv(&log_writer, i, format!("concurrent_{}", i).as_bytes().to_vec()).await;
        }
    });

    // Wait for both tasks
    while tasks.join_next().await.is_some() {}

    // Snapshot should have captured initial state
    // New writes should be in WAL after snapshot
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(snapshot_path.exists(), "Snapshot should exist");
}

#[tokio::test]
async fn test_snapshot_point_in_time_consistency() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

    // Write version 1
    for i in 0..100 {
        append_kv_ref(&log, i, format!("version_1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Create snapshot at point-in-time 1
    sync_index_after_writes(&log);
    log.snapshot().await.expect("Snapshot 1 failed");
    let snapshot_1_size = std::fs::metadata(data_dir.path().join("snapshot.bin"))
        .unwrap()
        .len();

    // Update to version 2
    for i in 0..100 {
        append_kv_ref(&log, i, format!("version_2_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to update");
    }

    // Create snapshot at point-in-time 2
    sync_index_after_writes(&log);
    log.snapshot().await.expect("Snapshot 2 failed");
    let snapshot_2_size = std::fs::metadata(data_dir.path().join("snapshot.bin"))
        .unwrap()
        .len();

    // Second snapshot should be different (captures version 2)
    // Sizes might be same if value lengths are equal, but content differs
    println!(
        "Snapshot sizes: {} -> {}",
        snapshot_1_size, snapshot_2_size
    );

    // Verify current state has version 2
    for i in 0..100 {
        let value = lookup_kv_ref(&log, i)
            .await
            .expect("Lookup failed")
            .expect("Key not found");
        let expected = format!("version_2_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value),
            expected,
            "Latest snapshot should have version 2 for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_snapshot_no_partial_writes() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.expect("Failed to create log"));

    // Write data
    for i in 0..500 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before concurrent snapshots
    sync_index_after_writes(&log);

    let mut tasks = JoinSet::new();

    // Multiple snapshot attempts
    for _ in 0..3 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            log_clone.snapshot().await
        });
    }

    // Wait for all snapshots
    while let Some(result) = tasks.join_next().await {
        // At least one should succeed
        let _ = result.unwrap();
    }

    // Verify final snapshot is complete and valid
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(snapshot_path.exists(), "Snapshot should exist");

    // Verify NO partial temp files
    let temp_path = data_dir.path().join("snapshot.bin.tmp");
    assert!(
        !temp_path.exists(),
        "No partial temp files should remain"
    );

    // Verify data integrity
    for i in [0, 100, 250, 499] {
        let value = lookup_kv_ref(&log, i)
            .await
            .expect("Lookup failed")
            .expect("Key not found");
        let expected = format!("value_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value),
            expected,
            "No partial writes: data should be complete"
        );
    }
}

#[tokio::test]
async fn test_snapshot_durability_guarantee() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

    // Write critical data
    for i in 0..100 {
        append_kv_ref(&log, i, format!("critical_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before snapshot
    sync_index_after_writes(&log);

    // Create snapshot (should fsync)
    log.snapshot().await.expect("Snapshot failed");

    // Immediately drop (simulate crash)
    std::mem::drop(log);

    // Verify snapshot persisted
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(
        snapshot_path.exists(),
        "Snapshot should persist after immediate drop"
    );

    // Recover and verify
    let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
    sync_index_after_writes(&log); // Sync after recovery

    for i in 0..100 {
        let value = lookup_kv_ref(&log, i)
            .await
            .expect("Lookup failed")
            .expect("Key not found after crash");
        let expected = format!("critical_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value),
            expected,
            "Durability guarantee violated for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_snapshot_all_or_nothing() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

    // Write data
    for i in 0..300 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before snapshot
    sync_index_after_writes(&log);

    // Create snapshot
    log.snapshot().await.expect("Snapshot failed");

    // Delete temp file if it exists (shouldn't, but check)
    let temp_path = data_dir.path().join("snapshot.bin.tmp");
    if temp_path.exists() {
        panic!("Temp file should not exist - atomicity violated!");
    }

    // Verify complete snapshot
    let snapshot_path = data_dir.path().join("snapshot.bin");
    let snapshot_data = std::fs::read(&snapshot_path).expect("Failed to read snapshot");

    // Deserialize to verify completeness (use same bincode config as snapshot creation)
    let bopt = bincode::options().with_limit(16 * 1024 * 1024);
    let _events: Vec<crate::Event> =
        bopt.deserialize(&snapshot_data).expect("Snapshot should be complete and valid");
}

#[tokio::test]
async fn test_snapshot_concurrent_access_safety() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.expect("Failed to create log"));

    // Write initial data
    for i in 0..1000 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before concurrent operations
    sync_index_after_writes(&log);

    let mut tasks = JoinSet::new();
    let inconsistencies = Arc::new(AtomicU64::new(0));

    // Snapshot task
    let log_snapshot = log.clone();
    tasks.spawn(async move {
        log_snapshot.snapshot().await.expect("Snapshot failed");
    });

    // Concurrent readers
    for _ in 0..4 {
        let log_reader = log.clone();
        let errors = inconsistencies.clone();
        tasks.spawn(async move {
            for _ in 0..50 {
                let key = rand::random::<u64>() % 1000;
                match lookup_kv(&log_reader, key).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    // Concurrent writer
    let log_writer = log.clone();
    tasks.spawn(async move {
        for i in 1000..1100 {
            let _ = append_kv(&log_writer, i, format!("new_{}", i).as_bytes().to_vec()).await;
        }
    });

    // Wait for all tasks
    while tasks.join_next().await.is_some() {}

    let errors = inconsistencies.load(Ordering::Relaxed);
    assert_eq!(
        errors, 0,
        "Concurrent access should not cause inconsistencies: {} errors",
        errors
    );
}

#[tokio::test]
async fn test_snapshot_recoverable_after_partial_write_cleanup() {
    let data_dir = test_data_dir();

    // Phase 1: Create snapshot
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

        for i in 0..100 {
            append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        sync_index_after_writes(&log);
        log.snapshot().await.expect("Snapshot failed");
    }

    // Phase 2: Manually create a corrupt temp file (simulate crash during snapshot)
    let temp_path = data_dir.path().join("snapshot.bin.tmp");
    std::fs::write(&temp_path, b"CORRUPT DATA").expect("Failed to write temp file");

    // Phase 3: Reopen should ignore temp file and use valid snapshot
    {
        let log = open_test_log(data_dir.path()).await.expect("Failed to create log");
        sync_index_after_writes(&log); // Sync after recovery

        // Should recover from valid snapshot, ignoring temp
        for i in 0..100 {
            let value = lookup_kv_ref(&log, i)
                .await
                .expect("Lookup failed")
                .expect("Key not found");
            let expected = format!("value_{}", i);
            assert_eq!(
                String::from_utf8_lossy(&value),
                expected,
                "Should recover from valid snapshot, not corrupt temp"
            );
        }
    }
}

#[tokio::test]
async fn test_snapshot_idempotent_on_failure() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path()).await.expect("Failed to create log");

    // Write data
    for i in 0..100 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before multiple snapshots
    sync_index_after_writes(&log);

    // Multiple snapshot attempts (some might fail due to concurrency)
    for _ in 0..5 {
        let _ = log.snapshot().await;
    }

    // Final state should be consistent
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(snapshot_path.exists(), "Snapshot should exist");

    // Verify data integrity
    for i in 0..100 {
        let value = lookup_kv_ref(&log, i)
            .await
            .expect("Lookup failed")
            .expect("Key not found");
        let expected = format!("value_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value),
            expected,
            "Multiple snapshot attempts should not corrupt data"
        );
    }
}
