//! Snapshot Creation Tests
//!
//! Tests for snapshot file creation, atomicity, and correctness

use crate::test::utils::*;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_basic_snapshot_creation() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path())
        .await
        .expect("Failed to create log");

    // Write some data
    for i in 0..100 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync RMI to make writes visible
    sync_index_after_writes(&log);

    // Create snapshot
    log.snapshot().await.expect("Failed to create snapshot");

    // Verify snapshot file exists
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(snapshot_path.exists(), "Snapshot file should exist");

    // Verify snapshot has content
    let metadata = std::fs::metadata(&snapshot_path).expect("Failed to read snapshot metadata");
    assert!(metadata.len() > 0, "Snapshot should have content");
}

#[tokio::test]
async fn test_snapshot_atomicity() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path())
        .await
        .expect("Failed to create log");

    // Write data
    for i in 0..100 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync RMI to make writes visible
    sync_index_after_writes(&log);

    // Create snapshot
    log.snapshot().await.expect("Failed to create snapshot");

    // Verify temp file does not exist (should be renamed atomically)
    let temp_path = data_dir.path().join("snapshot.bin.tmp");
    assert!(
        !temp_path.exists(),
        "Temp snapshot file should not exist after creation"
    );

    // Verify final snapshot exists
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(snapshot_path.exists(), "Final snapshot file should exist");
}

#[tokio::test]
async fn test_snapshot_with_empty_database() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path())
        .await
        .expect("Failed to create log");

    // Create snapshot on empty database
    log.snapshot()
        .await
        .expect("Failed to create snapshot on empty database");

    // Verify snapshot file exists
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(
        snapshot_path.exists(),
        "Snapshot should be created even for empty database"
    );
}

#[tokio::test]
async fn test_multiple_snapshots_overwrite() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path())
        .await
        .expect("Failed to create log");

    // First snapshot with 50 keys
    for i in 0..50 {
        append_kv_ref(&log, i, format!("value_v1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }
    sync_index_after_writes(&log);
    log.snapshot()
        .await
        .expect("Failed to create first snapshot");

    let snapshot_path = data_dir.path().join("snapshot.bin");
    let size1 = std::fs::metadata(&snapshot_path)
        .expect("Failed to read snapshot 1")
        .len();

    // Add more data
    for i in 50..150 {
        append_kv_ref(&log, i, format!("value_v2_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before second snapshot
    sync_index_after_writes(&log);

    // Second snapshot should overwrite
    log.snapshot()
        .await
        .expect("Failed to create second snapshot");

    let size2 = std::fs::metadata(&snapshot_path)
        .expect("Failed to read snapshot 2")
        .len();

    // Second snapshot should be larger (more data)
    assert!(
        size2 > size1,
        "Second snapshot should be larger: {} vs {}",
        size2,
        size1
    );
}

#[tokio::test]
async fn test_snapshot_with_large_dataset() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path())
        .await
        .expect("Failed to create log");

    // Write 10K keys
    for i in 0..10_000 {
        let value = format!("large_value_with_more_content_{}", i).repeat(10);
        append_kv_ref(&log, i, value.as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before snapshot
    sync_index_after_writes(&log);

    // Create snapshot
    log.snapshot()
        .await
        .expect("Failed to create snapshot with large dataset");

    // Verify snapshot exists and has substantial size
    let snapshot_path = data_dir.path().join("snapshot.bin");
    let size = std::fs::metadata(&snapshot_path).unwrap().len();
    assert!(
        size > 1_000_000,
        "Large dataset snapshot should be > 1MB, got {}",
        size
    );
}

#[tokio::test]
async fn test_snapshot_during_writes() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        open_test_log(data_dir.path())
            .await
            .expect("Failed to create log"),
    );

    let mut tasks = JoinSet::new();

    // Continuous writer
    let log_writer = log.clone();
    tasks.spawn(async move {
        for i in 0..1000 {
            let _ = append_kv(&log_writer, i, format!("value_{}", i).as_bytes().to_vec()).await;
            if i % 100 == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        }
        sync_index_after_writes(&log_writer);
    });

    // Snapshot creator
    let log_snapshot = log.clone();
    tasks.spawn(async move {
        sleep(Duration::from_millis(50)).await;
        // Sync before snapshot to capture existing data
        sync_index_after_writes(&log_snapshot);
        log_snapshot
            .snapshot()
            .await
            .expect("Snapshot failed during writes");
    });

    // Wait for both tasks
    while tasks.join_next().await.is_some() {}

    // Verify snapshot was created successfully
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(
        snapshot_path.exists(),
        "Snapshot should exist after concurrent writes"
    );
}

#[tokio::test]
async fn test_snapshot_idempotency() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path())
        .await
        .expect("Failed to create log");

    // Write data
    for i in 0..100 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before snapshots
    sync_index_after_writes(&log);

    // Create multiple snapshots rapidly
    log.snapshot().await.expect("Snapshot 1 failed");
    log.snapshot().await.expect("Snapshot 2 failed");
    log.snapshot().await.expect("Snapshot 3 failed");

    // Verify final snapshot is valid
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(snapshot_path.exists(), "Final snapshot should exist");

    // Should only have one snapshot file (not snapshot.1, snapshot.2, etc.)
    let entries: Vec<_> = std::fs::read_dir(data_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.starts_with("snapshot.bin"))
                .unwrap_or(false)
        })
        .collect();

    assert_eq!(
        entries.len(),
        1,
        "Should only have one snapshot file, found {}",
        entries.len()
    );
}

#[tokio::test]
async fn test_snapshot_preserves_all_data() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path())
        .await
        .expect("Failed to create log");

    let test_data: Vec<(u64, Vec<u8>)> = (0..500)
        .map(|i| (i, format!("test_value_{}", i).as_bytes().to_vec()))
        .collect();

    // Write test data
    for (key, value) in &test_data {
        append_kv_ref(&log, *key, value.clone())
            .await
            .expect("Failed to append");
    }

    // Sync before snapshot
    sync_index_after_writes(&log);

    // Create snapshot
    log.snapshot().await.expect("Failed to create snapshot");

    // Verify all data is still accessible
    for (key, expected_value) in &test_data {
        let actual_value = lookup_kv_ref(&log, *key)
            .await
            .expect("Lookup failed")
            .expect("Key not found");
        assert_eq!(
            *expected_value, actual_value,
            "Data mismatch for key {}",
            key
        );
    }
}

#[tokio::test]
async fn test_snapshot_with_updates() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path())
        .await
        .expect("Failed to create log");

    // Write initial values
    for i in 0..100 {
        append_kv_ref(&log, i, format!("version_1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Update some values
    for i in 0..50 {
        append_kv_ref(&log, i, format!("version_2_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to update");
    }

    // Sync before snapshot
    sync_index_after_writes(&log);

    // Create snapshot
    log.snapshot().await.expect("Failed to create snapshot");

    // Verify latest values are preserved
    for i in 0..50 {
        let value = lookup_kv_ref(&log, i)
            .await
            .expect("Lookup failed")
            .expect("Key not found");
        let expected = format!("version_2_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value),
            expected,
            "Updated value not preserved for key {}",
            i
        );
    }

    for i in 50..100 {
        let value = lookup_kv_ref(&log, i)
            .await
            .expect("Lookup failed")
            .expect("Key not found");
        let expected = format!("version_1_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value),
            expected,
            "Original value not preserved for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_snapshot_fsync_durability() {
    let data_dir = test_data_dir();
    let log = open_test_log(data_dir.path())
        .await
        .expect("Failed to create log");

    // Write data
    for i in 0..100 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before snapshot
    sync_index_after_writes(&log);

    // Create snapshot (should fsync)
    log.snapshot().await.expect("Failed to create snapshot");

    // Snapshot file should be durable on disk
    // (We can't easily test actual durability, but we can verify the file is written and flushed)
    let snapshot_path = data_dir.path().join("snapshot.bin");
    let metadata = std::fs::metadata(&snapshot_path).expect("Snapshot file should exist");
    assert!(
        metadata.len() > 0,
        "Snapshot should have content after fsync"
    );
}

#[tokio::test]
async fn test_concurrent_snapshot_requests() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        open_test_log(data_dir.path())
            .await
            .expect("Failed to create log"),
    );

    // Write data
    for i in 0..1000 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Sync before concurrent snapshots
    sync_index_after_writes(&log);

    // Try to create multiple snapshots concurrently
    let mut tasks = JoinSet::new();
    for _ in 0..5 {
        let log_clone = log.clone();
        tasks.spawn(async move { log_clone.snapshot().await });
    }

    // All should succeed (or at least not corrupt the snapshot)
    let mut success_count = 0;
    while let Some(result) = tasks.join_next().await {
        if result.unwrap().is_ok() {
            success_count += 1;
        }
    }

    assert!(success_count > 0, "At least one snapshot should succeed");

    // Verify final snapshot is valid
    let snapshot_path = data_dir.path().join("snapshot.bin");
    assert!(snapshot_path.exists(), "Final snapshot should exist");

    // Verify data is still accessible
    for i in 0..100 {
        let value = lookup_kv_ref(&log, i)
            .await
            .expect("Lookup failed")
            .expect("Key not found");
        let expected = format!("value_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value),
            expected,
            "Data corrupted after concurrent snapshots"
        );
    }
}
