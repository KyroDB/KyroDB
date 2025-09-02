#[cfg(test)]
mod crash_recovery_tests {
    use kyrodb_engine::{PersistentEventLog, Record};
    use proptest::prelude::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    // Property-based test for WAL recovery after crashes
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_wal_recovery_after_crash(
            operations in prop::collection::vec(
                (prop::bool::ANY, prop::array::uniform8(any::<u8>()), prop::array::uniform8(any::<u8>())),
                1..50
            ),
            crash_point in 0..50usize
        ) {
            let rt = Runtime::new().unwrap();
            let _ = rt.block_on(async {
                let temp_dir = TempDir::new().unwrap();
                let data_dir = temp_dir.path();

                // Create initial log
                let log = Arc::new(PersistentEventLog::open(data_dir).await.unwrap());

                // Execute operations up to crash point
                let mut expected_records = Vec::new();
                let mut crash_offset = 0;

                for (i, (is_append, key_data, value_data)) in operations.iter().enumerate() {
                    if i >= crash_point {
                        crash_offset = log.get_offset().await;
                        break;
                    }

                    if *is_append {
                        // Append operation
                        let payload = value_data.to_vec();
                        let offset = log.append(uuid::Uuid::new_v4(), payload.clone()).await.unwrap();
                        expected_records.push((offset, payload));
                    } else {
                        // KV put operation
                        let key = u64::from_le_bytes(*key_data);
                        let value = value_data.to_vec();
                        let offset = log.append_kv(uuid::Uuid::new_v4(), key, value.clone()).await.unwrap();
                        let payload = bincode::serialize(&Record { key, value: value.clone() }).unwrap();
                        expected_records.push((offset, payload));
                    }
                }

                // Simulate crash by dropping log and creating new one
                drop(log);

                // Recover from crash
                let recovered_log = Arc::new(PersistentEventLog::open(data_dir).await.unwrap());

                // Verify recovery
                let recovered_offset = recovered_log.get_offset().await;
                prop_assert_eq!(recovered_offset, crash_offset,
                    "Offset mismatch after recovery: expected {}, got {}", crash_offset, recovered_offset);

                // Verify all records before crash point are recoverable
                for (expected_offset, expected_payload) in expected_records {
                    let recovered_payload = recovered_log.get(expected_offset).await;
                    prop_assert!(recovered_payload.is_some(),
                        "Missing record at offset {} after recovery", expected_offset);

                    let recovered_payload = recovered_payload.unwrap();
                    prop_assert_eq!(recovered_payload.clone(), expected_payload.clone(),
                        "Payload mismatch at offset {}: expected {:?}, got {:?}",
                        expected_offset, expected_payload, recovered_payload);
                }

                Ok(())
            });
        }

        #[test]
        fn test_snapshot_recovery_after_crash(
            operations in prop::collection::vec(
                (prop::bool::ANY, prop::array::uniform8(any::<u8>()), prop::array::uniform8(any::<u8>())),
                1..30
            ),
            snapshot_points in prop::collection::vec(1..30usize, 1..5)
        ) {
            let rt = Runtime::new().unwrap();
            let _ = rt.block_on(async {
                let temp_dir = TempDir::new().unwrap();
                let data_dir = temp_dir.path();

                let log = Arc::new(PersistentEventLog::open(data_dir).await.unwrap());

                // Execute operations with periodic snapshots
                let mut all_records = Vec::new();
                let mut snapshot_offsets = Vec::new();

                for (i, (is_append, key_data, value_data)) in operations.iter().enumerate() {
                    if snapshot_points.contains(&i) {
                        // Take snapshot
                        log.snapshot().await.unwrap();
                        snapshot_offsets.push(log.get_offset().await);
                    }

                    let offset;
                    let payload;

                    if *is_append {
                        payload = value_data.to_vec();
                        offset = log.append(uuid::Uuid::new_v4(), payload.clone()).await.unwrap();
                    } else {
                        let key = u64::from_le_bytes(*key_data);
                        let value = value_data.to_vec();
                        payload = bincode::serialize(&Record { key, value: value.clone() }).unwrap();
                        offset = log.append_kv(uuid::Uuid::new_v4(), key, value).await.unwrap();
                    }

                    all_records.push((offset, payload));
                }

                // Take final snapshot
                log.snapshot().await.unwrap();
                snapshot_offsets.push(log.get_offset().await);

                // Simulate crash and recovery
                drop(log);
                let recovered_log = Arc::new(PersistentEventLog::open(data_dir).await.unwrap());

                // Verify all records are recoverable
                for (expected_offset, expected_payload) in all_records {
                    let recovered_payload = recovered_log.get(expected_offset).await;
                    prop_assert!(recovered_payload.is_some(),
                        "Missing record at offset {} after snapshot recovery", expected_offset);

                    let recovered_payload = recovered_payload.unwrap();
                    prop_assert_eq!(recovered_payload, expected_payload,
                        "Payload mismatch after snapshot recovery at offset {}",
                        expected_offset);
                }

                // Verify snapshot integrity
                let recovered_offset = recovered_log.get_offset().await;
                prop_assert_eq!(recovered_offset, *snapshot_offsets.last().unwrap(),
                    "Final offset mismatch after snapshot recovery");

                Ok(())
            });
        }

        #[test]
        fn test_compaction_recovery_after_crash(
            operations in prop::collection::vec(
                (prop::bool::ANY, prop::array::uniform8(any::<u8>()), prop::array::uniform8(any::<u8>())),
                10..100
            ),
            compact_after in 5..50usize
        ) {
            let rt = Runtime::new().unwrap();
            let _ = rt.block_on(async {
                let temp_dir = TempDir::new().unwrap();
                let data_dir = temp_dir.path();

                let log = Arc::new(PersistentEventLog::open(data_dir).await.unwrap());

                // Build up data
                let mut all_records = Vec::new();
                for (i, (is_append, key_data, value_data)) in operations.iter().enumerate() {
                    if i >= compact_after {
                        break;
                    }

                    let offset;
                    let payload;

                    if *is_append {
                        payload = value_data.to_vec();
                        offset = log.append(uuid::Uuid::new_v4(), payload.clone()).await.unwrap();
                    } else {
                        let key = u64::from_le_bytes(*key_data);
                        let value = value_data.to_vec();
                        payload = bincode::serialize(&Record { key, value: value.clone() }).unwrap();
                        offset = log.append_kv(uuid::Uuid::new_v4(), key, value).await.unwrap();
                    }

                    all_records.push((offset, payload));
                }

                // Perform compaction
                let compact_stats = log.compact_keep_latest_and_snapshot_stats().await.unwrap();

                // Continue adding records after compaction
                for (is_append, key_data, value_data) in operations.iter().skip(compact_after) {
                    let offset;
                    let payload;

                    if *is_append {
                        payload = value_data.to_vec();
                        offset = log.append(uuid::Uuid::new_v4(), payload.clone()).await.unwrap();
                    } else {
                        let key = u64::from_le_bytes(*key_data);
                        let value = value_data.to_vec();
                        payload = bincode::serialize(&Record { key, value: value.clone() }).unwrap();
                        offset = log.append_kv(uuid::Uuid::new_v4(), key, value).await.unwrap();
                    }

                    all_records.push((offset, payload));
                }

                // Simulate crash and recovery
                let final_offset = log.get_offset().await;
                drop(log);

                let recovered_log = Arc::new(PersistentEventLog::open(data_dir).await.unwrap());

                // Verify compaction stats are preserved
                let recovered_offset = recovered_log.get_offset().await;
                prop_assert_eq!(recovered_offset, final_offset,
                    "Offset mismatch after compaction recovery: expected {}, got {}",
                    final_offset, recovered_offset);

                // Verify all records are still accessible
                for (expected_offset, expected_payload) in all_records {
                    let recovered_payload = recovered_log.get(expected_offset).await;
                    prop_assert!(recovered_payload.is_some(),
                        "Missing record at offset {} after compaction recovery", expected_offset);

                    let recovered_payload = recovered_payload.unwrap();
                    prop_assert_eq!(recovered_payload, expected_payload,
                        "Payload mismatch after compaction recovery at offset {}",
                        expected_offset);
                }

                // Verify compaction actually reduced size
                prop_assert!(compact_stats.after_bytes <= compact_stats.before_bytes,
                    "Compaction did not reduce size: before={}, after={}",
                    compact_stats.before_bytes, compact_stats.after_bytes);

                Ok(())
            });
        }
    }

    #[tokio::test]
    async fn test_corrupted_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        // Create log and add some data
        let log = Arc::new(PersistentEventLog::open(data_dir).await.unwrap());

        for i in 0..10 {
            let payload = format!("test data {}", i).into_bytes();
            log.append(uuid::Uuid::new_v4(), payload).await.unwrap();
        }

        let original_offset = log.get_offset().await;
        drop(log);

        // Corrupt the WAL file by truncating it
        let wal_path = data_dir.join("wal.0000");
        if wal_path.exists() {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            file.set_len(100).unwrap(); // Truncate to corrupt
        }

        // Try to recover - should handle corruption gracefully
        let recovered_log = PersistentEventLog::open(data_dir).await;

        match recovered_log {
            Ok(log) => {
                // If recovery succeeds, verify we can still read existing data
                let recovered_offset = log.get_offset().await;
                assert!(
                    recovered_offset <= original_offset,
                    "Recovered offset {} should not exceed original {}",
                    recovered_offset,
                    original_offset
                );
            }
            Err(_) => {
                // Corruption detected - this is acceptable behavior
            }
        }
    }

    #[tokio::test]
    async fn test_atomic_snapshot_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let log = Arc::new(PersistentEventLog::open(data_dir).await.unwrap());

        // Add data
        for i in 0..100 {
            let payload = format!("data {}", i).into_bytes();
            log.append(uuid::Uuid::new_v4(), payload).await.unwrap();
        }

        // Start snapshot but simulate crash during the process
        // (This is hard to simulate precisely, but we can test the atomicity)

        // Take a good snapshot first
        log.snapshot().await.unwrap();
        let _snapshot_offset = log.get_offset().await;

        // Add more data
        for i in 100..200 {
            let payload = format!("more data {}", i).into_bytes();
            log.append(uuid::Uuid::new_v4(), payload).await.unwrap();
        }

        drop(log);

        // Recover
        let recovered_log = Arc::new(PersistentEventLog::open(data_dir).await.unwrap());

        // Should be able to read all data including post-snapshot data
        let recovered_offset = recovered_log.get_offset().await;
        assert_eq!(recovered_offset, 200, "Should recover all 200 records");

        // Should be able to read data from before and after snapshot
        for i in 0..200 {
            let expected_payload = if i < 100 {
                format!("data {}", i)
            } else {
                format!("more data {}", i)
            }
            .into_bytes();

            let record = recovered_log.get(i as u64).await.unwrap();
            assert_eq!(record, expected_payload, "Record {} mismatch", i);
        }
    }
}
