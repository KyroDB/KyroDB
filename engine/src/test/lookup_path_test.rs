/// Comprehensive tests for lookup path validation
///
/// These tests verify the complete write→lookup cycle works correctly with:
/// 1. Hot buffer lookups (immediate writes)
/// 2. Overflow buffer lookups (when hot_buffer full)
/// 3. Snapshot lookups (background-merged writes)
/// 4. End-to-end write-read cycles

#[cfg(test)]
mod lookup_path_tests {
    use crate::adaptive_rmi::AdaptiveRMI;
    use std::sync::Arc;

    /// Test 1: Verify hot_buffer provides immediate write visibility
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_hot_buffer_immediate_visibility() {
        let rmi = Arc::new(AdaptiveRMI::new());

        println!("=== TEST: Hot Buffer Immediate Visibility ===");

        // Write 100 keys to hot_buffer
        for i in 0..100 {
            rmi.insert(i, i * 10).expect("Insert should succeed");
        }

        println!("Wrote 100 keys to hot_buffer");

        // Immediately read them back (should be visible in hot_buffer)
        let mut found_count = 0;
        let mut missing_keys = Vec::new();

        for i in 0..100 {
            if let Some(value) = rmi.lookup_key_ultra_fast(i) {
                assert_eq!(value, i * 10, "Key {} should return correct value", i);
                found_count += 1;
            } else {
                missing_keys.push(i);
            }
        }

        println!("Found {}/100 keys immediately after write", found_count);

        if !missing_keys.is_empty() {
            println!(
                "Missing keys: {:?}",
                &missing_keys[..missing_keys.len().min(10)]
            );
        }

        assert_eq!(
            found_count, 100,
            "All keys should be immediately visible in hot_buffer, but only found {}",
            found_count
        );

        println!("=== TEST PASSED ===");
    }

    /// Test 2: Verify background worker processes writes into snapshot
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_background_worker_processes_writes() {
        let rmi = Arc::new(AdaptiveRMI::new());

        println!("=== TEST: Background Worker Processes Writes ===");

        // Queue 100 writes directly (bypassing hot_buffer for this test)
        for i in 0..100 {
            rmi.queue_write(i, i * 10);
        }

        println!("Queued 100 writes to background worker");

        // Force merge to ensure writes are processed
        rmi.queue_merge(true);

        // Wait for background worker to process
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        println!("Waited 200ms for background worker");

        // Verify writes are visible via lookup (don't need to access internal snapshot structure)
        println!("Verifying writes are visible via lookup...");

        // Verify all keys are findable
        let mut found_count = 0;
        for i in 0..100 {
            if let Some(value) = rmi.lookup_key_ultra_fast(i) {
                assert_eq!(value, i * 10, "Key {} should return correct value", i);
                found_count += 1;
            }
        }

        println!("Found {}/100 keys via lookup", found_count);

        assert_eq!(
            found_count, 100,
            "All keys should be visible after merge, but only found {}",
            found_count
        );

        println!("=== TEST PASSED ===");
    }

    /// Test 3: End-to-End Write-Read Cycle with 1000 keys
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_write_read_cycle_1000_keys() {
        let rmi = Arc::new(AdaptiveRMI::new());

        println!("=== TEST: End-to-End Write-Read Cycle (1000 keys) ===");

        // Write 1000 keys
        for i in 0..1000 {
            rmi.insert(i, i * 100).expect("Insert should succeed");
        }

        println!("Wrote 1000 keys");

        // Force sync (like build_rmi does)
        rmi.queue_merge(true);
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        println!("Forced merge and waited 300ms");

        // Read all keys
        let mut found_count = 0;
        let mut mismatched_values = 0;

        for i in 0..1000 {
            match rmi.lookup_key_ultra_fast(i) {
                Some(value) => {
                    if value == i * 100 {
                        found_count += 1;
                    } else {
                        mismatched_values += 1;
                        if mismatched_values <= 5 {
                            println!(
                                "Key {} returned wrong value: expected {}, got {}",
                                i,
                                i * 100,
                                value
                            );
                        }
                    }
                }
                None => {
                    if found_count + mismatched_values < 10 {
                        println!("Key {} not found", i);
                    }
                }
            }
        }

        println!("Found {}/1000 keys with correct values", found_count);
        println!("Mismatched values: {}", mismatched_values);

        assert_eq!(
            found_count, 1000,
            "All keys should return correct values, but only {} matched",
            found_count
        );

        println!("=== TEST PASSED ===");
    }

    /// Test 4: Concurrent writes and reads
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_writes_and_reads() {
        let rmi = Arc::new(AdaptiveRMI::new());

        println!("=== TEST: Concurrent Writes and Reads ===");

        // Spawn 5 writers
        let mut writer_handles = Vec::new();
        for writer_id in 0..5 {
            let rmi_clone = Arc::clone(&rmi);
            let handle = tokio::spawn(async move {
                for i in 0..200 {
                    let key = (writer_id * 200 + i) as u64;
                    rmi_clone
                        .insert(key, key * 2)
                        .expect("Insert should succeed");
                }
            });
            writer_handles.push(handle);
        }

        // Spawn 5 readers (concurrent with writes)
        let mut reader_handles = Vec::new();
        for _ in 0..5 {
            let rmi_clone = Arc::clone(&rmi);
            let handle = tokio::spawn(async move {
                let mut found = 0;
                for _ in 0..100 {
                    for key in 0..1000 {
                        if rmi_clone.lookup_key_ultra_fast(key).is_some() {
                            found += 1;
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                found
            });
            reader_handles.push(handle);
        }

        // Wait for writers
        for handle in writer_handles {
            handle.await.expect("Writer should complete");
        }

        println!("All writers completed (1000 keys written)");

        // Force merge
        rmi.queue_merge(true);
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Verify all 1000 keys are readable
        let mut found_count = 0;
        for key in 0..1000 {
            if let Some(value) = rmi.lookup_key_ultra_fast(key) {
                assert_eq!(value, key * 2, "Key {} should have correct value", key);
                found_count += 1;
            }
        }

        println!("Found {}/1000 keys after merge", found_count);

        // Wait for readers
        let mut total_reads = 0;
        for handle in reader_handles {
            let reads = handle.await.expect("Reader should complete");
            total_reads += reads;
        }

        println!("Readers performed {} total lookups", total_reads);

        assert_eq!(
            found_count, 1000,
            "All keys should be readable after merge, but only found {}",
            found_count
        );

        println!("=== TEST PASSED ===");
    }

    /// Test 5: Verify lookup path handles updates correctly
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_lookup_with_updates() {
        let rmi = Arc::new(AdaptiveRMI::new());

        println!("=== TEST: Lookup with Updates ===");

        // Write initial values
        for i in 0..100 {
            rmi.insert(i, i * 10).expect("Insert should succeed");
        }

        println!("Wrote 100 initial values");

        // Update all values
        for i in 0..100 {
            rmi.insert(i, i * 20).expect("Update should succeed");
        }

        println!("Updated all 100 values");

        // Force merge
        rmi.queue_merge(true);
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Verify we get the LATEST values
        let mut correct_values = 0;
        let mut wrong_values = 0;

        for i in 0..100 {
            match rmi.lookup_key_ultra_fast(i) {
                Some(value) => {
                    if value == i * 20 {
                        correct_values += 1;
                    } else if value == i * 10 {
                        wrong_values += 1;
                        if wrong_values <= 5 {
                            println!(
                                "Key {} returned old value: {} (expected {})",
                                i,
                                value,
                                i * 20
                            );
                        }
                    } else {
                        println!("Key {} returned unexpected value: {}", i, value);
                    }
                }
                None => {
                    println!("Key {} not found", i);
                }
            }
        }

        println!("Correct values: {}/100", correct_values);
        println!("Wrong values (old): {}/100", wrong_values);

        assert_eq!(
            correct_values, 100,
            "All keys should return latest values, but only {} correct",
            correct_values
        );

        println!("=== TEST PASSED ===");
    }

    /// Test 6: High-volume stress test
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_lookup_stress_10k_keys() {
        let rmi = Arc::new(AdaptiveRMI::new());

        println!("=== TEST: Lookup Stress Test (10K keys) ===");

        // Write 10,000 keys
        for i in 0..10_000 {
            rmi.insert(i, i * 7).expect("Insert should succeed");
        }

        println!("Wrote 10,000 keys");

        // Force merge
        rmi.queue_merge(true);
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        println!("Forced merge and waited 500ms");

        // Read all keys
        let mut found_count = 0;

        for i in 0..10_000 {
            if let Some(value) = rmi.lookup_key_ultra_fast(i) {
                assert_eq!(value, i * 7, "Key {} should have correct value", i);
                found_count += 1;
            }
        }

        println!("Found {}/10000 keys", found_count);

        assert_eq!(
            found_count, 10_000,
            "All keys should be findable, but only found {}",
            found_count
        );

        println!("=== TEST PASSED ===");
    }
}
