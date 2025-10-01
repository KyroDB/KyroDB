// Test to verify background worker processes writes correctly

#[cfg(test)]
mod background_worker_tests {
    use crate::adaptive_rmi::AdaptiveRMI;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_background_worker_processes_writes() {
        eprintln!("\n=== TEST: Background Worker Processes Writes ===");

        let rmi = Arc::new(AdaptiveRMI::new());

        // Write 100 keys
        eprintln!("Writing 100 keys...");
        for i in 0..100 {
            rmi.insert(i, i * 10).expect("Insert should succeed");
        }

        eprintln!("Forcing merge...");
        rmi.queue_merge(true);

        // Wait for background worker to process
        sleep(Duration::from_millis(500)).await;

        // Check snapshot
        let snapshot = rmi.get_snapshot();
        eprintln!("Snapshot generation: {}", snapshot.get_generation());
        eprintln!("Snapshot hot_data size: {}", snapshot.get_hot_data().len());

        // Verify writes are in snapshot
        let mut found_count = 0;
        for i in 0..100 {
            let found = snapshot
                .get_hot_data()
                .iter()
                .any(|(k, v)| *k == i && *v == i * 10);
            if found {
                found_count += 1;
            }
        }

        eprintln!("Found {}/100 keys in snapshot", found_count);

        assert!(
            found_count >= 90,
            "Should find at least 90% of keys in snapshot (found {})",
            found_count
        );

        // Test lookup
        eprintln!("Testing lookups...");
        let mut lookup_success = 0;
        for i in 0..100 {
            if let Some(value) = rmi.lookup_key_ultra_fast(i) {
                assert_eq!(value, i * 10, "Value mismatch for key {}", i);
                lookup_success += 1;
            }
        }

        eprintln!("Successful lookups: {}/100", lookup_success);
        assert_eq!(lookup_success, 100, "All lookups should succeed");

        // Cleanup
        rmi.shutdown_zero_lock();
        sleep(Duration::from_millis(100)).await;

        eprintln!("=== TEST PASSED ===\n");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_writes_with_background_worker() {
        eprintln!("\n=== TEST: Concurrent Writes with Background Worker ===");

        let rmi = Arc::new(AdaptiveRMI::new());

        // Spawn 10 writer tasks
        let mut handles = vec![];
        for worker_id in 0..10 {
            let rmi_clone = Arc::clone(&rmi);
            let handle = tokio::spawn(async move {
                for i in 0..100 {
                    let key = worker_id * 100 + i;
                    let value = key * 10;
                    rmi_clone.insert(key, value).ok();
                }
            });
            handles.push(handle);
        }

        // Wait for all writers
        for handle in handles {
            handle.await.unwrap();
        }

        eprintln!("All writers completed, forcing merge...");
        rmi.queue_merge(true);
        sleep(Duration::from_millis(1000)).await;

        // Verify lookups
        eprintln!("Verifying 1000 keys...");
        let mut success_count = 0;
        for key in 0..1000 {
            if let Some(value) = rmi.lookup_key_ultra_fast(key) {
                assert_eq!(value, key * 10);
                success_count += 1;
            }
        }

        eprintln!("Successful lookups: {}/1000", success_count);

        // Should have at least 95% success (some might be in-flight)
        assert!(
            success_count >= 950,
            "Should find at least 95% of keys (found {})",
            success_count
        );

        // Cleanup
        rmi.shutdown_zero_lock();
        sleep(Duration::from_millis(100)).await;

        eprintln!("=== TEST PASSED ===\n");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sync_snapshot_for_test_method() {
        eprintln!("\n=== TEST: sync_snapshot_for_test() Method ===");

        let rmi = Arc::new(AdaptiveRMI::new());

        // Write keys
        eprintln!("Writing 50 keys...");
        for i in 0..50 {
            rmi.insert(i, i * 100).expect("Insert should succeed");
        }

        // Call sync_snapshot_for_test (used by benchmarks)
        eprintln!("Calling sync_snapshot_for_test()...");
        rmi.sync_snapshot_for_test();

        // Immediately verify (no wait needed)
        let snapshot = rmi.get_snapshot();
        eprintln!("Snapshot generation: {}", snapshot.get_generation());
        eprintln!("Snapshot hot_data size: {}", snapshot.get_hot_data().len());

        // All keys should be visible
        let mut found_count = 0;
        for i in 0..50 {
            if snapshot
                .get_hot_data()
                .iter()
                .any(|(k, v)| *k == i && *v == i * 100)
            {
                found_count += 1;
            }
        }

        eprintln!("Found {}/50 keys in snapshot", found_count);
        assert_eq!(found_count, 50, "All keys should be in snapshot after sync");

        // Test lookups
        for i in 0..50 {
            let value = rmi.lookup_key_ultra_fast(i);
            assert_eq!(
                value,
                Some(i * 100),
                "Key {} should return correct value",
                i
            );
        }

        eprintln!("=== TEST PASSED ===\n");
    }
}
