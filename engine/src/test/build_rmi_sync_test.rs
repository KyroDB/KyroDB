/// Tests for build_rmi() synchronization with background worker
///
/// These tests verify that build_rmi() correctly waits for the background worker
/// to process all pending writes before building the RMI index.

#[cfg(test)]
mod build_rmi_sync_tests {
    use crate::PersistentEventLog;
    use anyhow::Result;
    use tempfile::TempDir;

    /// Test that build_rmi() waits for pending writes to be processed
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_build_rmi_waits_for_pending_writes() -> Result<()> {
        use uuid::Uuid;

        let temp_dir = TempDir::new()?;
        let log = PersistentEventLog::open(temp_dir.path()).await?;

        println!("=== TEST: build_rmi() Waits for Pending Writes ===");

        // Write 1000 keys
        for i in 0..1000 {
            let key = i;
            let value = format!("value_{}", i).into_bytes();
            let request_id = Uuid::new_v4();
            log.append_kv(request_id, key, value).await?;
        }

        println!("Wrote 1000 keys to WAL");

        // Build RMI (should wait for background worker to process all writes)
        log.build_rmi().await?;
        println!("Built RMI after waiting for background worker");

        // Verify all keys are visible via RMI lookup
        let mut found_count = 0;
        let mut missing_keys = Vec::new();

        for i in 0..1000 {
            let key = i;
            if log.lookup_key(key).await.is_some() {
                found_count += 1;
            } else {
                missing_keys.push(key);
            }
        }

        println!("Found {}/1000 keys via RMI lookup", found_count);

        if !missing_keys.is_empty() {
            println!(
                "Missing keys: {:?}",
                &missing_keys[..missing_keys.len().min(10)]
            );
        }

        assert_eq!(
            found_count, 1000,
            "Expected all 1000 keys to be visible after build_rmi(), but only found {}",
            found_count
        );

        println!("=== TEST PASSED ===");
        Ok(())
    }

    /// Test that build_rmi() handles large batches correctly
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_build_rmi_with_large_batch() -> Result<()> {
        use uuid::Uuid;

        let temp_dir = TempDir::new()?;
        let log = PersistentEventLog::open(temp_dir.path()).await?;

        println!("=== TEST: build_rmi() with Large Batch (10K keys) ===");

        // Write 10,000 keys in rapid succession
        for i in 0..10_000 {
            let key = i;
            let value = format!("value_{}", i).into_bytes();
            let request_id = Uuid::new_v4();
            log.append_kv(request_id, key, value).await?;
        }

        println!("Wrote 10,000 keys to WAL");

        // Build RMI (should wait for all pending writes)
        let build_start = std::time::Instant::now();
        log.build_rmi().await?;
        let build_duration = build_start.elapsed();

        println!(
            "Built RMI in {:?} after waiting for background worker",
            build_duration
        );

        // Verify all keys are visible
        let mut found_count = 0;

        for i in 0..10_000 {
            let key = i;
            if log.lookup_key(key).await.is_some() {
                found_count += 1;
            }
        }

        println!("Found {}/10000 keys via RMI lookup", found_count);

        assert_eq!(
            found_count, 10_000,
            "Expected all 10,000 keys to be visible after build_rmi(), but only found {}",
            found_count
        );

        println!("=== TEST PASSED ===");
        Ok(())
    }

    /// Test that build_rmi() works correctly with concurrent writes
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_build_rmi_with_concurrent_writes() -> Result<()> {
        use uuid::Uuid;

        let temp_dir = TempDir::new()?;
        let log = PersistentEventLog::open(temp_dir.path()).await?;

        println!("=== TEST: build_rmi() with Concurrent Writes ===");

        // Spawn 10 concurrent writers, each writing 100 keys
        let mut handles = Vec::new();

        for worker_id in 0..10 {
            let log_clone = log.clone();
            let handle = tokio::spawn(async move {
                for i in 0..100 {
                    let key = (worker_id * 100 + i) as u64;
                    let value = format!("worker_{}_value_{}", worker_id, i).into_bytes();
                    let request_id = Uuid::new_v4();
                    log_clone.append_kv(request_id, key, value).await.unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all writers to complete
        for handle in handles {
            handle.await?;
        }

        println!("10 workers wrote 100 keys each (1000 total)");

        // Build RMI (should wait for all pending writes)
        log.build_rmi().await?;
        println!("Built RMI after waiting for background worker");

        // Verify all 1000 keys are visible
        let mut found_count = 0;

        for key in 0..1000 {
            if log.lookup_key(key).await.is_some() {
                found_count += 1;
            }
        }

        println!("Found {}/1000 keys via RMI lookup", found_count);

        assert_eq!(
            found_count, 1000,
            "Expected all 1000 keys to be visible after build_rmi(), but only found {}",
            found_count
        );

        println!("=== TEST PASSED ===");
        Ok(())
    }

    /// Test that build_rmi() timeout doesn't cause failure
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_build_rmi_timeout_is_non_fatal() -> Result<()> {
        use uuid::Uuid;

        let temp_dir = TempDir::new()?;
        let log = PersistentEventLog::open(temp_dir.path()).await?;

        println!("=== TEST: build_rmi() Timeout is Non-Fatal ===");

        // Write a few keys
        for i in 0..100 {
            let key = i;
            let value = format!("value_{}", i).into_bytes();
            let request_id = Uuid::new_v4();
            log.append_kv(request_id, key, value).await?;
        }

        println!("Wrote 100 keys to WAL");

        // Build RMI (even if timeout occurs, should not fail)
        let result = log.build_rmi().await;

        assert!(
            result.is_ok(),
            "build_rmi() should succeed even if timeout occurs, but got error: {:?}",
            result.err()
        );

        println!("=== TEST PASSED ===");
        Ok(())
    }
}
