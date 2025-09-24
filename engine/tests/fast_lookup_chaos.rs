//! Chaos tests for fast lookup paths under extreme load

use kyrodb_engine::PersistentEventLog;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[tokio::test]
async fn test_sync_lookup_race_conditions() {
    let dir = tempdir().unwrap();
    let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

    // Populate with test data
    for i in 0..10000u64 {
        let _ = log
            .append_kv(Uuid::new_v4(), i, vec![i as u8; 16])
            .await
            .unwrap();
    }
    log.snapshot().await.unwrap();

    // Spawn many concurrent readers
    let mut handles = Vec::new();
    for _ in 0..20 {
        let log_clone = log.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..1000 {
                let key = rand::random::<u64>() % 10000;
                let _ = log_clone.lookup_key(key).await;
            }
        });
        handles.push(handle);
    }

    // Concurrent writers during reads
    for _ in 0..5 {
        let log_clone = log.clone();
        let handle = tokio::spawn(async move {
            for i in 10000..10100 {
                let _ = log_clone
                    .append_kv(Uuid::new_v4(), i, vec![i as u8; 16])
                    .await;
                sleep(Duration::from_micros(100)).await;
            }
        });
        handles.push(handle);
    }

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify data consistency
    for i in 0..10000u64 {
        assert!(
            log.lookup_key(i).await.is_some(),
            "lookup failed for key {}",
            i
        );
    }
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_rmi_rebuild_during_fast_lookups() {
    let dir = tempdir().unwrap();
    let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

    // Initial data
    for i in 0..5000u64 {
        let _ = log
            .append_kv(Uuid::new_v4(), i, vec![i as u8; 8])
            .await
            .unwrap();
    }
    log.snapshot().await.unwrap();

    // Build initial AdaptiveRMI
    let pairs = log.collect_key_offset_pairs().await;
    let index = kyrodb_engine::index::PrimaryIndex::new_adaptive_rmi_from_pairs(&pairs);
    let _ = log.swap_primary_index(index).await;

    // Continuous fast lookups
    let lookup_log = log.clone();
    let lookup_handle = tokio::spawn(async move {
        for _ in 0..10000 {
            let key = rand::random::<u64>() % 5000;
            let _ = lookup_log.lookup_key(key).await;
        }
    });

    // Concurrent RMI rebuilds
    sleep(Duration::from_millis(10)).await;
    for _ in 0..3 {
        // Add more data
        for i in 5000..5100 {
            let _ = log
                .append_kv(Uuid::new_v4(), i, vec![i as u8; 8])
                .await
                .unwrap();
        }

        // Rebuild AdaptiveRMI
        let pairs = log.collect_key_offset_pairs().await;
        let index = kyrodb_engine::index::PrimaryIndex::new_adaptive_rmi_from_pairs(&pairs);
        let _ = log.swap_primary_index(index).await;

        sleep(Duration::from_millis(5)).await;
    }

    lookup_handle.await.unwrap();

    // Verify no data loss during rebuilds
    for i in 0..5000u64 {
        assert!(
            log.lookup_key(i).await.is_some(),
            "Lost key {} during RMI rebuilds",
            i
        );
    }
}

#[tokio::test]
async fn test_memory_pressure_offset_cache() {
    let dir = tempdir().unwrap();
    let log = PersistentEventLog::open(dir.path()).await.unwrap();

    // Create a large dataset that should stress the offset→record cache
    for i in 0..50000u64 {
        let large_value = vec![i as u8; 1024]; // 1KB per record
        let _ = log.append_kv(Uuid::new_v4(), i, large_value).await.unwrap();

        // Periodically check memory doesn't explode
        if i % 10000 == 0 {
            // Force some lookups
            for j in 0..100 {
                let _ = log.lookup_key(j).await;
            }
        }
    }

    // Verify all data is still accessible despite large cache
    for i in (0..50000).step_by(1000) {
        assert!(
            log.lookup_key(i).await.is_some(),
            "Lost key {} under memory pressure",
            i
        );
    }

    // Compact should clean up and reduce memory usage
    let _ = log.compact_keep_latest_and_snapshot().await; // Allow to fail on size limit

    // Verify data still accessible after compaction
    for i in (0..50000).step_by(1000) {
        assert!(
            log.lookup_key(i).await.is_some(),
            "Lost key {} after compaction",
            i
        );
    }
}
