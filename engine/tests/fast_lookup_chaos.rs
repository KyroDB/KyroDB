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
        let _ = log.append_kv(Uuid::new_v4(), i, vec![i as u8; 16]).await.unwrap();
    }
    log.snapshot().await.unwrap();

    // Spawn many concurrent sync readers
    let mut handles = Vec::new();
    for _ in 0..20 {
        let log_clone = log.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..1000 {
                let key = rand::random::<u64>() % 10000;
                // Mix sync and async calls to stress lock contention
                if rand::random::<bool>() {
                    let _ = log_clone.lookup_key_sync(key);
                } else {
                    let _ = log_clone.lookup_key(key).await;
                }
            }
        });
        handles.push(handle);
    }

    // Concurrent writers during reads
    for _ in 0..5 {
        let log_clone = log.clone();
        let handle = tokio::spawn(async move {
            for i in 10000..10100 {
                let _ = log_clone.append_kv(Uuid::new_v4(), i, vec![i as u8; 16]).await;
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
        assert!(log.lookup_key_sync(i).is_some(), "Sync lookup failed for key {}", i);
        assert!(log.get_sync(i).is_some(), "Sync get failed for key {}", i);
    }
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_rmi_rebuild_during_fast_lookups() {
    let dir = tempdir().unwrap();
    let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

    // Initial data
    for i in 0..5000u64 {
        let _ = log.append_kv(Uuid::new_v4(), i, vec![i as u8; 8]).await.unwrap();
    }
    log.snapshot().await.unwrap();

    // Build initial RMI
    let pairs = log.collect_key_offset_pairs().await;
    let tmp = dir.path().join("index-rmi.tmp");
    let dst = dir.path().join("index-rmi.bin");
    kyrodb_engine::index::RmiIndex::write_from_pairs(&tmp, &pairs, 1024).unwrap();
    std::fs::rename(&tmp, &dst).unwrap();
    if let Some(rmi) = kyrodb_engine::index::RmiIndex::load_from_file(&dst) {
        log.swap_primary_index(kyrodb_engine::index::PrimaryIndex::Rmi(rmi)).await;
    }

    // Continuous fast lookups
    let lookup_log = log.clone();
    let lookup_handle = tokio::spawn(async move {
        for _ in 0..10000 {
            let key = rand::random::<u64>() % 5000;
            let _ = lookup_log.lookup_key_sync(key);
            let _ = lookup_log.get_sync(key);
        }
    });

    // Concurrent RMI rebuilds
    sleep(Duration::from_millis(10)).await;
    for _ in 0..3 {
        // Add more data
        for i in 5000..5100 {
            let _ = log.append_kv(Uuid::new_v4(), i, vec![i as u8; 8]).await.unwrap();
        }
        
        // Rebuild RMI
        let pairs = log.collect_key_offset_pairs().await;
        kyrodb_engine::index::RmiIndex::write_from_pairs(&tmp, &pairs, 1024).unwrap();
        std::fs::rename(&tmp, &dst).unwrap();
        if let Some(rmi) = kyrodb_engine::index::RmiIndex::load_from_file(&dst) {
            log.swap_primary_index(kyrodb_engine::index::PrimaryIndex::Rmi(rmi)).await;
        }
        
        sleep(Duration::from_millis(5)).await;
    }

    lookup_handle.await.unwrap();

    // Verify no data loss during rebuilds
    for i in 0..5000u64 {
        assert!(log.get(i).await.is_some(), "Lost key {} during RMI rebuilds", i);
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
            // Force some lookups to populate cache
            for j in 0..100 {
                let _ = log.get(j).await;
            }
        }
    }

    // Verify all data is still accessible despite large cache
    for i in (0..50000).step_by(1000) {
        assert!(log.get(i).await.is_some(), "Lost key {} under memory pressure", i);
    }
    
    // Compact should clean up and reduce memory usage
    let _ = log.compact_keep_latest_and_snapshot().await.unwrap();
    
    // Verify data still accessible after compaction
    for i in (0..50000).step_by(1000) {
        assert!(log.get(i).await.is_some(), "Lost key {} after compaction", i);
    }
}
