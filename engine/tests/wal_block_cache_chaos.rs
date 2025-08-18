//! Chaos tests for WAL block cache under concurrent access

use kyrodb_engine::PersistentEventLog;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[tokio::test]
async fn test_concurrent_wal_cache_access() {
    let dir = tempdir().unwrap();
    let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

    // Configure small WAL segments to force rotation
    log.configure_wal_rotation(Some(1024), 3).await;

    // Spawn multiple writers to stress WAL rotation and caching
    let mut handles = Vec::new();
    for writer_id in 0..5 {
        let log_clone = log.clone();
        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let key = writer_id * 1000 + i;
                let value = format!("writer_{}_item_{}", writer_id, i).into_bytes();
                let _ = log_clone.append_kv(Uuid::new_v4(), key, value).await;
                if i % 10 == 0 {
                    sleep(Duration::from_millis(1)).await; // Allow rotation
                }
            }
        });
        handles.push(handle);
    }

    // Concurrent readers during writes
    for reader_id in 0..3 {
        let log_clone = log.clone();
        let handle = tokio::spawn(async move {
            for i in 0..50 {
                let key = reader_id * 100 + i;
                let _ = log_clone.get(key).await;
                sleep(Duration::from_millis(2)).await;
            }
        });
        handles.push(handle);
    }

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify data integrity
    for writer_id in 0..5 {
        for i in 0..100 {
            let key = writer_id * 1000 + i;
            if let Some(record) = log.get(key).await {
                let expected = format!("writer_{}_item_{}", writer_id, i);
                assert_eq!(record.value, expected.as_bytes());
            }
        }
    }
}

#[tokio::test]
async fn test_cache_eviction_during_compaction() {
    let dir = tempdir().unwrap();
    let log = PersistentEventLog::open(dir.path()).await.unwrap();

    // Fill with data
    for i in 0..1000u64 {
        let _ = log.append_kv(Uuid::new_v4(), i, vec![i as u8; 8]).await.unwrap();
    }

    // Start concurrent readers
    let log_arc = Arc::new(log);
    let reader_log = log_arc.clone();
    let reader_handle = tokio::spawn(async move {
        for _ in 0..100 {
            for i in 0..100 {
                let _ = reader_log.get(i).await;
            }
            sleep(Duration::from_millis(1)).await;
        }
    });

    // Trigger compaction while reading
    sleep(Duration::from_millis(10)).await;
    let _ = log_arc.compact_keep_latest_and_snapshot().await;

    reader_handle.await.unwrap();

    // Verify all data still accessible after compaction
    for i in 0..1000u64 {
        assert!(log_arc.get(i).await.is_some(), "Key {} missing after compaction", i);
    }
}

#[tokio::test]
async fn test_manifest_corruption_recovery() {
    let dir = tempdir().unwrap();
    let log = PersistentEventLog::open(dir.path()).await.unwrap();

    // Add some data
    for i in 0..100u64 {
        let _ = log.append_kv(Uuid::new_v4(), i, vec![i as u8; 8]).await.unwrap();
    }
    log.snapshot().await.unwrap();

    // Corrupt the manifest
    let manifest_path = dir.path().join("manifest.json");
    std::fs::write(&manifest_path, "corrupted json data").unwrap();

    // Should recover gracefully with B-Tree fallback
    let log2 = PersistentEventLog::open(dir.path()).await.unwrap();
    
    // Data should still be accessible
    for i in 0..100u64 {
        assert!(log2.get(i).await.is_some(), "Key {} not recovered after manifest corruption", i);
    }
}
