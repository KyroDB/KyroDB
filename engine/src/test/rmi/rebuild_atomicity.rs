//! RMI Rebuild Atomicity Tests
//!
//! Tests for atomic index swaps, zero-lock reads, and generation tracking

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_zero_lock_reads_during_rebuild() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write initial data
    for i in 0..5000 {
        append_kv(&log, i, format!("initial_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build initial RMI
    log.build_rmi().await.expect("Failed to build initial RMI");

    // Start concurrent readers
    let log_clone = Arc::clone(&log);
    let reader_handle = tokio::spawn(async move {
        for _ in 0..100 {
            for i in (0..5000).step_by(50) {
                let value = lookup_kv(&log_clone, i).await.expect("Failed to lookup");
                assert!(value.is_some(), "Read blocked during rebuild: key {} not found", i);
            }
            sleep(Duration::from_micros(100)).await;
        }
    });

    // Add more data and rebuild multiple times
    for round in 0..5 {
        sleep(Duration::from_millis(10)).await;
        
        for i in 0..100 {
            let key = 5000 + round * 100 + i;
            append_kv(&log, key, format!("round_{}_{}", round, i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        // Rebuild (should not block readers)
        log.build_rmi().await.expect("Failed to rebuild RMI");
    }

    // Wait for readers to complete
    reader_handle.await.expect("Reader thread failed (likely blocked)");
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_atomic_index_swap() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write data
    for i in 0..3000 {
        append_kv(&log, i, format!("swap_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Add more data
    for i in 3000..6000 {
        append_kv(&log, i, format!("swap_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild (atomic swap)
    log.build_rmi().await.expect("Failed to rebuild RMI");

    // Verify all keys accessible after swap
    for i in (0..6000).step_by(50) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Atomic swap: key {} not found after swap", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_consistency_during_rebuild() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write data
    for i in 0..8000 {
        append_kv(&log, i, format!("consistency_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Concurrent readers during rebuild
    let mut handles = vec![];
    for thread_id in 0..4 {
        let log_clone = Arc::clone(&log);
        let handle = tokio::spawn(async move {
            for _ in 0..50 {
                let key = thread_id * 100;
                let value = lookup_kv(&log_clone, key).await.expect("Failed to lookup");
                assert!(
                    value.is_some(),
                    "Consistency: thread {} saw inconsistent state during rebuild",
                    thread_id
                );
                sleep(Duration::from_micros(500)).await;
            }
        });
        handles.push(handle);
    }

    // Rebuild while readers are active
    sleep(Duration::from_millis(5)).await;
    log.build_rmi().await.expect("Failed to rebuild during concurrent reads");

    // Wait for all readers
    for handle in handles {
        handle.await.expect("Reader thread failed");
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_no_stale_reads_after_swap() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Initial data
    for i in 0..2000 {
        append_kv(&log, i, format!("v1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Update values
    for i in 0..2000 {
        append_kv(&log, i, format!("v2_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild with new values
    log.build_rmi().await.expect("Failed to rebuild RMI");

    // Verify no stale reads (all readers should see v2)
    for i in (0..2000).step_by(50) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let value_bytes = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_bytes);
        assert!(
            value_str.starts_with("v2_"),
            "Stale read: key {} returned old value after swap",
            i
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_multiple_rebuild_cycles() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Initial data
    for i in 0..1000 {
        append_kv(&log, i, format!("cycle_0_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Multiple rebuild cycles
    for cycle in 0..10 {
        // Build RMI
        log.build_rmi().await.expect(&format!("Failed to build RMI cycle {}", cycle));

        // Verify existing keys
        for i in (0..1000).step_by(100) {
            let value = lookup_kv(&log, i).await.expect("Failed to lookup");
            assert!(value.is_some(), "Cycle {}: key {} not found", cycle, i);
        }

        // Add more data for next cycle
        for i in 0..200 {
            let key = 1000 + cycle * 200 + i;
            append_kv(&log, key, format!("cycle_{}_{}", cycle + 1, key).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }
    }

    // Final verification
    log.build_rmi().await.expect("Failed to final build");
    
    for i in (0..1000).step_by(50) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Final cycle: key {} not found", i);
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_concurrent_reads_during_rebuild() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write data
    for i in 0..10000 {
        append_kv(&log, i, format!("concurrent_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build initial RMI
    log.build_rmi().await.expect("Failed to build initial RMI");

    // Spawn many concurrent readers
    let mut handles = vec![];
    for thread_id in 0..8 {
        let log_clone = Arc::clone(&log);
        let handle = tokio::spawn(async move {
            for _ in 0..100 {
                let key = (thread_id * 1000) + (thread_id * 13) % 1000;
                let value = lookup_kv(&log_clone, key).await.expect("Failed to lookup");
                assert!(
                    value.is_some(),
                    "Concurrent read: thread {} key {} not found during rebuild",
                    thread_id,
                    key
                );
                sleep(Duration::from_micros(100)).await;
            }
        });
        handles.push(handle);
    }

    // Rebuild while readers are active
    sleep(Duration::from_millis(10)).await;
    log.build_rmi().await.expect("Failed to rebuild during concurrent reads");

    // All readers should complete without blocking
    for (i, handle) in handles.into_iter().enumerate() {
        handle.await.expect(&format!("Reader thread {} failed", i));
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rebuild_after_large_insertions() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Small initial dataset
    for i in 0..500 {
        append_kv(&log, i, format!("small_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Large insertion
    for i in 500..15000 {
        append_kv(&log, i, format!("large_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild after large growth
    log.build_rmi().await.expect("Failed to rebuild after large insertion");

    // Verify all keys accessible
    for i in (0..15000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(
            value.is_some(),
            "Large insertion rebuild: key {} not found",
            i
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rebuild_with_sparse_updates() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Initial dense data
    for i in 0..5000 {
        append_kv(&log, i, format!("dense_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Sparse updates
    for i in 0..100 {
        let key = i * 50;
        append_kv(&log, key, format!("updated_{}", key).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild
    log.build_rmi().await.expect("Failed to rebuild after sparse updates");

    // Verify updated keys
    for i in (0..100).step_by(5) {
        let key = i * 50;
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        let value_bytes = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_bytes);
        assert!(
            value_str.starts_with("updated_"),
            "Sparse update rebuild: key {} not updated",
            key
        );
    }
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_atomicity_guarantees() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write data
    for i in 0..4000 {
        append_kv(&log, i, format!("atomic_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    log.build_rmi().await.expect("Failed to build RMI");

    // Verify atomicity: readers never see partial state
    let log_clone = Arc::clone(&log);
    let reader = tokio::spawn(async move {
        for _ in 0..200 {
            // Either all old keys or all old+new keys, never partial
            let has_3999 = lookup_kv(&log_clone, 3999).await.unwrap().is_some();
            let has_4000 = lookup_kv(&log_clone, 4000).await.unwrap().is_some();
            
            if has_4000 {
                assert!(
                    has_3999,
                    "Atomicity violation: saw new key but not old keys"
                );
            }
            
            sleep(Duration::from_micros(100)).await;
        }
    });

    // Add new keys and rebuild
    sleep(Duration::from_millis(5)).await;
    
    for i in 4000..5000 {
        append_kv(&log, i, format!("atomic_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }
    
    log.build_rmi().await.expect("Failed to rebuild");

    reader.await.expect("Reader detected atomicity violation");
}
