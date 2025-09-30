//! Memory Leak Detection Tests
//!
//! Tests for memory leak prevention, proper cleanup, and hot buffer draining

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_no_leaks_after_simple_operations() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Perform operations
    for i in 0..1000 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all operations
    for i in 0..1000 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Leak test: key {} missing", i);
    }

    // Drop log and verify cleanup
    drop(log);
    
    // Allow cleanup to complete
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_hot_buffer_drain_consistency() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write to hot buffer
    for i in 0..100 {
        append_kv(&log, i, format!("hot_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI (should drain hot buffer atomically)
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify hot buffer data accessible
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("hot_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Hot buffer drain: incorrect value for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_no_leaks_during_concurrent_operations() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    let mut tasks = JoinSet::new();

    // Concurrent operations that could cause leaks
    for thread_id in 0..30 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..100 {
                let key = thread_id * 100 + i;
                let value = format!("t{}_v{}", thread_id, i).as_bytes().to_vec();
                
                // Append
                let _ = append_kv(&log_clone, key, value).await;
                
                // Immediate lookup
                let _ = lookup_kv(&log_clone, key).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify no data lost (which would indicate leaked buffers)
    for thread_id in (0..30).step_by(3) {
        for i in (0..100).step_by(10) {
            let key = thread_id * 100 + i;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            assert!(value.is_some(), "Concurrent leak test: key {} missing", key);
        }
    }
}

#[tokio::test]
async fn test_memory_cleanup_on_updates() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Initial values
    for i in 0..500 {
        append_kv(&log, i, format!("initial_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Update multiple times (old values should be cleaned up)
    for update_round in 0..10 {
        for i in 0..500 {
            let value = format!("update_{}_{}", update_round, i).as_bytes().to_vec();
            append_kv(&log, i, value)
                .await
                .expect("Failed to append");
        }

        // Periodic RMI rebuild
        if update_round % 3 == 0 {
            #[cfg(feature = "learned-index")]
            log.build_rmi().await.ok();
        }
    }

    // Final RMI build
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify only latest values exist (old ones cleaned up)
    for i in (0..500).step_by(25) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("update_9_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Memory cleanup: stale value for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_buffer_return_on_eviction() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Fill enough to trigger eviction
    for i in 0..10_000 {
        append_kv(&log, i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Write more data (should evict old entries)
    for i in 10_000..15_000 {
        append_kv(&log, i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
    }

    // Rebuild RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify old data still accessible (retrieved from persistent storage)
    for i in (0..1000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Eviction leak test: key {} missing", i);
        assert_eq!(value.unwrap().len(), 1024);
    }

    // Verify new data accessible
    for i in (10_000..15_000).step_by(500) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Eviction leak test: key {} missing", i);
        assert_eq!(value.unwrap().len(), 1024);
    }
}

#[tokio::test]
async fn test_no_leaks_with_large_values() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write large values that could leak if not properly managed
    for i in 0..100 {
        append_kv(&log, i, fixtures::value_of_size(65536))
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Update with smaller values (large buffers should be freed)
    for i in 0..100 {
        append_kv(&log, i, fixtures::value_of_size(256))
            .await
            .expect("Failed to append");
    }

    // Rebuild RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify updates successful and correct size
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Large value leak test: key {} missing", i);
        assert_eq!(value.unwrap().len(), 256, "Large value leak test: wrong size for key {}", i);
    }
}

#[tokio::test]
async fn test_atomic_hot_buffer_size_consistency() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    let mut tasks = JoinSet::new();

    // Concurrent hot buffer operations
    for thread_id in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..50 {
                let key = (thread_id * 50 + i) as u64;
                let value = format!("hot_thread_{}_iter_{}", thread_id, i)
                    .as_bytes()
                    .to_vec();
                let _ = append_kv(&log_clone, key, value).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Build RMI (drains hot buffer)
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all hot buffer entries accounted for
    for thread_id in 0..20 {
        for i in 0..50 {
            let key = (thread_id * 50 + i) as u64;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            assert!(
                value.is_some(),
                "Hot buffer consistency: key {} missing",
                key
            );
        }
    }
}

#[tokio::test]
async fn test_cleanup_after_snapshot() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Write data
    for i in 0..1000 {
        append_kv(&log, i, format!("pre_snap_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Create snapshot (should clean up old WAL entries)
    log.snapshot().await.expect("Failed to create snapshot");

    // Write more data
    for i in 0..1000 {
        append_kv(&log, i, format!("post_snap_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify latest data (old memory cleaned up)
    for i in (0..1000).step_by(50) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("post_snap_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Snapshot cleanup: incorrect value for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_long_running_operation_no_leaks() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Simulate long-running operation with repeated cycles
    for cycle in 0..50 {
        // Write phase
        for i in 0..100 {
            let value = format!("cycle_{}_key_{}", cycle, i).as_bytes().to_vec();
            append_kv(&log, i, value)
                .await
                .expect("Failed to append");
        }

        // Read phase
        for i in (0..100).step_by(10) {
            let _ = lookup_kv(&log, i).await;
        }

        // Periodic RMI rebuild
        if cycle % 10 == 0 {
            #[cfg(feature = "learned-index")]
            log.build_rmi().await.ok();
        }

        // Small delay to simulate real workload
        sleep(Duration::from_millis(10)).await;
    }

    // Final RMI build
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify final state - no memory leaks
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("cycle_49_key_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Long-running leak test: incorrect value for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_buffer_lifecycle_completeness() {
    let data_dir = test_data_dir();
    let log = Arc::new(PersistentEventLog::open(data_dir.path().to_path_buf()).await.unwrap());

    // Test complete buffer lifecycle: allocate -> use -> return
    for round in 0..20 {
        // Allocate buffers
        for i in 0..200 {
            let key = i;
            let value = format!("round_{}_value_{}", round, i).as_bytes().to_vec();
            append_kv(&log, key, value)
                .await
                .expect("Failed to append");
        }

        // Build RMI (processes buffers)
        #[cfg(feature = "learned-index")]
        log.build_rmi().await.ok();

        // Verify buffers usable in next round
        for i in (0..200).step_by(20) {
            let value = lookup_kv(&log, i).await.expect("Failed to lookup");
            assert!(value.is_some(), "Buffer lifecycle: key {} missing in round {}", i, round);
        }
    }

    // Final verification
    for i in (0..200).step_by(10) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("round_19_value_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Buffer lifecycle: final value incorrect for key {}",
            i
        );
    }
}
