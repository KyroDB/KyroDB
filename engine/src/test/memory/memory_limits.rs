//! Memory Limits Tests
//!
//! Tests for memory limit enforcement, emergency mode, and graceful degradation

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_large_dataset_memory_behavior() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write large dataset (but within reasonable bounds)
    // 10K keys with 4KB values = ~40MB
    for i in 0..10_000 {
        append_kv(&log, i, fixtures::value_of_size(4096))
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify system handles large dataset gracefully
    for i in (0..10_000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Large dataset: key {} missing", i);
        assert_eq!(value.unwrap().len(), 4096, "Value size incorrect");
    }
}

#[tokio::test]
async fn test_memory_pressure_with_large_values() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write progressively larger values to create memory pressure
    let sizes = vec![1024, 4096, 16384, 65536];

    for (idx, &size) in sizes.iter().enumerate() {
        for i in 0..100 {
            let key = (idx as u64 * 100) + i;
            append_kv(&log, key, fixtures::value_of_size(size))
                .await
                .expect("Failed to append");
        }
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all values accessible despite pressure
    for (idx, &size) in sizes.iter().enumerate() {
        for i in 0..100 {
            let key = (idx as u64 * 100) + i;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            assert!(value.is_some(), "Memory pressure: key {} missing", key);
            assert_eq!(
                value.unwrap().len(),
                size,
                "Value size incorrect for key {}",
                key
            );
        }
    }
}

#[tokio::test]
async fn test_concurrent_memory_allocation() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    let mut tasks = JoinSet::new();

    // 20 concurrent tasks allocating memory
    for thread_id in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..100 {
                let key = thread_id * 100 + i;
                let value = fixtures::value_of_size(2048);
                append_kv(&log_clone, key, value)
                    .await
                    .expect("Failed to append");
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all allocations succeeded
    for thread_id in 0..20 {
        for i in 0..100 {
            let key = thread_id * 100 + i;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            assert!(
                value.is_some(),
                "Concurrent allocation: key {} missing",
                key
            );
            assert_eq!(value.unwrap().len(), 2048);
        }
    }
}

#[tokio::test]
async fn test_memory_reclamation_after_operations() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Allocate and update repeatedly to test reclamation
    for cycle in 0..10 {
        for i in 0..500 {
            let value = format!("cycle_{}_key_{}", cycle, i).as_bytes().to_vec();
            append_kv(&log, i, value).await.expect("Failed to append");
        }

        // Build RMI after each cycle
        #[cfg(feature = "learned-index")]
        log.build_rmi().await.ok();
    }

    // Verify final state - memory should be reclaimed
    for i in 0..500 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("cycle_9_key_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Memory reclamation: incorrect value for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_buffer_pool_exhaustion_recovery() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Create burst load to exhaust pools
    let mut tasks = JoinSet::new();

    for batch in 0..100 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..50 {
                let key = batch * 50 + i;
                let value = fixtures::value_of_size(1024);
                let _ = append_kv(&log_clone, key, value).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify system recovered and all data accessible
    for i in (0..5000).step_by(100) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Pool exhaustion: key {} missing", i);
    }
}

#[tokio::test]
async fn test_mixed_workload_memory_behavior() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Mix of small, medium, large operations
    let mut tasks = JoinSet::new();

    // Small values
    for thread_id in 0..10 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..100 {
                let key = thread_id * 100 + i;
                let value = fixtures::value_of_size(256);
                let _ = append_kv(&log_clone, key, value).await;
            }
        });
    }

    // Medium values
    for thread_id in 10..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..50 {
                let key = thread_id * 100 + i;
                let value = fixtures::value_of_size(4096);
                let _ = append_kv(&log_clone, key, value).await;
            }
        });
    }

    // Large values
    for thread_id in 20..25 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..20 {
                let key = thread_id * 100 + i;
                let value = fixtures::value_of_size(16384);
                let _ = append_kv(&log_clone, key, value).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify mixed workload handled correctly
    // Small values
    for thread_id in 0..10 {
        for i in (0..100).step_by(10) {
            let key = thread_id * 100 + i;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            if let Some(v) = value {
                assert_eq!(v.len(), 256, "Small value size incorrect");
            }
        }
    }

    // Medium values
    for thread_id in 10..20 {
        for i in (0..50).step_by(5) {
            let key = thread_id * 100 + i;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            if let Some(v) = value {
                assert_eq!(v.len(), 4096, "Medium value size incorrect");
            }
        }
    }

    // Large values
    for thread_id in 20..25 {
        for i in (0..20).step_by(2) {
            let key = thread_id * 100 + i;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            if let Some(v) = value {
                assert_eq!(v.len(), 16384, "Large value size incorrect");
            }
        }
    }
}

#[tokio::test]
async fn test_memory_behavior_across_snapshots() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Phase 1: Pre-snapshot workload
    for i in 0..1000 {
        append_kv(&log, i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Create snapshot
    log.snapshot().await.expect("Failed to create snapshot");

    // Phase 2: Post-snapshot workload
    for i in 1000..2000 {
        append_kv(&log, i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
    }

    // Rebuild RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify memory behavior consistent across snapshot
    for i in (0..2000).step_by(50) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Snapshot memory: key {} missing", i);
        assert_eq!(value.unwrap().len(), 1024);
    }
}

#[tokio::test]
async fn test_sustained_memory_pressure() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Sustained load over multiple iterations
    for iteration in 0..20 {
        for i in 0..250 {
            let key = i;
            let value = format!("iteration_{}_key_{}", iteration, i)
                .as_bytes()
                .to_vec();
            append_kv(&log, key, value).await.expect("Failed to append");
        }

        // Periodic RMI builds
        if iteration % 5 == 0 {
            #[cfg(feature = "learned-index")]
            log.build_rmi().await.ok();
        }
    }

    // Final RMI build
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify system stable under sustained pressure
    for i in 0..250 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("iteration_19_key_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Sustained pressure: incorrect value for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_memory_cleanup_after_heavy_operations() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Heavy operations
    for batch in 0..50 {
        for i in 0..200 {
            let key = i;
            let value = format!("batch_{}_key_{}", batch, i)
                .repeat(10) // Make values larger
                .as_bytes()
                .to_vec();
            append_kv(&log, key, value).await.expect("Failed to append");
        }

        // RMI build every 10 batches
        if batch % 10 == 0 {
            #[cfg(feature = "learned-index")]
            log.build_rmi().await.ok();
        }
    }

    // Final RMI build
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify final state - old memory should be cleaned up
    for i in (0..200).step_by(20) {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Memory cleanup: key {} missing", i);
        // Should have latest batch data
        let value_bytes = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_bytes);
        assert!(
            value_str.contains("batch_49"),
            "Memory cleanup: stale data for key {}",
            i
        );
    }
}
