//! Cache Coherency Tests
//!
//! Tests for cache consistency, invalidation, and coherency under concurrent operations

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_cache_invalidation_on_update() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write initial value
    append_kv(&log, 42, b"initial".to_vec())
        .await
        .expect("Failed to append");

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Read and verify initial value
    let value1 = lookup_kv(&log, 42).await.expect("Failed to lookup");
    assert_eq!(value1.unwrap(), b"initial");

    // Update value
    append_kv(&log, 42, b"updated".to_vec())
        .await
        .expect("Failed to append");

    // Rebuild RMI after update
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Read and verify cache shows updated value
    let value2 = lookup_kv(&log, 42).await.expect("Failed to lookup");
    assert_eq!(
        value2.unwrap(),
        b"updated",
        "Cache not invalidated on update"
    );
}

#[tokio::test]
async fn test_concurrent_cache_updates() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Initialize keys
    for i in 0..100 {
        append_kv(&log, i, format!("init_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    let mut tasks = JoinSet::new();

    // Concurrent updates
    for thread_id in 0..10 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let key = rand::random::<u64>() % 100;
                let value = format!("thread_{}_update", thread_id).as_bytes().to_vec();
                let _ = append_kv(&log_clone, key, value).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Rebuild RMI after concurrent updates
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all keys are still accessible and cache is coherent
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} lost during concurrent updates", i);
    }
}

#[tokio::test]
async fn test_cache_coherency_across_snapshots() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write data before snapshot
    for i in 0..500 {
        append_kv(
            &log,
            i,
            format!("before_snapshot_{}", i).as_bytes().to_vec(),
        )
        .await
        .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Create snapshot
    log.snapshot().await.expect("Failed to create snapshot");

    // Write data after snapshot
    for i in 500..1000 {
        append_kv(&log, i, format!("after_snapshot_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild RMI after snapshot
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify cache shows all data correctly
    for i in 0..500 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("before_snapshot_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Cache incoherent for pre-snapshot data"
        );
    }

    for i in 500..1000 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("after_snapshot_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Cache incoherent for post-snapshot data"
        );
    }
}

#[tokio::test]
async fn test_hot_buffer_cache_coherency() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write hot keys
    let hot_keys = vec![10, 20, 30, 40, 50];
    for &key in &hot_keys {
        append_kv(
            &log,
            key,
            format!("hot_initial_{}", key).as_bytes().to_vec(),
        )
        .await
        .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Access repeatedly to make hot
    for _ in 0..100 {
        for &key in &hot_keys {
            let _ = lookup_kv(&log, key).await;
        }
    }

    // Update hot keys
    for &key in &hot_keys {
        append_kv(
            &log,
            key,
            format!("hot_updated_{}", key).as_bytes().to_vec(),
        )
        .await
        .expect("Failed to append");
    }

    // Rebuild RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify hot buffer shows updated values
    for &key in &hot_keys {
        let value = lookup_kv(&log, key).await.expect("Failed to lookup");
        let expected = format!("hot_updated_{}", key);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Hot buffer cache incoherent for key {}",
            key
        );
    }
}

#[tokio::test]
async fn test_cache_eviction_coherency() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Fill cache with enough data to trigger eviction
    for i in 0..5000 {
        append_kv(&log, i, fixtures::value_of_size(1024))
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Access old keys that may have been evicted
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Evicted key {} not retrievable", i);
    }

    // Update evicted keys
    for i in 0..100 {
        append_kv(&log, i, format!("updated_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify updates are visible
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        let expected = format!("updated_{}", i);
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            expected,
            "Cache incoherent after eviction for key {}",
            i
        );
    }
}

#[tokio::test]
async fn test_read_your_own_writes() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Write-read-write-read pattern
    for i in 0..100 {
        // Write
        append_kv(&log, i, format!("v1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");

        // Build RMI
        #[cfg(feature = "learned-index")]
        log.build_rmi().await.ok();

        // Read own write
        let v1 = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert_eq!(
            String::from_utf8_lossy(&v1.unwrap()),
            format!("v1_{}", i),
            "Failed to read own write (v1)"
        );

        // Update
        append_kv(&log, i, format!("v2_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");

        // Rebuild RMI
        #[cfg(feature = "learned-index")]
        log.build_rmi().await.ok();

        // Read own update
        let v2 = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert_eq!(
            String::from_utf8_lossy(&v2.unwrap()),
            format!("v2_{}", i),
            "Failed to read own write (v2)"
        );
    }
}

#[tokio::test]
async fn test_cache_consistency_under_load() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Initialize dataset
    for i in 0..1000 {
        append_kv(&log, i, format!("init_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    let mut tasks = JoinSet::new();

    // Heavy concurrent load
    for thread_id in 0..50 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for iter in 0..50 {
                let key = (thread_id * 20 + iter) % 1000;

                // Read
                let _ = lookup_kv(&log_clone, key).await;

                // Write
                let value = format!("t{}_i{}_k{}", thread_id, iter, key)
                    .as_bytes()
                    .to_vec();
                let _ = append_kv(&log_clone, key, value).await;

                // Read again
                let _ = lookup_kv(&log_clone, key).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Rebuild RMI after heavy load
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify cache consistency - all keys should be accessible
    for i in 0..1000 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Cache inconsistent: key {} lost", i);
    }
}

#[tokio::test]
async fn test_cache_after_rmi_rebuild() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Initial data
    for i in 0..500 {
        append_kv(&log, i, format!("v1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Build RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify initial values
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            format!("v1_{}", i)
        );
    }

    // Add more data
    for i in 500..1000 {
        append_kv(&log, i, format!("v1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Rebuild RMI
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify cache coherent after RMI rebuild
    for i in 0..1000 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert_eq!(
            String::from_utf8_lossy(&value.unwrap()),
            format!("v1_{}", i),
            "Cache incoherent after RMI rebuild for key {}",
            i
        );
    }
}
