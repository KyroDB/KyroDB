//! Integration Tests
//!
//! End-to-end integration tests using direct API

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;

#[tokio::test]
async fn test_basic_put_get() {
    let server = test_server::create_test_server()
        .await
        .expect("Failed to create server");

    // PUT operation
    append_kv(&server.log, 123, b"test value".to_vec())
        .await
        .expect("Failed to append");

    // Build RMI before lookup
    #[cfg(feature = "learned-index")]
    server.log.build_rmi().await.ok();

    // GET operation
    let value = lookup_kv(&server.log, 123).await.expect("Failed to lookup");
    assert!(value.is_some());
    assert_eq!(value.unwrap(), b"test value");

    server.shutdown().await;
}

#[tokio::test]
async fn test_batch_operations() {
    let server = test_server::create_test_server()
        .await
        .expect("Failed to create server");

    // Batch PUT
    let data = vec![
        (100u64, b"value 100".to_vec()),
        (101, b"value 101".to_vec()),
        (102, b"value 102".to_vec()),
    ];

    for (key, value) in &data {
        append_kv(&server.log, *key, value.clone())
            .await
            .expect("Failed to append");
    }

    // Build RMI before lookups
    #[cfg(feature = "learned-index")]
    server.log.build_rmi().await.ok();

    // Batch GET
    for (key, expected_value) in data {
        let value = lookup_kv(&server.log, key).await.expect("Failed to lookup");
        assert!(value.is_some());
        assert_eq!(value.unwrap(), expected_value);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_update_operations() {
    let server = test_server::create_test_server()
        .await
        .expect("Failed to create server");

    // Initial write
    append_kv(&server.log, 200, b"initial value".to_vec())
        .await
        .expect("Failed to append");

    // Update same key multiple times
    for i in 0..10 {
        append_kv(
            &server.log,
            200,
            format!("updated_{}", i).as_bytes().to_vec(),
        )
        .await
        .expect("Failed to append");
    }

    // Build RMI before lookup
    #[cfg(feature = "learned-index")]
    server.log.build_rmi().await.ok();

    // Should return latest value
    let value = lookup_kv(&server.log, 200).await.expect("Failed to lookup");
    assert_eq!(String::from_utf8_lossy(&value.unwrap()), "updated_9");

    server.shutdown().await;
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_rmi_integration() {
    let server = test_server::create_test_server()
        .await
        .expect("Failed to create server");

    // Insert data
    for i in 0..1000 {
        append_kv(&server.log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Trigger RMI rebuild
    server.log.build_rmi().await.expect("Failed to rebuild RMI");

    // Verify all lookups work
    for i in 0..1000 {
        let value = lookup_kv(&server.log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_snapshot_integration() {
    let server = test_server::create_test_server()
        .await
        .expect("Failed to create server");

    // Insert data
    for i in 0..100 {
        append_kv(&server.log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Trigger snapshot
    server
        .log
        .snapshot()
        .await
        .expect("Failed to create snapshot");

    // Build RMI before lookups
    #[cfg(feature = "learned-index")]
    server.log.build_rmi().await.ok();

    // Verify data still accessible
    for i in 0..100 {
        let value = lookup_kv(&server.log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_operations() {
    let server = test_server::create_test_server()
        .await
        .expect("Failed to create server");

    let mut tasks = tokio::task::JoinSet::new();

    // 50 concurrent operations
    for i in 0..50 {
        let log = server.log.clone();
        tasks.spawn(async move {
            append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        });
    }

    while tasks.join_next().await.is_some() {}

    // Build RMI before lookups
    #[cfg(feature = "learned-index")]
    server.log.build_rmi().await.ok();

    // Verify all data
    for i in 0..50 {
        let value = lookup_kv(&server.log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_nonexistent_key() {
    let server = test_server::create_test_server()
        .await
        .expect("Failed to create server");

    // Build RMI even though no data (shouldn't crash)
    #[cfg(feature = "learned-index")]
    server.log.build_rmi().await.ok();

    let value = lookup_kv(&server.log, 99999)
        .await
        .expect("Failed to lookup");
    assert!(value.is_none(), "Nonexistent key should return None");

    server.shutdown().await;
}

#[tokio::test]
async fn test_persistence_across_restarts() {
    // Set durability level to ensure writes are flushed
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");

    let data_dir = test_data_dir();
    let path = data_dir.path().to_path_buf();

    // First instance - write data
    {
        let log = Arc::new(PersistentEventLog::open(path.clone()).await.unwrap());

        for i in 0..100 {
            append_kv(&log, i, format!("persisted_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        // Explicitly drop log to flush
        drop(log);
    }

    // Second instance - verify data persisted
    {
        let log = Arc::new(PersistentEventLog::open(path.clone()).await.unwrap());

        // Build RMI after recovery
        #[cfg(feature = "learned-index")]
        log.build_rmi().await.ok();

        for i in 0..100 {
            let value = lookup_kv(&log, i).await.expect("Failed to lookup");
            assert!(value.is_some(), "Key {} not persisted across restart", i);
            assert_eq!(
                String::from_utf8_lossy(&value.unwrap()),
                format!("persisted_{}", i)
            );
        }

        drop(log);
    }
}
