//! Integration Tests
//!
//! End-to-end integration tests using direct API

use crate::test::utils::*;

#[tokio::test]
async fn test_basic_put_get() {
    let server = test_server::create_test_server()
        .await
        .expect("Failed to create server");

    // PUT operation
    server
        .log
        .append(123, b"test value".to_vec())
        .await
        .expect("Failed to append");

    // GET operation
    let value = server.log.lookup(123).await.expect("Failed to lookup");
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
        server
            .log
            .append(*key, value.clone())
            .await
            .expect("Failed to append");
    }

    // Batch GET
    for (key, expected_value) in data {
        let value = server.log.lookup(key).await.expect("Failed to lookup");
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
    server
        .log
        .append(200, b"initial value".to_vec())
        .await
        .expect("Failed to append");

    // Update same key multiple times
    for i in 0..10 {
        server
            .log
            .append(200, format!("updated_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Should return latest value
    let value = server.log.lookup(200).await.expect("Failed to lookup");
    assert_eq!(
        String::from_utf8_lossy(&value.unwrap()),
        "updated_9"
    );

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
        server
            .log
            .append(i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Trigger RMI rebuild
    server.log.rebuild_rmi().await.expect("Failed to rebuild RMI");

    // Verify all lookups work
    for i in 0..1000 {
        let value = server.log.lookup(i).await.expect("Failed to lookup");
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
        server
            .log
            .append(i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Trigger snapshot
    server.log.create_snapshot().await.expect("Failed to create snapshot");

    // Verify data still accessible
    for i in 0..100 {
        let value = server.log.lookup(i).await.expect("Failed to lookup");
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
            log.append(i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        });
    }

    while tasks.join_next().await.is_some() {}

    // Verify all data
    for i in 0..50 {
        let value = server.log.lookup(i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_nonexistent_key() {
    let server = test_server::create_test_server()
        .await
        .expect("Failed to create server");

    let value = server.log.lookup(99999).await.expect("Failed to lookup");
    assert!(value.is_none(), "Nonexistent key should return None");

    server.shutdown().await;
}

#[tokio::test]
async fn test_persistence_across_restarts() {
    let data_dir = test_data_dir();
    
    // First instance - write data
    {
        let config = test_server::TestServerConfig {
            data_dir: data_dir.path().to_path_buf(),
            enable_rmi: false,
            group_commit_ms: 10,
            snapshot_interval_ops: 10000,
        };
        
        let server = test_server::TestServer::start(config)
            .await
            .expect("Failed to start server");

        for i in 0..100 {
            server
                .log
                .append(i, format!("persisted_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
        }

        server.shutdown().await;
    }

    // Second instance - verify data persisted
    {
        let config = test_server::TestServerConfig {
            data_dir: data_dir.path().to_path_buf(),
            enable_rmi: false,
            group_commit_ms: 10,
            snapshot_interval_ops: 10000,
        };
        
        let server = test_server::TestServer::start(config)
            .await
            .expect("Failed to recover server");

        for i in 0..100 {
            let value = server.log.lookup(i).await.expect("Failed to lookup");
            assert!(
                value.is_some(),
                "Key {} not persisted across restart",
                i
            );
            assert_eq!(
                String::from_utf8_lossy(&value.unwrap()),
                format!("persisted_{}", i)
            );
        }

        server.shutdown().await;
    }
}
