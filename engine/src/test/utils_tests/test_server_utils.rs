//! Tests for Test Server Utilities

use crate::test::utils::test_server::*;
use crate::test::utils::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_test_server_start_default() {
    let server = TestServer::start_default().await.unwrap();
    
    assert!(server.data_dir.exists(), "Data directory should exist");
    assert!(server.log.lookup_key(0).await.is_none(), "New database should be empty");
    
    server.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_test_server_basic_operations() {
    let server = TestServer::start_default().await.unwrap();
    
    // Append some data
    append_kv(&server.log, 100, b"test_value".to_vec()).await.unwrap();
    append_kv(&server.log, 200, b"another_value".to_vec()).await.unwrap();
    
    // Snapshot to make data visible
    server.log.snapshot().await.unwrap();
    
    // Build RMI if enabled
    #[cfg(feature = "learned-index")]
    {
        server.log.build_rmi().await.unwrap();
    }
    
    // Lookup values
    let val1 = lookup_kv(&server.log, 100).await.unwrap();
    assert!(val1.is_some(), "Should find first key");
    assert_eq!(val1.unwrap(), b"test_value");
    
    let val2 = lookup_kv(&server.log, 200).await.unwrap();
    assert!(val2.is_some(), "Should find second key");
    assert_eq!(val2.unwrap(), b"another_value");
    
    server.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_test_server_with_group_commit() {
    let server = TestServer::start_with_group_commit(1000).await.unwrap();
    
    // Write data
    for i in 0..10 {
        append_kv(&server.log, i, format!("value_{}", i).into_bytes())
            .await
            .unwrap();
    }
    
    // Group commit should batch these writes
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    // Snapshot and verify
    server.log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        server.log.build_rmi().await.unwrap();
    }
    
    for i in 0..10 {
        let value = lookup_kv(&server.log, i).await.unwrap();
        assert!(value.is_some(), "Should find key {}", i);
    }
    
    server.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_test_server_with_async_durability() {
    let server = TestServer::start_with_async_durability().await.unwrap();
    
    // Write data
    for i in 0..20 {
        append_kv(&server.log, i * 10, format!("async_value_{}", i).into_bytes())
            .await
            .unwrap();
    }
    
    // Wait for background fsync
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Snapshot and verify
    server.log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        server.log.build_rmi().await.unwrap();
    }
    
    for i in 0..20 {
        let value = lookup_kv(&server.log, i * 10).await.unwrap();
        assert!(value.is_some(), "Should find key {}", i * 10);
    }
    
    server.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_test_server_shutdown_gracefully() {
    let server = TestServer::start_default().await.unwrap();
    let data_dir = server.data_dir.clone();
    
    // Write some data
    append_kv(&server.log, 1, b"data".to_vec()).await.unwrap();
    server.log.snapshot().await.unwrap();
    
    // Graceful shutdown
    server.shutdown().await;
    
    // Data directory should still exist after shutdown
    assert!(data_dir.exists(), "Data should persist after shutdown");
    
    // Cleanup
    std::fs::remove_dir_all(data_dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_test_server_config_custom() {
    let config = TestServerConfig {
        data_dir: temp_data_dir("custom_server"),
        enable_rmi: true,
        group_commit_ms: 5,
        snapshot_interval_ops: 5000,
    };
    
    let server = TestServer::start(config.clone()).await.unwrap();
    
    assert_eq!(server.data_dir, config.data_dir);
    assert!(server.data_dir.exists());
    
    server.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_create_test_server_helper() {
    let server = create_test_server().await.unwrap();
    
    assert!(server.data_dir.exists());
    assert!(server.log.lookup_key(0).await.is_none());
    
    server.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_create_test_cluster() {
    let cluster = create_test_cluster(3).await.unwrap();
    
    assert_eq!(cluster.len(), 3, "Should create 3 servers");
    
    // Verify each server is independent
    for (i, server) in cluster.iter().enumerate() {
        append_kv(&server.log, i as u64, format!("server_{}", i).into_bytes())
            .await
            .unwrap();
    }
    
    // Snapshot all
    for server in &cluster {
        server.log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            server.log.build_rmi().await.unwrap();
        }
    }
    
    // Verify data is isolated per server
    for (i, server) in cluster.iter().enumerate() {
        let value = lookup_kv(&server.log, i as u64).await.unwrap();
        assert!(value.is_some(), "Server {} should have its data", i);
        
        // Other servers shouldn't have this key
        for (j, other_server) in cluster.iter().enumerate() {
            if i != j {
                let other_value = lookup_kv(&other_server.log, i as u64).await.unwrap();
                assert!(other_value.is_none(), "Server {} should not have server {}'s data", j, i);
            }
        }
    }
    
    // Cleanup all
    for server in cluster {
        server.cleanup().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_test_server_persistence() {
    let data_dir = temp_data_dir("persistence_test");
    
    // First session: Write data
    {
        let config = TestServerConfig {
            data_dir: data_dir.clone(),
            enable_rmi: true,
            group_commit_ms: 0,
            snapshot_interval_ops: 10000,
        };
        
        let server = TestServer::start(config).await.unwrap();
        
        for i in 0..50 {
            append_kv(&server.log, i, format!("persistent_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        server.log.snapshot().await.unwrap();
        server.shutdown().await;
    }
    
    // Second session: Verify data persisted
    {
        let config = TestServerConfig {
            data_dir: data_dir.clone(),
            enable_rmi: true,
            group_commit_ms: 0,
            snapshot_interval_ops: 10000,
        };
        
        let server = TestServer::start(config).await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            server.log.build_rmi().await.unwrap();
        }
        
        // Verify all data
        for i in 0..50 {
            let value = lookup_kv(&server.log, i).await.unwrap();
            assert!(value.is_some(), "Should find persisted key {}", i);
            assert_eq!(value.unwrap(), format!("persistent_{}", i).into_bytes());
        }
        
        server.cleanup().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_test_server_concurrent_operations() {
    use tokio::task::JoinSet;
    
    let server = TestServer::start_default().await.unwrap();
    let log = server.log.clone();
    
    // Spawn concurrent writers
    let mut handles = JoinSet::new();
    
    for worker_id in 0..10 {
        let log_clone = log.clone();
        handles.spawn(async move {
            for i in 0..10 {
                let key = (worker_id * 100 + i) as u64;
                let value = format!("worker_{}_{}", worker_id, i).into_bytes();
                append_kv(&log_clone, key, value).await.unwrap();
            }
        });
    }
    
    // Wait for all writers
    while handles.join_next().await.is_some() {}
    
    // Snapshot and verify
    log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    // Verify all writes succeeded
    for worker_id in 0..10 {
        for i in 0..10 {
            let key = (worker_id * 100 + i) as u64;
            let value = lookup_kv(&log, key).await.unwrap();
            assert!(value.is_some(), "Should find key from worker {} iteration {}", worker_id, i);
        }
    }
    
    server.cleanup().await;
}

#[test]
fn test_test_server_config_default() {
    let config = TestServerConfig::default();
    
    assert!(config.enable_rmi);
    assert_eq!(config.group_commit_ms, 10);
    assert_eq!(config.snapshot_interval_ops, 10000);
}

#[test]
fn test_test_server_config_builder() {
    let config = TestServerConfig {
        data_dir: std::path::PathBuf::from("/tmp/test"),
        enable_rmi: false,
        group_commit_ms: 50,
        snapshot_interval_ops: 5000,
    };
    
    assert_eq!(config.data_dir, std::path::PathBuf::from("/tmp/test"));
    assert!(!config.enable_rmi);
    assert_eq!(config.group_commit_ms, 50);
    assert_eq!(config.snapshot_interval_ops, 5000);
}
