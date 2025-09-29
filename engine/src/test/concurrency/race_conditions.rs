//! Race Condition Tests
//!
//! Tests for race conditions in concurrent read/write operations

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_concurrent_read_write_safety() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Write initial data
    for i in 0..100 {
        log.append(i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Spawn concurrent readers and writers
    let mut tasks = JoinSet::new();
    
    // 10 writers
    for _ in 0..10 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 100..200 {
                let _ = log_clone.append(i, format!("value_{}", i).as_bytes().to_vec()).await;
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
        });
    }

    // 20 readers
    for _ in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let key = rand::random::<u64>() % 200;
                let _ = log_clone.lookup(key).await;
                tokio::time::sleep(Duration::from_micros(5)).await;
            }
        });
    }

    // Wait for all tasks
    while tasks.join_next().await.is_some() {}

    // Verify no data corruption
    for i in 0..100 {
        let value = log.lookup(i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
    }
}

#[tokio::test]
async fn test_hot_buffer_race_conditions() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Hammer same keys from multiple threads to stress hot buffer
    let hot_keys: Vec<u64> = (0..10).collect();
    let mut tasks = JoinSet::new();

    for _ in 0..50 {
        let log_clone = log.clone();
        let keys = hot_keys.clone();
        tasks.spawn(async move {
            for _ in 0..1000 {
                let key = keys[rand::random::<usize>() % keys.len()];
                let value = format!("value_{}", rand::random::<u64>());
                let _ = log_clone.append(key, value.as_bytes().to_vec()).await;
            }
        });
    }

    // Concurrent readers on hot keys
    for _ in 0..30 {
        let log_clone = log.clone();
        let keys = hot_keys.clone();
        tasks.spawn(async move {
            for _ in 0..1000 {
                let key = keys[rand::random::<usize>() % keys.len()];
                let _ = log_clone.lookup(key).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Verify all hot keys exist
    for key in hot_keys {
        assert!(log.lookup(key).await.expect("Failed to lookup").is_some());
    }
}

#[tokio::test]
async fn test_cache_invalidation_race() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, false)
            .expect("Failed to create log")
    );

    // Write initial data
    for i in 0..100 {
        log.append(i, format!("initial_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Concurrent updates and reads to test cache invalidation
    let mut tasks = JoinSet::new();

    // Writers that update same keys
    for _ in 0..10 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let key = rand::random::<u64>() % 100;
                let value = format!("updated_{}", rand::random::<u64>());
                let _ = log_clone.append(key, value.as_bytes().to_vec()).await;
            }
        });
    }

    // Readers that should never see stale data
    for _ in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let key = rand::random::<u64>() % 100;
                if let Ok(Some(value)) = log_clone.lookup(key).await {
                    // Value should be either initial or updated, never corrupted
                    let s = String::from_utf8_lossy(&value);
                    assert!(
                        s.starts_with("initial_") || s.starts_with("updated_"),
                        "Corrupted value: {}",
                        s
                    );
                }
            }
        });
    }

    while tasks.join_next().await.is_some() {}
}

#[tokio::test]
async fn test_snapshot_rebuild_race() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 100, false)
            .expect("Failed to create log")
    );

    let mut tasks = JoinSet::new();

    // Writer that triggers snapshots
    let log_clone = log.clone();
    tasks.spawn(async move {
        for i in 0..1000 {
            let _ = log_clone.append(i, format!("value_{}", i).as_bytes().to_vec()).await;
        }
    });

    // Concurrent readers during snapshot creation
    for _ in 0..10 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..500 {
                let key = rand::random::<u64>() % 1000;
                let _ = log_clone.lookup(key).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Verify data integrity
    for i in 0..1000 {
        let value = log.lookup(i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found after snapshot rebuild", i);
    }
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_rmi_rebuild_race() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::new(data_dir.path().to_path_buf(), 10, 10000, true)
            .expect("Failed to create log")
    );

    // Write data
    for i in 0..10000 {
        log.append(i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    let mut tasks = JoinSet::new();

    // Trigger RMI rebuild
    let log_clone = log.clone();
    tasks.spawn(async move {
        for _ in 0..5 {
            let _ = log_clone.rebuild_rmi().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    // Concurrent readers during RMI rebuild
    for _ in 0..20 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for _ in 0..1000 {
                let key = rand::random::<u64>() % 10000;
                let value = log_clone.lookup(key).await.expect("Failed to lookup");
                assert!(value.is_some(), "Key {} not found during RMI rebuild", key);
            }
        });
    }

    while tasks.join_next().await.is_some() {}
}
