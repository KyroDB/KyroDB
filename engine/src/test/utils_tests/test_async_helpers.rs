//! Tests for Async Helper Utilities

use crate::test::utils::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wait_for_immediate_success() {
    // Condition is already true
    let result = wait_for(|| true, 100).await;
    assert!(result, "Should return true immediately");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wait_for_eventual_success() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    
    // Increment counter in background
    tokio::spawn(async move {
        for _ in 0..10 {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            counter_clone.fetch_add(1, Ordering::Relaxed);
        }
    });
    
    // Wait for counter to reach 5
    let result = wait_for(
        || counter.load(Ordering::Relaxed) >= 5,
        500
    ).await;
    
    assert!(result, "Should succeed when condition becomes true");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wait_for_timeout() {
    let counter = Arc::new(AtomicUsize::new(0));
    
    // Wait for impossible condition
    let result = wait_for(
        || counter.load(Ordering::Relaxed) >= 100,
        50
    ).await;
    
    assert!(!result, "Should return false on timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wait_for_with_concurrent_modification() {
    let flag = Arc::new(AtomicUsize::new(0));
    let flag_clone = flag.clone();
    
    // Multiple threads modifying the flag
    for i in 0..5 {
        let fc = flag_clone.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(i * 10)).await;
            fc.fetch_add(1, Ordering::Relaxed);
        });
    }
    
    // Wait for all modifications
    let result = wait_for(
        || flag.load(Ordering::Relaxed) >= 5,
        300
    ).await;
    
    assert!(result, "Should handle concurrent modifications");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_retry_with_backoff_first_try() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();
    
    let result = retry_with_backoff(
        || {
            attempts_clone.fetch_add(1, Ordering::Relaxed);
            async move { Ok::<_, &str>("success") }
        },
        5,
    ).await;
    
    assert!(result.is_ok());
    assert_eq!(attempts.load(Ordering::Relaxed), 1, "Should succeed on first try");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_retry_with_backoff_eventual_success() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();
    
    let result = retry_with_backoff(
        || {
            let count = attempts_clone.fetch_add(1, Ordering::Relaxed);
            async move {
                if count < 3 {
                    Err("not ready")
                } else {
                    Ok("success")
                }
            }
        },
        10,
    ).await;
    
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "success");
    assert_eq!(attempts.load(Ordering::Relaxed), 4, "Should take 4 attempts");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_retry_with_backoff_max_retries() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();
    
    let result = retry_with_backoff(
        || {
            attempts_clone.fetch_add(1, Ordering::Relaxed);
            async move { Err::<(), _>("always fails") }
        },
        3,
    ).await;
    
    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::Relaxed), 4, "Should try initial + 3 retries");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_retry_with_backoff_exponential_delay() {
    use tokio::time::Instant;
    
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();
    let start = Instant::now();
    
    let _result = retry_with_backoff(
        || {
            attempts_clone.fetch_add(1, Ordering::Relaxed);
            async move { Err::<(), _>("fail") }
        },
        3,
    ).await;
    
    let elapsed = start.elapsed();
    
    // Initial: 0ms, Retry 1: 10ms, Retry 2: 20ms, Retry 3: 40ms
    // Total delay: ~70ms (with some tolerance)
    assert!(
        elapsed.as_millis() >= 60 && elapsed.as_millis() <= 150,
        "Exponential backoff should take ~70ms, took {}ms",
        elapsed.as_millis()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_retry_with_backoff_zero_retries() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();
    
    let result = retry_with_backoff(
        || {
            attempts_clone.fetch_add(1, Ordering::Relaxed);
            async move { Err::<(), _>("fail") }
        },
        0,
    ).await;
    
    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::Relaxed), 1, "Should only try once with 0 retries");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_append_kv_and_lookup_kv_integration() {
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    
    // Append multiple keys
    for i in 0..20 {
        append_kv(&log, i, format!("value_{}", i).into_bytes())
            .await
            .unwrap();
    }
    
    // Snapshot to persist
    log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    // Lookup all keys
    for i in 0..20 {
        let value = lookup_kv(&log, i).await.unwrap();
        assert!(value.is_some(), "Should find key {}", i);
        assert_eq!(
            value.unwrap(),
            format!("value_{}", i).into_bytes(),
            "Value should match for key {}",
            i
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_append_kv_concurrent_writers() {
    use tokio::task::JoinSet;
    
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    
    let mut handles = JoinSet::new();
    
    // Spawn 10 concurrent writers
    for worker in 0..10 {
        let log_clone = log.clone();
        handles.spawn(async move {
            for i in 0..10 {
                let key = (worker * 100 + i) as u64;
                append_kv(&log_clone, key, format!("w{}i{}", worker, i).into_bytes())
                    .await
                    .unwrap();
            }
        });
    }
    
    // Wait for all writers
    while handles.join_next().await.is_some() {}
    
    // Snapshot and build RMI
    log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    // Verify all writes
    for worker in 0..10 {
        for i in 0..10 {
            let key = (worker * 100 + i) as u64;
            let value = lookup_kv(&log, key).await.unwrap();
            assert!(value.is_some(), "Should find key from worker {} iteration {}", worker, i);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lookup_kv_missing_keys() {
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    
    // Append some keys
    for i in 0..10 {
        append_kv(&log, i * 10, b"value".to_vec()).await.unwrap();
    }
    
    log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    // Try to lookup keys that don't exist
    for i in 0..10 {
        let key = i * 10 + 5; // Between existing keys
        let value = lookup_kv(&log, key).await.unwrap();
        assert!(value.is_none(), "Should not find key {}", key);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wait_for_multiple_conditions() {
    let counter1 = Arc::new(AtomicUsize::new(0));
    let counter2 = Arc::new(AtomicUsize::new(0));
    
    let c1 = counter1.clone();
    let c2 = counter2.clone();
    
    // Increment both counters in background
    tokio::spawn(async move {
        for _ in 0..15 {
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            c1.fetch_add(1, Ordering::Relaxed);
            c2.fetch_add(2, Ordering::Relaxed);
        }
    });
    
    // Wait for both conditions
    let result1 = wait_for(|| counter1.load(Ordering::Relaxed) >= 10, 200).await;
    let result2 = wait_for(|| counter2.load(Ordering::Relaxed) >= 20, 200).await;
    
    assert!(result1, "First condition should be met");
    assert!(result2, "Second condition should be met");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_retry_with_varying_delays() {
    use tokio::time::Instant;
    
    // Test that each retry waits longer (exponential backoff)
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();
    
    let start = Instant::now();
    
    let _result = retry_with_backoff(
        || {
            let count = attempts_clone.fetch_add(1, Ordering::Relaxed);
            async move {
                if count < 4 {
                    Err("not ready")
                } else {
                    Ok("success")
                }
            }
        },
        10,
    ).await;
    
    let elapsed = start.elapsed();
    
    // Delays: 10ms, 20ms, 40ms, 80ms = ~150ms total
    assert!(
        elapsed.as_millis() >= 140,
        "Should have exponential backoff delays, took {}ms",
        elapsed.as_millis()
    );
}
