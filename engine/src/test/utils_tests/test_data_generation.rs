//! Tests for Data Generation Utilities

use crate::test::utils::*;
use std::sync::Arc;

#[test]
fn test_generate_test_data() {
    let data = generate_test_data(100);
    
    assert_eq!(data.len(), 100, "Should generate exactly 100 entries");
    
    // Validate structure
    for (i, (key, value)) in data.iter().enumerate() {
        assert_eq!(*key, i as u64, "Keys should be sequential");
        assert!(!value.is_empty(), "Values should not be empty");
        
        let expected_value = format!("value_{}", i).into_bytes();
        assert_eq!(value, &expected_value, "Value should match pattern");
    }
}

#[test]
fn test_sequential_keys() {
    let keys = sequential_keys(100, 50);
    
    assert_eq!(keys.len(), 50, "Should generate 50 keys");
    assert_eq!(keys[0], 100, "First key should be start value");
    assert_eq!(keys[49], 149, "Last key should be start + count - 1");
    
    // Validate sequential order
    for i in 1..keys.len() {
        assert_eq!(keys[i], keys[i-1] + 1, "Keys should be sequential");
    }
}

#[test]
fn test_random_keys() {
    let keys = random_keys(1000, 10000);
    
    assert_eq!(keys.len(), 1000, "Should generate 1000 keys");
    
    // All keys should be within range
    for key in &keys {
        assert!(*key < 10000, "All keys should be less than max");
    }
    
    // Keys should have some variance (not all the same)
    let first = keys[0];
    let has_variance = keys.iter().any(|k| *k != first);
    assert!(has_variance, "Random keys should have variance");
}

#[test]
fn test_skewed_keys_distribution() {
    let keys = skewed_keys(10000, 10000);
    
    assert_eq!(keys.len(), 10000, "Should generate 10000 keys");
    
    // Count keys in hot range (0..2000, which is 20% of 10000)
    let hot_range_max = 2000;
    let hot_count = keys.iter().filter(|&&k| k < hot_range_max).count();
    
    // Expect roughly 80% in hot range (with some tolerance)
    let hot_percentage = (hot_count as f64 / keys.len() as f64) * 100.0;
    assert!(
        hot_percentage >= 70.0 && hot_percentage <= 90.0,
        "Expected ~80% of keys in hot range, got {:.1}%",
        hot_percentage
    );
}

#[test]
fn test_temp_data_dir_creation() {
    let dir = test_data_dir();
    
    assert!(dir.path().exists(), "Temp directory should exist");
    assert!(dir.path().is_dir(), "Should be a directory");
    
    // Directory should be writable
    let test_file = dir.path().join("test.txt");
    std::fs::write(&test_file, b"test").expect("Should be able to write");
    assert!(test_file.exists(), "Should be able to create files");
}

#[test]
fn test_temp_data_dir_with_prefix() {
    let dir1 = test_data_dir_with_prefix("test1");
    let dir2 = test_data_dir_with_prefix("test2");
    
    assert!(dir1.path().exists(), "First directory should exist");
    assert!(dir2.path().exists(), "Second directory should exist");
    
    // Paths should be different
    assert_ne!(
        dir1.path(),
        dir2.path(),
        "Directories should have unique paths"
    );
    
    // Names should contain prefix
    let name1 = dir1.path().file_name().unwrap().to_str().unwrap();
    let name2 = dir2.path().file_name().unwrap().to_str().unwrap();
    
    assert!(name1.contains("test1"), "First directory should contain prefix 'test1'");
    assert!(name2.contains("test2"), "Second directory should contain prefix 'test2'");
}

#[test]
fn test_temp_data_dir_uniqueness() {
    let dir1 = temp_data_dir("test");
    let dir2 = temp_data_dir("test");
    
    assert_ne!(dir1, dir2, "Each call should create unique directory");
    
    // Both should be creatable
    std::fs::create_dir_all(&dir1).ok();
    std::fs::create_dir_all(&dir2).ok();
    
    assert!(dir1.exists(), "First directory should exist");
    assert!(dir2.exists(), "Second directory should exist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wait_for_success() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    
    let flag = Arc::new(AtomicBool::new(false));
    let flag_clone = flag.clone();
    
    // Set flag to true after 50ms
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        flag_clone.store(true, Ordering::Relaxed);
    });
    
    // Wait for flag with 200ms timeout (should succeed)
    let result = wait_for(|| flag.load(Ordering::Relaxed), 200).await;
    
    assert!(result, "wait_for should return true when condition is met");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wait_for_timeout() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    
    let flag = Arc::new(AtomicBool::new(false));
    
    // Never set flag to true
    let result = wait_for(|| flag.load(Ordering::Relaxed), 50).await;
    
    assert!(!result, "wait_for should return false on timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_retry_with_backoff_success() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();
    
    let result = retry_with_backoff(
        || {
            let count = attempts_clone.fetch_add(1, Ordering::Relaxed);
            async move {
                if count < 2 {
                    Err("Not ready yet")
                } else {
                    Ok("Success!")
                }
            }
        },
        5,
    )
    .await;
    
    assert!(result.is_ok(), "Should succeed after retries");
    assert_eq!(result.unwrap(), "Success!");
    assert_eq!(attempts.load(Ordering::Relaxed), 3, "Should take 3 attempts");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_retry_with_backoff_failure() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();
    
    let result = retry_with_backoff(
        || {
            attempts_clone.fetch_add(1, Ordering::Relaxed);
            async move { Err::<(), _>("Always fails") }
        },
        3,
    )
    .await;
    
    assert!(result.is_err(), "Should fail after max retries");
    assert_eq!(attempts.load(Ordering::Relaxed), 4, "Should attempt 4 times (initial + 3 retries)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_append_and_lookup_kv() {
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    
    // Test append
    let offset = append_kv(&log, 42, b"test_value".to_vec()).await.unwrap();
    // Offset can be 0 (valid), just check it succeeded
    let _ = offset;
    
    // Snapshot to make data visible
    log.snapshot().await.ok();
    
    // Build RMI if feature is enabled
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.ok();
    }
    
    // Test lookup
    let value = lookup_kv(&log, 42).await.unwrap();
    assert!(value.is_some(), "Should find the key");
    assert_eq!(value.unwrap(), b"test_value", "Value should match");
    
    // Test lookup of non-existent key
    let missing = lookup_kv(&log, 999).await.unwrap();
    assert!(missing.is_none(), "Non-existent key should return None");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_append_kv_ref() {
    let dir = test_data_dir();
    let log = open_test_log(dir.path()).await.unwrap();
    
    // Test with direct reference (not Arc)
    let offset = append_kv_ref(&log, 100, b"ref_test".to_vec()).await.unwrap();
    // Offset can be 0 (valid), just check it succeeded
    let _ = offset;
    
    // Snapshot and build RMI
    log.snapshot().await.ok();
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.ok();
    }
    
    // Lookup with reference
    let value = lookup_kv_ref(&log, 100).await.unwrap();
    assert!(value.is_some(), "Should find the key");
    assert_eq!(value.unwrap(), b"ref_test", "Value should match");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiple_appends_and_lookups() {
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    
    // Append multiple key-value pairs
    for i in 0..100 {
        let key = i;
        let value = format!("value_{}", i).into_bytes();
        append_kv(&log, key, value).await.unwrap();
    }
    
    // Snapshot and build RMI
    log.snapshot().await.unwrap();
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    // Verify all keys are retrievable
    for i in 0..100 {
        let expected_value = format!("value_{}", i).into_bytes();
        let actual_value = lookup_kv(&log, i).await.unwrap();
        
        assert!(actual_value.is_some(), "Should find key {}", i);
        assert_eq!(actual_value.unwrap(), expected_value, "Value mismatch for key {}", i);
    }
}

#[test]
fn test_to_path_conversion() {
    let dir = test_data_dir();
    let path = to_path(&dir);
    
    assert_eq!(path, dir.path(), "Converted path should match directory path");
    assert!(path.exists(), "Converted path should exist");
}
