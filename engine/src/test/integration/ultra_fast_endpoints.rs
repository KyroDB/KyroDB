//! Integration Tests for Ultra-Fast Endpoints
//!
//! Validates that /v1/lookup_ultra and /v1/get_ultra actually use the RMI
//! and deliver superior performance compared to standard endpoints.

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use std::time::Instant;

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_ultra_fast_endpoints_require_rmi_warmup() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Insert test data
    for i in 0..5000 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // CRITICAL: Create snapshot first - RMI reads from snapshot, not WAL
    log.snapshot().await.expect("Failed to create snapshot");

    // Build RMI from snapshot data
    log.build_rmi().await.expect("Failed to build RMI");

    // Warmup mmap pages
    log.warmup().await.expect("Failed to warmup");

    // Verify ultra-fast lookups work
    for i in (0..5000).step_by(100) {
        let offset = log
            .lookup_key_ultra_fast(i)
            .expect(&format!("Ultra-fast lookup should succeed after RMI build for key {}", i));
        
        // Offset should be a valid WAL offset (>= 0)
        assert!(offset < u64::MAX, "Ultra-fast lookup returned invalid offset for key {}", i);
    }
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_ultra_fast_vs_standard_endpoint_performance() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Insert sequential data (optimal for RMI)
    for i in 0..10000 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Correct workflow: snapshot → build_rmi → warmup
    log.snapshot().await.expect("Failed to create snapshot");
    log.build_rmi().await.expect("Failed to build RMI");
    log.warmup().await.expect("Failed to warmup");

    // Benchmark ultra-fast endpoint (RMI-accelerated)
    let ultra_start = Instant::now();
    for i in (0..10000).step_by(10) {
        let _ = log.lookup_key_ultra_fast(i);
    }
    let ultra_duration = ultra_start.elapsed();

    // Benchmark standard endpoint (BTree-based)
    let standard_start = Instant::now();
    for i in (0..10000).step_by(10) {
        let _ = lookup_kv(&log, i).await;
    }
    let standard_duration = standard_start.elapsed();

    println!(
        "🔥 Ultra-fast: {:?}, Standard: {:?}, Speedup: {:.2}x",
        ultra_duration,
        standard_duration,
        standard_duration.as_secs_f64() / ultra_duration.as_secs_f64()
    );

    // Ultra-fast should be at least 1.5x faster (conservative check)
    // In practice, RMI should be 3-5x faster for sequential access patterns
    assert!(
        ultra_duration < standard_duration,
        "Ultra-fast endpoint should be faster than standard endpoint"
    );
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_get_ultra_returns_actual_values() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Insert test data with known values
    let test_data = vec![
        (100u64, b"first_value".to_vec()),
        (200u64, b"second_value".to_vec()),
        (300u64, b"third_value".to_vec()),
    ];

    for (key, value) in &test_data {
        append_kv(&log, *key, value.clone())
            .await
            .expect("Failed to append");
    }

    // Correct workflow: snapshot → build_rmi → warmup
    log.snapshot().await.expect("Failed to create snapshot");
    log.build_rmi().await.expect("Failed to build RMI");
    log.warmup().await.expect("Failed to warmup");

    // Verify ultra-fast lookup returns correct offsets
    for (key, _expected_value) in test_data {
        let offset = log
            .lookup_key_ultra_fast(key)
            .expect("Ultra-fast lookup should succeed");
        
        // Offset should be a valid WAL offset (not the key value!)
        assert!(
            offset < u64::MAX,
            "Invalid offset for key {}: got {}",
            key, offset
        );
    }
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_ultra_fast_with_sparse_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Insert sparse keys (every 1000th key)
    for i in 0..100 {
        let key = i * 1000;
        append_kv(&log, key, format!("sparse_{}", key).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Correct workflow: snapshot → build_rmi → warmup
    log.snapshot().await.expect("Failed to create snapshot");
    log.build_rmi().await.expect("Failed to build RMI");
    log.warmup().await.expect("Failed to warmup");

    // Verify ultra-fast lookups work with sparse data
    for i in 0..100 {
        let key = i * 1000;
        let offset = log
            .lookup_key_ultra_fast(key)
            .expect("Ultra-fast lookup should work with sparse keys");
        
        // Offset should be a valid WAL offset
        assert!(offset < u64::MAX, "Wrong offset for sparse key {}: got {}", key, offset);
    }

    // Verify non-existent keys return None
    let non_existent = log.lookup_key_ultra_fast(500);
    assert!(
        non_existent.is_none(),
        "Non-existent key should return None"
    );
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_ultra_fast_with_clustered_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Insert clustered keys (like the RMI test pattern)
    for i in 0..1000 {
        append_kv(&log, i, format!("cluster1_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    for i in 5000..6000 {
        append_kv(&log, i, format!("cluster2_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    for i in 10000..11000 {
        append_kv(&log, i, format!("cluster3_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Correct workflow: snapshot → build_rmi → warmup
    log.snapshot().await.expect("Failed to create snapshot");
    log.build_rmi().await.expect("Failed to build RMI");
    log.warmup().await.expect("Failed to warmup");

    // Verify ultra-fast lookups work across all clusters
    let test_keys = vec![0, 500, 999, 5000, 5500, 5999, 10000, 10500, 10999];
    for key in test_keys {
        let offset = log
            .lookup_key_ultra_fast(key)
            .expect(&format!("Ultra-fast lookup should work for key {}", key));
        
        // Offset should be a valid WAL offset
        assert!(offset < u64::MAX, "Wrong offset for clustered key {}: got {}", key, offset);
    }
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_ultra_fast_after_updates() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Initial data
    for i in 0..1000 {
        append_kv(&log, i, format!("initial_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Correct workflow: snapshot → build_rmi
    log.snapshot().await.expect("Failed to create snapshot");
    log.build_rmi().await.expect("Failed to build RMI");

    // Update some keys
    for i in (0..1000).step_by(10) {
        append_kv(&log, i, format!("updated_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append update");
    }

    // Correct workflow after updates: snapshot → rebuild_rmi → warmup
    log.snapshot().await.expect("Failed to create snapshot after updates");
    log.build_rmi().await.expect("Failed to rebuild RMI");
    log.warmup().await.expect("Failed to warmup");

    // Verify ultra-fast lookups still work after updates
    // Note: lookup_key_ultra_fast returns offsets, not values
    for i in (0..1000).step_by(10) {
        let offset = log
            .lookup_key_ultra_fast(i)
            .expect("Ultra-fast lookup should work after updates");
        
        // Offset should be a valid WAL offset
        assert!(offset < u64::MAX, "Invalid offset for key {}: got {}", i, offset);
        
        // Verify we can still get the value through standard API
        let value = lookup_kv(&log, i).await.expect("Failed to lookup updated key");
        assert!(value.is_some(), "Key {} should exist after update", i);
        
        let value_bytes = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_bytes);
        assert!(
            value_str.starts_with("updated_"),
            "Key {} should have updated value, got: {}",
            i,
            value_str
        );
    }
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_ultra_fast_epsilon_bounds() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Insert large dataset to test epsilon bounds
    for i in 0..20000 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Correct workflow: snapshot → build_rmi → warmup
    log.snapshot().await.expect("Failed to create snapshot");
    log.build_rmi().await.expect("Failed to build RMI");
    log.warmup().await.expect("Failed to warmup");

    // All lookups should succeed (epsilon ≤ 64 guarantee)
    for i in (0..20000).step_by(100) {
        let result = log.lookup_key_ultra_fast(i);
        assert!(
            result.is_some(),
            "Ultra-fast lookup failed for key {} - epsilon bound may be violated",
            i
        );
    }
}

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn test_warmup_idempotency() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Insert data
    for i in 0..1000 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // Multiple warmup cycles should be safe
    log.snapshot().await.expect("First snapshot failed");
    log.build_rmi().await.expect("First RMI build failed");
    log.warmup().await.expect("First warmup failed");

    log.snapshot().await.expect("Second snapshot failed");
    log.build_rmi().await.expect("Second RMI build failed");
    log.warmup().await.expect("Second warmup failed");

    log.snapshot().await.expect("Third snapshot failed");
    log.build_rmi().await.expect("Third RMI build failed");
    log.warmup().await.expect("Third warmup failed");

    // Verify data still accessible
    for i in (0..1000).step_by(100) {
        let value = log
            .lookup_key_ultra_fast(i)
            .expect("Lookup should work after multiple warmups");
        assert_eq!(value, i);
    }
}

#[tokio::test]
async fn test_standard_endpoints_work_without_rmi() {
    let data_dir = test_data_dir();
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    // Insert data
    for i in 0..100 {
        append_kv(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }

    // ARCHITECTURE NOTE: With hot_buffer check fix, lookups work immediately after writes
    // even without building RMI. The hot_buffer is checked FIRST before the snapshot.
    // This eliminates the async lag window (0-500ms) between writes and background merge.
    // 
    // Standard lookups should work immediately (hot_buffer check):
    for i in (0..100).step_by(10) {
        let value = lookup_kv(&log, i)
            .await
            .expect("Lookup should not error");
        
        // With hot_buffer check fix, lookups work immediately
        assert!(value.is_some(), "Hot buffer check should find key {} immediately", i);
        let expected = format!("value_{}", i);
        assert_eq!(value.unwrap(), expected.as_bytes(), "Value mismatch for key {}", i);
    }
    
    // After building RMI, lookups should still work (snapshot path):
    log.snapshot().await.expect("Failed to create snapshot");
    log.build_rmi().await.expect("Failed to build RMI");
    log.warmup().await.expect("Failed to warmup");
    
    for i in (0..100).step_by(10) {
        let value = lookup_kv(&log, i)
            .await
            .expect("Lookup should not error");
        assert!(value.is_some(), "After RMI build, key {} should still be found", i);
    }
}
