//! Custom Test Assertions
//!
//! Domain-specific assertions for KyroDB testing

use std::time::Duration;

/// Assert that a value is eventually true within timeout
#[macro_export]
macro_rules! assert_eventually {
    ($condition:expr, $timeout_ms:expr) => {
        assert_eventually!($condition, $timeout_ms, "Condition did not become true within timeout")
    };
    ($condition:expr, $timeout_ms:expr, $msg:expr) => {
        {
            let start = std::time::Instant::now();
            let timeout = std::time::Duration::from_millis($timeout_ms);
            let mut last_result = false;
            
            while start.elapsed() < timeout {
                if $condition {
                    last_result = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            
            assert!(last_result, $msg);
        }
    };
}

/// Assert that operation completes within duration
#[macro_export]
macro_rules! assert_completes_within {
    ($duration:expr, $block:expr) => {
        {
            let start = std::time::Instant::now();
            let result = $block;
            let elapsed = start.elapsed();
            assert!(
                elapsed <= $duration,
                "Operation took {:?} but should complete within {:?}",
                elapsed,
                $duration
            );
            result
        }
    };
}

/// Assert memory usage is within bounds
pub fn assert_memory_within(actual_mb: usize, expected_mb: usize, tolerance_mb: usize) {
    assert!(
        actual_mb <= expected_mb + tolerance_mb,
        "Memory usage {} MB exceeds expected {} MB (tolerance: {} MB)",
        actual_mb,
        expected_mb,
        tolerance_mb
    );
}

/// Assert throughput is within expected range
pub fn assert_throughput(
    ops_completed: usize,
    duration: Duration,
    min_ops_per_sec: f64,
) {
    let ops_per_sec = ops_completed as f64 / duration.as_secs_f64();
    assert!(
        ops_per_sec >= min_ops_per_sec,
        "Throughput {} ops/sec is below minimum {} ops/sec",
        ops_per_sec,
        min_ops_per_sec
    );
}

/// Assert latency is within bounds
pub fn assert_latency(latency: Duration, max_latency: Duration) {
    assert!(
        latency <= max_latency,
        "Latency {:?} exceeds maximum {:?}",
        latency,
        max_latency
    );
}

/// Assert P99 latency is within bounds
pub fn assert_p99_latency(latencies: &[Duration], max_p99: Duration) {
    let mut sorted = latencies.to_vec();
    sorted.sort();
    let p99_idx = (latencies.len() as f64 * 0.99) as usize;
    let p99 = sorted[p99_idx];
    
    assert!(
        p99 <= max_p99,
        "P99 latency {:?} exceeds maximum {:?}",
        p99,
        max_p99
    );
}

/// Assert no data loss after crash
pub fn assert_no_data_loss(
    original_data: &[(u64, Vec<u8>)],
    recovered_data: &[(u64, Vec<u8>)],
) {
    assert_eq!(
        original_data.len(),
        recovered_data.len(),
        "Data count mismatch after recovery"
    );
    
    for ((k1, v1), (k2, v2)) in original_data.iter().zip(recovered_data.iter()) {
        assert_eq!(k1, k2, "Key mismatch after recovery");
        assert_eq!(v1, v2, "Value mismatch after recovery for key {}", k1);
    }
}

/// Assert SIMD speedup is significant
pub fn assert_simd_speedup(
    scalar_duration: Duration,
    simd_duration: Duration,
    min_speedup: f64,
) {
    let speedup = scalar_duration.as_secs_f64() / simd_duration.as_secs_f64();
    assert!(
        speedup >= min_speedup,
        "SIMD speedup {}x is below minimum {}x",
        speedup,
        min_speedup
    );
}

/// Assert RMI prediction accuracy
pub fn assert_rmi_accuracy(
    predictions: &[(u64, usize)],
    actual_positions: &[(u64, usize)],
    max_epsilon: usize,
) {
    for ((key, predicted), (_, actual)) in predictions.iter().zip(actual_positions.iter()) {
        let error = if predicted > actual {
            predicted - actual
        } else {
            actual - predicted
        };
        
        assert!(
            error <= max_epsilon,
            "RMI prediction error {} for key {} exceeds epsilon {}",
            error,
            key,
            max_epsilon
        );
    }
}
