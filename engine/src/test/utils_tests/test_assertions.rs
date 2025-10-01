//! Tests for Custom Assertions

use crate::test::utils::assertions::*;
use std::time::Duration;

#[test]
fn test_assert_memory_within_bounds() {
    // Should pass when within bounds
    assert_memory_within(100, 100, 10);
    assert_memory_within(95, 100, 10);
    assert_memory_within(105, 100, 10);
}

#[test]
#[should_panic(expected = "Memory usage")]
fn test_assert_memory_exceeds_bounds() {
    assert_memory_within(150, 100, 10);
}

#[test]
fn test_assert_throughput_meets_minimum() {
    // 10000 ops in 1 second = 10000 ops/sec (should pass with min 5000)
    assert_throughput(10000, Duration::from_secs(1), 5000.0);

    // 5000 ops in 1 second = 5000 ops/sec (should pass with min 5000)
    assert_throughput(5000, Duration::from_secs(1), 5000.0);

    // 20000 ops in 2 seconds = 10000 ops/sec (should pass with min 5000)
    assert_throughput(20000, Duration::from_secs(2), 5000.0);
}

#[test]
#[should_panic(expected = "Throughput")]
fn test_assert_throughput_below_minimum() {
    // 1000 ops in 1 second = 1000 ops/sec (should fail with min 5000)
    assert_throughput(1000, Duration::from_secs(1), 5000.0);
}

#[test]
fn test_assert_latency_within_bounds() {
    let latency = Duration::from_millis(50);
    let max = Duration::from_millis(100);

    assert_latency(latency, max);
}

#[test]
#[should_panic(expected = "Latency")]
fn test_assert_latency_exceeds_bounds() {
    let latency = Duration::from_millis(150);
    let max = Duration::from_millis(100);

    assert_latency(latency, max);
}

#[test]
fn test_assert_p99_latency_calculation() {
    let mut latencies = Vec::new();

    // 99 fast operations
    for _ in 0..99 {
        latencies.push(Duration::from_micros(100));
    }

    // 1 slow operation (P99)
    latencies.push(Duration::from_micros(5000));

    // P99 should be ~5000 microseconds
    let max_p99 = Duration::from_micros(6000);
    assert_p99_latency(&latencies, max_p99);
}

#[test]
#[should_panic(expected = "P99 latency")]
fn test_assert_p99_latency_exceeds() {
    let mut latencies = Vec::new();

    // 99 fast operations
    for _ in 0..99 {
        latencies.push(Duration::from_micros(100));
    }

    // 1 very slow operation
    latencies.push(Duration::from_micros(50000));

    // P99 limit is too strict
    let max_p99 = Duration::from_micros(10000);
    assert_p99_latency(&latencies, max_p99);
}

#[test]
fn test_assert_no_data_loss_success() {
    let original = vec![(1, vec![1, 2, 3]), (2, vec![4, 5, 6]), (3, vec![7, 8, 9])];

    let recovered = original.clone();

    assert_no_data_loss(&original, &recovered);
}

#[test]
#[should_panic(expected = "Data count mismatch")]
fn test_assert_no_data_loss_count_mismatch() {
    let original = vec![(1, vec![1, 2, 3]), (2, vec![4, 5, 6])];

    let recovered = vec![(1, vec![1, 2, 3])];

    assert_no_data_loss(&original, &recovered);
}

#[test]
#[should_panic(expected = "Key mismatch")]
fn test_assert_no_data_loss_key_mismatch() {
    let original = vec![(1, vec![1, 2, 3]), (2, vec![4, 5, 6])];

    let recovered = vec![
        (1, vec![1, 2, 3]),
        (3, vec![4, 5, 6]), // Wrong key
    ];

    assert_no_data_loss(&original, &recovered);
}

#[test]
#[should_panic(expected = "Value mismatch")]
fn test_assert_no_data_loss_value_mismatch() {
    let original = vec![(1, vec![1, 2, 3]), (2, vec![4, 5, 6])];

    let recovered = vec![
        (1, vec![1, 2, 3]),
        (2, vec![9, 9, 9]), // Wrong value
    ];

    assert_no_data_loss(&original, &recovered);
}

#[test]
fn test_assert_simd_speedup_sufficient() {
    let scalar = Duration::from_millis(100);
    let simd = Duration::from_millis(25);

    // 4x speedup
    assert_simd_speedup(scalar, simd, 3.0);
}

#[test]
#[should_panic(expected = "SIMD speedup")]
fn test_assert_simd_speedup_insufficient() {
    let scalar = Duration::from_millis(100);
    let simd = Duration::from_millis(80);

    // Only 1.25x speedup, but expecting 2x
    assert_simd_speedup(scalar, simd, 2.0);
}

#[test]
fn test_assert_rmi_accuracy_within_epsilon() {
    let predictions = vec![(100, 10), (200, 20), (300, 30)];

    let actual = vec![
        (100, 12), // Error: 2
        (200, 19), // Error: 1
        (300, 28), // Error: 2
    ];

    assert_rmi_accuracy(&predictions, &actual, 5);
}

#[test]
#[should_panic(expected = "RMI prediction error")]
fn test_assert_rmi_accuracy_exceeds_epsilon() {
    let predictions = vec![(100, 10), (200, 20)];

    let actual = vec![
        (100, 12), // Error: 2
        (200, 30), // Error: 10 (exceeds epsilon of 5)
    ];

    assert_rmi_accuracy(&predictions, &actual, 5);
}

#[test]
fn test_assert_rmi_accuracy_exact_match() {
    let predictions = vec![(100, 10), (200, 20), (300, 30)];

    let actual = predictions.clone();

    // Should pass with any epsilon
    assert_rmi_accuracy(&predictions, &actual, 0);
}

#[test]
fn test_assert_rmi_accuracy_reverse_error() {
    // Test when actual > predicted
    let predictions = vec![(100, 50), (200, 100)];

    let actual = vec![
        (100, 45), // Error: 5 (predicted is higher)
        (200, 95), // Error: 5 (predicted is higher)
    ];

    assert_rmi_accuracy(&predictions, &actual, 10);
}

#[test]
fn test_throughput_fractional_seconds() {
    // 5000 ops in 500ms = 10000 ops/sec
    assert_throughput(5000, Duration::from_millis(500), 9000.0);
}

#[test]
fn test_throughput_very_fast_operations() {
    // 1M ops in 100ms = 10M ops/sec
    assert_throughput(1_000_000, Duration::from_millis(100), 5_000_000.0);
}

#[test]
fn test_latency_microseconds() {
    let latency = Duration::from_micros(500);
    let max = Duration::from_micros(1000);

    assert_latency(latency, max);
}

#[test]
fn test_p99_latency_large_dataset() {
    let mut latencies = Vec::new();

    // 9900 fast operations (1-10 microseconds)
    for i in 0..9900 {
        latencies.push(Duration::from_micros((i % 10 + 1) as u64));
    }

    // 100 slow operations (100-1000 microseconds)
    for i in 0..100 {
        latencies.push(Duration::from_micros((i * 10 + 100) as u64));
    }

    // P99 should be around 1000 microseconds
    let max_p99 = Duration::from_micros(1500);
    assert_p99_latency(&latencies, max_p99);
}

#[test]
fn test_simd_speedup_realistic() {
    // Realistic SIMD speedup scenarios

    // AVX2 4x speedup
    let scalar = Duration::from_micros(1000);
    let simd_avx2 = Duration::from_micros(250);
    assert_simd_speedup(scalar, simd_avx2, 3.5);

    // NEON 2x speedup
    let simd_neon = Duration::from_micros(500);
    assert_simd_speedup(scalar, simd_neon, 1.8);
}

#[test]
fn test_memory_within_zero_tolerance() {
    // Exact match with zero tolerance
    assert_memory_within(100, 100, 0);
}

#[test]
#[should_panic(expected = "Memory usage")]
fn test_memory_exceeds_by_one() {
    // Just 1 MB over tolerance should fail
    assert_memory_within(111, 100, 10);
}
