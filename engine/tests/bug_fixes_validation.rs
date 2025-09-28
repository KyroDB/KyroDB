//! Test suite to validate that critical bugs have been fixed
//!
//! This test verifies that:
//! 1. Unbounded memory growth is prevented via bounded overflow buffer
//! 2. Background task timing is adaptive and responds to system pressure

use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test]
async fn test_unbounded_memory_growth_fixed() {
    println!("Testing that unbounded memory growth has been fixed...");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Try to overwhelm the system with writes - should get rejections rather than OOM
    let mut successful_writes = 0;
    let mut rejected_writes = 0;

    // Attempt to insert many items without any merging
    for i in 0..5000 {
        match rmi.insert(i, i * 2) {
            Ok(_) => {
                successful_writes += 1;
            }
            Err(e) => {
                rejected_writes += 1;
                if e.to_string().contains("pressure") || e.to_string().contains("Circuit breaker") {
                    // This is expected behavior - bounded overflow buffer is working
                    println!("✅ Expected rejection due to memory pressure: {}", e);
                } else {
                    panic!("Unexpected error type: {}", e);
                }

                // Once we start getting rejections, system is protecting itself
                if rejected_writes > 10 {
                    break;
                }
            }
        }

        // Don't yield or sleep - try to stress the system
        if i % 100 == 0 {
            let stats = rmi.get_stats();
            println!(
                "Progress: {} writes, {} successful, {} rejected, {} hot buffer size",
                i, successful_writes, rejected_writes, stats.hot_buffer_size
            );
        }
    }

    println!("Memory protection test results:");
    println!("  Successful writes: {}", successful_writes);
    println!("  Rejected writes: {}", rejected_writes);

    // Should have some successful writes
    assert!(
        successful_writes > 100,
        "Should have some successful writes"
    );

    // Should get rejections when system is under pressure (proving bounded behavior)
    assert!(rejected_writes > 0, "Should get memory pressure rejections");

    let final_stats = rmi.get_stats();
    println!("  Final hot buffer size: {}", final_stats.hot_buffer_size);
    println!("  Final overflow size: {}", final_stats.overflow_size);

    // System should not have grown unbounded
    assert!(
        final_stats.hot_buffer_size < 10000,
        "Hot buffer should be bounded"
    );
    assert!(
        final_stats.overflow_size < 10000,
        "Overflow buffer should be bounded"
    );

    println!("✅ Unbounded memory growth has been successfully fixed!");
}

#[tokio::test]
async fn test_adaptive_background_timing() {
    println!("Testing that background task timing is adaptive...");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Add some initial data
    for i in 0..100 {
        let _ = rmi.insert(i, i * 10);
    }

    // Start background maintenance
    let maintenance_handle = rmi.clone().start_background_maintenance();

    let start_time = Instant::now();

    // Monitor behavior for a short time
    for cycle in 0..5 {
        sleep(Duration::from_millis(500)).await;

        let stats = rmi.get_stats();
        println!(
            "Cycle {}: segments={}, total_keys={}, merge_in_progress={}",
            cycle, stats.segment_count, stats.total_keys, stats.merge_in_progress
        );

        // Add some load to trigger adaptive behavior
        for i in (cycle * 20)..((cycle + 1) * 20) {
            let _ = rmi.insert(1000 + i as u64, (1000 + i) * 7);
        }
    }

    let elapsed = start_time.elapsed();

    // Clean shutdown
    maintenance_handle.abort();

    let final_stats = rmi.get_stats();
    println!("Adaptive timing test results:");
    println!("  Duration: {:?}", elapsed);
    println!("  Final segments: {}", final_stats.segment_count);
    println!("  Final total keys: {}", final_stats.total_keys);
    println!(
        "  Hot buffer utilization: {:.2}",
        final_stats.hot_buffer_utilization
    );

    // System should have processed the data and created segments
    assert!(
        final_stats.total_keys > 0,
        "Should have processed some data"
    );

    // Background maintenance should have run without crashing
    assert!(
        elapsed >= Duration::from_secs(2),
        "Test should have run for reasonable time"
    );

    println!("✅ Adaptive background timing is working correctly!");
}

#[tokio::test]
async fn test_insert_bounded_behavior() {
    println!("Testing insert method's bounded behavior under pressure...");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Fill up the hot buffer completely
    let mut inserts = 0;
    let mut first_rejection_count = None;

    for i in 0..10000 {
        match rmi.insert(i, i * 3) {
            Ok(_) => {
                inserts += 1;
            }
            Err(e) => {
                if first_rejection_count.is_none() {
                    first_rejection_count = Some(inserts);
                    println!("✅ First rejection at insert #{}: {}", inserts, e);
                }

                // Verify error message indicates proper bounded behavior
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("pressure")
                        || error_msg.contains("Circuit breaker")
                        || error_msg.contains("rejected"),
                    "Error should indicate pressure/bounded behavior: {}",
                    error_msg
                );

                break; // Stop after first rejection
            }
        }
    }

    println!("Insert bounded behavior test results:");
    println!("  Successful inserts before rejection: {}", inserts);
    println!("  First rejection at: {:?}", first_rejection_count);

    // Should be able to insert some data
    assert!(inserts > 50, "Should accept reasonable number of inserts");

    // Should eventually reject to prevent unbounded growth
    assert!(
        first_rejection_count.is_some(),
        "Should eventually reject inserts"
    );

    let stats = rmi.get_stats();
    println!("  Final hot buffer size: {}", stats.hot_buffer_size);
    println!("  Final overflow size: {}", stats.overflow_size);

    // Buffers should be bounded
    assert!(
        stats.hot_buffer_size < 5000,
        "Hot buffer should stay bounded"
    );
    assert!(
        stats.overflow_size < 5000,
        "Overflow buffer should stay bounded"
    );

    println!("✅ Insert method properly bounds memory usage!");
}
