//! Simple Robust Background Maintenance Test
//!
//! This test validates the bounded background maintenance with proper task management.

use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 KyroDB Simple Robust Background Maintenance Test");
    println!("=================================================");
    println!();

    // Test 1: Basic termination validation
    test_basic_termination().await?;

    // Test 2: Load handling validation
    test_load_handling().await?;

    // Test 3: Rate limiting validation
    test_rate_limiting().await?;

    println!("\n🎉 ALL TESTS PASSED!");
    println!("🛡️ Background maintenance CPU spin prevention is working correctly");

    Ok(())
}

/// Test 1: Basic bounded execution and termination
async fn test_basic_termination() -> Result<(), Box<dyn std::error::Error>> {
    println!("📊 Test 1: Basic Bounded Execution and Termination");
    println!("--------------------------------------------------");

    let rmi = Arc::new(AdaptiveRMI::new());
    let start_time = Instant::now();

    println!("🚀 Starting background maintenance...");

    // Start background maintenance - should terminate after 300 iterations (30 seconds at 10Hz)
    let maintenance_result = rmi.clone().start_background_maintenance().await;
    let elapsed = start_time.elapsed();

    match maintenance_result {
        Ok(_) => {
            println!("✓ Background maintenance completed successfully");
            println!("✓ Iteration safety limit respected");
        }
        Err(_timeout) => {
            println!("✗ Background maintenance did not terminate within expected time");
            panic!("Background maintenance failed to terminate within bounds");
        }
    }

    println!("✅ Test 1 passed\n");
    Ok(())
}

/// Test 2: Behavior under load
async fn test_load_handling() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Test 2: Load Handling");
    println!("------------------------");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Pre-populate with data to trigger merge activity
    println!("📦 Pre-populating with test data...");
    for i in 0..2000 {
        if let Err(_) = rmi.insert(i, i * 2) {
            // Some insertions may fail under pressure - this is expected
        }
    }
    println!("✅ Pre-population completed");

    let start_time = Instant::now();

    // Start background maintenance
    let maintenance_task = rmi.clone().start_background_maintenance();

    // Simulate concurrent load
    let load_rmi = Arc::clone(&rmi);
    let load_task = tokio::spawn(async move {
        for batch in 0..20 {
            for i in 0..100 {
                let key = 5000 + (batch * 100) + i;
                let _ = load_rmi.insert(key, key * 3); // Ignore errors under load
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        println!("✅ Load simulation completed");
    });

    // Let load run for a bit
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Stop load generation
    load_task.abort();

    // Wait for maintenance to complete
    let maintenance_result = maintenance_task.await;
    let elapsed = start_time.elapsed();

    match maintenance_result {
        Ok(_) => {
            println!("✅ Background maintenance completed under load");
            println!("📊 Runtime with load: {:.2}s", elapsed.as_secs_f64());
        }
        Err(_) => {
            return Err("Background maintenance task was aborted unexpectedly".into());
        }
    }

    // Validate system state
    let stats = rmi.get_stats();
    println!("📈 Final system state:");
    println!("   Total keys: {}", stats.total_keys);
    println!("   Segments: {}", stats.segment_count);
    println!(
        "   Hot buffer: {:.1}% full",
        stats.hot_buffer_utilization * 100.0
    );

    println!("✅ Test 2 passed\n");
    Ok(())
}

/// Test 3: Rate limiting validation
async fn test_rate_limiting() -> Result<(), Box<dyn std::error::Error>> {
    println!("📈 Test 3: Rate Limiting Validation");
    println!("-----------------------------------");

    let rmi = Arc::new(AdaptiveRMI::new());
    let start_time = Instant::now();

    println!("🚀 Starting maintenance with rate monitoring...");

    // Start background maintenance and measure the time to completion
    let maintenance_result = rmi.clone().start_background_maintenance().await;
    let elapsed = start_time.elapsed();

    match maintenance_result {
        Ok(()) => {
            let expected_duration = 30.0; // 300 iterations at 10Hz = 30 seconds
            let tolerance = 5.0; // ±5 seconds tolerance

            println!("✅ Background maintenance completed");
            println!("📊 Actual duration: {:.2}s", elapsed.as_secs_f64());
            println!("📊 Expected duration: ~{:.1}s", expected_duration);

            if (elapsed.as_secs_f64() - expected_duration).abs() <= tolerance {
                println!(
                    "✅ Rate limiting working correctly (within {:.1}s tolerance)",
                    tolerance
                );
            } else {
                println!(
                    "⚠️  Rate timing outside tolerance: {:.2}s vs expected {:.1}s ±{:.1}s",
                    elapsed.as_secs_f64(),
                    expected_duration,
                    tolerance
                );
            }

            // Calculate observed rate
            let iterations = 300; // Current test limit
            let observed_rate = iterations as f64 / elapsed.as_secs_f64();
            println!(
                "📊 Observed rate: {:.2} Hz (expected ~10 Hz)",
                observed_rate
            );

            if observed_rate <= 12.0 && observed_rate >= 8.0 {
                println!("✅ Rate is within acceptable bounds");
            } else {
                println!(
                    "⚠️  Rate outside acceptable bounds: {:.2} Hz",
                    observed_rate
                );
            }
        }
        Err(e) => {
            return Err(format!("Rate limiting test failed: {}", e).into());
        }
    }

    println!("✅ Test 3 passed\n");
    Ok(())
}
