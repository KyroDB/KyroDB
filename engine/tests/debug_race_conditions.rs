//! Debug race condition issues to understand why lookups are failing

use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn debug_simple_lookup_issue() {
    println!("🔍 Debugging simple lookup issues...");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Insert some simple data
    println!("📝 Inserting test data...");
    for i in 0..100 {
        match rmi.insert(i, i * 10) {
            Ok(_) => println!("✅ Inserted key {} -> value {}", i, i * 10),
            Err(e) => println!("❌ Failed to insert key {}: {}", i, e),
        }
    }

    // Merge to create segments
    println!("🔄 Merging hot buffer...");
    match rmi.merge_hot_buffer().await {
        Ok(_) => println!("✅ Hot buffer merged successfully"),
        Err(e) => println!("❌ Hot buffer merge failed: {}", e),
    }

    // Check stats
    let stats = rmi.get_stats();
    println!(
        "📊 Stats after merge: segments={}, keys={}",
        stats.segment_count, stats.total_keys
    );

    // Test lookups
    println!("🔍 Testing lookups...");
    let mut successful_lookups = 0;
    let mut failed_lookups = 0;

    for i in 0..100 {
        match rmi.lookup_key_ultra_fast(i) {
            Some(value) => {
                successful_lookups += 1;
                if value != i * 10 {
                    println!(
                        "❌ Value mismatch for key {}: expected {}, got {}",
                        i,
                        i * 10,
                        value
                    );
                } else {
                    println!("✅ Key {} -> value {} (correct)", i, value);
                }
            }
            None => {
                failed_lookups += 1;
                println!("❌ Key {} not found", i);
            }
        }
    }

    println!(
        "📈 Lookup results: {} successful, {} failed",
        successful_lookups, failed_lookups
    );

    // Add more data and test concurrent operations
    println!("🚀 Starting background maintenance...");
    let maintenance_handle = rmi.clone().start_background_maintenance();

    // Insert more data
    println!("📝 Inserting more data during background operations...");
    for i in 100..200 {
        match rmi.insert(i, i * 10) {
            Ok(_) => println!("✅ Inserted key {} -> value {}", i, i * 10),
            Err(e) => println!("❌ Failed to insert key {}: {}", i, e),
        }
    }

    // Wait a bit for background operations
    sleep(Duration::from_millis(500)).await;

    // Test lookups again
    println!("🔍 Testing lookups after background operations...");
    let mut successful_lookups2 = 0;
    let mut failed_lookups2 = 0;

    for i in 0..200 {
        match rmi.lookup_key_ultra_fast(i) {
            Some(value) => {
                successful_lookups2 += 1;
                if value != i * 10 {
                    println!(
                        "❌ Value mismatch for key {}: expected {}, got {}",
                        i,
                        i * 10,
                        value
                    );
                }
            }
            None => {
                failed_lookups2 += 1;
                println!("❌ Key {} not found (expected value: {})", i, i * 10);
            }
        }
    }

    println!(
        "📈 Second lookup results: {} successful, {} failed",
        successful_lookups2, failed_lookups2
    );

    // Clean shutdown
    maintenance_handle.abort();

    let final_stats = rmi.get_stats();
    println!(
        "📊 Final stats: segments={}, keys={}",
        final_stats.segment_count, final_stats.total_keys
    );

    // Basic assertions
    assert!(
        successful_lookups > 80,
        "Should have most lookups successful initially, got {}",
        successful_lookups
    );
    assert!(
        successful_lookups2 > 150,
        "Should have most lookups successful after background ops, got {}",
        successful_lookups2
    );

    println!("✅ Debug test completed successfully!");
}

#[tokio::test]
async fn debug_router_prediction() {
    println!("🔍 Debugging router prediction logic...");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Insert sorted data
    for i in 0..50 {
        rmi.insert(i * 10, i * 100).unwrap();
    }

    // Check hot buffer lookups first
    println!("🔍 Testing hot buffer lookups...");
    for i in 0..10 {
        let key = i * 10;
        let expected = i * 100;
        match rmi.lookup_key_ultra_fast(key) {
            Some(value) => {
                if value == expected {
                    println!("✅ Hot buffer: key {} -> value {} (correct)", key, value);
                } else {
                    println!(
                        "❌ Hot buffer: key {} -> value {} (expected {})",
                        key, value, expected
                    );
                }
            }
            None => {
                println!("❌ Hot buffer: key {} not found", key);
            }
        }
    }

    // Merge to segments
    rmi.merge_hot_buffer().await.unwrap();
    let stats = rmi.get_stats();
    println!(
        "📊 After merge: segments={}, keys={}",
        stats.segment_count, stats.total_keys
    );

    // Test segment lookups
    println!("🔍 Testing segment lookups...");
    for i in 0..50 {
        let key = i * 10;
        let expected = i * 100;
        match rmi.lookup_key_ultra_fast(key) {
            Some(value) => {
                if value == expected {
                    println!("✅ Segment: key {} -> value {} (correct)", key, value);
                } else {
                    println!(
                        "❌ Segment: key {} -> value {} (expected {})",
                        key, value, expected
                    );
                }
            }
            None => {
                println!("❌ Segment: key {} not found", key);
            }
        }
    }

    println!("✅ Router prediction debug completed!");
}
