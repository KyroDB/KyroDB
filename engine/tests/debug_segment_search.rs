//! More detailed debugging of segment search

use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;

#[tokio::test]
async fn debug_segment_search_detailed() {
    println!("🔍 Debugging segment search in detail...");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Insert a small amount of sorted data for easier debugging
    let test_data = vec![(10, 100), (20, 200), (30, 300), (40, 400), (50, 500)];

    for (key, value) in &test_data {
        rmi.insert(*key, *value).unwrap();
        println!("✅ Inserted key {} -> value {}", key, value);
    }

    // Check hot buffer first
    println!("\n🔍 Testing hot buffer lookups:");
    for (key, expected_value) in &test_data {
        match rmi.lookup(*key) {
            Some(value) => {
                if value == *expected_value {
                    println!("✅ Hot buffer: key {} -> value {} (correct)", key, value);
                } else {
                    println!(
                        "❌ Hot buffer: key {} -> value {} (expected {})",
                        key, value, expected_value
                    );
                }
            }
            None => {
                println!("❌ Hot buffer: key {} not found", key);
            }
        }
    }

    // Merge to segments
    println!("\n🔄 Merging to segments...");
    rmi.merge_hot_buffer().await.unwrap();

    let stats = rmi.get_stats();
    println!(
        "📊 After merge: segments={}, total_keys={}",
        stats.segment_count, stats.total_keys
    );

    // Test segment lookups
    println!("\n🔍 Testing segment lookups:");
    for (key, expected_value) in &test_data {
        match rmi.lookup(*key) {
            Some(value) => {
                if value == *expected_value {
                    println!("✅ Segment: key {} -> value {} (correct)", key, value);
                } else {
                    println!(
                        "❌ Segment: key {} -> value {} (expected {})",
                        key, value, expected_value
                    );
                }
            }
            None => {
                println!("❌ Segment: key {} not found", key);
            }
        }
    }

    // Try some non-existent keys
    println!("\n🔍 Testing non-existent keys:");
    for key in [5, 15, 25, 35, 45, 55] {
        match rmi.lookup(key) {
            Some(value) => {
                println!(
                    "❌ Unexpected result for key {}: found value {}",
                    key, value
                );
            }
            None => {
                println!("✅ Correctly returned None for non-existent key {}", key);
            }
        }
    }

    println!("\n✅ Detailed segment search debug completed!");
}
