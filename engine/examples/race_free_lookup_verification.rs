//! Verification of race-free lookup implementation
//! Tests that the new generation-based lookup is both correct and performant

#[cfg(feature = "learned-index")]
use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[cfg(feature = "learned-index")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("🔒 Verifying race-free lookup implementation...\n");

    // Create RMI with test data
    let rmi = Arc::new(AdaptiveRMI::new());

    // Insert test data
    println!("📊 Inserting 100,000 test keys...");
    let start = Instant::now();
    for i in 0..100_000u64 {
        rmi.insert(i, i * 2)?;
    }
    let insert_time = start.elapsed();
    println!("✅ Inserted 100,000 keys in {:?}", insert_time);

    // Wait for any background operations to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Test single-threaded lookup performance
    println!("\n🚀 Testing single-threaded lookup performance...");
    let lookup_start = Instant::now();
    let mut found_count = 0;
    for i in 0..100_000u64 {
        if let Some(value) = rmi.lookup(i) {
            assert_eq!(value, i * 2, "Value mismatch for key {}", i);
            found_count += 1;
        }
    }
    let lookup_time = lookup_start.elapsed();
    println!("✅ Found {}/100,000 keys in {:?}", found_count, lookup_time);
    println!(
        "📈 Average lookup time: {:?} per key",
        lookup_time / 100_000
    );

    // Test multi-threaded concurrent lookup safety
    println!("\n🔀 Testing multi-threaded concurrent lookup safety...");
    let rmi_clone = Arc::clone(&rmi);

    let concurrent_start = Instant::now();
    let mut handles = vec![];

    // Spawn 8 threads doing concurrent lookups
    for thread_id in 0..8 {
        let rmi_thread = Arc::clone(&rmi_clone);
        let handle = thread::spawn(move || {
            let mut thread_found = 0;
            let start_key = thread_id * 12_500;
            let end_key = start_key + 12_500;

            for i in start_key..end_key {
                if let Some(value) = rmi_thread.lookup(i) {
                    assert_eq!(
                        value,
                        i * 2,
                        "Race condition detected: value mismatch for key {}",
                        i
                    );
                    thread_found += 1;
                }
            }
            thread_found
        });
        handles.push(handle);
    }

    // Wait for all threads and collect results
    let mut total_concurrent_found = 0;
    for handle in handles {
        total_concurrent_found += handle.join().unwrap();
    }

    let concurrent_time = concurrent_start.elapsed();
    println!(
        "✅ Concurrent lookup: found {}/100,000 keys in {:?}",
        total_concurrent_found, concurrent_time
    );
    println!(
        "📈 Average concurrent lookup time: {:?} per key",
        concurrent_time / 100_000
    );

    // Verify consistency between single and multi-threaded results
    if found_count == total_concurrent_found {
        println!("🎯 SUCCESS: Single-threaded and multi-threaded results match!");
        println!("🔒 Race-free lookup implementation is working correctly!");
    } else {
        println!("❌ FAILURE: Results don't match - race conditions may still exist");
        println!(
            "   Single-threaded: {}, Multi-threaded: {}",
            found_count, total_concurrent_found
        );
    }

    // Performance comparison
    let single_avg_ns = lookup_time.as_nanos() / 100_000;
    let concurrent_avg_ns = concurrent_time.as_nanos() / 100_000;

    println!("\n📊 Performance Summary:");
    println!("   Single-threaded: {} ns/lookup", single_avg_ns);
    println!("   Multi-threaded:  {} ns/lookup", concurrent_avg_ns);

    if concurrent_avg_ns <= single_avg_ns * 2 {
        println!("✅ Multi-threaded performance is acceptable (within 2x of single-threaded)");
    } else {
        println!("⚠️  Multi-threaded performance degradation detected");
    }

    Ok(())
}

#[cfg(not(feature = "learned-index"))]
fn main() {
    println!("This example requires the 'learned-index' feature to be enabled.");
    println!(
        "Run with: cargo run --example race_free_lookup_verification --features learned-index"
    );
}
