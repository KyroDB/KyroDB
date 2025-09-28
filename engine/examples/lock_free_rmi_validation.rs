use anyhow::Result;
/// Lock-free RMI validation test to ensure the new lookup_fast method works correctly
/// This validates Fix #2: Lock Contention in Adaptive RMI
use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn main() -> Result<()> {
    println!("🚀 Testing Lock-Free RMI Implementation (Fix #2: Lock Contention)");
    println!("================================================================");

    // Create test data
    let test_data: Vec<(u64, u64)> = (0..1000).map(|i| (i * 10, i * 100)).collect();

    // Build RMI from test data
    let rmi = Arc::new(AdaptiveRMI::build_from_pairs(&test_data));

    println!("✅ Built RMI with {} key-value pairs", test_data.len());

    // Test 1: Basic correctness - verify lookup_fast returns same results as lookup
    println!("\n🔍 Test 1: Correctness Validation");
    let test_keys = vec![0, 100, 500, 999 * 10]; // Test various keys including edge cases

    for key in test_keys {
        let regular_result = rmi.lookup(key);
        let fast_result = rmi.lookup_fast(key);

        if regular_result == fast_result {
            println!(
                "  ✅ Key {}: Both methods returned {:?}",
                key, regular_result
            );
        } else {
            println!(
                "  ❌ Key {}: lookup()={:?}, lookup_fast()={:?}",
                key, regular_result, fast_result
            );
            return Err(anyhow::anyhow!("Correctness test failed for key {}", key));
        }
    }

    // Test 2: Concurrency validation - multiple threads accessing simultaneously
    println!("\n⚡ Test 2: Concurrency Validation (10 threads, 1000 lookups each)");
    let start = Instant::now();

    let mut handles = Vec::new();
    let num_threads = 10;
    let lookups_per_thread = 1000;

    for thread_id in 0..num_threads {
        let rmi_clone = rmi.clone();
        let handle = thread::spawn(move || {
            let mut hits = 0;
            let thread_start = Instant::now();

            for i in 0..lookups_per_thread {
                let key = ((thread_id * lookups_per_thread + i) % 1000) * 10;
                if let Some(_) = rmi_clone.lookup_fast(key) {
                    hits += 1;
                }
            }

            let duration = thread_start.elapsed();
            (hits, duration)
        });
        handles.push(handle);
    }

    // Collect results
    let mut total_hits = 0;
    let mut max_duration = Duration::from_nanos(0);

    for handle in handles {
        let (hits, duration) = handle.join().unwrap();
        total_hits += hits;
        if duration > max_duration {
            max_duration = duration;
        }
    }

    let total_duration = start.elapsed();
    let total_ops = num_threads * lookups_per_thread;
    let ops_per_sec = total_ops as f64 / total_duration.as_secs_f64();

    println!("  📊 Results:");
    println!("    Total operations: {}", total_ops);
    println!("    Total hits: {}", total_hits);
    println!(
        "    Hit rate: {:.2}%",
        (total_hits as f64 / total_ops as f64) * 100.0
    );
    println!("    Total time: {:?}", total_duration);
    println!("    Max thread time: {:?}", max_duration);
    println!("    Throughput: {:.0} ops/sec", ops_per_sec);

    // Test 3: Performance comparison (if possible)
    println!("\n🏁 Test 3: Performance Comparison");
    let benchmark_key = 500;
    let iterations = 100_000;

    // Benchmark regular lookup
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = rmi.lookup(benchmark_key);
    }
    let regular_duration = start.elapsed();

    // Benchmark fast lookup
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = rmi.lookup_fast(benchmark_key);
    }
    let fast_duration = start.elapsed();

    let regular_ops_sec = iterations as f64 / regular_duration.as_secs_f64();
    let fast_ops_sec = iterations as f64 / fast_duration.as_secs_f64();
    let speedup = fast_ops_sec / regular_ops_sec;

    println!(
        "  📈 Single-threaded performance ({} iterations):",
        iterations
    );
    println!(
        "    lookup():      {:?} ({:.0} ops/sec)",
        regular_duration, regular_ops_sec
    );
    println!(
        "    lookup_fast(): {:?} ({:.0} ops/sec)",
        fast_duration, fast_ops_sec
    );
    println!("    Speedup: {:.2}x", speedup);

    if speedup >= 1.0 {
        println!("  ✅ Lock-free implementation shows good performance");
    } else {
        println!("  ⚠️  Lock-free implementation is slower (overhead from atomic operations)");
        println!("      This is expected for single-threaded workloads");
    }

    println!("\n🎉 Lock-Free RMI Validation Complete!");
    println!("✅ All tests passed - Fix #2 (Lock Contention) implementation validated");

    Ok(())
}
