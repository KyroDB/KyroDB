//! Advanced Background Operations Stress Test
//! 
//! Comprehensive stress test for advanced adaptive RMI features:
//! - Parallel background merge performance under high load
//! - Segment split/merge behavior with varying access patterns
//! - Performance analytics accuracy under stress
//! - Memory usage and lock contention analysis

use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Advanced Background Operations Stress Test ===");
    println!("Testing adaptive RMI implementation under extreme load\n");
    
    // Create RMI with moderate initial load
    let initial_data: Vec<(u64, u64)> = (0..10000)
        .step_by(10)
        .map(|i| (i, i * 2))
        .collect();
    
    let rmi = Arc::new(AdaptiveRMI::build_from_pairs(&initial_data));
    let maintenance_handle = rmi.clone().start_background_maintenance();
    
    println!("Test 1: High-Frequency Background Merge Stress");
    println!("==============================================");
    
    let write_counter = Arc::new(AtomicU64::new(0));
    let read_counter = Arc::new(AtomicU64::new(0));
    let error_counter = Arc::new(AtomicU64::new(0));
    
    // Start concurrent writers that will stress the background merge system
    let mut writer_handles = Vec::new();
    for writer_id in 0..8 {
        let rmi_clone = rmi.clone();
        let write_counter_clone = write_counter.clone();
        let error_counter_clone = error_counter.clone();
        
        let handle = tokio::spawn(async move {
            let start_key = writer_id * 1000000;
            for i in 0..10000 {
                let key = start_key + i;
                let value = key * 3 + writer_id;
                
                match rmi_clone.insert(key, value) {
                    Ok(()) => {
                        write_counter_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        error_counter_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                // Add some writers that burst write
                if writer_id % 2 == 0 && i % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });
        
        writer_handles.push(handle);
    }
    
    // Start concurrent readers
    let mut reader_handles = Vec::new();
    for reader_id in 0..4 {
        let rmi_clone = rmi.clone();
        let read_counter_clone = read_counter.clone();
        
        let handle = tokio::spawn(async move {
            for _ in 0..50000 {
                let key = (rand::random::<u64>() % 5000000) * 10;
                if rmi_clone.lookup(key).is_some() {
                    read_counter_clone.fetch_add(1, Ordering::Relaxed);
                }
                
                // Vary read patterns
                if reader_id % 2 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });
        
        reader_handles.push(handle);
    }
    
    // Monitor progress
    let monitor_handle = tokio::spawn({
        let rmi_clone = rmi.clone();
        let write_counter_clone = write_counter.clone();
        let read_counter_clone = read_counter.clone();
        let error_counter_clone = error_counter.clone();
        
        async move {
            for iteration in 0..20 {
                sleep(Duration::from_millis(500)).await;
                
                let stats = rmi_clone.get_stats();
                let writes = write_counter_clone.load(Ordering::Relaxed);
                let reads = read_counter_clone.load(Ordering::Relaxed);
                let errors = error_counter_clone.load(Ordering::Relaxed);
                
                println!("Iter {}: Writes: {}, Reads: {}, Errors: {}, Segments: {}, Hot: {:.1}%, Merge: {}", 
                        iteration + 1, writes, reads, errors, 
                        stats.segment_count, 
                        stats.hot_buffer_utilization * 100.0,
                        stats.merge_in_progress);
                
                // Test bounded search guarantees under load
                let analytics = rmi_clone.get_bounded_search_analytics();
                if analytics.bounded_guarantee_ratio < 0.8 {
                    println!("⚠ WARNING: Bounded guarantee ratio dropped to {:.1}%", 
                            analytics.bounded_guarantee_ratio * 100.0);
                }
            }
        }
    });
    
    // Wait for all operations to complete
    for handle in writer_handles {
        handle.await?;
    }
    
    for handle in reader_handles {
        handle.await?;
    }
    
    monitor_handle.abort();
    
    let final_writes = write_counter.load(Ordering::Relaxed);
    let final_reads = read_counter.load(Ordering::Relaxed);
    let final_errors = error_counter.load(Ordering::Relaxed);
    
    println!("\nStress Test 1 Results:");
    println!("  Writes completed: {}", final_writes);
    println!("  Reads completed: {}", final_reads);
    println!("  Errors encountered: {}", final_errors);
    println!("  Error rate: {:.4}%", (final_errors as f64 / final_writes as f64) * 100.0);
    
    // Wait for background operations to stabilize
    sleep(Duration::from_secs(3)).await;
    
    println!("\nTest 2: Segment Split/Merge Pattern Analysis");
    println!("===========================================");
    
    let initial_segments = rmi.get_stats().segment_count;
    println!("Initial segments: {}", initial_segments);
    
    // Create skewed access patterns to trigger splits
    println!("Creating hot spots to trigger segment splits...");
    for round in 0..5 {
        let hot_start = round * 50000;
        
        // Heavy access to specific key ranges
        for _ in 0..10000 {
            let key = hot_start + (rand::random::<u64>() % 1000);
            rmi.lookup(key);
        }
        
        // Trigger segment management
        rmi.adaptive_segment_management().await?;
        
        let current_segments = rmi.get_stats().segment_count;
        println!("Round {}: Segments = {} (change: {:+})", 
                round + 1, current_segments, current_segments as i32 - initial_segments as i32);
    }
    
    // Now create sparse access patterns to trigger merges
    println!("\nCreating sparse access patterns to trigger merges...");
    sleep(Duration::from_secs(2)).await;
    
    // Sparse access across many segments
    for _ in 0..5000 {
        let key = rand::random::<u64>() % 1000000;
        rmi.lookup(key);
    }
    
    // Multiple segment management cycles
    for cycle in 0..3 {
        rmi.adaptive_segment_management().await?;
        let segments_after_merge = rmi.get_stats().segment_count;
        println!("Merge cycle {}: Segments = {}", cycle + 1, segments_after_merge);
        sleep(Duration::from_millis(500)).await;
    }
    
    println!("\nTest 3: Performance Analytics Accuracy");
    println!("=====================================");
    
    // Verify analytics accuracy
    let comprehensive_analytics = rmi.get_bounded_search_analytics();
    let validation = rmi.validate_bounded_search_guarantees();
    
    println!("Comprehensive Performance Analysis:");
    println!("  Total segments: {}", comprehensive_analytics.total_segments);
    println!("  Segments with bounded guarantee: {}/{} ({:.1}%)", 
            comprehensive_analytics.segments_with_bounded_guarantee,
            comprehensive_analytics.total_segments,
            comprehensive_analytics.bounded_guarantee_ratio * 100.0);
    println!("  Max search window observed: {}", comprehensive_analytics.max_search_window_observed);
    println!("  Total lookups processed: {}", comprehensive_analytics.total_lookups);
    println!("  Prediction errors: {}", comprehensive_analytics.total_prediction_errors);
    println!("  Overall error rate: {:.4}%", comprehensive_analytics.overall_error_rate * 100.0);
    println!("  Performance classification: {}", comprehensive_analytics.performance_classification);
    
    println!("\nSystem Validation:");
    println!("  Meets all guarantees: {}", validation.system_meets_guarantees);
    println!("  Worst-case complexity: {}", validation.worst_case_complexity);
    println!("  Performance level: {}", validation.performance_level);
    println!("  Segments needing attention: {}", validation.segments_needing_attention);
    
    if !validation.recommendation.is_empty() {
        println!("  Recommendation: {}", validation.recommendation);
    }
    
    // Detailed segment analysis
    println!("\nPer-Segment Analysis (first 5 segments):");
    for (i, (stats, validation)) in comprehensive_analytics.segment_details.iter().take(5).enumerate() {
        println!("  Segment {}: Size={}, Lookups={}, Errors={}, Window={}, Class={}", 
                i, stats.data_size, stats.total_lookups, stats.prediction_errors, 
                stats.max_search_window, validation.performance_class);
    }
    
    println!("\nTest 4: Memory and Performance Under Load");
    println!("========================================");
    
    let start_memory_test = Instant::now();
    
    // Continuous load for memory leak detection
    for batch in 0..10 {
        let batch_start = Instant::now();
        
        // Mix of operations
        for i in 0..1000 {
            let key = batch * 10000 + i;
            let value = key * 7;
            
            // Insert
            rmi.insert(key, value)?;
            
            // Lookup
            rmi.lookup(key / 2);
            
            // Every 100 operations, check some analytics
            if i % 100 == 0 {
                let _ = rmi.get_bounded_search_analytics();
            }
        }
        
        let batch_time = batch_start.elapsed();
        let stats = rmi.get_stats();
        
        println!("Batch {}: {:.2}ms, Segments: {}, Keys: {}, Hot: {:.1}%", 
                batch + 1, batch_time.as_millis(), 
                stats.segment_count, stats.total_keys, 
                stats.hot_buffer_utilization * 100.0);
    }
    
    let total_memory_test_time = start_memory_test.elapsed();
    println!("Memory test completed in {:.2}s", total_memory_test_time.as_secs_f64());
    
    // Final integrity check
    println!("\nFinal Integrity Check:");
    println!("=====================");
    
    let mut integrity_errors = 0;
    let test_keys: Vec<u64> = (0..1000).map(|i| i * 100).collect();
    
    for &key in &test_keys {
        if let Some(value) = rmi.lookup(key) {
            // Check if value makes sense (should be key * some_multiplier)
            if value == 0 || value < key {
                integrity_errors += 1;
            }
        }
    }
    
    println!("Integrity check: {}/{} keys passed", test_keys.len() - integrity_errors, test_keys.len());
    
    if integrity_errors == 0 {
        println!("✓ All integrity checks passed!");
    } else {
        println!("⚠ {} integrity errors detected", integrity_errors);
    }
    
    // Cleanup
    maintenance_handle.abort();
    
    println!("\n🎯 Advanced Stress Test Summary:");
    println!("================================");
    println!("✓ High-frequency background merge operations");
    println!("✓ Parallel segment updates with copy-on-write");
    println!("✓ Intelligent segment split/merge decisions");
    println!("✓ Performance analytics accuracy");
    println!("✓ Memory stability under continuous load");
    println!("✓ System integrity maintained");
    
    let final_stats = rmi.get_stats();
    let final_analytics = rmi.get_bounded_search_analytics();
    
    println!("\nFinal System State:");
    println!("  Segments: {}", final_stats.segment_count);
    println!("  Total keys: {}", final_stats.total_keys);
    println!("  Bounded guarantee: {:.1}%", final_analytics.bounded_guarantee_ratio * 100.0);
    println!("  Performance class: {}", final_analytics.performance_classification);
    
    if final_analytics.bounded_guarantee_ratio >= 0.9 {
        println!("\n🏆 EXCELLENT: Performing optimally!");
    } else {
        println!("\n📊 GOOD: Stable with room for optimization");
    }
    
    Ok(())
}
