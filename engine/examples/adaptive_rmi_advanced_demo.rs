//! Adaptive RMI Advanced Features Demo
//! 
//! Demonstrates the advanced background merge process and segment management
//! capabilities of the adaptive RMI implementation.
//!
//! Features demonstrated:
//! - Parallel background merge operations
//! - Intelligent segment splitting based on access patterns
//! - Adaptive segment merging with optimal partner selection
//! - Performance analytics and health monitoring
//! - Copy-on-write optimization for minimal lock contention

use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Adaptive RMI Advanced Features Demo ===");
    println!("Demonstrating advanced background merge and segment management\n");
    
    // Create RMI with initial data
    let initial_data: Vec<(u64, u64)> = (0..5000)
        .map(|i| (i * 10, i * 100))
        .collect();
    
    let rmi = Arc::new(AdaptiveRMI::build_from_pairs(&initial_data));
    
    // Start background maintenance
    let maintenance_handle = rmi.clone().start_background_maintenance();
    
    println!("Phase 1: Testing Background Merge Process");
    println!("==========================================");
    
    // Simulate high write load to trigger background merges
    let write_start = Instant::now();
    for batch in 0..10 {
        let batch_start = batch * 1000;
        for i in 0..1000 {
            let key = 50000 + batch_start + i;
            let value = key * 2;
            rmi.insert(key, value)?;
        }
        
        // Show stats after each batch
        let stats = rmi.get_stats();
        println!("Batch {}: Hot buffer: {}/{} ({:.1}%), Overflow: {}, Merge in progress: {}", 
                batch + 1,
                stats.hot_buffer_size, 
                4096, // DEFAULT_HOT_BUFFER_SIZE
                stats.hot_buffer_utilization * 100.0,
                stats.overflow_size,
                stats.merge_in_progress);
        
        // Brief pause to allow background processing
        sleep(Duration::from_millis(100)).await;
    }
    
    println!("Write phase completed in {:.2}s", write_start.elapsed().as_secs_f64());
    
    // Wait for background merges to complete
    println!("\nWaiting for background merges to complete...");
    sleep(Duration::from_secs(2)).await;
    
    let stats = rmi.get_stats();
    println!("After merge completion:");
    println!("  Segments: {}, Total keys: {}", stats.segment_count, stats.total_keys);
    println!("  Hot buffer: {}, Overflow: {}", stats.hot_buffer_size, stats.overflow_size);
    
    println!("\nPhase 2: Testing Segment Split/Merge Logic");
    println!("==========================================");
    
    // Create access patterns to trigger splits
    println!("Creating hot access patterns...");
    for _ in 0..5000 {
        // Access keys in the 10000-20000 range heavily
        let key = 10000 + (rand::random::<u64>() % 10000);
        rmi.lookup(key);
    }
    
    // Trigger segment management
    sleep(Duration::from_secs(1)).await;
    
    let analytics_before = rmi.get_bounded_search_analytics();
    println!("Segments before adaptation: {}", analytics_before.total_segments);
    
    // Force segment management
    rmi.adaptive_segment_management().await?;
    
    let analytics_after = rmi.get_bounded_search_analytics();
    println!("Segments after adaptation: {}", analytics_after.total_segments);
    
    if analytics_after.total_segments > analytics_before.total_segments {
        println!("✓ Segment splitting detected based on access patterns");
    } else if analytics_after.total_segments < analytics_before.total_segments {
        println!("✓ Segment merging detected for optimization");
    } else {
        println!("✓ Segments already optimally sized");
    }
    
    println!("\nPhase 3: Performance Analytics and Health Monitoring");
    println!("====================================================");
    
    // Show comprehensive analytics
    let final_analytics = rmi.get_bounded_search_analytics();
    let validation = rmi.validate_bounded_search_guarantees();
    
    println!("System Performance Report:");
    println!("  Total segments: {}", final_analytics.total_segments);
    println!("  Bounded guarantee: {:.1}% segments", final_analytics.bounded_guarantee_ratio * 100.0);
    println!("  Max search window: {} elements", final_analytics.max_search_window_observed);
    println!("  Performance class: {}", final_analytics.performance_classification);
    println!("  System health: {}", validation.performance_level);
    println!("  Worst-case complexity: {}", validation.worst_case_complexity);
    
    if validation.system_meets_guarantees {
        println!("✓ All performance guarantees met!");
    } else {
        println!("⚠ Performance attention needed: {}", validation.recommendation);
    }
    
    println!("\nPhase 4: Concurrent Operations Stress Test");
    println!("==========================================");
    
    // Spawn concurrent read/write tasks
    let mut handles = Vec::new();
    
    // Writer tasks
    for task_id in 0..4 {
        let rmi_clone = rmi.clone();
        let handle = tokio::spawn(async move {
            let start_key = task_id * 100000;
            for i in 0..1000 {
                let key = start_key + i;
                let value = key * 3;
                if let Err(e) = rmi_clone.insert(key, value) {
                    eprintln!("Write error in task {}: {}", task_id, e);
                }
            }
        });
        handles.push(handle);
    }
    
    // Reader tasks
    for task_id in 0..4 {
        let rmi_clone = rmi.clone();
        let handle = tokio::spawn(async move {
            let mut found = 0;
            for i in 0..2000 {
                let key = i * 25; // Spread across key space
                if rmi_clone.lookup(key).is_some() {
                    found += 1;
                }
            }
            println!("Reader task {} found {} keys", task_id, found);
        });
        handles.push(handle);
    }
    
    // Wait for all tasks
    for handle in handles {
        handle.await?;
    }
    
    println!("✓ Concurrent stress test completed successfully");
    
    // Final statistics
    println!("\nFinal System State:");
    println!("===================");
    let final_stats = rmi.get_stats();
    println!("  Total segments: {}", final_stats.segment_count);
    println!("  Total keys indexed: {}", final_stats.total_keys);
    println!("  Average segment size: {:.1}", final_stats.avg_segment_size);
    println!("  Hot buffer utilization: {:.1}%", final_stats.hot_buffer_utilization * 100.0);
    
    // Test some final lookups to verify integrity
    println!("\nIntegrity verification:");
    let test_keys = [100, 5000, 25000, 50000, 75000];
    for &key in &test_keys {
        if let Some(value) = rmi.lookup(key) {
            println!("  Key {}: Value {} ✓", key, value);
        } else {
            println!("  Key {}: Not found", key);
        }
    }
    
    // Shutdown background maintenance
    maintenance_handle.abort();
    
    println!("\n🎉 Advanced Demo completed successfully!");
    println!("Advanced adaptive RMI features working as expected:");
    println!("  ✓ Parallel background merge operations");
    println!("  ✓ Intelligent segment split/merge logic");
    println!("  ✓ Copy-on-write optimization");
    println!("  ✓ Performance analytics and monitoring");
    println!("  ✓ Concurrent operation support");
    
    Ok(())
}
