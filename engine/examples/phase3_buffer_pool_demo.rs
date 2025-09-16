#!/usr/bin/env cargo +nightly run --example

//! Phase 3 Buffer Pool Demonstration
//! 
//! This example showcases the enterprise-grade zero-allocation buffer pool
//! with multiple size categories, memory pressure handling, and comprehensive
//! performance statistics.

use kyrodb_engine::{get_ultra_fast_pool, BufferSize, PoolHealth};

fn main() {
    println!("🚀 Phase 3: Ultra-Fast Buffer Pool Demonstration");
    println!("================================================");
    
    // Get the singleton buffer pool
    let pool = get_ultra_fast_pool();
    
    // Test different buffer sizes
    test_json_buffers(pool);
    test_binary_buffers(pool);
    test_performance_characteristics(pool);
    
    // Show final statistics
    show_final_stats(pool);
}

fn test_json_buffers(pool: &kyrodb_engine::UltraFastBufferPool) {
    println!("\n📄 Testing JSON Buffer Management:");
    println!("--------------------------------");
    
    // Test small buffers (80% of requests)
    let mut small_buffers = Vec::new();
    for i in 0..10 {
        let mut buf = pool.get_json_buffer_sized(BufferSize::Small);
        buf.push_str(&format!("{{\"id\":{},\"type\":\"small\"}}", i));
        small_buffers.push(buf);
    }
    println!("✅ Allocated 10 small JSON buffers");
    
    // Return them to pool
    for buf in small_buffers {
        pool.return_json_buffer_sized(buf, BufferSize::Small);
    }
    println!("✅ Returned 10 small JSON buffers to pool");
    
    // Test medium buffers (18% of requests)
    let mut medium_buf = pool.get_json_buffer_sized(BufferSize::Medium);
    medium_buf.push_str(&format!("{{\"batch\":[{}],\"size\":\"medium\"}}", 
        (0..50).map(|i| format!("{{\"id\":{}}}", i)).collect::<Vec<_>>().join(",")));
    println!("✅ Allocated 1 medium JSON buffer with batch data");
    pool.return_json_buffer_sized(medium_buf, BufferSize::Medium);
    
    // Test large buffers (2% of requests)  
    let mut large_buf = pool.get_json_buffer_sized(BufferSize::Large);
    large_buf.push_str(&format!("{{\"large_payload\":\"{}\"}}", "x".repeat(2048)));
    println!("✅ Allocated 1 large JSON buffer with large payload");
    pool.return_json_buffer_sized(large_buf, BufferSize::Large);
}

fn test_binary_buffers(pool: &kyrodb_engine::UltraFastBufferPool) {
    println!("\n🔢 Testing Binary Buffer Management:");
    println!("----------------------------------");
    
    // Test small binary buffers
    let mut small_binary = pool.get_binary_buffer_sized(BufferSize::Small);
    small_binary.extend_from_slice(b"small_binary_data");
    println!("✅ Allocated small binary buffer ({}B)", small_binary.len());
    pool.return_binary_buffer_sized(small_binary, BufferSize::Small);
    
    // Test medium binary buffers
    let mut medium_binary = pool.get_binary_buffer_sized(BufferSize::Medium);
    medium_binary.extend_from_slice(&vec![0u8; 256]); // 256 bytes of data
    println!("✅ Allocated medium binary buffer ({}B)", medium_binary.len());
    pool.return_binary_buffer_sized(medium_binary, BufferSize::Medium);
    
    // Test large binary buffers
    let mut large_binary = pool.get_binary_buffer_sized(BufferSize::Large);
    large_binary.extend_from_slice(&vec![1u8; 1024]); // 1KB of data
    println!("✅ Allocated large binary buffer ({}B)", large_binary.len());
    pool.return_binary_buffer_sized(large_binary, BufferSize::Large);
}

fn test_performance_characteristics(pool: &kyrodb_engine::UltraFastBufferPool) {
    println!("\n⚡ Testing Performance Characteristics:");
    println!("------------------------------------");
    
    use std::time::Instant;
    
    // Benchmark buffer allocation/return cycles
    let start = Instant::now();
    let iterations = 1000;
    
    for _ in 0..iterations {
        // Simulate typical request pattern: 80% small, 18% medium, 2% large
        for _ in 0..80 {
            let buf = pool.get_json_buffer_sized(BufferSize::Small);
            pool.return_json_buffer_sized(buf, BufferSize::Small);
        }
        
        for _ in 0..18 {
            let buf = pool.get_json_buffer_sized(BufferSize::Medium);
            pool.return_json_buffer_sized(buf, BufferSize::Medium);
        }
        
        for _ in 0..2 {
            let buf = pool.get_json_buffer_sized(BufferSize::Large);
            pool.return_json_buffer_sized(buf, BufferSize::Large);
        }
    }
    
    let duration = start.elapsed();
    let ops_per_sec = (iterations * 100) as f64 / duration.as_secs_f64();
    
    println!("✅ Completed {} allocation/return cycles in {:?}", iterations * 100, duration);
    println!("✅ Performance: {:.0} ops/sec", ops_per_sec);
    
    // Check pool health
    let health = pool.health_check();
    println!("✅ Pool Health: {:?}", health);
    
    match health {
        PoolHealth::Excellent => println!("   🟢 Pool is performing optimally!"),
        PoolHealth::Good => println!("   🟡 Pool is performing well"),
        PoolHealth::Fair => println!("   🟠 Pool performance is acceptable"),
        PoolHealth::Poor => println!("   🔴 Pool performance needs attention"),
    }
}

fn show_final_stats(pool: &kyrodb_engine::UltraFastBufferPool) {
    println!("\n📊 Final Pool Statistics:");
    println!("========================");
    
    let stats = pool.stats();
    
    println!("📈 Allocations: {}", stats.allocations);
    println!("♻️  Reuses: {}", stats.reuses);
    println!("🎯 Cache Hits: {}", stats.cache_hits);
    println!("❌ Cache Misses: {}", stats.cache_misses);
    println!("💧 Memory Pressure Drops: {}", stats.memory_pressure_drops);
    println!("📊 Cache Hit Rate: {:.2}%", stats.cache_hit_rate);
    println!("💾 Current Memory Usage: {}B", stats.current_memory_usage);
    
    let efficiency = if stats.allocations > 0 {
        (stats.reuses as f64 / (stats.reuses + stats.allocations) as f64) * 100.0
    } else {
        100.0
    };
    
    println!("🚀 Buffer Reuse Efficiency: {:.2}%", efficiency);
    
    if efficiency > 95.0 {
        println!("🏆 EXCELLENT: Zero-allocation goal achieved!");
    } else if efficiency > 90.0 {
        println!("✅ GOOD: Near zero-allocation performance");
    } else if efficiency > 80.0 {
        println!("⚠️  FAIR: Some allocations occurring");
    } else {
        println!("🔥 ATTENTION: High allocation rate detected");
    }
}
