use kyrodb_engine::memory::{MemoryManager};
use std::time::Instant;

/// Benchmark demonstrating the performance improvement of fast-path memory allocation
fn main() {
    println!("🧠 KyroDB Memory Manager Fast-Path Benchmark");
    println!("==========================================");
    
    let memory_manager = MemoryManager::new();
    
    // Benchmark parameters
    const ITERATIONS: usize = 100_000;
    const SMALL_ALLOCATION_SIZE: usize = 1024;  // 1KB
    const MEDIUM_ALLOCATION_SIZE: usize = 32 * 1024; // 32KB
    
    // Warm up
    println!("🔥 Warming up...");
    for _ in 0..1000 {
        let buf = memory_manager.allocate_fast(SMALL_ALLOCATION_SIZE);
        memory_manager.deallocate_fast(buf);
    }
    
    println!("\n📊 Benchmarking Small Allocations ({}KB)", SMALL_ALLOCATION_SIZE / 1024);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    // Test 1: Normal allocation path
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let buf = match memory_manager.allocate(SMALL_ALLOCATION_SIZE) {
            kyrodb_engine::memory::MemoryResult::Success(buf) => buf,
            kyrodb_engine::memory::MemoryResult::CacheEvicted(buf) => buf,
            kyrodb_engine::memory::MemoryResult::OutOfMemory => Vec::new(),
        };
        memory_manager.deallocate(buf);
    }
    let normal_duration = start.elapsed();
    
    // Test 2: Fast path allocation
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let buf = memory_manager.allocate_fast(SMALL_ALLOCATION_SIZE);
        memory_manager.deallocate_fast(buf);
    }
    let fast_duration = start.elapsed();
    
    // Test 3: Benchmark mode allocation
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let buf = memory_manager.allocate_benchmark(SMALL_ALLOCATION_SIZE);
        memory_manager.deallocate_benchmark(buf);
    }
    let benchmark_duration = start.elapsed();
    
    println!("Normal Path:    {:8.2}ms ({:6.0} ops/sec)", 
             normal_duration.as_secs_f64() * 1000.0,
             ITERATIONS as f64 / normal_duration.as_secs_f64());
    
    println!("Fast Path:      {:8.2}ms ({:6.0} ops/sec)", 
             fast_duration.as_secs_f64() * 1000.0,
             ITERATIONS as f64 / fast_duration.as_secs_f64());
    
    println!("Benchmark Mode: {:8.2}ms ({:6.0} ops/sec)", 
             benchmark_duration.as_secs_f64() * 1000.0,
             ITERATIONS as f64 / benchmark_duration.as_secs_f64());
    
    let speedup_fast = normal_duration.as_secs_f64() / fast_duration.as_secs_f64();
    let speedup_bench = normal_duration.as_secs_f64() / benchmark_duration.as_secs_f64();
    
    println!("\n🚀 Performance Gains:");
    println!("Fast Path:      {:.1}x faster", speedup_fast);
    println!("Benchmark Mode: {:.1}x faster", speedup_bench);
    
    // Test medium-sized allocations 
    println!("\n📊 Benchmarking Medium Allocations ({}KB)", MEDIUM_ALLOCATION_SIZE / 1024);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let medium_iterations = ITERATIONS / 10; // Fewer iterations for larger allocations
    
    // Normal path for medium allocations
    let start = Instant::now();
    for _ in 0..medium_iterations {
        let buf = match memory_manager.allocate(MEDIUM_ALLOCATION_SIZE) {
            kyrodb_engine::memory::MemoryResult::Success(buf) => buf,
            kyrodb_engine::memory::MemoryResult::CacheEvicted(buf) => buf,
            kyrodb_engine::memory::MemoryResult::OutOfMemory => Vec::new(),
        };
        memory_manager.deallocate(buf);
    }
    let medium_normal_duration = start.elapsed();
    
    // Fast path for medium allocations  
    let start = Instant::now();
    for _ in 0..medium_iterations {
        let buf = memory_manager.allocate_fast(MEDIUM_ALLOCATION_SIZE);
        memory_manager.deallocate_fast(buf);
    }
    let medium_fast_duration = start.elapsed();
    
    println!("Normal Path:    {:8.2}ms ({:6.0} ops/sec)", 
             medium_normal_duration.as_secs_f64() * 1000.0,
             medium_iterations as f64 / medium_normal_duration.as_secs_f64());
    
    println!("Fast Path:      {:8.2}ms ({:6.0} ops/sec)", 
             medium_fast_duration.as_secs_f64() * 1000.0,
             medium_iterations as f64 / medium_fast_duration.as_secs_f64());
    
    let medium_speedup = medium_normal_duration.as_secs_f64() / medium_fast_duration.as_secs_f64();
    println!("Fast Path:      {:.1}x faster", medium_speedup);
    
    // Memory usage statistics
    println!("\n💾 Memory Statistics:");
    println!("━━━━━━━━━━━━━━━━━━━━━");
    let stats = memory_manager.stats();
    println!("Total Allocated:   {:8} bytes", stats.total_allocated);
    println!("Peak Allocated:    {:8} bytes", stats.peak_allocated);
    println!("Allocation Count:  {:8}", stats.allocation_count);
    println!("Pool Hits:         {:8}", stats.pool_hits);
    println!("Pool Misses:       {:8}", stats.pool_misses);
    println!("Memory Pressure:   {:?}", stats.pressure);
    
    println!("\n✅ Fast-path memory allocation provides significant performance improvements!");
    println!("   • Small allocations: Up to {:.0}x faster", speedup_fast);
    println!("   • Benchmark mode: Up to {:.0}x faster for maximum performance", speedup_bench);
    println!("   • Perfect for hot-path database operations");
}
