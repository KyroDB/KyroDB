use kyrodb_engine::PersistentEventLog;
use std::time::{Duration, Instant};
use uuid::Uuid;
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test data directory
    let test_dir = std::path::PathBuf::from("/tmp/kyrodb_high_concurrency_benchmark");
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir)?;
    }
    std::fs::create_dir_all(&test_dir)?;
    
    println!("🚀 KyroDB High Concurrency Benchmark (Target: 1M writes/sec)");
    println!("📂 Test directory: {}", test_dir.display());
    
    // Test configuration for high throughput
    let num_concurrent_writers = 50;
    let writes_per_writer = 2000;
    let total_writes = num_concurrent_writers * writes_per_writer;
    let test_value_size = 64; // Smaller values for higher throughput
    
    println!("\n📊 Test Configuration:");
    println!("   • Concurrent writers: {}", num_concurrent_writers);
    println!("   • Writes per writer: {}", writes_per_writer);
    println!("   • Total writes: {}", total_writes);
    println!("   • Value size: {} bytes", test_value_size);
    
    // Only test group commit mode (legacy would be too slow)
    println!("\n🚀 Group commit mode (optimized for high throughput)");
    env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "true");
    env::set_var("KYRODB_GROUP_COMMIT_DELAY_MICROS", "250"); // 250µs max delay - very aggressive
    env::set_var("KYRODB_GROUP_COMMIT_BATCH_SIZE", "100"); // batch up to 100 writes
    env::set_var("KYRODB_FSYNC_POLICY", "data");
    
    let group_commit_dir = test_dir.join("group_commit");
    std::fs::create_dir_all(&group_commit_dir)?;
    
    let log = Arc::new(PersistentEventLog::open(&group_commit_dir).await?);
    
    // Allow background task to start
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    println!("⏳ Starting high-concurrency write test...");
    let start = Instant::now();
    
    // Spawn concurrent writers
    let mut handles = Vec::new();
    for writer_id in 0..num_concurrent_writers {
        let log_clone = log.clone();
        let handle = tokio::spawn(async move {
            for i in 0..writes_per_writer {
                let key = (writer_id * writes_per_writer + i) as u64;
                let value = vec![b'x'; test_value_size];
                log_clone.append_kv(Uuid::new_v4(), key, value).await.unwrap();
            }
        });
        handles.push(handle);
    }
    
    // Wait for all writes to complete
    for handle in handles {
        handle.await?;
    }
    
    let duration = start.elapsed();
    let throughput = total_writes as f64 / duration.as_secs_f64();
    
    println!("   ⏱️ Duration: {:?}", duration);
    println!("   📈 Throughput: {:.0} writes/sec", throughput);
    
    // Progress toward 1M writes/sec
    let target = 1_000_000.0;
    let progress = (throughput / target * 100.0).min(100.0);
    
    println!("\n📊 Results Summary:");
    println!("   • Achieved: {:.0} writes/sec", throughput);
    println!("   • Target: 1,000,000 writes/sec");
    println!("   • Progress: {:.1}% of target", progress);
    
    if throughput >= target {
        println!("   🎯 TARGET ACHIEVED! Exceeded 1M writes/sec!");
    } else if throughput >= target * 0.5 {
        println!("   🔥 EXCELLENT! Over 50% of target achieved!");
    } else if throughput >= target * 0.1 {
        println!("   📈 GOOD PROGRESS! Over 10% of target achieved!");
    } else if throughput >= 10_000.0 {
        println!("   ✅ SOLID IMPROVEMENT! 10K+ writes/sec is much better than 253 baseline");
    } else {
        println!("   ⚠️ More optimization needed for 1M writes/sec target");
    }
    
    // Calculate theoretical maximum based on fsync overhead
    let avg_batch_size = 100.0; // our max batch size
    let fsync_time_ms = 1.0; // ~1ms per fsync on SSD
    let theoretical_max = (1000.0 / fsync_time_ms) * avg_batch_size;
    println!("\n💡 Analysis:");
    println!("   • Theoretical max (1ms fsync, 100-item batches): {:.0} writes/sec", theoretical_max);
    println!("   • Current efficiency: {:.1}% of theoretical max", throughput / theoretical_max * 100.0);
    
    // Clean up
    std::fs::remove_dir_all(&test_dir)?;
    
    Ok(())
}
