use kyrodb_engine::PersistentEventLog;
use std::time::{Duration, Instant};
use uuid::Uuid;
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test data directory
    let test_dir = std::path::PathBuf::from("/tmp/kyrodb_concurrent_benchmark");
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir)?;
    }
    std::fs::create_dir_all(&test_dir)?;
    
    println!("🚀 KyroDB Concurrent Benchmark");
    println!("📂 Test directory: {}", test_dir.display());
    
    // Test configuration
    let num_concurrent_writers = 10;
    let writes_per_writer = 1000;
    let total_writes = num_concurrent_writers * writes_per_writer;
    let test_value_size = 100; // 100 byte values
    
    println!("\n📊 Test Configuration:");
    println!("   • Concurrent writers: {}", num_concurrent_writers);
    println!("   • Writes per writer: {}", writes_per_writer);
    println!("   • Total writes: {}", total_writes);
    println!("   • Value size: {} bytes", test_value_size);
    
    // Test 1: With group commit disabled (legacy mode)
    println!("\n🔧 Test 1: Legacy mode (per-write fsync)");
    env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "false");
    env::set_var("KYRODB_FSYNC_POLICY", "data");
    
    let legacy_dir = test_dir.join("legacy");
    std::fs::create_dir_all(&legacy_dir)?;
    
    let log = Arc::new(PersistentEventLog::open(&legacy_dir).await?);
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
    
    let legacy_duration = start.elapsed();
    let legacy_throughput = total_writes as f64 / legacy_duration.as_secs_f64();
    
    println!("   ⏱️ Duration: {:?}", legacy_duration);
    println!("   📈 Throughput: {:.0} writes/sec", legacy_throughput);
    
    // Test 2: With group commit enabled
    println!("\n🚀 Test 2: Group commit mode");
    env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "true");
    env::set_var("KYRODB_GROUP_COMMIT_DELAY_MICROS", "500"); // 500µs max delay
    env::set_var("KYRODB_GROUP_COMMIT_BATCH_SIZE", "50"); // batch up to 50 writes
    env::set_var("KYRODB_FSYNC_POLICY", "data");
    
    let group_commit_dir = test_dir.join("group_commit");
    std::fs::create_dir_all(&group_commit_dir)?;
    
    let log = Arc::new(PersistentEventLog::open(&group_commit_dir).await?);
    
    // Allow background task to start
    tokio::time::sleep(Duration::from_millis(10)).await;
    
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
    
    let group_commit_duration = start.elapsed();
    let group_commit_throughput = total_writes as f64 / group_commit_duration.as_secs_f64();
    
    println!("   ⏱️ Duration: {:?}", group_commit_duration);
    println!("   📈 Throughput: {:.0} writes/sec", group_commit_throughput);
    
    // Calculate improvement
    let improvement = group_commit_throughput / legacy_throughput;
    
    println!("\n📊 Results Summary:");
    println!("   • Legacy mode: {:.0} writes/sec", legacy_throughput);
    println!("   • Group commit: {:.0} writes/sec", group_commit_throughput);
    println!("   • Improvement: {:.1}x faster", improvement);
    
    if improvement > 5.0 {
        println!("   ✅ EXCELLENT: {}x performance improvement!", improvement as u32);
    } else if improvement > 2.0 {
        println!("   ✅ GOOD: {}x performance improvement", improvement as u32);
    } else if improvement > 1.1 {
        println!("   ✅ MODEST: {:.1}x performance improvement", improvement);
    } else if improvement > 0.9 {
        println!("   ⚠️ MARGINAL: {:.1}x improvement (within noise)", improvement);
    } else {
        println!("   ❌ Performance regression detected!");
    }
    
    // Show if we're getting close to the target
    if group_commit_throughput > 50_000.0 {
        println!("   🎯 On track for 1M writes/sec target!");
    } else if group_commit_throughput > 10_000.0 {
        println!("   📈 Good progress toward 1M writes/sec target");
    }
    
    // Clean up
    std::fs::remove_dir_all(&test_dir)?;
    
    Ok(())
}
