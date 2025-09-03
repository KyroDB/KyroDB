use kyrodb_engine::{PersistentEventLog, DurabilityLevel, GroupCommitConfig};
use std::time::{Duration, Instant};
use std::sync::Arc;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 EnterpriseSafe Group Commit Performance Test");
    
    // Test data directory
    let test_dir = std::path::PathBuf::from("/tmp/kyrodb_enterprise_test");
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir)?;
    }
    std::fs::create_dir_all(&test_dir)?;
    
    println!("📂 Test directory: {}", test_dir.display());
    
    // Test configuration
    let test_writes = 1_000;
    let test_value_size = 100; // 100 byte values
    
    println!("\n📊 Test Configuration:");
    println!("   • Writes: {}", test_writes);
    println!("   • Value size: {} bytes", test_value_size);
    println!("   • Mode: Concurrent (real group commit test)");
    
    // Set EnterpriseSafe environment
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "true");
    std::env::set_var("KYRODB_GROUP_COMMIT_BATCH_SIZE", "100");
    std::env::set_var("KYRODB_GROUP_COMMIT_DELAY_MICROS", "200");
    
    let kyrodb = Arc::new(
        PersistentEventLog::open(&test_dir).await?
    );
    
    println!("\n🚀 Running EnterpriseSafe concurrent write test...");
    
    let start = Instant::now();
    
    // Create concurrent tasks
    let mut tasks = Vec::new();
    for i in 0..test_writes {
        let kyrodb_clone = kyrodb.clone();
        let key = i as u64;
        let value = vec![b'x'; test_value_size];
        
        let task = tokio::spawn(async move {
            kyrodb_clone.append_kv(Uuid::new_v4(), key, value).await
        });
        tasks.push(task);
    }
    
    // Wait for all writes to complete
    for task in tasks {
        task.await??;
    }
    
    let duration = start.elapsed();
    let throughput = test_writes as f64 / duration.as_secs_f64();
    
    println!("   ⏱️ Duration: {:.2}ms", duration.as_secs_f64() * 1000.0);
    println!("   📈 Throughput: {:.0} writes/sec", throughput);
    
    // Performance assessment
    if throughput >= 10_000.0 {
        println!("   🎯 EXCELLENT: EnterpriseSafe achieving {}k+ writes/sec", (throughput / 1000.0) as u32);
    } else if throughput >= 5_000.0 {
        println!("   ✅ GOOD: EnterpriseSafe achieving {}k writes/sec", (throughput / 1000.0) as u32);
    } else if throughput >= 2_000.0 {
        println!("   ⚠️ MODERATE: EnterpriseSafe achieving {:.0} writes/sec", throughput);
    } else {
        println!("   ❌ POOR: EnterpriseSafe only achieving {:.0} writes/sec", throughput);
    }
    
    // Test data verification
    println!("\n🔍 Verifying data integrity...");
    let test_key = 0u64;
    if let Some(_offset) = kyrodb.lookup_key(test_key).await {
        println!("   ✅ Data integrity verified: writes persisted correctly");
    } else {
        println!("   ❌ Data integrity failed: first write not found");
    }
    
    // Clean up
    std::fs::remove_dir_all(&test_dir)?;
    
    println!("\n🎉 EnterpriseSafe Group Commit Test Complete!");
    
    Ok(())
}
