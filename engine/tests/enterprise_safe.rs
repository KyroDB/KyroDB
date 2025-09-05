use kyrodb_engine::{KyroDb, DurabilityLevel, GroupCommitConfig};
use anyhow::Result;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;

#[tokio::test]
async fn test_enterprise_safe_group_commit() -> Result<()> {
    println!("🚀 Testing EnterpriseSafe Group Commit Implementation");
    
    // Initialize temporary directory for testing
    let temp_dir = tempfile::tempdir()?;
    let wal_path = temp_dir.path().join("test.wal");
    
    // Enterprise-Safe Configuration
    let config = GroupCommitConfig {
        max_batch_size: 1000,       // Enterprise batch size for performance
        max_delay: Duration::from_micros(500), // 500µs max delay for responsiveness
        durability: DurabilityLevel::EnterpriseSafe, // Zero data loss mode
    };
    
    let kyrodb = Arc::new(
        KyroDb::with_group_commit(wal_path, config).await?
    );
    
    println!("✅ KyroDB initialized with EnterpriseSafe mode");
    
    // Test concurrent writes performance
    println!("\n📊 Testing concurrent write performance");
    let concurrent_writes = 100;
    let start = Instant::now();
    
    let mut tasks = Vec::new();
    for i in 0..concurrent_writes {
        let kyrodb_clone = kyrodb.clone();
        let key = format!("concurrent_key_{}", i);
        let value = serde_json::json!({
            "id": i,
            "message": "Enterprise concurrent write",
            "durability": "EnterpriseSafe"
        }).to_string();
        
        let task = tokio::spawn(async move {
            kyrodb_clone.append(key, value).await
        });
        tasks.push(task);
    }
    
    // Wait for all writes to complete
    for task in tasks {
        task.await??;
    }
    
    let concurrent_duration = start.elapsed();
    let writes_per_sec = concurrent_writes as f64 / concurrent_duration.as_secs_f64();
    
    println!("   {} writes completed in {:.2}ms", concurrent_writes, concurrent_duration.as_secs_f64() * 1000.0);
    println!("   Enterprise-safe throughput: {:.0} writes/sec", writes_per_sec);
    
    // Verify the results
    assert!(writes_per_sec > 1000.0, "Enterprise-safe mode should achieve at least 1000 writes/sec");
    
    println!("🎉 EnterpriseSafe Group Commit Test Complete!");
    
    Ok(())
}
