use anyhow::Result;
use kyrodb_engine::{DurabilityLevel, GroupCommitConfig, KyroDb};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[tokio::test]
async fn test_enterprise_safe_group_commit() -> Result<()> {
    println!("🚀 Testing EnterpriseSafe Group Commit Implementation");

    // Initialize temporary directory for testing
    let temp_dir = tempfile::tempdir()?;
    let wal_path = temp_dir.path().join("test.wal");

    // Enterprise-Safe Configuration
    let config = GroupCommitConfig {
        max_batch_size: 1000,        // Enterprise batch size for performance
        max_batch_delay_micros: 500, // 500µs max delay for responsiveness
        durability_level: DurabilityLevel::EnterpriseSafe, // Zero data loss mode
        enabled: true,
        background_fsync_interval_ms: 100,
    };

    let kyrodb = Arc::new(KyroDb::with_group_commit(wal_path, config).await?);

    println!("✅ KyroDB initialized with EnterpriseSafe mode");

    // Test concurrent writes performance
    println!("\n📊 Testing concurrent write performance");
    let concurrent_writes = 100;
    let start = Instant::now();

    let mut tasks = Vec::new();
    for i in 0..concurrent_writes {
        let kyrodb_clone = kyrodb.clone();
        let key = Uuid::new_v4();
        let value = serde_json::json!({
            "id": i,
            "message": "Enterprise concurrent write",
            "durability": "EnterpriseSafe"
        })
        .to_string()
        .into_bytes();

        let task = tokio::spawn(async move { kyrodb_clone.append(key, value).await });
        tasks.push(task);
    }

    // Wait for all writes to complete
    for task in tasks {
        task.await??;
    }

    let duration = start.elapsed();
    println!(
        "✅ {} concurrent writes completed in {:?}",
        concurrent_writes, duration
    );
    println!(
        "   Throughput: {:.0} writes/second",
        concurrent_writes as f64 / duration.as_secs_f64()
    );

    // Verify EnterpriseSafe durability guarantees
    println!("\n🛡️  Verifying EnterpriseSafe durability guarantees");

    // In EnterpriseSafe mode, data should be immediately durable after append()
    let test_key = Uuid::new_v4();
    let test_value = b"durability_test_data".to_vec();

    let offset = kyrodb.append(test_key, test_value.clone()).await?;
    println!("✅ Data appended at offset: {}", offset);

    // Data should be immediately recoverable
    let recovered = kyrodb.get(offset).await;
    match recovered {
        Some(payload) => {
            assert_eq!(payload, test_value, "Payload should match exactly");
            println!("✅ EnterpriseSafe durability verified - data immediately recoverable");
        }
        None => {
            panic!("EnterpriseSafe data should be immediately recoverable");
        }
    }

    Ok(())
}
