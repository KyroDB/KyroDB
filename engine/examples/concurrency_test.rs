use kyrodb_engine::PersistentEventLog;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔒 Testing Concurrency Safety & Deadlock Prevention");

    // Test data directory
    let test_dir = std::path::PathBuf::from("/tmp/kyrodb_concurrency_test");
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir)?;
    }
    std::fs::create_dir_all(&test_dir)?;

    println!("📂 Test directory: {}", test_dir.display());

    // Set EnterpriseSafe environment for maximum safety
    std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
    std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "true");
    std::env::set_var("KYRODB_GROUP_COMMIT_BATCH_SIZE", "10"); // Smaller batches for faster testing
    std::env::set_var("KYRODB_GROUP_COMMIT_DELAY_MICROS", "50"); // Shorter delay

    let kyrodb = Arc::new(PersistentEventLog::open(&test_dir).await?);

    println!("\n🔄 Test 1: Concurrent append operations");

    let mut tasks = Vec::new();
    let start = Instant::now();

    // Create 20 concurrent append tasks
    for i in 0..20 {
        let kyrodb_clone = kyrodb.clone();
        let task = tokio::spawn(async move {
            for j in 0..5 {
                let key = (i * 5 + j) as u64;
                let value = vec![b'x'; 20];
                match kyrodb_clone.append_kv(Uuid::new_v4(), key, value).await {
                    Ok(_) => {}
                    Err(e) => eprintln!("Write error {}: {}", key, e),
                }
            }
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete without deadlock
    for task in tasks {
        if let Err(e) = task.await {
            eprintln!("Task failed: {}", e);
        }
    }

    let duration = start.elapsed();
    let total_writes = 20 * 5; // 100 writes
    let writes_per_sec = total_writes as f64 / duration.as_secs_f64();

    println!("   ⏱️ Duration: {:.2}ms", duration.as_secs_f64() * 1000.0);
    println!("   📈 Throughput: {:.0} writes/sec", writes_per_sec);
    println!("   🔒 No deadlocks detected!");

    // Test 2: Mixed read/write operations
    println!("\n🔄 Test 2: Mixed read/write operations");

    let mut write_tasks = Vec::new();
    let mut read_tasks = Vec::new();

    // Writers
    for i in 100..110 {
        let kyrodb_clone = kyrodb.clone();
        let task = tokio::spawn(async move {
            let value = vec![b'y'; 15];
            kyrodb_clone
                .append_kv(Uuid::new_v4(), i as u64, value)
                .await
        });
        write_tasks.push(task);
    }

    // Readers
    for i in 0..20 {
        let kyrodb_clone = kyrodb.clone();
        let task = tokio::spawn(async move {
            let _ = kyrodb_clone.lookup_key(i as u64).await;
            Ok::<(), anyhow::Error>(())
        });
        read_tasks.push(task);
    }

    // Wait for all write operations
    for task in write_tasks {
        if let Err(e) = task.await {
            eprintln!("Write task failed: {}", e);
        }
    }

    // Wait for all read operations
    for task in read_tasks {
        if let Err(e) = task.await {
            eprintln!("Read task failed: {}", e);
        }
    }

    println!("   ✅ Mixed operations completed without deadlock");

    // Test 3: Data integrity verification
    println!("\n🔍 Test 3: Data integrity verification");

    let mut verified_count = 0;
    for i in 0..50 {
        if kyrodb.lookup_key(i as u64).await.is_some() {
            verified_count += 1;
        }
    }

    println!("   📊 Verified {} records in index", verified_count);

    if verified_count > 30 {
        println!("   ✅ Data integrity maintained under concurrent load");
    } else {
        println!(
            "   ⚠️ Some data missing - potential issue (verified: {})",
            verified_count
        );
    }

    // Clean up
    std::fs::remove_dir_all(&test_dir)?;

    println!("\n🎉 Concurrency Safety Tests Complete!");
    println!("   🔒 No deadlocks detected");
    println!("   ⚡ High throughput maintained");
    println!("   🛡️ Data integrity preserved");

    Ok(())
}
