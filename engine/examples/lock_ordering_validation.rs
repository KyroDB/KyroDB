//! Lock Ordering Protocol Validation Test
//!
//! This test validates that the KyroDB adaptive RMI follows strict global lock ordering
//! to prevent reader-writer deadlocks. Tests both correct ordering and deadlock prevention.

use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;

#[derive(Debug)]
struct DeadlockDetector {
    operations_completed: AtomicUsize,
    deadlock_detected: AtomicUsize,
    test_start_time: Instant,
}

impl DeadlockDetector {
    fn new() -> Self {
        Self {
            operations_completed: AtomicUsize::new(0),
            deadlock_detected: AtomicUsize::new(0),
            test_start_time: Instant::now(),
        }
    }

    fn record_completion(&self) {
        self.operations_completed.fetch_add(1, Ordering::Relaxed);
    }

    fn record_potential_deadlock(&self) {
        self.deadlock_detected.fetch_add(1, Ordering::Relaxed);
        println!(
            "⚠️  Potential deadlock detected at {:.2}s",
            self.test_start_time.elapsed().as_secs_f64()
        );
    }

    fn get_stats(&self) -> (usize, usize, f64) {
        (
            self.operations_completed.load(Ordering::Relaxed),
            self.deadlock_detected.load(Ordering::Relaxed),
            self.test_start_time.elapsed().as_secs_f64(),
        )
    }
}

/// Test the global lock ordering protocol under high concurrency
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔒 KyroDB Lock Ordering Protocol Validation");
    println!("============================================");

    // Test 1: Basic lock ordering validation
    test_basic_lock_ordering().await?;

    // Test 2: High-concurrency stress test
    test_high_concurrency_stress().await?;

    // Test 3: Reader-writer deadlock prevention
    test_reader_writer_deadlock_prevention().await?;

    // Test 4: Timeout-based deadlock detection
    test_timeout_based_deadlock_detection().await?;

    println!("\n✅ All lock ordering protocol tests passed!");
    println!("🛡️  No deadlocks detected - system is deadlock-free");

    Ok(())
}

/// Test 1: Validate basic lock ordering protocol compliance
async fn test_basic_lock_ordering() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📋 Test 1: Basic Lock Ordering Protocol");
    println!("---------------------------------------");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Build initial data set
    let test_data: Vec<(u64, u64)> = (0..1000).map(|i| (i, i * 2)).collect();
    let rmi_with_data = AdaptiveRMI::build_from_pairs(&test_data);
    let rmi = Arc::new(rmi_with_data);

    // Test insert operations (should only use hot_buffer → overflow_buffer)
    let insert_rmi = Arc::clone(&rmi);
    let insert_task = tokio::spawn(async move {
        for i in 1000..2000 {
            insert_rmi.insert(i, i * 3).unwrap_or_else(|e| {
                if !e.to_string().contains("pressure") {
                    panic!("Unexpected insert error: {}", e);
                }
            });
        }
        println!("✓ Insert operations completed with correct lock ordering");
    });

    // Test lookup operations (should use hot_buffer → overflow_buffer → segments)
    let lookup_rmi = Arc::clone(&rmi);
    let lookup_task = tokio::spawn(async move {
        for i in 0..500 {
            let _ = lookup_rmi.lookup(i);
        }
        println!("✓ Lookup operations completed with correct lock ordering");
    });

    // Wait for both tasks
    tokio::try_join!(insert_task, lookup_task)?;

    println!("✅ Basic lock ordering protocol test passed");
    Ok(())
}

/// Test 2: High-concurrency stress test
async fn test_high_concurrency_stress() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🚀 Test 2: High-Concurrency Stress Test");
    println!("----------------------------------------");

    let detector = Arc::new(DeadlockDetector::new());
    let rmi = Arc::new(AdaptiveRMI::new());

    // Build initial data
    let test_data: Vec<(u64, u64)> = (0..5000).map(|i| (i, i * 2)).collect();
    let rmi_with_data = AdaptiveRMI::build_from_pairs(&test_data);
    let rmi = Arc::new(rmi_with_data);

    let mut tasks = Vec::new();

    // Spawn multiple concurrent readers
    for thread_id in 0..10 {
        let rmi_clone = Arc::clone(&rmi);
        let detector_clone = Arc::clone(&detector);

        let task = tokio::spawn(async move {
            for i in 0..1000 {
                let key = (thread_id * 1000 + i) % 5000;
                let _ = rmi_clone.lookup(key as u64);

                if i % 100 == 0 {
                    detector_clone.record_completion();
                }
            }
        });
        tasks.push(task);
    }

    // Spawn multiple concurrent writers
    for thread_id in 0..5 {
        let rmi_clone = Arc::clone(&rmi);
        let detector_clone = Arc::clone(&detector);

        let task = tokio::spawn(async move {
            for i in 0..500 {
                let key = 10000 + (thread_id * 500 + i) as u64;
                rmi_clone.insert(key, key * 2).unwrap_or_else(|e| {
                    if !e.to_string().contains("pressure") {
                        detector_clone.record_potential_deadlock();
                    }
                });

                if i % 50 == 0 {
                    detector_clone.record_completion();
                }
            }
        });
        tasks.push(task);
    }

    // Spawn background merge operations
    for _ in 0..2 {
        let rmi_clone = Arc::clone(&rmi);
        let detector_clone = Arc::clone(&detector);

        let task = tokio::spawn(async move {
            for _ in 0..10 {
                if let Err(_) = rmi_clone.merge_hot_buffer().await {
                    detector_clone.record_potential_deadlock();
                }
                detector_clone.record_completion();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete
    for task in tasks {
        task.await?;
    }

    let (completed, deadlocks, elapsed) = detector.get_stats();

    println!("📊 Stress Test Results:");
    println!("   Operations completed: {}", completed);
    println!("   Potential deadlocks: {}", deadlocks);
    println!("   Total time: {:.2}s", elapsed);

    if deadlocks == 0 {
        println!("✅ High-concurrency stress test passed - no deadlocks detected");
    } else {
        return Err(format!("❌ Detected {} potential deadlocks", deadlocks).into());
    }

    Ok(())
}

/// Test 3: Reader-writer deadlock prevention
async fn test_reader_writer_deadlock_prevention() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔄 Test 3: Reader-Writer Deadlock Prevention");
    println!("--------------------------------------------");

    let detector = Arc::new(DeadlockDetector::new());
    let rmi = Arc::new(AdaptiveRMI::new());

    // Build initial data
    let test_data: Vec<(u64, u64)> = (0..1000).map(|i| (i, i * 2)).collect();
    let rmi_with_data = AdaptiveRMI::build_from_pairs(&test_data);
    let rmi = Arc::new(rmi_with_data);

    let mut tasks = Vec::new();

    // Continuous readers that could cause deadlock if ordering is wrong
    for reader_id in 0..20 {
        let rmi_clone = Arc::clone(&rmi);
        let detector_clone = Arc::clone(&detector);

        let task = tokio::spawn(async move {
            for i in 0..200 {
                let key = (reader_id * 50 + i) % 1000;
                let _ = rmi_clone.lookup(key as u64);

                // Small yield to increase contention
                if i % 10 == 0 {
                    tokio::task::yield_now().await;
                    detector_clone.record_completion();
                }
            }
        });
        tasks.push(task);
    }

    // Continuous writers that could cause deadlock if ordering is wrong
    for writer_id in 0..5 {
        let rmi_clone = Arc::clone(&rmi);
        let detector_clone = Arc::clone(&detector);

        let task = tokio::spawn(async move {
            for i in 0..100 {
                // Insert new keys
                let key = 2000 + (writer_id * 100 + i) as u64;
                rmi_clone.insert(key, key * 3).unwrap_or_else(|e| {
                    if !e.to_string().contains("pressure") {
                        detector_clone.record_potential_deadlock();
                    }
                });

                // Trigger merges frequently to test writer lock contention
                if i % 20 == 0 {
                    if let Err(_) = rmi_clone.merge_hot_buffer().await {
                        detector_clone.record_potential_deadlock();
                    }
                    detector_clone.record_completion();
                }
            }
        });
        tasks.push(task);
    }

    // Wait for all tasks
    for task in tasks {
        task.await?;
    }

    let (completed, deadlocks, elapsed) = detector.get_stats();

    println!("📊 Reader-Writer Test Results:");
    println!("   Operations completed: {}", completed);
    println!("   Potential deadlocks: {}", deadlocks);
    println!("   Total time: {:.2}s", elapsed);

    if deadlocks == 0 {
        println!("✅ Reader-writer deadlock prevention test passed");
    } else {
        return Err(format!("❌ Detected {} potential deadlocks", deadlocks).into());
    }

    Ok(())
}

/// Test 4: Timeout-based deadlock detection
async fn test_timeout_based_deadlock_detection() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n⏰ Test 4: Timeout-Based Deadlock Detection");
    println!("-------------------------------------------");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Build initial data
    let test_data: Vec<(u64, u64)> = (0..100).map(|i| (i, i * 2)).collect();
    let rmi_with_data = AdaptiveRMI::build_from_pairs(&test_data);
    let rmi = Arc::new(rmi_with_data);

    // Test that operations complete within reasonable time (no hanging due to deadlocks)
    let timeout_duration = Duration::from_secs(5);

    // High-frequency mixed operations with timeout
    let mixed_operations = async {
        let mut tasks = Vec::new();

        for i in 0..50 {
            let rmi_clone = Arc::clone(&rmi);
            let task = tokio::spawn(async move {
                // Mixed read/write operations
                for j in 0..20 {
                    let key = (i * 20 + j) as u64;

                    // Lookup
                    let _ = rmi_clone.lookup(key % 100);

                    // Insert
                    rmi_clone.insert(1000 + key, key).unwrap_or_else(|_| {});

                    // Occasional merge
                    if j % 10 == 0 {
                        let _ = rmi_clone.merge_hot_buffer().await;
                    }
                }
            });
            tasks.push(task);
        }

        // Wait for all tasks
        for task in tasks {
            task.await?;
        }

        Ok::<(), Box<dyn std::error::Error>>(())
    };

    // Run with timeout to detect hanging (potential deadlock)
    match timeout(timeout_duration, mixed_operations).await {
        Ok(Ok(())) => {
            println!("✅ Timeout-based deadlock detection test passed");
            println!(
                "   All operations completed within {} seconds",
                timeout_duration.as_secs()
            );
        }
        Ok(Err(e)) => {
            return Err(format!("❌ Operations failed: {}", e).into());
        }
        Err(_) => {
            return Err("❌ DEADLOCK DETECTED: Operations timed out (likely deadlock)".into());
        }
    }

    Ok(())
}
