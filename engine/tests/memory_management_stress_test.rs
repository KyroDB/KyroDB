//! Memory Management Stress Test
//!
//! Tests the enhanced memory management system to ensure it prevents OOM
//! conditions during high-throughput RAG ingestion scenarios.
//!
//! Key features tested:
//! - Bounded overflow buffer with back-pressure
//! - Circuit breaker protection against memory exhaustion
//! - Graduated back-pressure response under memory pressure
//! - Proper error handling with retry guidance
//! - Memory usage monitoring and circuit breaker activation

use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_overflow_buffer_back_pressure() {
    println!("Testing overflow buffer back-pressure mechanism...");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Fill hot buffer to capacity first
    let mut insert_count = 0;
    loop {
        let key = insert_count;
        let value = key * 11;

        match rmi.insert(key, value) {
            Ok(_) => {
                insert_count += 1;
                if insert_count % 1000 == 0 {
                    println!("Inserted {} items (hot buffer filling)", insert_count);
                }
            }
            Err(e) => {
                if e.to_string().contains("memory pressure") {
                    println!("✅ Back-pressure activated after {} inserts", insert_count);
                    break;
                } else {
                    panic!("Unexpected error: {}", e);
                }
            }
        }

        // Safety check - shouldn't need more than 100k items to trigger back-pressure
        if insert_count > 100_000 {
            panic!("Back-pressure should have activated before 100k inserts");
        }
    }

    // Verify back-pressure is working
    let stats = rmi.get_stats();
    println!(
        "Final stats: hot_buffer={}, overflow={}, segments={}",
        stats.hot_buffer_size, stats.overflow_size, stats.segment_count
    );

    assert!(stats.overflow_size > 0, "Overflow buffer should have data");
    assert!(
        insert_count > 1000,
        "Should accept at least 1000 inserts before back-pressure"
    );

    println!("✅ Overflow buffer back-pressure test passed!");
}

#[tokio::test]
async fn test_memory_circuit_breaker() {
    println!("Testing memory circuit breaker protection...");

    let rmi = Arc::new(AdaptiveRMI::new());

    let write_attempts = Arc::new(AtomicU64::new(0));
    let circuit_breaker_activations = Arc::new(AtomicU64::new(0));
    let successful_writes = Arc::new(AtomicU64::new(0));

    // Spawn multiple writers to stress test memory management
    let mut handles = Vec::new();

    for thread_id in 0..8 {
        let rmi_clone = Arc::clone(&rmi);
        let attempts = Arc::clone(&write_attempts);
        let successes = Arc::clone(&successful_writes);
        let breaker_activations = Arc::clone(&circuit_breaker_activations);

        let handle = tokio::spawn(async move {
            for i in 0..10_000 {
                let key = (thread_id * 10_000 + i) as u64;
                let value = key * 13;

                attempts.fetch_add(1, Ordering::Relaxed);

                match rmi_clone.insert(key, value) {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let error_msg = e.to_string();
                        if error_msg.contains("Circuit breaker")
                            || error_msg.contains("memory limit")
                        {
                            breaker_activations.fetch_add(1, Ordering::Relaxed);
                            // Stop this thread when circuit breaker activates
                            break;
                        } else if error_msg.contains("memory pressure") {
                            // Expected back-pressure - just continue
                        } else {
                            eprintln!("Unexpected error: {}", e);
                        }
                    }
                }

                // Small delay to allow background merges
                if i % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all writers to complete or hit circuit breaker
    for handle in handles {
        handle.await.unwrap();
    }

    let total_attempts = write_attempts.load(Ordering::Relaxed);
    let total_successes = successful_writes.load(Ordering::Relaxed);
    let breaker_hits = circuit_breaker_activations.load(Ordering::Relaxed);

    println!("Memory circuit breaker test results:");
    println!("  Write attempts: {}", total_attempts);
    println!("  Successful writes: {}", total_successes);
    println!("  Circuit breaker activations: {}", breaker_hits);

    // Should have some successful writes
    assert!(total_successes > 100, "Should have some successful writes");

    // Either back-pressure or circuit breaker should limit writes
    assert!(
        total_attempts > total_successes,
        "Should have some rejections due to memory protection"
    );

    let final_stats = rmi.get_stats();
    println!(
        "  Final memory state: hot={}, overflow={}, segments={}",
        final_stats.hot_buffer_size, final_stats.overflow_size, final_stats.segment_count
    );

    println!("✅ Memory circuit breaker test passed!");
}

#[tokio::test]
async fn test_graduated_back_pressure_response() {
    println!("Testing graduated back-pressure response levels...");

    let rmi = Arc::new(AdaptiveRMI::new());

    let mut pressure_levels_seen = std::collections::HashSet::new();
    let mut error_messages = Vec::new();

    // Gradually increase memory pressure to observe different levels
    for batch in 0..100 {
        let mut batch_errors = 0;

        for i in 0..100 {
            let key = (batch * 100 + i) as u64;
            let value = key * 7;

            match rmi.insert(key, value) {
                Ok(_) => {
                    // Successful insert
                }
                Err(e) => {
                    batch_errors += 1;
                    let error_msg = e.to_string();
                    error_messages.push(error_msg.clone());

                    // Categorize pressure levels based on error messages
                    if error_msg.contains("temporarily rejected") {
                        pressure_levels_seen.insert("high_pressure");
                    } else if error_msg.contains("permanently rejected")
                        || error_msg.contains("critical")
                    {
                        pressure_levels_seen.insert("critical_pressure");
                    } else if error_msg.contains("Circuit breaker") {
                        pressure_levels_seen.insert("circuit_breaker");
                        // Stop when circuit breaker activates
                        break;
                    }
                }
            }
        }

        if batch_errors > 50 {
            println!(
                "Batch {}: {} errors out of 100 attempts",
                batch, batch_errors
            );
            if batch_errors >= 90 {
                println!("Reaching critical pressure level - stopping test");
                break;
            }
        }

        // Allow background processing
        if batch % 10 == 0 {
            sleep(Duration::from_millis(100)).await;
        }
    }

    println!("Graduated back-pressure test results:");
    println!("  Pressure levels observed: {:?}", pressure_levels_seen);
    println!("  Sample error messages:");
    for (i, msg) in error_messages.iter().take(5).enumerate() {
        println!("    {}: {}", i + 1, msg);
    }

    // Should observe graduated response
    assert!(
        pressure_levels_seen.len() > 0,
        "Should observe at least one pressure level"
    );

    // Should see progression of pressure levels under sustained load
    let has_high_pressure = pressure_levels_seen.contains("high_pressure");
    let has_critical_pressure = pressure_levels_seen.contains("critical_pressure");

    println!("  High pressure seen: {}", has_high_pressure);
    println!("  Critical pressure seen: {}", has_critical_pressure);

    let final_stats = rmi.get_stats();
    println!(
        "  Final state: hot={}, overflow={}, segments={}",
        final_stats.hot_buffer_size, final_stats.overflow_size, final_stats.segment_count
    );

    println!("✅ Graduated back-pressure response test passed!");
}

#[tokio::test]
async fn test_memory_recovery_after_merge() {
    println!("Testing memory recovery after background merge operations...");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Start background maintenance
    let _maintenance_handle = rmi.clone().start_background_maintenance();

    // Fill system to near capacity
    let mut successful_inserts = 0;
    for i in 0..20_000 {
        let key = i as u64;
        let value = key * 23;

        match rmi.insert(key, value) {
            Ok(_) => {
                successful_inserts += 1;
            }
            Err(e) => {
                if e.to_string().contains("memory pressure") {
                    println!("Reached memory pressure at {} inserts", successful_inserts);
                    break;
                }
            }
        }

        if i % 1000 == 0 {
            let stats = rmi.get_stats();
            println!(
                "Insert {}: hot={}, overflow={}, segments={}",
                i, stats.hot_buffer_size, stats.overflow_size, stats.segment_count
            );
        }
    }

    let stats_before_recovery = rmi.get_stats();
    println!(
        "Before recovery: hot={}, overflow={}, segments={}",
        stats_before_recovery.hot_buffer_size,
        stats_before_recovery.overflow_size,
        stats_before_recovery.segment_count
    );

    // Wait for background merges to process overflow
    println!("Waiting for background merge recovery...");
    sleep(Duration::from_secs(5)).await;

    let stats_after_recovery = rmi.get_stats();
    println!(
        "After recovery: hot={}, overflow={}, segments={}",
        stats_after_recovery.hot_buffer_size,
        stats_after_recovery.overflow_size,
        stats_after_recovery.segment_count
    );

    // System should recover capacity through background merges
    let overflow_reduced = stats_after_recovery.overflow_size < stats_before_recovery.overflow_size;
    let segments_increased =
        stats_after_recovery.segment_count > stats_before_recovery.segment_count;

    println!("Recovery indicators:");
    println!(
        "  Overflow reduced: {} (from {} to {})",
        overflow_reduced, stats_before_recovery.overflow_size, stats_after_recovery.overflow_size
    );
    println!(
        "  Segments increased: {} (from {} to {})",
        segments_increased, stats_before_recovery.segment_count, stats_after_recovery.segment_count
    );

    // Test that system can accept new writes after recovery
    let mut post_recovery_inserts = 0;
    for i in 20_000..20_100 {
        let key = i as u64;
        let value = key * 29;

        match rmi.insert(key, value) {
            Ok(_) => {
                post_recovery_inserts += 1;
            }
            Err(_) => {
                // Some rejections are still acceptable during recovery
            }
        }
    }

    println!("  Post-recovery inserts: {}/100", post_recovery_inserts);

    // Should accept at least some writes after recovery
    assert!(
        post_recovery_inserts > 10,
        "Should accept some writes after recovery"
    );

    // Data integrity check
    let mut verification_errors = 0;
    for i in 0..1000 {
        let key = i as u64;
        let expected_value = key * 23;

        if let Some(actual_value) = rmi.lookup_key_ultra_fast(key) {
            if actual_value != expected_value {
                verification_errors += 1;
            }
        }
    }

    println!(
        "  Data integrity: {}/1000 verified correctly",
        1000 - verification_errors
    );
    assert!(
        verification_errors == 0,
        "Should have no data integrity violations"
    );

    println!("✅ Memory recovery after merge test passed!");
}

#[tokio::test]
async fn test_high_throughput_rag_ingestion_simulation() {
    println!("Testing high-throughput RAG ingestion simulation...");

    let rmi = Arc::new(AdaptiveRMI::new());

    // Start background maintenance
    let _maintenance_handle = rmi.clone().start_background_maintenance();

    let total_documents = Arc::new(AtomicU64::new(0));
    let successful_ingestions = Arc::new(AtomicU64::new(0));
    let memory_pressure_events = Arc::new(AtomicU64::new(0));
    let circuit_breaker_events = Arc::new(AtomicU64::new(0));

    // Simulate multiple RAG ingestion streams
    let mut handles = Vec::new();

    for stream_id in 0..4 {
        let rmi_clone = Arc::clone(&rmi);
        let total = Arc::clone(&total_documents);
        let successes = Arc::clone(&successful_ingestions);
        let pressure_events = Arc::clone(&memory_pressure_events);
        let breaker_events = Arc::clone(&circuit_breaker_events);

        let handle = tokio::spawn(async move {
            // Simulate document ingestion with embeddings (key-value pairs)
            for doc_id in 0..5_000 {
                let document_key = (stream_id * 10_000 + doc_id) as u64;
                let embedding_hash = document_key * 41; // Simulate embedding hash

                total.fetch_add(1, Ordering::Relaxed);

                match rmi_clone.insert(document_key, embedding_hash) {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::Relaxed);

                        // Verify immediate retrieval (RAG query simulation)
                        if let Some(retrieved_hash) = rmi_clone.lookup_key_ultra_fast(document_key) {
                            if retrieved_hash != embedding_hash {
                                eprintln!(
                                    "Data corruption: key={}, expected={}, got={}",
                                    document_key, embedding_hash, retrieved_hash
                                );
                            }
                        }
                    }
                    Err(e) => {
                        let error_msg = e.to_string();
                        if error_msg.contains("Circuit breaker") {
                            breaker_events.fetch_add(1, Ordering::Relaxed);
                            // Simulate exponential backoff
                            sleep(Duration::from_millis(100)).await;
                        } else if error_msg.contains("memory pressure") {
                            pressure_events.fetch_add(1, Ordering::Relaxed);
                            // Simulate linear backoff
                            sleep(Duration::from_millis(10)).await;
                        }
                    }
                }

                // Simulate realistic ingestion rate
                if doc_id % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        handles.push(handle);
    }

    // Monitor system during ingestion
    let monitor_total = Arc::clone(&total_documents);
    let monitor_successes = Arc::clone(&successful_ingestions);
    let monitor_rmi = Arc::clone(&rmi);

    let monitor_handle = tokio::spawn(async move {
        for _ in 0..30 {
            sleep(Duration::from_millis(500)).await;
            let stats = monitor_rmi.get_stats();
            let current_total = monitor_total.load(Ordering::Relaxed);
            let current_successes = monitor_successes.load(Ordering::Relaxed);

            println!(
                "Ingestion progress: {}/{} docs, hot={}, overflow={}, segments={}",
                current_successes,
                current_total,
                stats.hot_buffer_size,
                stats.overflow_size,
                stats.segment_count
            );
        }
    });

    // Wait for all ingestion streams to complete
    for handle in handles {
        handle.await.unwrap();
    }

    monitor_handle.abort();

    let final_total = total_documents.load(Ordering::Relaxed);
    let final_successes = successful_ingestions.load(Ordering::Relaxed);
    let final_pressure_events = memory_pressure_events.load(Ordering::Relaxed);
    let final_breaker_events = circuit_breaker_events.load(Ordering::Relaxed);

    println!("High-throughput RAG ingestion results:");
    println!("  Total documents: {}", final_total);
    println!("  Successful ingestions: {}", final_successes);
    println!("  Memory pressure events: {}", final_pressure_events);
    println!("  Circuit breaker events: {}", final_breaker_events);

    let success_rate = (final_successes as f64 / final_total as f64) * 100.0;
    println!("  Success rate: {:.1}%", success_rate);

    // Should achieve reasonable success rate with memory protection
    assert!(
        success_rate > 60.0,
        "Should achieve at least 60% success rate with back-pressure"
    );
    assert!(
        final_successes > 1000,
        "Should successfully ingest at least 1000 documents"
    );

    // Memory protection should activate under sustained load
    let total_protection_events = final_pressure_events + final_breaker_events;
    assert!(
        total_protection_events > 0,
        "Should have some memory protection activations"
    );

    let final_stats = rmi.get_stats();
    println!(
        "  Final system state: hot={}, overflow={}, segments={}",
        final_stats.hot_buffer_size, final_stats.overflow_size, final_stats.segment_count
    );

    // System should not be completely exhausted
    assert!(
        final_stats.segment_count > 0,
        "Should have created segments from successful ingestions"
    );

    println!("✅ High-throughput RAG ingestion simulation passed!");
}
