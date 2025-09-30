//! Debug test to understand group commit and index updates

use crate::test::utils::*;
use crate::PersistentEventLog;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_debug_group_commit_timing() {
    let data_dir = test_data_dir();
    let log = PersistentEventLog::open(data_dir.path().to_path_buf())
        .await
        .expect("Failed to create log");

    println!("========== PHASE 1: WRITE ONE KEY ==========");
    
    let offset = append_kv_ref(&log, 42, b"test_value".to_vec())
        .await
        .expect("Failed to append");
    println!("  append_kv_ref returned offset={}", offset);

    println!("\n========== PHASE 2: IMMEDIATE LOOKUP ==========");
    
    match log.lookup_key(42).await {
        Some(found_offset) => {
            println!("  ✅ lookup_key(42) IMMEDIATELY returned offset={}", found_offset);
        }
        None => {
            println!("  ❌ lookup_key(42) IMMEDIATELY returned None");
        }
    }

    println!("\n========== PHASE 3: WAIT 100ms THEN LOOKUP ==========");
    
    sleep(Duration::from_millis(100)).await;
    
    match log.lookup_key(42).await {
        Some(found_offset) => {
            println!("  ✅ lookup_key(42) AFTER 100ms returned offset={}", found_offset);
        }
        None => {
            println!("  ❌ lookup_key(42) AFTER 100ms returned None");
        }
    }

    println!("\n========== PHASE 4: WAIT 1s THEN LOOKUP ==========");
    
    sleep(Duration::from_secs(1)).await;
    
    match log.lookup_key(42).await {
        Some(found_offset) => {
            println!("  ✅ lookup_key(42) AFTER 1s returned offset={}", found_offset);
        }
        None => {
            println!("  ❌ lookup_key(42) AFTER 1s returned None");
        }
    }

    println!("\n========== PHASE 5: CHECK INTERNAL STATE ==========");
    
    // Check if the event is in the inner state
    let inner = log.inner.read().await;
    println!("  Inner events count: {}", inner.len());
    if inner.len() > 0 {
        println!("  First event offset: {}", inner[0].offset);
    }
    drop(inner);

    // Try direct get() call
    match log.get(offset).await {
        Some(payload) => {
            println!("  ✅ get({}) returned {} bytes", offset, payload.len());
        }
        None => {
            println!("  ❌ get({}) returned None", offset);
        }
    }
}
