//! Test to verify RMI sync method makes writes immediately visible

use crate::test::utils::*;
use crate::PersistentEventLog;

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_with_sync() {
    // Enable RMI explicitly (should be default anyway)
    std::env::set_var("KYRODB_USE_ADAPTIVE_RMI", "true");
    
    let data_dir = test_data_dir();
    let log = PersistentEventLog::open(data_dir.path().to_path_buf())
        .await
        .expect("Failed to create log");

    println!("========== WRITE KEYS WITH RMI ENABLED ==========");
    
    for i in 0..10 {
        let offset = append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
        println!("  Wrote key={}, offset={}", i, offset);
    }

    println!("\n========== SYNC INDEX ==========");
    log.sync_index_for_test();
    println!("  Index synced");

    println!("\n========== LOOKUP AFTER SYNC ==========");
    
    let mut found = 0;
    for i in 0..10 {
        match log.lookup_key(i).await {
            Some(offset) => {
                found += 1;
                println!("  ✅ lookup_key({}) returned offset={}", i, offset);
            }
            None => {
                println!("  ❌ lookup_key({}) returned None", i);
            }
        }
    }
    
    println!("\n  Summary: {}/10 keys found", found);
    assert_eq!(found, 10, "All keys should be found after sync with RMI");
    
    // Cleanup
    std::env::remove_var("KYRODB_USE_ADAPTIVE_RMI");
}

#[tokio::test]
#[cfg(feature = "learned-index")]
async fn test_rmi_sync_after_snapshot() {
    std::env::set_var("KYRODB_USE_ADAPTIVE_RMI", "true");
    
    let data_dir = test_data_dir();
    let log = PersistentEventLog::open(data_dir.path().to_path_buf())
        .await
        .expect("Failed to create log");

    println!("========== PHASE 1: WRITE AND SYNC ==========");
    
    for i in 0..100 {
        append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
    }
    
    log.sync_index_for_test();
    println!("  Wrote and synced 100 keys");

    println!("\n========== PHASE 2: CREATE SNAPSHOT ==========");
    log.snapshot().await.expect("Failed to create snapshot");
    println!("  Snapshot created");

    println!("\n========== PHASE 3: VERIFY DATA ACCESSIBLE ==========");
    
    let mut found = 0;
    for i in 0..100 {
        if lookup_kv_ref(&log, i).await.expect("Lookup error").is_some() {
            found += 1;
        }
    }
    
    println!("  Summary: {}/100 keys found", found);
    assert_eq!(found, 100, "All keys should be accessible after snapshot with RMI");
    
    std::env::remove_var("KYRODB_USE_ADAPTIVE_RMI");
}
