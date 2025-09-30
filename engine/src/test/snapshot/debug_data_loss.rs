//! Debug test to understand why data isn't found after snapshot

use crate::test::utils::*;
use crate::PersistentEventLog;

#[tokio::test]
async fn test_debug_data_after_snapshot() {
    let data_dir = test_data_dir();
    let log = PersistentEventLog::open(data_dir.path().to_path_buf())
        .await
        .expect("Failed to create log");

    println!("========== PHASE 1: WRITE DATA ==========");
    
    // Write 10 keys
    for i in 0..10 {
        let offset = append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
        println!("  Wrote key={}, offset={}", i, offset);
    }

    println!("\n========== PHASE 2: VERIFY BEFORE SNAPSHOT ==========");
    
    let mut found_before = 0;
    for i in 0..10 {
        match lookup_kv_ref(&log, i).await {
            Ok(Some(_)) => {
                found_before += 1;
                println!("  ✅ Key {} found BEFORE snapshot", i);
            }
            Ok(None) => {
                println!("  ❌ Key {} NOT found BEFORE snapshot", i);
            }
            Err(e) => {
                println!("  ❌ Key {} error BEFORE snapshot: {}", i, e);
            }
        }
    }
    println!("  Summary: {}/10 found BEFORE snapshot", found_before);

    println!("\n========== PHASE 3: CREATE SNAPSHOT ==========");
    log.snapshot().await.expect("Failed to create snapshot");
    println!("  Snapshot created");

    println!("\n========== PHASE 4: VERIFY AFTER SNAPSHOT ==========");
    
    let mut found_after = 0;
    for i in 0..10 {
        match lookup_kv_ref(&log, i).await {
            Ok(Some(_)) => {
                found_after += 1;
                println!("  ✅ Key {} found AFTER snapshot", i);
            }
            Ok(None) => {
                println!("  ❌ Key {} NOT found AFTER snapshot", i);
            }
            Err(e) => {
                println!("  ❌ Key {} error AFTER snapshot: {}", i, e);
            }
        }
    }
    println!("  Summary: {}/10 found AFTER snapshot", found_after);

    println!("\n========== PHASE 5: CHECK INDEX STATE ==========");
    
    // Try using lookup_key instead of lookup_kv_ref
    for i in 0..3 {
        match log.lookup_key(i).await {
            Some(offset) => {
                println!("  ✅ lookup_key({}) returned offset={}", i, offset);
                
                // Try to get the payload
                match log.get(offset).await {
                    Some(payload) => {
                        println!("      ✅ get({}) returned {} bytes", offset, payload.len());
                    }
                    None => {
                        println!("      ❌ get({}) returned None", offset);
                    }
                }
            }
            None => {
                println!("  ❌ lookup_key({}) returned None", i);
            }
        }
    }

    assert_eq!(
        found_before, found_after,
        "Data accessibility changed after snapshot: {} before, {} after",
        found_before, found_after
    );
}
