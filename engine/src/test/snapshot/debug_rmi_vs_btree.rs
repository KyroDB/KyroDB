//! Test to verify that disabling RMI fixes the lookup issue

use crate::test::utils::*;
use crate::PersistentEventLog;

#[tokio::test]
async fn test_lookup_with_rmi_disabled() {
    // Disable RMI to use BTree index
    std::env::set_var("KYRODB_USE_ADAPTIVE_RMI", "false");

    let data_dir = test_data_dir();
    let log = PersistentEventLog::open(data_dir.path().to_path_buf())
        .await
        .expect("Failed to create log");

    println!("========== WRITE KEYS WITH RMI DISABLED ==========");

    for i in 0..10 {
        let offset = append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
            .await
            .expect("Failed to append");
        println!("  Wrote key={}, offset={}", i, offset);
    }

    println!("\n========== IMMEDIATE LOOKUP ==========");

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
    assert_eq!(
        found, 10,
        "All keys should be found immediately with BTree index"
    );

    // Cleanup
    std::env::remove_var("KYRODB_USE_ADAPTIVE_RMI");
}
