//! Debug test to check group commit flush behavior

use crate::test::utils::*;
use crate::{GroupCommitConfig, PersistentEventLog};
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_debug_group_commit_with_custom_config() {
    // Create log with very aggressive group commit settings
    let data_dir = test_data_dir();

    // Set minimal batch settings to force immediate flush
    let config = GroupCommitConfig {
        enabled: true,
        max_batch_size: 1,         // Flush after just 1 item
        max_batch_delay_micros: 1, // Flush after 1µs
        ..Default::default()
    };

    let log = PersistentEventLog::with_group_commit(data_dir.path().to_path_buf(), config)
        .await
        .expect("Failed to create log");

    println!("========== WRITE KEY WITH AGGRESSIVE FLUSH ==========");

    let offset = append_kv_ref(&log, 99, b"test_value".to_vec())
        .await
        .expect("Failed to append");
    println!("  append_kv_ref returned offset={}", offset);

    println!("\n========== IMMEDIATE LOOKUP ==========");

    match log.lookup_key(99).await {
        Some(found_offset) => {
            println!(
                "  ✅ lookup_key(99) IMMEDIATELY returned offset={}",
                found_offset
            );
        }
        None => {
            println!("  ❌ lookup_key(99) IMMEDIATELY returned None");
        }
    }

    // Also test with a small delay
    sleep(Duration::from_millis(10)).await;

    println!("\n========== LOOKUP AFTER 10ms ==========");

    match log.lookup_key(99).await {
        Some(found_offset) => {
            println!(
                "  ✅ lookup_key(99) AFTER 10ms returned offset={}",
                found_offset
            );
        }
        None => {
            println!("  ❌ lookup_key(99) AFTER 10ms returned None");
        }
    }
}

#[tokio::test]
async fn test_debug_group_commit_disabled() {
    // Create log with group commit DISABLED
    let data_dir = test_data_dir();

    let config = GroupCommitConfig {
        enabled: false,
        ..Default::default()
    };

    let log = PersistentEventLog::with_group_commit(data_dir.path().to_path_buf(), config)
        .await
        .expect("Failed to create log");

    println!("========== WRITE KEY WITH GROUP COMMIT DISABLED ==========");

    let offset = append_kv_ref(&log, 77, b"test_value".to_vec())
        .await
        .expect("Failed to append");
    println!("  append_kv_ref returned offset={}", offset);

    println!("\n========== IMMEDIATE LOOKUP ==========");

    match log.lookup_key(77).await {
        Some(found_offset) => {
            println!(
                "  ✅ lookup_key(77) IMMEDIATELY returned offset={}",
                found_offset
            );
        }
        None => {
            println!("  ❌ lookup_key(77) IMMEDIATELY returned None");
        }
    }
}
