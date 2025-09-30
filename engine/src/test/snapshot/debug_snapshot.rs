//! Debug test to understand snapshot serialization issue

use crate::test::utils::*;
use crate::{PersistentEventLog, Event, SCHEMA_VERSION};
use std::fs::File;
use std::io::BufReader;
use bincode::Options;

#[tokio::test]
async fn test_debug_snapshot_format() {
    let data_dir = test_data_dir();
    let data_path = data_dir.path().to_path_buf();

    println!("========== PHASE 1: CREATE DATA AND SNAPSHOT ==========");
    
    {
        let log = PersistentEventLog::open(data_path.clone())
            .await
            .expect("Failed to create log");

        // Write 10 keys
        for i in 0..10 {
            let offset = append_kv_ref(&log, i, format!("value_{}", i).as_bytes().to_vec())
                .await
                .expect("Failed to append");
            println!("  Wrote key={}, offset={}", i, offset);
        }

        println!("\n Creating snapshot...");
        log.snapshot().await.expect("Failed to create snapshot");
        println!("  Snapshot created");
    }

    // Check if snapshot file exists
    let snapshot_path = data_path.join("snapshot.bin");
    assert!(snapshot_path.exists(), "Snapshot file doesn't exist!");
    
    let metadata = std::fs::metadata(&snapshot_path).expect("Can't get metadata");
    println!("\n Snapshot file size: {} bytes", metadata.len());

    println!("\n========== PHASE 2: MANUALLY DESERIALIZE SNAPSHOT ==========");
    
    // Skip default bincode (incompatible with options-based serialization)
    
    // Try with limited bincode (recovery path)
    {
        let f = File::open(&snapshot_path).expect("Can't open snapshot");
        let mut rdr = BufReader::new(f);
        let bopt = bincode::options().with_limit(16 * 1024 * 1024);
        
        println!("\n  Attempting limited bincode deserialization (16 MiB limit)...");
        match bopt.deserialize_from::<_, Vec<Event>>(&mut rdr) {
            Ok(events) => {
                println!("  ✅ Limited bincode: SUCCESS");
                println!("  Events count: {}", events.len());
                
                // Check schema versions
                let all_correct = events.iter().all(|e| e.schema_version == SCHEMA_VERSION);
                println!("  All events have schema_version={}? {}", SCHEMA_VERSION, all_correct);
                
                if !all_correct {
                    for (i, ev) in events.iter().enumerate() {
                        if ev.schema_version != SCHEMA_VERSION {
                            println!("    ❌ Event {} has schema_version={}", i, ev.schema_version);
                        }
                    }
                }
            }
            Err(e) => {
                println!("  ❌ Limited bincode: FAILED - {}", e);
            }
        }
    }

    println!("\n========== PHASE 3: REOPEN DATABASE ==========");
    
    {
        let log = PersistentEventLog::open(data_path.clone())
            .await
            .expect("Failed to reopen log");

        println!("\n  Database reopened");
        
        // Try to lookup keys
        let mut found = 0;
        let mut missing = 0;
        for i in 0..10 {
            match lookup_kv_ref(&log, i).await {
                Ok(Some(value)) => {
                    found += 1;
                    let expected = format!("value_{}", i);
                    if String::from_utf8_lossy(&value) == expected {
                        // println!("  ✅ Key {}: FOUND and CORRECT", i);
                    } else {
                        println!("  ⚠️  Key {}: FOUND but WRONG VALUE", i);
                    }
                }
                Ok(None) => {
                    missing += 1;
                    println!("  ❌ Key {}: NOT FOUND", i);
                }
                Err(e) => {
                    println!("  ❌ Key {}: ERROR - {}", i, e);
                }
            }
        }
        
        println!("\n  Summary: {} found, {} missing out of 10", found, missing);
        
        if missing > 0 {
            panic!("Recovery failed: {} keys missing", missing);
        }
    }
}
