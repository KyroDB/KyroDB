//! Debug test to check payload serialization/deserialization

use crate::test::utils::*;
use crate::{PersistentEventLog, deserialize_record_compat};
use bytes::{BytesMut, BufMut};

#[tokio::test]
async fn test_debug_payload_format() {
    let data_dir = test_data_dir();
    let log = PersistentEventLog::open(data_dir.path().to_path_buf())
        .await
        .expect("Failed to create log");

    println!("========== MANUAL PAYLOAD SERIALIZATION ==========");
    
    // Manually create the same payload that append_kv creates
    let key = 42u64;
    let value = b"test_value".to_vec();
    
    let mut buf = BytesMut::with_capacity(16 + value.len());
    buf.put_u64_le(key);
    buf.put_u64_le(value.len() as u64);
    buf.put_slice(&value);
    let payload = buf.to_vec();
    
    println!("  Key: {}", key);
    println!("  Value len: {}", value.len());
    println!("  Payload len: {} bytes", payload.len());
    println!("  Payload hex: {}", hex::encode(&payload[..payload.len().min(32)]));

    println!("\n========== DESERIALIZE PAYLOAD ==========");
    
    match deserialize_record_compat(&payload) {
        Some(rec) => {
            println!("  ✅ Deserialization SUCCESS");
            println!("  Deserialized key: {}", rec.key);
            println!("  Deserialized value len: {}", rec.value.len());
            println!("  Deserialized value: {:?}", String::from_utf8_lossy(&rec.value));
            assert_eq!(rec.key, key);
            assert_eq!(rec.value, value);
        }
        None => {
            println!("  ❌ Deserialization FAILED");
            panic!("Failed to deserialize payload that we just created!");
        }
    }

    println!("\n========== APPEND VIA append_kv AND CHECK ==========");
    
    let offset = append_kv_ref(&log, 99, b"another_test".to_vec())
        .await
        .expect("Failed to append");
    println!("  append_kv_ref returned offset={}", offset);

    // Get the raw payload
    match log.get(offset).await {
        Some(payload) => {
            println!("  ✅ get({}) returned {} bytes", offset, payload.len());
            println!("  Payload hex: {}", hex::encode(&payload[..payload.len().min(32)]));
            
            // Try to deserialize it
            match deserialize_record_compat(&payload) {
                Some(rec) => {
                    println!("  ✅ Payload deserialization SUCCESS");
                    println!("      Key: {}", rec.key);
                    println!("      Value: {:?}", String::from_utf8_lossy(&rec.value));
                }
                None => {
                    println!("  ❌ Payload deserialization FAILED!");
                    panic!("Failed to deserialize payload from get()!");
                }
            }
        }
        None => {
            println!("  ❌ get({}) returned None", offset);
        }
    }
}
