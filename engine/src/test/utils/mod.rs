//! Test Utilities and Fixtures
//!
//! Reusable test infrastructure for all test modules

pub mod fixtures;
pub mod assertions;
pub mod test_server;

// Re-export commonly used items
pub use test_server::{TestServer, TestServerConfig};

use crate::PersistentEventLog;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Get a unique test data directory
pub fn test_data_dir() -> TempDir {
    TempDir::new().expect("Failed to create temp dir")
}

/// Create a temp directory at a specific path with a name
pub fn temp_data_dir(name: &str) -> PathBuf {
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!("kyrodb_{}_{}", name, counter));
    std::fs::create_dir_all(&path).expect("Failed to create temp dir");
    path
}

/// Get a unique test directory with a specific prefix
pub fn test_data_dir_with_prefix(prefix: &str) -> TempDir {
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    tempfile::Builder::new()
        .prefix(&format!("{}_test_{}_", prefix, counter))
        .tempdir()
        .expect("Failed to create temp dir")
}

/// Convert TempDir to PathBuf
pub fn to_path(dir: &TempDir) -> PathBuf {
    dir.path().to_path_buf()
}

/// Generate test key-value pairs
pub fn generate_test_data(count: usize) -> Vec<(u64, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = i as u64;
            let value = format!("value_{}", i).into_bytes();
            (key, value)
        })
        .collect()
}

/// Generate sequential keys
pub fn sequential_keys(start: u64, count: usize) -> Vec<u64> {
    (start..start + count as u64).collect()
}

/// Generate random keys
pub fn random_keys(count: usize, max: u64) -> Vec<u64> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..count).map(|_| rng.gen_range(0..max)).collect()
}

/// Generate skewed keys (Zipfian distribution simulation)
pub fn skewed_keys(count: usize, max: u64) -> Vec<u64> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|_| {
            // Simple zipfian: 80% of requests go to 20% of keys
            if rng.gen_bool(0.8) {
                rng.gen_range(0..max / 5)
            } else {
                rng.gen_range(0..max)
            }
        })
        .collect()
}

/// Wait for a condition with timeout
pub async fn wait_for<F>(mut condition: F, timeout_ms: u64) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);
    
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    false
}

/// Retry an operation with exponential backoff
pub async fn retry_with_backoff<F, Fut, T, E>(
    mut operation: F,
    max_retries: usize,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut retries = 0;
    let mut delay_ms = 10;
    
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) if retries >= max_retries => return Err(e),
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms *= 2; // Exponential backoff
                retries += 1;
            }
        }
    }
}

/// Helper: Append key-value pair to log
pub async fn append_kv(log: &Arc<PersistentEventLog>, key: u64, value: Vec<u8>) -> anyhow::Result<u64> {
    log.append_kv(Uuid::new_v4(), key, value).await
}

/// Helper: Lookup value by key
pub async fn lookup_kv(log: &Arc<PersistentEventLog>, key: u64) -> anyhow::Result<Option<Vec<u8>>> {
    if let Some(offset) = log.lookup_key(key).await {
        if let Some(payload) = log.get(offset).await {
            // Decode KV format: [key:u64][len:u64][value...]
            if payload.len() >= 16 {
                use bytes::Buf;
                let mut cursor = &payload[..];
                let _stored_key = cursor.get_u64_le();
                let value_len = cursor.get_u64_le() as usize;
                if cursor.len() >= value_len {
                    return Ok(Some(cursor[..value_len].to_vec()));
                }
            }
        }
    }
    Ok(None)
}
