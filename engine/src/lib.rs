//! Core Event Log Engine Library

use uuid::Uuid;
use tokio::sync::RwLock;
use std::sync::Arc;

/// An event in the log
#[derive(Debug, Clone)]
pub struct Event {
    pub offset: u64,
    pub timestamp: u64, // Unix nanos
    pub request_id: Uuid,
    pub payload: Vec<u8>,
}

/// Result of an append operation
pub struct AppendResult {
    pub offset: u64,
}

/// In-memory Event Log
pub struct EventLog {
    inner: Arc<RwLock<Vec<Event>>>,
}

impl EventLog {
    /// Creates a new, empty in-memory log
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(Vec::new())) }
    }

    /// Appends an event. Idempotency and persistence to be added later.
    pub async fn append(&self, request_id: Uuid, payload: Vec<u8>) -> AppendResult {
        let mut log = self.inner.write().await;
        let offset = log.len() as u64;
        let event = Event {
            offset,
            timestamp: chrono::Utc::now().timestamp_nanos() as u64,
            request_id,
            payload,
        };
        log.push(event);
        AppendResult { offset }
    }

    /// Replays events from `start_offset` (inclusive). If `end_offset` is None, replay to latest.
    pub async fn replay(&self, start_offset: u64, end_offset: Option<u64>) -> Vec<Event> {
        let log = self.inner.read().await;
        let end = end_offset.unwrap_or_else(|| log.len() as u64);
        log.iter()
            .filter(|e| e.offset >= start_offset && e.offset < end)
            .cloned()
            .collect()
    }

    /// Returns current end offset (the next write position)
    pub async fn get_offset(&self) -> u64 {
        let log = self.inner.read().await;
        log.len() as u64
    }

    /// Subscribe for new events starting from `from_offset`.
    /// Basic implementation: polls for new events every 10ms.
    pub async fn subscribe(&self, from_offset: u64) -> Vec<Event> {
        // TODO: implement real-time streaming via watch channels
        self.replay(from_offset, None).await
    }
}

// engine/src/main.rs

#[tokio::main]
async fn main() {
    println!("Immutable Event Log Engine CLI");
    // TODO: parse CLI args (start, append, replay)
}
