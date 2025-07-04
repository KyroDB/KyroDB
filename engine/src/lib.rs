// engine/src/lib.rs

//! Core Event Log Engine Library

use chrono::Utc;
use uuid::Uuid;
use tokio::sync::RwLock;
use std::sync::Arc;

/// An event in the log
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub offset: u64,
    pub timestamp: u64, // Unix nanos
    pub request_id: Uuid,
    pub payload: Vec<u8>,
}

/// Result of an append operation
#[derive(Debug, PartialEq, Eq)]
pub struct AppendResult {
    pub offset: u64,
}

/// In-memory Event Log
#[derive(Clone)]
pub struct EventLog {
    inner: Arc<RwLock<Vec<Event>>>,
}

impl EventLog {
    /// Creates a new, empty in-memory log
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Appends an event. Idempotency and persistence to be added later.
    pub async fn append(&self, request_id: Uuid, payload: Vec<u8>) -> AppendResult {
        let mut log = self.inner.write().await;
        // Dedupe if same request_id already appended
        if let Some(existing) = log.iter().find(|e| e.request_id == request_id) {
            return AppendResult {
                offset: existing.offset,
            };
        }

        let offset = log.len() as u64;
        let event = Event {
            offset,
            timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
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
    /// This simple implementation just returns the current slice.
    /// We'll replace with real streaming later.
    pub async fn subscribe(&self, from_offset: u64) -> Vec<Event> {
        self.replay(from_offset, None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_append_and_replay() {
        let log = EventLog::new();

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        // Append two events
        let r1 = log.append(id1, b"foo".to_vec()).await;
        let r2 = log.append(id2, b"bar".to_vec()).await;
        assert_eq!(r1.offset, 0);
        assert_eq!(r2.offset, 1);

        // Replay all
        let events = log.replay(0, None).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].payload, b"foo".to_vec());
        assert_eq!(events[1].payload, b"bar".to_vec());

        // Replay slice
        let slice = log.replay(1, Some(2)).await;
        assert_eq!(slice.len(), 1);
        assert_eq!(slice[0].payload, b"bar".to_vec());
    }

    #[tokio::test]
    async fn test_idempotency() {
        let log = EventLog::new();
        let req = Uuid::new_v4();

        let r1 = log.append(req, b"one".to_vec()).await;
        let r2 = log.append(req, b"one".to_vec()).await;
        assert_eq!(r1.offset, r2.offset);

        let events = log.replay(0, None).await;
        assert_eq!(events.len(), 1);
    }
}
