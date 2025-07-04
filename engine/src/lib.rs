
//! Durable, crash-recoverable Event Log

use anyhow::{Context, Result};
use bincode::{deserialize_from, serialize_into};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::RwLock;
use uuid::Uuid;

/// A log event
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub offset: u64,
    pub timestamp: u64, // Unix nanos
    pub request_id: Uuid,
    pub payload: Vec<u8>,
}

/// WAL‐and‐Snapshot durable log
pub struct PersistentEventLog {
    inner: Arc<RwLock<Vec<Event>>>,
    wal: Arc<RwLock<BufWriter<File>>>,
    data_dir: PathBuf,
}

impl PersistentEventLog {
    /// Open (or create) a log in `data_dir/` and recover state.
    pub async fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir).context("creating data dir")?;

        // Snapshot path and WAL path
        let snap_path = data_dir.join("snapshot.bin");
        let wal_path  = data_dir.join("wal.bin");

        // 1) Recover snapshot (if exists)
        let mut events: Vec<Event> = if snap_path.exists() {
            let f = File::open(&snap_path).context("opening snapshot")?;
            let mut rdr = BufReader::new(f);
            deserialize_from(&mut rdr).context("deserializing snapshot")?
        } else {
            Vec::new()
        };

        // 2) Open WAL for append, then replay any entries after snapshot
        let mut wal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&wal_path)
            .context("opening wal")?;
        // Seek to start for replay:
        wal_file.seek(SeekFrom::Start(0)).context("seek wal")?;
        {
            let mut rdr = BufReader::new(&wal_file);
            while let Ok(ev) = deserialize_from::<_, Event>(&mut rdr) {
                // Only append events beyond snapshot
                if ev.offset as usize >= events.len() {
                    events.push(ev);
                }
            }
        }
        // Now reposition WAL writer at end:
        wal_file.seek(SeekFrom::End(0)).context("seek wal to end")?;
        let wal = Arc::new(RwLock::new(BufWriter::new(wal_file)));

        let log = Self {
            inner: Arc::new(RwLock::new(events)),
            wal,
            data_dir,
        };

        Ok(log)
    }

    /// Append (durably) and return its offset.
    pub async fn append(&self, request_id: Uuid, payload: Vec<u8>) -> Result<u64> {
        // 1) In-memory idempotency check
        {
            let read = self.inner.read().await;
            if let Some(e) = read.iter().find(|e| e.request_id == request_id) {
                return Ok(e.offset);
            }
        }

        // 2) Build the new event
        let mut write = self.inner.write().await;
        let offset = write.len() as u64;
        let event = Event {
            offset,
            timestamp: Utc::now().timestamp_nanos() as u64,
            request_id,
            payload,
        };

        // 3) Serialize to WAL and flush
        {
            let mut w = self.wal.write().await;
            serialize_into(&mut *w, &event).context("writing to wal")?;
            w.flush().context("flushing wal")?;
        }

        // 4) Append in memory
        write.push(event);
        Ok(offset)
    }

    /// Replay events in-memory; this never touches disk.
    pub async fn replay(&self, start: u64, end: Option<u64>) -> Vec<Event> {
        let read = self.inner.read().await;
        let end = end.unwrap_or_else(|| read.len() as u64);
        read.iter()
            .filter(|e| e.offset >= start && e.offset < end)
            .cloned()
            .collect()
    }

    /// Force a new full snapshot to disk
    pub async fn snapshot(&self) -> Result<()> {
        let path = self.data_dir.join("snapshot.bin");
        let tmp  = self.data_dir.join("snapshot.tmp");
        // Serialize current state to temp
        {
            let f = File::create(&tmp).context("creating snapshot.tmp")?;
            let mut w = BufWriter::new(f);
            let read = self.inner.read().await;
            serialize_into(&mut w, &*read).context("writing snapshot")?;
            w.flush().context("flushing snapshot")?;
        }
        // Atomically rename
        std::fs::rename(&tmp, &path).context("renaming snapshot")?;
        Ok(())
    }

    /// Current log length
    pub async fn get_offset(&self) -> u64 {
        self.inner.read().await.len() as u64
    }
}
