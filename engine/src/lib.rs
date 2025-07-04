//! Durable, crash‑recoverable Event Log with real‑time subscribe.

use anyhow::{Context, Result};
use bincode::{deserialize_from, serialize_into};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

/// A log event
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub offset: u64,
    pub timestamp: u64,    // Unix nanos
    pub request_id: Uuid,
    pub payload: Vec<u8>,
}

/// WAL‑and‑Snapshot durable log with broadcast for live tailing
pub struct PersistentEventLog {
    inner:    Arc<RwLock<Vec<Event>>>,
    wal:      Arc<RwLock<BufWriter<File>>>,
    data_dir: PathBuf,
    tx:       broadcast::Sender<Event>,
}

impl PersistentEventLog {
    /// Open (or create) a log in `data_dir/` and recover state.
    pub async fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir).context("creating data dir")?;

        let snap_path = data_dir.join("snapshot.bin");
        let wal_path  = data_dir.join("wal.bin");

        // 1) Load snapshot if it exists
        let mut events: Vec<Event> = if snap_path.exists() {
            let f = File::open(&snap_path).context("opening snapshot")?;
            let mut rdr = BufReader::new(f);
            deserialize_from(&mut rdr).context("deserializing snapshot")?
        } else {
            Vec::new()
        };

        // 2) Open WAL, replay any events after snapshot
        let mut wal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&wal_path)
            .context("opening wal")?;
        wal_file.seek(SeekFrom::Start(0)).context("seek wal for replay")?;
        {
            let mut rdr = BufReader::new(&wal_file);
            while let Ok(ev) = deserialize_from::<_, Event>(&mut rdr) {
                if ev.offset as usize >= events.len() {
                    events.push(ev);
                }
            }
        }
        wal_file.seek(SeekFrom::End(0)).context("seek wal to end")?;
        let wal = Arc::new(RwLock::new(BufWriter::new(wal_file)));

        // 3) Setup broadcast channel
        let (tx, _) = broadcast::channel(1_024);

        // 4) Build log
        let log = Self {
            inner:    Arc::new(RwLock::new(events)),
            wal,
            data_dir,
            tx,
        };

        Ok(log)
    }

    /// Append (durably) and return its offset.
    pub async fn append(&self, request_id: Uuid, payload: Vec<u8>) -> Result<u64> {
        // Idempotency check
        {
            let read = self.inner.read().await;
            if let Some(e) = read.iter().find(|e| e.request_id == request_id) {
                return Ok(e.offset);
            }
        }

        // Build event
        let mut write = self.inner.write().await;
        let offset = write.len() as u64;
        let event = Event {
            offset,
            timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            request_id,
            payload,
        };

        // Write to WAL
        {
            let mut w = self.wal.write().await;
            serialize_into(&mut *w, &event).context("writing to wal")?;
            w.flush().context("flushing wal")?;
        }

        // Append in-memory
        write.push(event.clone());

        // Broadcast to subscribers
        let _ = self.tx.send(event);

        Ok(offset)
    }

    /// Replay events from `start` (inclusive) to `end` (exclusive).  
    /// If `end` is `None`, replay to the latest.
    pub async fn replay(&self, start: u64, end: Option<u64>) -> Vec<Event> {
        let read = self.inner.read().await;
        let end = end.unwrap_or_else(|| read.len() as u64);
        read.iter()
            .filter(|e| e.offset >= start && e.offset < end)
            .cloned()
            .collect()
    }

    /// Subscribe to all events ≥ `from_offset`.  
    /// Returns `(past_events, live_receiver)`.
    pub async fn subscribe(
        &self,
        from_offset: u64,
    ) -> (Vec<Event>, broadcast::Receiver<Event>) {
        let past = self.replay(from_offset, None).await;
        let rx = self.tx.subscribe();
        (past, rx)
    }

    /// Force-write a full snapshot to disk.
    pub async fn snapshot(&self) -> Result<()> {
        let path = self.data_dir.join("snapshot.bin");
        let tmp  = self.data_dir.join("snapshot.tmp");

        {
            let f = File::create(&tmp).context("creating snapshot.tmp")?;
            let mut w = BufWriter::new(f);
            let read = self.inner.read().await;
            serialize_into(&mut w, &*read).context("writing snapshot")?;
            w.flush().context("flushing snapshot")?;
        }

        std::fs::rename(&tmp, &path).context("renaming snapshot")?;
        Ok(())
    }

    /// Get the next write offset (i.e. current log length).
    pub async fn get_offset(&self) -> u64 {
        self.inner.read().await.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_recovery_after_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // 1st run: append two events, snapshot
        {
            let log = PersistentEventLog::open(&path).await.unwrap();
            assert_eq!(log.append(Uuid::new_v4(), b"a".to_vec()).await.unwrap(), 0);
            assert_eq!(log.append(Uuid::new_v4(), b"b".to_vec()).await.unwrap(), 1);
            log.snapshot().await.unwrap();
        }

        // 2nd run: recover and append one more
        {
            let log = PersistentEventLog::open(&path).await.unwrap();
            assert_eq!(log.get_offset().await, 2);
            assert_eq!(log.append(Uuid::new_v4(), b"c".to_vec()).await.unwrap(), 2);
        }

        // 3rd run: full replay
        {
            let log = PersistentEventLog::open(&path).await.unwrap();
            let all = log.replay(0, None).await;
            let payloads: Vec<_> = all.iter().map(|e| e.payload.clone()).collect();
            assert_eq!(payloads, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        }
    }
}
