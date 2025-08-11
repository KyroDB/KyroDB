//! Durable, crash‑recoverable Event Log with real‑time subscribe.

use anyhow::{Context, Result};
use bincode::Options;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;
pub mod index;
pub use index::{BTreeIndex, Index, PrimaryIndex};
pub mod metrics;
pub mod schema;

/// Current on-disk schema version. Increment when Event/Record layout changes.
pub const SCHEMA_VERSION: u8 = 1;

/// A key-value record stored as event payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub key:   u64,
    pub value: Vec<u8>,
}

/// A vector record stored as event payload for exact similarity search
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorRecord {
    pub key:    u64,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub schema_version: u8,
    pub offset: u64,
    pub timestamp: u64,    // Unix nanos
    pub request_id: Uuid,
    pub payload: Vec<u8>,
}

/// WAL‑and‑Snapshot durable log with broadcast for live tailing
#[derive(Clone)]
pub struct PersistentEventLog {
    inner:    Arc<RwLock<Vec<Event>>>,
    wal:      Arc<RwLock<BufWriter<File>>>,
    data_dir: PathBuf,
    tx:       broadcast::Sender<Event>,
    index:    Arc<RwLock<index::PrimaryIndex>>, // primary key → offset
    #[cfg(feature = "ann-hnsw")]
    ann:      Arc<RwLock<Option<hora::index::hnsw_idx::HNSWIndex<f32, u64>>>>,
}

impl PersistentEventLog {
    /// Open (or create) a log in `data_dir/` and recover state.
    pub async fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir).context("creating data dir")?;

        let snap_path = data_dir.join("snapshot.bin");
        let wal_path  = data_dir.join("wal.bin");

        // bincode options with a safety limit (16 MiB) to avoid huge allocations on corrupt/legacy files
        let bopt = bincode::options().with_limit(16 * 1024 * 1024);

        // 1) Load snapshot if it exists (best-effort; tolerate legacy or corrupt files)
        let mut events: Vec<Event> = if snap_path.exists() {
            let f = File::open(&snap_path).context("opening snapshot")?;
            let mut rdr = BufReader::new(f);
            match bopt.deserialize_from::<_, Vec<Event>>(&mut rdr) {
                Ok(v) if v.iter().all(|e| e.schema_version == SCHEMA_VERSION) => v,
                _ => {
                    eprintln!("⚠️  snapshot incompatible or corrupt; starting with empty state");
                    Vec::new()
                }
            }
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
            loop {
                match bopt.deserialize_from::<_, Event>(&mut rdr) {
                    Ok(ev) => {
                        if ev.schema_version != SCHEMA_VERSION {
                            eprintln!("⚠️  skipping WAL record with mismatched schema_version: {}", ev.schema_version);
                            continue;
                        }
                        if ev.offset as usize >= events.len() {
                            events.push(ev);
                        }
                    }
                    Err(_) => break,
                }
            }
        }
        wal_file.seek(SeekFrom::End(0)).context("seek wal to end")?;
        let wal = Arc::new(RwLock::new(BufWriter::new(wal_file)));

        // 3) Setup broadcast channel
        let (tx, _) = broadcast::channel(1_024);

        // 4) Build log
        // Build index from recovered events
        let mut idx = index::PrimaryIndex::new_btree();
        for ev in &events {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                idx.insert(rec.key, ev.offset);
            }
        }

        // If learned-index feature is enabled and RMI file exists, attempt to load
        #[cfg(feature = "learned-index")]
        {
            let rmi_path = data_dir.join("index-rmi.bin");
            if rmi_path.exists() {
                if let Some(rmi) = crate::index::RmiIndex::load_from_file(&rmi_path) {
                    idx = index::PrimaryIndex::Rmi(rmi);
                }
            }
        }

        let log = Self {
            inner:    Arc::new(RwLock::new(events)),
            wal,
            data_dir,
            tx,
            index:   Arc::new(RwLock::new(idx)),
            #[cfg(feature = "ann-hnsw")]
            ann:     Arc::new(RwLock::new(None)),
        };

        Ok(log)
    }

    /// Append (durably) and return its offset.
    pub async fn append(&self, request_id: Uuid, payload: Vec<u8>) -> Result<u64> {
        let timer = metrics::APPEND_LATENCY_SECONDS.start_timer();
        // bincode options with a safety limit for serialization
        let bopt = bincode::options().with_limit(16 * 1024 * 1024);
        // Idempotency check
        {
            let read = self.inner.read().await;
            if let Some(e) = read.iter().find(|e| e.request_id == request_id) {
                metrics::APPENDS_TOTAL.inc();
                timer.observe_duration();
                return Ok(e.offset);
            }
        }

        // Build event
        let mut write = self.inner.write().await;
        let offset = write.len() as u64;
        let event = Event {
            schema_version: SCHEMA_VERSION,
            offset,
            timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            request_id,
            payload,
        };

        // Write to WAL
        {
            let mut w = self.wal.write().await;
            bopt.serialize_into(&mut *w, &event).context("writing to wal")?;
            w.flush().context("flushing wal")?;
        }

        // Append in-memory
        write.push(event.clone());

        // Update index if payload is Record
        if let Ok(rec) = bincode::deserialize::<Record>(&event.payload) {
            let mut idx = self.index.write().await;
            idx.insert(rec.key, event.offset);
        }

        // Broadcast to subscribers
        let _ = self.tx.send(event);

        metrics::APPENDS_TOTAL.inc();
        timer.observe_duration();
        Ok(offset)
    }

    /// Append a key-value record. Serializes the record with bincode and stores as payload.
    pub async fn append_kv(&self, request_id: Uuid, key: u64, value: Vec<u8>) -> Result<u64> {
        let rec = Record { key, value };
        let bytes = bincode::serialize(&rec)?;
        self.append(request_id, bytes).await
    }

    /// Append a vector record
    pub async fn append_vector(&self, request_id: Uuid, key: u64, vector: Vec<f32>) -> Result<u64> {
        let rec = VectorRecord { key, vector };
        let bytes = bincode::serialize(&rec)?;
        self.append(request_id, bytes).await
    }

    /// Get offset for a given key if present via index.
    pub async fn lookup_key(&self, key: u64) -> Option<u64> {
        let idx = self.index.read().await;
        idx.get(&key)
    }

    /// Fallback: scan the log to find the first event matching key, returning (offset, Record)
    pub async fn find_key_scan(&self, key: u64) -> Option<(u64, Record)> {
        let read = self.inner.read().await;
        for ev in read.iter() {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                if rec.key == key {
                    return Some((ev.offset, rec));
                }
            }
        }
        None
    }

    /// Exact L2 search over all VectorRecord events. Returns top-k (key, distance) ascending.
    pub async fn search_vector_l2(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        if k == 0 { return Vec::new(); }
        let read = self.inner.read().await;
        let mut results: Vec<(u64, f32)> = Vec::new();
        for ev in read.iter() {
            if let Ok(vrec) = bincode::deserialize::<VectorRecord>(&ev.payload) {
                if vrec.vector.len() != query.len() { continue; }
                let mut dist: f32 = 0.0;
                for (a, b) in vrec.vector.iter().zip(query.iter()) {
                    let d = a - b;
                    dist += d * d;
                }
                if results.len() < k {
                    results.push((vrec.key, dist));
                } else if let Some((idx, _)) = results
                    .iter()
                    .enumerate()
                    .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                {
                    if dist < results[idx].1 {
                        results[idx] = (vrec.key, dist);
                    }
                }
            }
        }
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Collect latest (key, offset) pairs for all KV records, sorted by key
    pub async fn collect_key_offset_pairs(&self) -> Vec<(u64, u64)> {
        use std::collections::BTreeMap;
        let read = self.inner.read().await;
        let mut latest: BTreeMap<u64, u64> = BTreeMap::new();
        for ev in read.iter() {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                latest.insert(rec.key, ev.offset);
            }
        }
        latest.into_iter().collect()
    }

    /// Placeholder ANN search: currently forwards to exact L2 until ANN backend is integrated
    pub async fn search_vector_ann(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        #[cfg(feature = "ann-hnsw")]
        {
            use hora::core::ann_index::ANNIndex;
            // Build index lazily if not present
            let mut annw = self.ann.write().await;
            if annw.is_none() {
                let read = self.inner.read().await;
                let dim = query.len();
                let mut idx = hora::index::hnsw_idx::HNSWIndex::<f32, u64>::new(
                    dim,
                    &hora::index::hnsw_params::HNSWParams::<f32>::default(),
                );
                for ev in read.iter() {
                    if let Ok(vrec) = bincode::deserialize::<VectorRecord>(&ev.payload) {
                        if vrec.vector.len() == dim {
                            let _ = idx.add(&vrec.vector, vrec.key);
                        }
                    }
                }
                let _ = idx.build(hora::core::metrics::Metric::Euclidean);
                *annw = Some(idx);
            }
            if let Some(idx) = annw.as_ref() {
                let res = idx.search(&query.to_vec(), k);
                return res.into_iter().map(|id| (id as u64, 0.0)).collect();
            }
        }
        // Fallback
        self.search_vector_l2(query, k).await
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
        let timer = metrics::SNAPSHOT_LATENCY_SECONDS.start_timer();
        // bincode options with a safety limit for serialization
        let bopt = bincode::options().with_limit(16 * 1024 * 1024);
        let path = self.data_dir.join("snapshot.bin");
        let tmp  = self.data_dir.join("snapshot.tmp");
        let wal_path = self.data_dir.join("wal.bin");

        {
            let f = File::create(&tmp).context("creating snapshot.tmp")?;
            let mut w = BufWriter::new(f);
            let read = self.inner.read().await;
            bopt.serialize_into(&mut w, &*read).context("writing snapshot")?;
            w.flush().context("flushing snapshot")?;
        }

        std::fs::rename(&tmp, &path).context("renaming snapshot")?;

        // After a successful snapshot, truncate WAL to checkpoint.
        {
            let mut wal_writer = self.wal.write().await;
            wal_writer.flush().context("flush wal before truncate")?;
            let new_wal = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&wal_path)
                .context("truncate wal after snapshot")?;
            *wal_writer = BufWriter::new(new_wal);
        }
        metrics::SNAPSHOTS_TOTAL.inc();
        timer.observe_duration();
        Ok(())
    }

    /// Get the next write offset (i.e. current log length).
    pub async fn get_offset(&self) -> u64 {
        self.inner.read().await.len() as u64
    }

    /// Path to the schema registry JSON file.
    pub fn registry_path(&self) -> std::path::PathBuf {
        self.data_dir.join("schema.json")
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
