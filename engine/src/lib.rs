//! Durable, crash‑recoverable Event Log with real‑time subscribe.

use anyhow::{Context, Result};
use bincode::Options;
use chrono::Utc;
use crc32c;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write, Read};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;
pub mod index;
pub use index::{BTreeIndex, Index, PrimaryIndex};
pub mod metrics;
pub mod schema;

// fsync helper for directories (ensure rename/truncate is durable)
fn fsync_dir(dir: &std::path::Path) -> std::io::Result<()> {
    // On Unix, opening a directory and calling sync_all is supported.
    // On other platforms, this may be a no-op if unsupported.
    let f = File::open(dir)?;
    f.sync_all()
}

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
    next_offset: Arc<RwLock<u64>>,              // monotonic sequence (not tied to vec length)
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
                // read length prefix (u32), then payload, then crc32c(u32)
                let mut len_buf = [0u8; 4];
                if let Err(_) = rdr.read_exact(&mut len_buf) { break; }
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut buf = vec![0u8; len];
                if let Err(_) = rdr.read_exact(&mut buf) { break; }
                let mut crc_buf = [0u8; 4];
                if let Err(_) = rdr.read_exact(&mut crc_buf) { break; }
                let crc_read = u32::from_le_bytes(crc_buf);
                let crc_calc = crc32c::crc32c(&buf);
                if crc_read != crc_calc { crate::metrics::WAL_CRC_ERRORS_TOTAL.inc(); break; }
                match bopt.deserialize::<Event>(&buf) {
                    Ok(ev) => {
                        if ev.schema_version != SCHEMA_VERSION { continue; }
                        if ev.offset as usize >= events.len() { events.push(ev); }
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
        let mut max_off = 0u64;
        for ev in &events {
            if ev.offset > max_off { max_off = ev.offset; }
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                idx.insert(rec.key, ev.offset);
            }
        }
        let mut next = if events.is_empty() { 0 } else { max_off.saturating_add(1) };

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

        // 5) Read manifest (best-effort) to seed next_offset and paths
        let manifest_path = data_dir.join("manifest.json");
        if manifest_path.exists() {
            if let Ok(text) = std::fs::read_to_string(&manifest_path) {
                if let Ok(j) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(n) = j.get("next_offset").and_then(|v| v.as_u64()) {
                        if n > next { next = n; }
                    }
                }
            }
        }

        let log = Self {
            inner:    Arc::new(RwLock::new(events)),
            wal,
            data_dir: data_dir.clone(),
            tx,
            index:   Arc::new(RwLock::new(idx)),
            next_offset: Arc::new(RwLock::new(next)),
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
        let mut noff = self.next_offset.write().await;
        let offset = *noff;
        *noff = noff.saturating_add(1);
        let event = Event {
            schema_version: SCHEMA_VERSION,
            offset,
            timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            request_id,
            payload,
        };
        drop(noff);

        // Write to WAL (flush + fsync for durability)
        {
            let mut w = self.wal.write().await;
            let mut frame = bopt.serialize(&event).context("encode wal frame")?;
            let len = (frame.len() as u32).to_le_bytes();
            let crc = crc32c::crc32c(&frame).to_le_bytes();
            // write [len][frame][crc]
            w.write_all(&len).context("write wal len")?;
            w.write_all(&frame).context("write wal frame")?;
            w.write_all(&crc).context("write wal crc")?;
            w.flush().context("flushing wal")?;
            w.get_ref().sync_data().context("fsync wal (data)")?;
        }

        // Append in-memory
        write.push(event.clone());
        drop(write);

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
            // Ensure snapshot.tmp contents are durable before rename
            let mut file = w.into_inner().context("snapshot into_inner")?;
            file.sync_all().context("fsync snapshot.tmp")?;
        }

        std::fs::rename(&tmp, &path).context("renaming snapshot")?;
        // Ensure the rename is durable
        fsync_dir(&self.data_dir).ok();

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
            // fsync the truncated WAL file to persist size change
            new_wal.sync_all().ok();
            *wal_writer = BufWriter::new(new_wal);
        }
        // And fsync directory to persist metadata updates
        fsync_dir(&self.data_dir).ok();

        metrics::SNAPSHOTS_TOTAL.inc();
        timer.observe_duration();
        // Update manifest after snapshot
        self.write_manifest().await;
        Ok(())
    }

    /// Compact to latest values per key, preserving original offsets. Then snapshot and truncate WAL.
    pub async fn compact_keep_latest_and_snapshot(&self) -> Result<()> {
        use std::collections::BTreeMap;
        let timer = metrics::COMPACTION_DURATION_SECONDS.start_timer();
        // 1) Build latest offset per key and capture the corresponding events
        let current_events = { self.inner.read().await.clone() };
        let mut latest: BTreeMap<u64, (u64, &Event)> = BTreeMap::new();
        for ev in &current_events {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                // Keep event with the highest offset for this key
                match latest.get(&rec.key) {
                    Some((off, _)) if *off >= ev.offset => {},
                    _ => { latest.insert(rec.key, (ev.offset, ev)); }
                }
            }
        }
        // 2) Build compacted vector ordered by offset (ascending) to keep time order of latest writes
        let mut compacted: Vec<Event> = latest.into_values().map(|(_, e)| e.clone()).collect();
        compacted.sort_by_key(|e| e.offset);

        // 3) Swap in-memory state under write lock and rebuild index consistently
        {
            let mut w = self.inner.write().await;
            *w = compacted.clone();
        }
        {
            let mut idx = self.index.write().await;
            *idx = index::PrimaryIndex::new_btree();
            for ev in &compacted {
                if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                    idx.insert(rec.key, ev.offset);
                }
            }
        }

        // 4) Persist snapshot of compacted state and truncate WAL
        self.snapshot().await?;
        metrics::COMPACTIONS_TOTAL.inc();
        timer.observe_duration();
        // Update manifest after compaction
        self.write_manifest().await;
        Ok(())
    }

    /// Get the next write offset (i.e. current monotonic sequence).
    pub async fn get_offset(&self) -> u64 {
        *self.next_offset.read().await
    }

    /// Path to the schema registry JSON file.
    pub fn registry_path(&self) -> std::path::PathBuf {
        self.data_dir.join("schema.json")
    }

    /// Current WAL size in bytes (best-effort). Useful for size-based rotation/compaction triggers.
    pub fn wal_size_bytes(&self) -> u64 {
        let path = self.data_dir.join("wal.bin");
        std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
    }

    /// Minimal manifest path
    pub fn manifest_path(&self) -> std::path::PathBuf { self.data_dir.join("manifest.json") }

    /// Write minimal manifest with next_offset and active files.
    pub async fn write_manifest(&self) {
        let manifest = serde_json::json!({
            "next_offset": self.get_offset().await,
            "snapshot": "snapshot.bin",
            "wal": "wal.bin",
            "rmi": "index-rmi.bin",
            "ts": chrono::Utc::now().timestamp()
        });
        let tmp = self.data_dir.join("manifest.tmp");
        if let Ok(mut f) = File::create(&tmp) {
            let _ = writeln!(f, "{}", manifest);
            let _ = f.sync_all();
            let _ = std::fs::rename(&tmp, self.manifest_path());
            let _ = fsync_dir(&self.data_dir);
        }
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

// Minimal HTTP filter for compaction to enable testing without the binary main
#[cfg(feature = "http-test")] // optional feature to avoid polluting default build
pub mod http_filters {
    use super::*;
    use warp::Filter;

    pub fn compact_route(log: Arc<PersistentEventLog>) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path("compact")
            .and(warp::post())
            .and_then(move || {
                let log = log.clone();
                async move {
                    match log.compact_keep_latest_and_snapshot().await {
                        Ok(_) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({"compact":"ok"})),
                            warp::http::StatusCode::OK,
                        )),
                        Err(e) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({"error": e.to_string()})),
                            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                        )),
                    }
                }
            })
    }
}
