//! Durable, crash‑recoverable Event Log with real‑time subscribe.

use anyhow::{Context, Result};
use bincode::Options;
use chrono::Utc;
#[cfg(feature = "ann-hnsw")]
use hora::core::ann_index::ANNIndex;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;
pub mod index;
pub use index::{BTreeIndex, Index, PrimaryIndex};
pub mod metrics;
pub mod schema;

// Convenient alias to reduce type complexity for the mmap payload index
type SnapshotIndex = std::collections::HashMap<u64, (usize, usize)>;

// --- New: fsync policy knob for WAL appends ---
#[derive(Clone, Copy, Debug)]
enum FsyncPolicy {
    Data,
    All,
    None,
}
impl FsyncPolicy {
    fn from_env() -> Self {
        let v = std::env::var("KYRODB_FSYNC_POLICY").unwrap_or_else(|_| "data".to_string());
        match v.to_ascii_lowercase().as_str() {
            "all" => FsyncPolicy::All,
            "none" | "off" | "0" => FsyncPolicy::None,
            _ => FsyncPolicy::Data,
        }
    }
    fn sync_file(&self, f: &File) -> std::io::Result<()> {
        match self {
            FsyncPolicy::Data => f.sync_data(),
            FsyncPolicy::All => f.sync_all(),
            FsyncPolicy::None => Ok(()),
        }
    }
}

// fsync helper for directories (ensure rename/truncate is durable)
pub fn fsync_dir(dir: &std::path::Path) -> std::io::Result<()> {
    // On Unix, opening a directory and calling sync_all is supported.
    // On other platforms, this may be a no-op if unsupported.
    let f = File::open(dir)?;
    f.sync_all()
}

/// Safe parser for a WAL byte stream: [len u32][frame bytes][crc32c u32] repeating.
/// Returns number of well-formed frames consumed; ignores trailing junk or partial frames.
/// Intended for fuzzing and defensive parse tests.
pub fn parse_wal_stream_bytes(mut data: &[u8]) -> usize {
    let mut ok = 0usize;
    while data.len() >= 8 {
        let (len_bytes, rest) = data.split_at(4);
        let len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        if rest.len() < len + 4 {
            break; // partial frame at tail
        }
        let (frame, rest2) = rest.split_at(len);
        let (crc_bytes, rest3) = rest2.split_at(4);
        let crc_read = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        let crc_calc = crc32c::crc32c(frame);
        // attempt to deserialize Event, but don't fail the parser on bincode errors
        let _ = bincode::options()
            .with_limit(16 * 1024 * 1024)
            .deserialize::<Event>(frame);
        if crc_read == crc_calc {
            ok = ok.saturating_add(1);
        }
        data = rest3;
    }
    ok
}

/// Parse snapshot.data payload index from raw bytes. Same layout as build_snapshot_data_index.
/// Returns None on structural error (out-of-bounds) to avoid panics.
pub fn parse_snapshot_data_index_bytes(bytes: &[u8]) -> Option<SnapshotIndex> {
    use std::collections::HashMap;
    let mut pos: usize = 0;
    let mut map: HashMap<u64, (usize, usize)> = HashMap::new();
    while pos + 16 <= bytes.len() {
        let off = u64::from_le_bytes(bytes[pos..pos + 8].try_into().ok()?);
        let len = u64::from_le_bytes(bytes[pos + 8..pos + 16].try_into().ok()?) as usize;
        let start = pos + 16;
        let end = start.checked_add(len)?;
        if end > bytes.len() {
            return None;
        }
        map.insert(off, (start, len));
        pos = end;
    }
    Some(map)
}

/// Current on-disk schema version. Increment when Event/Record layout changes.
pub const SCHEMA_VERSION: u8 = 1;

/// A key-value record stored as event payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub key: u64,
    pub value: Vec<u8>,
}

/// A vector record stored as event payload for exact similarity search
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorRecord {
    pub key: u64,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub schema_version: u8,
    pub offset: u64,
    pub timestamp: u64, // Unix nanos
    pub request_id: Uuid,
    pub payload: Vec<u8>,
}

/// Compaction stats returned by compaction APIs and /compact endpoint
#[derive(Debug, Clone, Serialize)]
pub struct CompactionStats {
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub segments_removed: usize,
    pub segments_active: usize,
    pub keys_retained: usize,
}

/// WAL‑and‑Snapshot durable log with broadcast for live tailing
#[derive(Clone)]
pub struct PersistentEventLog {
    inner: Arc<RwLock<Vec<Event>>>,
    wal: Arc<RwLock<BufWriter<File>>>,
    data_dir: PathBuf,
    tx: broadcast::Sender<Event>,
    index: Arc<RwLock<index::PrimaryIndex>>, // primary key → offset
    // Replace with epoch-guarded shared pointer for linearizable swaps
    next_offset: Arc<RwLock<u64>>, // monotonic sequence (not tied to vec length)
    // current WAL segment index for rotation
    wal_seg_index: Arc<RwLock<u32>>,
    // rotation config
    wal_segment_bytes: Arc<RwLock<Option<u64>>>,
    wal_max_segments: Arc<RwLock<usize>>,
    #[cfg(feature = "ann-hnsw")]
    ann: Arc<RwLock<Option<hora::index::hnsw_idx::HNSWIndex<f32, u64>>>>,
    // --- NEW: snapshot mmap + payload index for direct reads by offset ---
    snapshot_mmap: Arc<RwLock<Option<memmap2::Mmap>>>,
    // offset -> (payload_start, payload_len) within snapshot mmap
    snapshot_payload_index: Arc<RwLock<Option<SnapshotIndex>>>,
    // Small hot payload cache keyed by offset
    wal_block_cache: Arc<RwLock<LruCache<u64, Vec<u8>>>>,
    // new: fsync policy for WAL appends
    fsync_policy: FsyncPolicy,
}

impl PersistentEventLog {
    fn wal_segment_name(idx: u32) -> String {
        format!("wal.{idx:04}")
    }
    fn wal_segment_path(dir: &std::path::Path, idx: u32) -> PathBuf {
        dir.join(Self::wal_segment_name(idx))
    }

    fn list_wal_segments(dir: &std::path::Path) -> Vec<(u32, PathBuf)> {
        let mut segs: Vec<(u32, PathBuf)> = Vec::new();
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                if let Some(name) = e.file_name().to_str() {
                    if name.starts_with("wal.") && name.len() == 8 {
                        if let Ok(idx) = name[4..].parse::<u32>() {
                            segs.push((idx, e.path()));
                        }
                    } else if name == "wal.bin" {
                        // legacy single WAL; treat as segment 0 for replay
                        segs.push((0, e.path()));
                    }
                }
            }
        }
        segs.sort_by_key(|(i, _)| *i);
        segs
    }

    fn wal_total_bytes(dir: &std::path::Path) -> u64 {
        Self::list_wal_segments(dir)
            .into_iter()
            .map(|(_, p)| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .sum()
    }

    /// Open (or create) a log in `data_dir/` and recover state.
    pub async fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir).context("creating data dir")?;

        let snap_path = data_dir.join("snapshot.bin");

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

        // 2) Replay WAL segments in order
        let segs = Self::list_wal_segments(&data_dir);
        let mut last_seg_idx: u32 = 0;
        for (idx, path) in &segs {
            last_seg_idx = (*idx).max(last_seg_idx);
            let mut wal_file = OpenOptions::new()
                .read(true)
                .open(path)
                .with_context(|| format!("opening wal segment: {}", path.display()))?;
            wal_file
                .seek(SeekFrom::Start(0))
                .context("seek wal for replay")?;
            let mut rdr = BufReader::new(&wal_file);
            loop {
                let mut len_buf = [0u8; 4];
                if rdr.read_exact(&mut len_buf).is_err() {
                    break;
                }
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut buf = vec![0u8; len];
                if rdr.read_exact(&mut buf).is_err() {
                    break;
                }
                let mut crc_buf = [0u8; 4];
                if rdr.read_exact(&mut crc_buf).is_err() {
                    break;
                }
                let crc_read = u32::from_le_bytes(crc_buf);
                let crc_calc = crc32c::crc32c(&buf);
                if crc_read != crc_calc {
                    crate::metrics::WAL_CRC_ERRORS_TOTAL.inc();
                    break;
                }
                match bopt.deserialize::<Event>(&buf) {
                    Ok(ev) => {
                        if ev.schema_version != SCHEMA_VERSION {
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

        // 3) Open current segment for append (create if none)
        let seg_to_open = if segs.is_empty() { 0 } else { last_seg_idx };
        let seg_path = Self::wal_segment_path(&data_dir, seg_to_open);
        let mut wal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&seg_path)
            .context("opening wal segment for append")?;
        wal_file.seek(SeekFrom::End(0)).context("seek wal to end")?;
        let wal = Arc::new(RwLock::new(BufWriter::new(wal_file)));

        // 4) Setup broadcast channel
        let (tx, _) = broadcast::channel(1_024);

        // 5) Build index from recovered events
        let mut idx = index::PrimaryIndex::new_btree();
        let mut max_off = 0u64;
        for ev in &events {
            if ev.offset > max_off {
                max_off = ev.offset;
            }
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                idx.insert(rec.key, ev.offset);
            }
        }
        let mut next = if events.is_empty() {
            0
        } else {
            max_off.saturating_add(1)
        };

        // Remove unconditional RMI load by file existence; we'll defer to manifest below
        // #[cfg(feature = "learned-index")]
        // {
        //     let rmi_path = data_dir.join("index-rmi.bin");
        //     if rmi_path.exists() {
        //         if let Some(rmi) = crate::index::RmiIndex::load_from_file(&rmi_path) {
        //             idx = index::PrimaryIndex::Rmi(rmi);
        //         }
        //     }
        // }

        // 6) Read manifest (best-effort) to seed next_offset and paths, and (if enabled) commit RMI selection
        let manifest_path = data_dir.join("manifest.json");
        if manifest_path.exists() {
            if let Ok(text) = std::fs::read_to_string(&manifest_path) {
                if let Ok(j) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(n) = j.get("next_offset").and_then(|v| v.as_u64()) {
                        if n > next {
                            next = n;
                        }
                    }
                    #[cfg(feature = "learned-index")]
                    if let Some(rmi_name) = j.get("rmi").and_then(|v| v.as_str()) {
                        let rmi_path = data_dir.join(rmi_name);
                        if rmi_path.exists() {
                            if let Some(rmi) = crate::index::RmiIndex::load_from_file(&rmi_path) {
                                idx = index::PrimaryIndex::Rmi(rmi);
                            }
                        }
                    }
                }
            }
        }

        let log = Self {
            inner: Arc::new(RwLock::new(events)),
            wal,
            data_dir: data_dir.clone(),
            tx,
            index: Arc::new(RwLock::new(idx)),
            // TODO: migrate to ArcSwap<index::PrimaryIndex>
            next_offset: Arc::new(RwLock::new(next)),
            wal_seg_index: Arc::new(RwLock::new(seg_to_open)),
            wal_segment_bytes: Arc::new(RwLock::new(None)),
            wal_max_segments: Arc::new(RwLock::new(8)),
            #[cfg(feature = "ann-hnsw")]
            ann: Arc::new(RwLock::new(None)),
            snapshot_mmap: Arc::new(RwLock::new(None)),
            snapshot_payload_index: Arc::new(RwLock::new(None)),
            wal_block_cache: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(4096).unwrap()))),
            fsync_policy: FsyncPolicy::from_env(),
        };

        // Try to mmap snapshot.data and build index for fast get() access.
        {
            let data_path = log.data_dir.join("snapshot.data");
            if data_path.exists() {
                if let Ok(file) = File::open(&data_path) {
                    if let Ok(mmap) = unsafe { memmap2::MmapOptions::new().map(&file) } {
                        if let Some(index_map) = Self::build_snapshot_data_index(&mmap) {
                            *log.snapshot_payload_index.write().await = Some(index_map);
                            *log.snapshot_mmap.write().await = Some(mmap);
                        }
                    }
                }
            }
        }

        Ok(log)
    }

    // Parse snapshot.data layout: repeating records of [offset u64][len u64][payload bytes]
    fn build_snapshot_data_index(m: &memmap2::Mmap) -> Option<SnapshotIndex> {
        use std::collections::HashMap;
        let bytes: &[u8] = &m[..];
        let mut pos: usize = 0;
        let mut map: HashMap<u64, (usize, usize)> = HashMap::new();
        while pos + 16 <= bytes.len() {
            let off = u64::from_le_bytes(bytes[pos..pos + 8].try_into().ok()?);
            let len = u64::from_le_bytes(bytes[pos + 8..pos + 16].try_into().ok()?) as usize;
            let start = pos + 16;
            let end = start.checked_add(len)?;
            if end > bytes.len() {
                return None;
            }
            map.insert(off, (start, len));
            pos = end;
        }
        Some(map)
    }

    /// Get the payload bytes for a given logical offset, if present.
    /// Uses mmap-backed snapshot when possible, with in-memory/WAL fallback.
    pub async fn get(&self, offset: u64) -> Option<Vec<u8>> {
        // Cache hit
        if let Some(bytes) = self.wal_block_cache.write().await.get(&offset).cloned() {
            crate::metrics::WAL_BLOCK_CACHE_HITS_TOTAL.inc();
            return Some(bytes);
        }
        crate::metrics::WAL_BLOCK_CACHE_MISSES_TOTAL.inc();
        // Fast path: read from mmap snapshot if indexed
        if let Some(ref mmap) = *self.snapshot_mmap.read().await {
            if let Some(ref idx) = *self.snapshot_payload_index.read().await {
                if let Some((start, len)) = idx.get(&offset).copied() {
                    let end = start.saturating_add(len);
                    if end <= mmap.len() {
                        let bytes = mmap[start..end].to_vec();
                        self.wal_block_cache
                            .write()
                            .await
                            .put(offset, bytes.clone());
                        return Some(bytes);
                    }
                }
            }
        }
        // Fallback: search in-memory events
        let read = self.inner.read().await;
        if let Some(ev) = read.iter().find(|e| e.offset == offset) {
            let bytes = ev.payload.clone();
            self.wal_block_cache
                .write()
                .await
                .put(offset, bytes.clone());
            return Some(bytes);
        }
        None
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

        // Write to WAL current segment (flush + fsync based on policy)
        {
            let mut w = self.wal.write().await;
            let frame = bopt.serialize(&event).context("encode wal frame")?;
            let len = (frame.len() as u32).to_le_bytes();
            let crc = crc32c::crc32c(&frame).to_le_bytes();
            // write [len][frame][crc]
            w.write_all(&len).context("write wal len")?;
            w.write_all(&frame).context("write wal frame")?;
            w.write_all(&crc).context("write wal crc")?;
            w.flush().context("flushing wal")?;
            // policy-controlled fsync
            self.fsync_policy
                .sync_file(w.get_ref())
                .context("fsync wal (policy)")?;
        }
        // maybe rotate
        self.rotate_wal_if_needed().await;

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

    /// Get offset for a given key if present via index with delta-first semantics.
    pub async fn lookup_key(&self, key: u64) -> Option<u64> {
        // Try index first
        {
            let idx = self.index.read().await;
            match &*idx {
                index::PrimaryIndex::BTree(b) => return b.get(&key),
                #[cfg(feature = "learned-index")]
                index::PrimaryIndex::Rmi(r) => {
                    if let Some(v) = r.delta_get(&key) {
                        return Some(v);
                    }
                    // Measure RMI lookup latency overall and, if applicable, during rebuild window
                    let timer_all = crate::metrics::RMI_LOOKUP_LATENCY_SECONDS.start_timer();
                    #[cfg(not(feature = "bench-no-metrics"))]
                    let rebuild_timer_opt = if crate::metrics::rmi_rebuild_in_progress() {
                        Some(
                            crate::metrics::RMI_LOOKUP_LATENCY_DURING_REBUILD_SECONDS.start_timer(),
                        )
                    } else {
                        None
                    };
                    let res = r.predict_get(&key);
                    timer_all.observe_duration();
                    #[cfg(not(feature = "bench-no-metrics"))]
                    if let Some(t) = rebuild_timer_opt {
                        t.observe_duration();
                    }
                    if let Some(v) = res {
                        crate::metrics::RMI_HITS_TOTAL.inc();
                        crate::metrics::RMI_READS_TOTAL.inc();
                        return Some(v);
                    } else {
                        crate::metrics::RMI_MISSES_TOTAL.inc();
                    }
                }
            }
        }
        // Correctness fallback: linear scan for latest record
        crate::metrics::LOOKUP_FALLBACK_SCAN_TOTAL.inc();
        if let Some((off, _rec)) = self.find_key_scan(key).await {
            return Some(off);
        }
        None
    }

    /// Best-effort warmup: fault-in index pages and snapshot mmap.
    pub async fn warmup(&self) {
        // Warm RMI index if present
        #[cfg(feature = "learned-index")]
        {
            let idx = self.index.read().await;
            if let index::PrimaryIndex::Rmi(r) = &*idx {
                r.warm();
            }
        }
        // Touch snapshot mmap pages to reduce first access latency
        if let Some(ref mmap) = *self.snapshot_mmap.read().await {
            let bytes: &[u8] = &mmap[..];
            let page = 4096usize;
            let mut i = 0usize;
            while i < bytes.len() {
                let _ = std::hint::black_box(bytes[i]);
                i = i.saturating_add(page);
            }
        }
    }

    /// Force-write a full snapshot to disk.
    pub async fn snapshot(&self) -> Result<()> {
        let timer = metrics::SNAPSHOT_LATENCY_SECONDS.start_timer();
        // bincode options for serialization; no artificial size limit for full snapshots
        let bopt = bincode::options();
        let path = self.data_dir.join("snapshot.bin");
        let tmp = self.data_dir.join("snapshot.tmp");

        #[cfg(feature = "failpoints")]
        fail::fail_point!("snapshot_before_write", |_| {
            Err(anyhow::anyhow!("failpoint: snapshot_before_write"))
        });
        {
            let f = File::create(&tmp).context("creating snapshot.tmp")?;
            let mut w = BufWriter::new(f);
            let read = self.inner.read().await;
            bopt.serialize_into(&mut w, &*read)
                .context("writing snapshot")?;
            w.flush().context("flushing snapshot")?;
            // Ensure snapshot.tmp contents are durable before rename
            let file = w.into_inner().context("snapshot into_inner")?;
            file.sync_all().context("fsync snapshot.tmp")?;
        }

        #[cfg(feature = "failpoints")]
        fail::fail_point!("snapshot_before_rename", |_| {
            Err(anyhow::anyhow!("failpoint: snapshot_before_rename"))
        });
        std::fs::rename(&tmp, &path).context("renaming snapshot")?;
        // Ensure the rename is durable
        if let Err(e) = fsync_dir(&self.data_dir) {
            eprintln!("⚠️ fsync data dir after snapshot rename failed: {}", e);
        }

        // Also write a contiguous payloads file for mmap-backed direct reads
        let data_path = self.data_dir.join("snapshot.data");
        let data_tmp = self.data_dir.join("snapshot.data.tmp");
        {
            let mut f =
                BufWriter::new(File::create(&data_tmp).context("creating snapshot.data.tmp")?);
            let read = self.inner.read().await;
            for ev in read.iter() {
                let len = ev.payload.len() as u64;
                f.write_all(&ev.offset.to_le_bytes())
                    .context("write snapshot.data offset")?;
                f.write_all(&len.to_le_bytes())
                    .context("write snapshot.data len")?;
                f.write_all(&ev.payload)
                    .context("write snapshot.data payload")?;
            }
            f.flush().context("flush snapshot.data.tmp")?;
            let inner = f.into_inner().context("into_inner snapshot.data.tmp")?;
            inner.sync_all().context("fsync snapshot.data.tmp")?;
        }
        std::fs::rename(&data_tmp, &data_path).context("rename snapshot.data.tmp")?;
        if let Err(e) = fsync_dir(&self.data_dir) {
            eprintln!("⚠️ fsync after snapshot.data rename failed: {}", e);
        }

        // Refresh mmap + payload index to point at the latest snapshot.data
        if let Ok(file) = File::open(&data_path) {
            if let Ok(mmap) = unsafe { memmap2::MmapOptions::new().map(&file) } {
                if let Some(index_map) = Self::build_snapshot_data_index(&mmap) {
                    *self.snapshot_payload_index.write().await = Some(index_map);
                    *self.snapshot_mmap.write().await = Some(mmap);
                }
            }
        }

        // After a successful snapshot, reset WAL segments (truncate to a fresh segment)
        {
            let segs = Self::list_wal_segments(&self.data_dir);
            for (_, p) in segs {
                let _ = std::fs::remove_file(p);
            }
            let seg0 = Self::wal_segment_path(&self.data_dir, 0);
            let new_wal = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&seg0)
                .context("create fresh wal segment after snapshot")?;
            new_wal
                .sync_all()
                .context("fsync fresh wal segment after snapshot")?;
            let mut wal_writer = self.wal.write().await;
            *wal_writer = BufWriter::new(new_wal);
            let mut seg_idx = self.wal_seg_index.write().await;
            *seg_idx = 0;
        }
        // And fsync directory to persist metadata updates
        if let Err(e) = fsync_dir(&self.data_dir) {
            eprintln!("⚠️ fsync data dir after wal reset failed: {}", e);
        }

        metrics::SNAPSHOTS_TOTAL.inc();
        timer.observe_duration();
        // Update manifest after snapshot
        self.write_manifest().await?;
        Ok(())
    }

    /// Compact to latest values per key, preserving original offsets. Then snapshot and truncate WAL.
    pub async fn compact_keep_latest_and_snapshot_stats(&self) -> Result<CompactionStats> {
        use std::collections::BTreeMap;
        let timer = metrics::COMPACTION_DURATION_SECONDS.start_timer();
        let before_bytes = Self::wal_total_bytes(&self.data_dir);
        let before_segments = Self::list_wal_segments(&self.data_dir).len();
        // 1) Build latest offset per key and capture the corresponding events
        let current_events = { self.inner.read().await.clone() };
        let mut latest: BTreeMap<u64, (u64, &Event)> = BTreeMap::new();
        for ev in &current_events {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                // Keep event with the highest offset for this key
                match latest.get(&rec.key) {
                    Some((off, _)) if *off >= ev.offset => {}
                    _ => {
                        latest.insert(rec.key, (ev.offset, ev));
                    }
                }
            }
        }
        // 2) Build compacted vector ordered by offset (ascending)
        let mut compacted: Vec<Event> = latest.into_values().map(|(_, e)| e.clone()).collect();
        compacted.sort_by_key(|e| e.offset);

        // 3) Swap in-memory state and rebuild index consistently
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

        // 4) Persist snapshot of compacted state and reset WAL segments
        self.snapshot().await?;
        metrics::COMPACTIONS_TOTAL.inc();
        timer.observe_duration();
        // Update manifest after compaction
        self.write_manifest().await?;
        let after_bytes = Self::wal_total_bytes(&self.data_dir);
        let after_segments = Self::list_wal_segments(&self.data_dir).len();
        Ok(CompactionStats {
            before_bytes,
            after_bytes,
            segments_removed: before_segments.saturating_sub(after_segments),
            segments_active: after_segments,
            keys_retained: compacted.len(),
        })
    }

    /// Backwards-compatible wrapper without stats
    pub async fn compact_keep_latest_and_snapshot(&self) -> Result<()> {
        let _ = self.compact_keep_latest_and_snapshot_stats().await?;
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

    /// Current WAL size in bytes (sum of segments).
    pub fn wal_size_bytes(&self) -> u64 {
        Self::wal_total_bytes(&self.data_dir)
    }

    /// Minimal manifest path
    pub fn manifest_path(&self) -> std::path::PathBuf {
        self.data_dir.join("manifest.json")
    }

    /// List current WAL segments' basenames
    fn current_wal_segments(&self) -> Vec<String> {
        Self::list_wal_segments(&self.data_dir)
            .into_iter()
            .map(|(_, p)| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect()
    }

    /// Write minimal manifest with next_offset and active files.
    pub async fn write_manifest(&self) -> Result<()> {
        #[cfg(feature = "failpoints")]
        fail::fail_point!("manifest_before_write", |_| {
            Err(anyhow::anyhow!("failpoint: manifest_before_write"))
        });
        let manifest = serde_json::json!({
            "next_offset": self.get_offset().await,
            "snapshot": "snapshot.bin",
            "wal_segments": self.current_wal_segments(),
            "wal_total_bytes": self.wal_size_bytes(),
            "rmi": "index-rmi.bin",
            "ts": chrono::Utc::now().timestamp()
        });
        let tmp = self.data_dir.join("manifest.tmp");
        let mut f = File::create(&tmp).context("create manifest.tmp")?;
        writeln!(f, "{}", manifest).context("write manifest.tmp")?;
        f.sync_all().context("fsync manifest.tmp")?;
        #[cfg(feature = "failpoints")]
        fail::fail_point!("manifest_before_rename", |_| {
            Err(anyhow::anyhow!("failpoint: manifest_before_rename"))
        });
        std::fs::rename(&tmp, self.manifest_path())
            .context("rename manifest.tmp -> manifest.json")?;
        let _ = fsync_dir(&self.data_dir).map_err(|e| {
            eprintln!("⚠️ fsync data dir after manifest rename failed: {}", e);
            e
        });
        Ok(())
    }

    #[cfg(feature = "learned-index")]
    /// Swap the in-memory primary index (e.g., after RMI rebuild) while preserving delta.
    pub async fn swap_primary_index(&self, mut new_index: index::PrimaryIndex) {
        let mut guard = self.index.write().await;
        match (&*guard, &mut new_index) {
            (index::PrimaryIndex::Rmi(old_rmi), index::PrimaryIndex::Rmi(new_rmi)) => {
                for (k, v) in old_rmi.delta_pairs() {
                    new_rmi.insert_delta(k, v);
                }
            }
            (index::PrimaryIndex::Rmi(old_rmi), index::PrimaryIndex::BTree(btree)) => {
                for (k, v) in old_rmi.delta_pairs() {
                    btree.insert(k, v);
                }
            }
            _ => {}
        }
        *guard = new_index;
    }

    /// Replay events from `start` (inclusive) to `end` (exclusive). If None, to latest.
    pub async fn replay(&self, start: u64, end: Option<u64>) -> Vec<Event> {
        let read = self.inner.read().await;
        let end = end.unwrap_or_else(|| read.len() as u64);
        read.iter()
            .filter(|e| e.offset >= start && e.offset < end)
            .cloned()
            .collect()
    }
}

impl PersistentEventLog {
    async fn rotate_wal_if_needed(&self) {
        let seg_bytes = *self.wal_segment_bytes.read().await;
        let Some(max_seg_bytes) = seg_bytes else {
            return;
        };
        if max_seg_bytes == 0 {
            return;
        }
        let cur_idx = *self.wal_seg_index.read().await;
        let cur_path = Self::wal_segment_path(&self.data_dir, cur_idx);
        let cur_size = std::fs::metadata(&cur_path).map(|m| m.len()).unwrap_or(0);
        if cur_size < max_seg_bytes {
            return;
        }
        // rotate
        #[cfg(feature = "failpoints")]
        fail::fail_point!("wal_before_rotate", |_| {});
        let mut w = self.wal.write().await;
        let mut seg_idx = self.wal_seg_index.write().await;
        let next_idx = seg_idx.saturating_add(1);
        let next_path = Self::wal_segment_path(&self.data_dir, next_idx);
        if let Ok(new_file) = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&next_path)
        {
            #[cfg(feature = "failpoints")]
            fail::fail_point!("wal_after_open_new_before_switch", |_| {});
            *w = BufWriter::new(new_file);
            #[cfg(feature = "failpoints")]
            fail::fail_point!("wal_after_switch_before_dirsync", |_| {});
            *seg_idx = next_idx;
            if let Err(e) = fsync_dir(&self.data_dir) {
                eprintln!("⚠️ fsync data dir after wal rotate failed: {}", e);
            }
            drop(w);
            drop(seg_idx);
            #[cfg(feature = "failpoints")]
            fail::fail_point!("wal_after_rotate_before_retention", |_| {});
            #[cfg(feature = "failpoints")]
            fail::fail_point!("wal_before_retention", |_| {});
            // retention
            let max_keep = *self.wal_max_segments.read().await;
            if max_keep > 0 {
                let mut segs = Self::list_wal_segments(&self.data_dir);
                if segs.len() > max_keep {
                    let to_remove = segs.len() - max_keep;
                    for (idx, path) in segs.drain(..to_remove) {
                        if idx != next_idx {
                            let _ = std::fs::remove_file(path);
                        }
                    }
                    if let Err(e) = fsync_dir(&self.data_dir) {
                        eprintln!("⚠️ fsync data dir after wal retention failed: {}", e);
                    }
                }
            }
            #[cfg(feature = "failpoints")]
            fail::fail_point!("wal_after_retention_before_manifest", |_| {});
            if let Err(e) = self.write_manifest().await {
                eprintln!("⚠️ manifest write after rotation failed: {}", e);
            }
        }
    }

    /// Subscribe from a starting offset: returns (past events >= from, live receiver)
    pub async fn subscribe(&self, from: u64) -> (Vec<Event>, broadcast::Receiver<Event>) {
        let past = {
            let read = self.inner.read().await;
            read.iter()
                .filter(|e| e.offset >= from)
                .cloned()
                .collect::<Vec<_>>()
        };
        let rx = self.tx.subscribe();
        (past, rx)
    }

    /// Collect latest (key, offset) pairs for all keys, suitable for RMI building.
    pub async fn collect_key_offset_pairs(&self) -> Vec<(u64, u64)> {
        use std::collections::BTreeMap;
        let read = self.inner.read().await;
        let mut latest: BTreeMap<u64, u64> = BTreeMap::new();
        for ev in read.iter() {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                // keep highest offset per key
                match latest.get(&rec.key) {
                    Some(off) if *off >= ev.offset => {}
                    _ => {
                        latest.insert(rec.key, ev.offset);
                    }
                }
            }
        }
        latest.into_iter().collect()
    }

    /// Linear scan fallback to find the latest record for a key (O(n)).
    pub async fn find_key_scan(&self, key: u64) -> Option<(u64, Record)> {
        let read = self.inner.read().await;
        for ev in read.iter().rev() {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                if rec.key == key {
                    return Some((ev.offset, rec));
                }
            }
        }
        None
    }

    /// Configure WAL rotation policy.
    pub async fn configure_wal_rotation(
        &self,
        max_segment_bytes: Option<u64>,
        max_segments: usize,
    ) {
        *self.wal_segment_bytes.write().await = max_segment_bytes;
        *self.wal_max_segments.write().await = max_segments.max(1);
    }

    /// Backwards-compatible alias used by older tests.
    pub async fn set_wal_rotation(&self, max_segment_bytes: Option<u64>, max_segments: usize) {
        self.configure_wal_rotation(max_segment_bytes, max_segments)
            .await;
    }

    /// Naive L2 search over latest vectors per key (CPU baseline).
    pub async fn search_vector_l2(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        use std::collections::HashSet;
        let mut out: Vec<(u64, f32)> = Vec::new();
        let mut seen: HashSet<u64> = HashSet::new();
        let read = self.inner.read().await;
        for ev in read.iter().rev() {
            if let Ok(vr) = bincode::deserialize::<VectorRecord>(&ev.payload) {
                if seen.contains(&vr.key) {
                    continue;
                }
                if vr.vector.len() != query.len() {
                    continue;
                }
                seen.insert(vr.key);
                let mut dist = 0.0f32;
                for (a, b) in vr.vector.iter().zip(query.iter()) {
                    let d = a - b;
                    dist += d * d;
                }
                out.push((vr.key, dist));
            }
        }
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(k);
        out
    }

    /// Approximate ANN search when HNSW index is available; falls back to L2 scan otherwise.
    pub async fn search_vector_ann(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        #[cfg(feature = "ann-hnsw")]
        {
            if let Some(idx) = self.ann.read().await.as_ref() {
                // hora's search returns Vec<u64> (just the IDs), we need to add dummy distances
                let res = idx.search(query, k);
                return res.into_iter().map(|id| (id, 0.0)).collect();
            }
        }
        // Fallback if ANN disabled or not built
        self.search_vector_l2(query, k).await
    }

    /// Get data directory path (for internal use)
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Get current WAL segments (for internal use)
    pub fn get_wal_segments(&self) -> Vec<String> {
        self.current_wal_segments()
    }
}
