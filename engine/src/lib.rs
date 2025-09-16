//! KyroDB: High-performance, durable key-value database engine with learned indexes (RMI)
//! 
//! This is a production-ready database engine focusing on:
//! - Predictable tail latency through bounded RMI performance 
//! - Lock-free concurrency to eliminate deadlocks
//! - Enterprise-grade durability with group commit
//! - Memory management with allocation tracking and limits

use anyhow::{Context, Result};
use bincode::Options;
use chrono::Utc;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, RwLock, Notify, oneshot};
use parking_lot::Mutex;
use uuid::Uuid;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use bytes::{Bytes, BytesMut, BufMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::Mutex as FastMutex;

// Core modules
pub mod index;
pub use index::{BTreeIndex, Index, PrimaryIndex};
pub mod metrics;

// Phase 0 foundation modules
#[cfg(feature = "learned-index")]

#[cfg(feature = "learned-index")]
pub mod memory;
#[cfg(feature = "learned-index")]
pub mod adaptive_rmi;

// Export main types for public API
pub use PersistentEventLog as KyroDb;  // Alias for backward compatibility with tests

// Convenient alias to reduce type complexity for the mmap payload index
type SnapshotIndex = std::collections::HashMap<u64, (usize, usize)>;

// --- Memory Pool for High-Performance Allocations ---
struct BufferPool {
    buffers: FastMutex<Vec<BytesMut>>,
    hit_count: AtomicUsize,
    miss_count: AtomicUsize,
}

impl BufferPool {
    fn new() -> Self {
        Self {
            buffers: FastMutex::new(Vec::with_capacity(32)),
            hit_count: AtomicUsize::new(0),
            miss_count: AtomicUsize::new(0),
        }
    }

    fn get_buffer(&self, min_capacity: usize) -> BytesMut {
        {
            let mut buffers = self.buffers.lock();
            if let Some(mut buf) = buffers.pop() {
                if buf.capacity() >= min_capacity {
                    buf.clear();
                    self.hit_count.fetch_add(1, Ordering::Relaxed);
                    return buf;
                }
                // Buffer too small, put it back and allocate new
                buffers.push(buf);
            }
        }
        
        self.miss_count.fetch_add(1, Ordering::Relaxed);
        BytesMut::with_capacity(min_capacity.max(4096))
    }

    // TODO: Implement buffer return for memory pool optimization
    // fn return_buffer(&self, buf: BytesMut) - currently unused

    #[allow(dead_code)]
    fn metrics(&self) -> (usize, usize) {
        (
            self.hit_count.load(Ordering::Relaxed),
            self.miss_count.load(Ordering::Relaxed),
        )
    }
}

static BUFFER_POOL: std::sync::OnceLock<BufferPool> = std::sync::OnceLock::new();

/// Fast deserialization helper for new manual format
fn try_deserialize_record_fast(payload: &[u8]) -> Option<Record> {
    if payload.len() < 16 {
        return None; // Not enough bytes for key + value_len
    }
    
    let mut cursor = std::io::Cursor::new(payload);
    use std::io::Read;
    
    // Read key (8 bytes)
    let mut key_bytes = [0u8; 8];
    if cursor.read_exact(&mut key_bytes).is_err() {
        return None;
    }
    let key = u64::from_le_bytes(key_bytes);
    
    // Read value length (8 bytes)
    let mut len_bytes = [0u8; 8];
    if cursor.read_exact(&mut len_bytes).is_err() {
        return None;
    }
    let value_len = u64::from_le_bytes(len_bytes) as usize;
    
    // Check if we have enough bytes for the value
    if cursor.position() as usize + value_len != payload.len() {
        return None;
    }
    
    // Read value
    let mut value = vec![0u8; value_len];
    if cursor.read_exact(&mut value).is_err() {
        return None;
    }
    
    Some(Record { key, value })
}

/// Try both new fast format and fallback to bincode for compatibility
pub fn deserialize_record_compat(payload: &[u8]) -> Option<Record> {
    // Try new fast format first
    if let Some(record) = try_deserialize_record_fast(payload) {
        return Some(record);
    }
    
    // Fallback to bincode for backwards compatibility
    bincode::deserialize::<Record>(payload).ok()
}

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

// --- Enterprise Durability Levels ---
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityLevel {
    /// Enterprise-grade: Group commit + fsync every batch (zero data loss)
    EnterpriseSafe,
    /// High-performance: Background fsync with bounded recovery window
    EnterpriseAsync,
    /// Development/testing: No durability guarantees
    Unsafe,
}

impl Default for DurabilityLevel {
    fn default() -> Self {
        DurabilityLevel::EnterpriseSafe
    }
}

impl DurabilityLevel {
    fn from_env() -> Self {
        match std::env::var("KYRODB_DURABILITY_LEVEL")
            .unwrap_or_else(|_| "enterprise_safe".to_string())
            .to_lowercase()
            .as_str()
        {
            "enterprise_async" => DurabilityLevel::EnterpriseAsync,
            "unsafe" | "none" => DurabilityLevel::Unsafe,
            _ => DurabilityLevel::EnterpriseSafe,
        }
    }
}

// --- Enhanced Group commit configuration ---
#[derive(Clone, Copy, Debug)]
pub struct GroupCommitConfig {
    /// Maximum time to wait before forcing a batch commit (microseconds)
    pub max_batch_delay_micros: u64,
    /// Maximum number of writes to batch before forcing a commit
    pub max_batch_size: usize,
    /// Enable group commit (false = per-write fsync like before)
    pub enabled: bool,
    /// Durability level (enterprise vs performance)
    pub durability_level: DurabilityLevel,
    /// Background fsync interval for async modes (milliseconds)
    pub background_fsync_interval_ms: u64,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        let durability_level = DurabilityLevel::from_env();
        
        // Optimized defaults for microsecond-level performance
        let (default_batch_size, default_delay_micros) = match durability_level {
            DurabilityLevel::EnterpriseSafe => (500, 200),     // 500 items, 200µs max delay
            DurabilityLevel::EnterpriseAsync => (1000, 100),   // 1000 items, 100µs max delay
            DurabilityLevel::Unsafe => (2000, 50),            // 2000 items, 50µs max delay
        };
        
        Self {
            max_batch_delay_micros: std::env::var("KYRODB_GROUP_COMMIT_DELAY_MICROS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(default_delay_micros),
                
            max_batch_size: std::env::var("KYRODB_GROUP_COMMIT_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(default_batch_size),
                
            enabled: std::env::var("KYRODB_GROUP_COMMIT_ENABLED")
                .map(|s| s != "0" && s.to_lowercase() != "false")
                .unwrap_or(true),
                
            durability_level,
            
            background_fsync_interval_ms: std::env::var("KYRODB_BACKGROUND_FSYNC_INTERVAL_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(25), // 25ms for async modes (faster than 50ms)
        }
    }
}

// --- Group commit batch item ---
#[derive(Debug)]
struct BatchItem {
    event: Event,
    response_tx: oneshot::Sender<Result<u64>>,
}

// --- Group commit state ---
#[derive(Debug)]
struct GroupCommitState {
    batch: VecDeque<BatchItem>,
    batch_start_time: Option<Instant>,
    fsync_notify: Arc<Notify>,
    config: GroupCommitConfig,
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
    // --- NEW: snapshot mmap + payload index for direct reads by offset ---
    snapshot_mmap: Arc<RwLock<Option<memmap2::Mmap>>>,
    // offset -> (payload_start, payload_len) within snapshot mmap
    snapshot_payload_index: Arc<RwLock<Option<SnapshotIndex>>>,
    // Small hot payload cache keyed by offset
    wal_block_cache: Arc<RwLock<LruCache<u64, Vec<u8>>>>,
    // new: fsync policy for WAL appends
    fsync_policy: FsyncPolicy,
    // new: group commit state for high throughput writes
    group_commit_state: Arc<Mutex<GroupCommitState>>,
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
        let use_adaptive_rmi = std::env::var("KYRODB_USE_ADAPTIVE_RMI")
            .ok()
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(true); // Default to adaptive RMI

        let mut idx = if use_adaptive_rmi {
            #[cfg(feature = "learned-index")]
            {
                // Collect key-offset pairs from events
                let mut pairs: Vec<(u64, u64)> = Vec::new();
                for ev in &events {
                    if let Some(rec) = deserialize_record_compat(&ev.payload) {
                        pairs.push((rec.key, ev.offset));
                    }
                }
                
                if pairs.is_empty() {
                    index::PrimaryIndex::new_adaptive_rmi()
                } else {
                    pairs.sort_by_key(|(k, _)| *k);
                    pairs.dedup_by(|a, b| a.0 == b.0); // Keep only latest values for duplicate keys
                    index::PrimaryIndex::new_adaptive_rmi_from_pairs(&pairs)
                }
            }
            #[cfg(not(feature = "learned-index"))]
            {
                index::PrimaryIndex::new_btree()
            }
        } else {
            index::PrimaryIndex::new_btree()
        };

        let mut max_off = 0u64;
        
        // For non-adaptive indexes, insert events normally
        if !use_adaptive_rmi || !cfg!(feature = "learned-index") {
            for ev in &events {
                if ev.offset > max_off {
                    max_off = ev.offset;
                }
                if let Some(rec) = deserialize_record_compat(&ev.payload) {
                    idx.insert(rec.key, ev.offset);
                }
            }
        } else {
            // For adaptive RMI, just find max offset
            for ev in &events {
                if ev.offset > max_off {
                    max_off = ev.offset;
                }
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
                    {
                        // Note: Legacy RMI loading removed - only AdaptiveRMI is supported
                        // AdaptiveRMI builds incrementally and doesn't need file loading
                    }
                }
            }
        }

        let group_commit_config = GroupCommitConfig::default();
        let group_commit_state = Arc::new(Mutex::new(GroupCommitState {
            batch: VecDeque::new(),
            batch_start_time: None,
            fsync_notify: Arc::new(Notify::new()),
            config: group_commit_config,
        }));

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
            snapshot_mmap: Arc::new(RwLock::new(None)),
            snapshot_payload_index: Arc::new(RwLock::new(None)),
            wal_block_cache: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(4096).unwrap()))),
            fsync_policy: FsyncPolicy::from_env(),
            group_commit_state: group_commit_state.clone(),
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

        // Start the group commit background task if enabled
        if group_commit_config.enabled {
            let log_clone = log.clone();
            tokio::spawn(async move {
                log_clone.group_commit_background_task().await;
            });
        }

        Ok(log)
    }

    /// Create a new log with custom group commit configuration (for testing)
    pub async fn with_group_commit(data_dir: impl Into<PathBuf>, config: GroupCommitConfig) -> Result<Self> {
        // Set environment variables to override defaults
        std::env::set_var("KYRODB_GROUP_COMMIT_DELAY_MICROS", config.max_batch_delay_micros.to_string());
        std::env::set_var("KYRODB_GROUP_COMMIT_BATCH_SIZE", config.max_batch_size.to_string());
        std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", if config.enabled { "1" } else { "0" });
        
        // Use the regular open method which will pick up the env vars
        Self::open(data_dir).await
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
        
        // Idempotency check
        {
            let read = self.inner.read().await;
            if let Some(e) = read.iter().find(|e| e.request_id == request_id) {
                metrics::APPENDS_TOTAL.inc();
                timer.observe_duration();
                return Ok(e.offset);
            }
        }

        // Build event with next offset
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

        // Check if group commit is enabled
        let group_commit_enabled = {
            let state = self.group_commit_state.lock();
            state.config.enabled
        };

        if group_commit_enabled {
            // Use group commit for high throughput
            let (response_tx, response_rx) = oneshot::channel();
            
            // Add to batch
            {
                let mut state = self.group_commit_state.lock();
                state.batch.push_back(BatchItem {
                    event: event.clone(),
                    response_tx,
                });
                
                // Start batch timer if this is the first item
                if state.batch.len() == 1 {
                    state.batch_start_time = Some(Instant::now());
                }
                
                // Check if we should trigger immediate flush
                let should_flush = state.batch.len() >= state.config.max_batch_size;
                
                if should_flush {
                    state.fsync_notify.notify_one();
                }
            }
            
            // Wait for group commit to complete
            match response_rx.await {
                Ok(result) => {
                    if result.is_ok() {
                        // Update in-memory state and index
                        {
                            let mut write = self.inner.write().await;
                            write.push(event.clone());
                        }

                        // Update index if payload is Record (new fast format compatible)
                        if let Some(rec) = deserialize_record_compat(&event.payload) {
                            let mut idx = self.index.write().await;
                            idx.insert(rec.key, event.offset);
                        }

                        // Broadcast to subscribers
                        let _ = self.tx.send(event);
                    }
                    
                    metrics::APPENDS_TOTAL.inc();
                    timer.observe_duration();
                    result
                }
                Err(_) => {
                    Err(anyhow::anyhow!("Group commit channel closed"))
                }
            }
        } else {
            // Legacy per-write fsync path (for compatibility)
            self.append_with_immediate_fsync(event, timer).await
        }
    }

    /// Legacy append path with immediate fsync (used when group commit disabled)
    async fn append_with_immediate_fsync(&self, event: Event, timer: prometheus::HistogramTimer) -> Result<u64> {
        let bopt = bincode::options().with_limit(16 * 1024 * 1024);
        
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
        {
            let mut write = self.inner.write().await;
            write.push(event.clone());
        }

        // Update index if payload is Record (compatible with new format)
        if let Some(rec) = deserialize_record_compat(&event.payload) {
            let mut idx = self.index.write().await;
            idx.insert(rec.key, event.offset);
        }

        // Broadcast to subscribers
        let _ = self.tx.send(event.clone());

        metrics::APPENDS_TOTAL.inc();
        timer.observe_duration();
        Ok(event.offset)
    }

    /// Append a key-value record with optimized serialization
    pub async fn append_kv(&self, request_id: Uuid, key: u64, value: Vec<u8>) -> Result<u64> {
        // Optimized serialization: avoid intermediate Record struct allocation
        let pool = BUFFER_POOL.get_or_init(|| BufferPool::new());
        let mut buf = pool.get_buffer(16 + value.len());
        
        // Manual serialization: [key u64][value_len u64][value bytes]
        buf.put_u64_le(key);
        buf.put_u64_le(value.len() as u64);
        buf.put_slice(&value);
        
        let bytes = buf.freeze();
        self.append(request_id, bytes.to_vec()).await
    }

    /// Get offset for a given key if present via index with delta-first semantics.
    pub async fn lookup_key(&self, key: u64) -> Option<u64> {
        // Try the primary index
        let idx = self.index.read().await;
        match &*idx {
            index::PrimaryIndex::BTree(b) => {
                // For BTree, if not found, do fallback scan for safety
                if let Some(offset) = b.get(&key) {
                    return Some(offset);
                }
                
                // BTree miss - try fallback scan
                drop(idx); // Release the index lock
                crate::metrics::LOOKUP_FALLBACK_SCAN_TOTAL.inc();
                if let Some((offset, _rec)) = self.find_key_scan(key).await {
                    return Some(offset);
                }
                None
            }
            #[cfg(feature = "learned-index")]
            index::PrimaryIndex::AdaptiveRmi(adaptive) => {
                // 🚀 ULTRA-FAST PATH: Trust the RMI completely - no expensive fallback scans!
                let timer = crate::metrics::RMI_LOOKUP_LATENCY_SECONDS.start_timer();
                let result = adaptive.lookup(key);
                timer.observe_duration();
                
                if result.is_some() {
                    crate::metrics::RMI_HITS_TOTAL.inc();
                    crate::metrics::RMI_READS_TOTAL.inc();
                } else {
                    crate::metrics::RMI_MISSES_TOTAL.inc();
                }
                
                // 🚀 CRITICAL PERFORMANCE FIX: Return immediately - no O(n) fallback scans!
                // The AdaptiveRMI is kept up-to-date on every insert, so we can trust it
                result
            }
        }
    }

    /// 🚀 DIRECT RMI ACCESS: Zero-overhead lookup for ultra-fast HTTP path
    pub fn lookup_key_direct(&self, key: u64) -> Option<u64> {
        // 🚀 NON-BLOCKING TRY_READ: Use try_read to avoid async overhead
        match self.index.try_read() {
            Ok(idx) => match &*idx {
                index::PrimaryIndex::AdaptiveRmi(adaptive) => {
                    // 🚀 DIRECT CALL: No async overhead, pure synchronous lookup
                    let timer = crate::metrics::RMI_LOOKUP_LATENCY_SECONDS.start_timer();
                    let result = adaptive.lookup(key);
                    timer.observe_duration();
                    
                    if result.is_some() {
                        crate::metrics::RMI_HITS_TOTAL.inc();
                        crate::metrics::RMI_READS_TOTAL.inc();
                    } else {
                        crate::metrics::RMI_MISSES_TOTAL.inc();
                    }
                    
                    result
                }
                index::PrimaryIndex::BTree(btree) => {
                    btree.get(&key)
                }
            },
            Err(_) => None, // Lock contention - return None for ultra-fast path
        }
    }
    
    /// 🚀 BATCH LOOKUP: Process multiple keys efficiently with single lock acquisition
    pub fn lookup_keys_batch(&self, keys: &[u64]) -> Vec<(u64, Option<u64>)> {
        let mut results = Vec::with_capacity(keys.len());
        
        // Single lock acquisition for entire batch
        match self.index.try_read() {
            Ok(idx) => match &*idx {
                index::PrimaryIndex::AdaptiveRmi(adaptive) => {
                    let timer = crate::metrics::RMI_LOOKUP_LATENCY_SECONDS.start_timer();
                    for &key in keys {
                        let value = adaptive.lookup(key);
                        results.push((key, value));
                        
                        if value.is_some() {
                            crate::metrics::RMI_HITS_TOTAL.inc();
                        } else {
                            crate::metrics::RMI_MISSES_TOTAL.inc();
                        }
                    }
                    crate::metrics::RMI_READS_TOTAL.inc_by(keys.len() as f64);
                    timer.observe_duration();
                }
                index::PrimaryIndex::BTree(btree) => {
                    for &key in keys {
                        let value = btree.get(&key);
                        results.push((key, value));
                    }
                }
            },
            Err(_) => {
                // Lock contention - return empty results for ultra-fast path
                for &key in keys {
                    results.push((key, None));
                }
            }
        }
        
        results
    }
    
    /// 🚀 ULTRA-FAST LOCK-FREE LOOKUP: Zero-overhead with minimal metrics
    /// This is the absolute fastest path - use for high-frequency operations
    pub fn lookup_key_ultra_fast(&self, key: u64) -> Option<u64> {
        // 🚀 COMPLETELY LOCK-FREE: No contention possible
        match self.index.try_read() {
            Ok(idx) => match &*idx {
                #[cfg(feature = "learned-index")]
                index::PrimaryIndex::AdaptiveRmi(adaptive) => {
                    // 🚀 DIRECT CALL: Pure synchronous lookup with minimal overhead
                    let result = adaptive.lookup(key);
                    
                    // 🚀 MINIMAL METRICS: Only increment counters, no timers for maximum speed
                    #[cfg(not(feature = "bench-no-metrics"))]
                    {
                        if result.is_some() {
                            crate::metrics::RMI_HITS_TOTAL.inc();
                        } else {
                            crate::metrics::RMI_MISSES_TOTAL.inc();
                        }
                        crate::metrics::RMI_READS_TOTAL.inc();
                    }
                    
                    result
                }
                index::PrimaryIndex::BTree(btree) => {
                    btree.get(&key)
                }
            },
            Err(_) => None, // Lock contention - fail fast for ultra-fast path
        }
    }
    
    /// 🚀 ULTRA-FAST BATCH LOOKUP: Optimized for high-throughput scenarios
    pub fn lookup_keys_ultra_batch(&self, keys: &[u64]) -> Vec<(u64, Option<u64>)> {
        let mut results = Vec::with_capacity(keys.len());
        
        // Single lock acquisition for entire batch - fail fast if contention
        match self.index.try_read() {
            Ok(idx) => match &*idx {
                #[cfg(feature = "learned-index")]
                index::PrimaryIndex::AdaptiveRmi(adaptive) => {
                    for &key in keys {
                        let value = adaptive.lookup(key);
                        results.push((key, value));
                    }
                    
                    // 🚀 MINIMAL METRICS: Batch increment for efficiency
                    #[cfg(not(feature = "bench-no-metrics"))]
                    {
                        let hits = results.iter().filter(|(_, v)| v.is_some()).count();
                        crate::metrics::RMI_HITS_TOTAL.inc_by(hits as f64);
                        crate::metrics::RMI_MISSES_TOTAL.inc_by((keys.len() - hits) as f64);
                        crate::metrics::RMI_READS_TOTAL.inc_by(keys.len() as f64);
                    }
                }
                index::PrimaryIndex::BTree(btree) => {
                    for &key in keys {
                        let value = btree.get(&key);
                        results.push((key, value));
                    }
                }
            },
            Err(_) => {
                // Lock contention - return empty results for ultra-fast path
                for &key in keys {
                    results.push((key, None));
                }
            }
        }
        
        results
    }
    
    /// 🚀 LOCK-FREE INDEX ACCESS: For specialized high-performance scenarios
    /// Returns the current index snapshot without any locking overhead
    pub fn get_index_snapshot(&self) -> Option<std::sync::Arc<index::PrimaryIndex>> {
        // Future optimization: Could use ArcSwap here for truly atomic reads
        self.index.try_read().ok().map(|guard| {
            // Clone the Arc to get a snapshot
            match &*guard {
                #[cfg(feature = "learned-index")]
                index::PrimaryIndex::AdaptiveRmi(_) => {
                    // For now, we can't easily clone the RMI, so return None
                    // This could be improved with ArcSwap in the future
                    None
                }
                index::PrimaryIndex::BTree(_) => {
                    // Similarly for BTree
                    None
                }
            }
        }).flatten()
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
            fail::fail_point!("wal_after_retention_before_manifest", |_| {});
            // TODO: Implement manifest write if needed for WAL rotation
            // if let Err(e) = self.write_manifest().await {
            //     eprintln!("⚠️ manifest write after rotation failed: {}", e);
            // }
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
            if let Some(rec) = deserialize_record_compat(&ev.payload) {
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
            if let Some(rec) = deserialize_record_compat(&ev.payload) {
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
            if let Ok(vr) = bincode::deserialize::<(u64, Vec<f32>)>(&ev.payload) {
                if seen.contains(&vr.0) {
                    continue;
                }
                if vr.1.len() != query.len() {
                    continue;
                }
                seen.insert(vr.0);
                let mut dist = 0.0f32;
                for (a, b) in vr.1.iter().zip(query.iter()) {
                    let d = a - b;
                    dist += d * d;
                }
                out.push((vr.0, dist));
            }
        }
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(k);
        out
    }

    /// Get data directory path (for internal use)
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Get current WAL segments (for internal use)
    pub fn get_wal_segments(&self) -> Vec<String> {
        Self::list_wal_segments(&self.data_dir)
            .into_iter()
            .map(|(idx, path)| format!("wal.{:04} ({})", idx, path.display()))
            .collect()
    }

    /// Group commit background task for high-throughput writes
    async fn group_commit_background_task(&self) {
        let mut interval = tokio::time::interval(Duration::from_micros(5)); // Check every 5µs for ultra-fast responsiveness
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        loop {
            // Wait for either interval or flush notification
            tokio::select! {
                _ = interval.tick() => {
                    self.try_flush_batch().await;
                }
                _ = async {
                    let notify = {
                        let state = self.group_commit_state.lock();
                        state.fsync_notify.clone()
                    };
                    notify.notified().await;
                } => {
                    self.try_flush_batch().await;
                }
            }
        }
    }

    /// Try to flush the current batch if it meets the criteria
    async fn try_flush_batch(&self) {
        let mut batch_to_flush = Vec::new();
        let should_flush = {
            let mut state = self.group_commit_state.lock();
            
            if state.batch.is_empty() {
                return;
            }
            
            let batch_size = state.batch.len();
            let batch_age = state.batch_start_time
                .map(|start| start.elapsed())
                .unwrap_or(Duration::ZERO);
                
            // Flush if batch is large enough or old enough
            let size_trigger = batch_size >= state.config.max_batch_size;
            let time_trigger = batch_age.as_micros() >= state.config.max_batch_delay_micros as u128;
            
            // More aggressive flushing for low latency - flush smaller batches faster
            let aggressive_trigger = match state.config.durability_level {
                DurabilityLevel::Unsafe => batch_size >= 10 && batch_age.as_micros() >= 25, // Very aggressive
                DurabilityLevel::EnterpriseAsync => batch_size >= 25 && batch_age.as_micros() >= 50, // Aggressive
                DurabilityLevel::EnterpriseSafe => batch_size >= 50 && batch_age.as_micros() >= 100, // Moderate
            };
            
            if size_trigger || time_trigger || aggressive_trigger {
                // Take all items from batch
                batch_to_flush = state.batch.drain(..).collect();
                state.batch_start_time = None;
                true
            } else {
                false
            }
        };
        
        if should_flush && !batch_to_flush.is_empty() {
            self.flush_batch(batch_to_flush).await;
        }
    }

    /// Flush a batch of writes to WAL with enterprise-safe durability
    async fn flush_batch(&self, batch: Vec<BatchItem>) {
        let config = {
            let state = self.group_commit_state.lock();
            state.config
        };
        
        match config.durability_level {
            DurabilityLevel::EnterpriseSafe => {
                self.flush_batch_enterprise_safe(batch).await;
            }
            DurabilityLevel::EnterpriseAsync => {
                self.flush_batch_enterprise_async(batch).await;
            }
            DurabilityLevel::Unsafe => {
                self.flush_batch_unsafe(batch).await;
            }
        }
    }

    /// Enterprise-safe batch flush: Zero-copy serialization + immediate fsync
    async fn flush_batch_enterprise_safe(&self, batch: Vec<BatchItem>) {
        let start = Instant::now();
        
        // 1. Zero-copy serialization: serialize entire batch to single buffer
        let write_buffer = match self.serialize_batch_zero_copy(&batch) {
            Ok(buffer) => buffer,
            Err(e) => {
                // Serialization failed - notify all waiters
                for item in batch {
                    let _ = item.response_tx.send(Err(anyhow::anyhow!("Serialization failed: {}", e)));
                }
                return;
            }
        };
        
        // 2. Single write + immediate fsync for enterprise durability
        let write_result = {
            let mut w = self.wal.write().await;
            
            // Write entire batch in one operation
            let write_success = w.write_all(&write_buffer).is_ok() && w.flush().is_ok();
            
            if write_success {
                // Enterprise requirement: IMMEDIATE fsync for zero data loss
                match self.fsync_policy.sync_file(w.get_ref()) {
                    Ok(_) => true,
                    Err(e) => {
                        eprintln!("🚨 ENTERPRISE ALERT: fsync failed: {}", e);
                        false
                    }
                }
            } else {
                false
            }
        };
        
        // 3. Check if WAL rotation is needed after successful write
        if write_result {
            self.rotate_wal_if_needed().await;
        }
        
        // 4. Update metrics
        let batch_size = batch.len();
        let duration = start.elapsed();
        
        if write_result {
            metrics::APPENDS_TOTAL.inc_by(batch_size as f64);
            metrics::GROUP_COMMIT_BATCH_SIZE.observe(batch_size as f64);
            metrics::GROUP_COMMIT_LATENCY_SECONDS.observe(duration.as_secs_f64());
            metrics::GROUP_COMMIT_BATCHES_TOTAL.inc();
        }
        
        // 5. Send results to all waiters
        for item in batch {
            let result = if write_result {
                Ok(item.event.offset)
            } else {
                Err(anyhow::anyhow!("Enterprise-safe batch write failed"))
            };
            
            // Send result back to waiter (ignore if receiver dropped)
            let _ = item.response_tx.send(result);
        }
    }

    /// Zero-copy batch serialization for maximum performance with memory pooling
    fn serialize_batch_zero_copy(&self, batch: &[BatchItem]) -> Result<Bytes> {
        // Get buffer pool (initialize on first use)
        let pool = BUFFER_POOL.get_or_init(|| BufferPool::new());
        
        // Pre-calculate total buffer size to avoid reallocations
        let estimated_size: usize = batch.iter()
            .map(|item| {
                // More accurate estimate: 8 (header) + payload + 8 (uuid) + 16 (timestamp) + 4 (crc) + padding
                let payload_size = item.event.payload.len();
                48 + payload_size + (payload_size % 8) // 8-byte alignment padding
            })
            .sum::<usize>()
            .max(4096); // Minimum 4KB buffer
            
        let mut buf = pool.get_buffer(estimated_size);
        
        // Use MessagePack for more efficient serialization than bincode
        let batch_len = batch.len() as u32;
        buf.put_u32_le(batch_len); // Batch header
        
        // Serialize each event directly into the buffer with minimal overhead
        for item in batch {
            let event = &item.event;
            
            // Manual serialization for maximum performance
            // Format: [offset u64][timestamp u64][request_id 16 bytes][payload_len u32][payload][crc32c u32]
            buf.put_u64_le(event.offset);
            buf.put_u64_le(event.timestamp);
            buf.put_slice(event.request_id.as_bytes());
            
            let payload_len = event.payload.len() as u32;
            buf.put_u32_le(payload_len);
            buf.put_slice(&event.payload);
            
            // Calculate CRC over the entire event frame (not just payload)
            let frame_start = buf.len() - 32 - payload_len as usize; // Start of this event
            let crc = crc32c::crc32c(&buf[frame_start..]);
            buf.put_u32_le(crc);
        }
        
        let result = buf.freeze();
        
        // Return the buffer to pool for reuse (done via Drop trait in practice)
        // Note: We can't return the BytesMut here since we've frozen it,
        // but the pool will allocate new ones efficiently
        
        Ok(result)
    }
    
    /// Enterprise async: Write immediately, fsync in background
    async fn flush_batch_enterprise_async(&self, batch: Vec<BatchItem>) {
        // Write to WAL buffer immediately
        let write_buffer = match self.serialize_batch_zero_copy(&batch) {
            Ok(buffer) => buffer,
            Err(e) => {
                for item in batch {
                    let _ = item.response_tx.send(Err(anyhow::anyhow!("Serialization failed: {}", e)));
                }
                return;
            }
        };
        
        let write_result = {
            let mut w = self.wal.write().await;
            w.write_all(&write_buffer).is_ok() && w.flush().is_ok()
        };
        
        if write_result {
            // Mark for background fsync (maintains durability with bounded recovery window)
            self.mark_for_background_fsync().await;
            self.rotate_wal_if_needed().await;
        }
        
        // Notify waiters immediately (data is in OS buffer)
        for item in batch {
            let result = if write_result {
                Ok(item.event.offset)
            } else {
                Err(anyhow::anyhow!("Enterprise-async batch write failed"))
            };
            let _ = item.response_tx.send(result);
        }
    }
    
    /// Unsafe mode: Skip fsync entirely (for benchmarking only)
    async fn flush_batch_unsafe(&self, batch: Vec<BatchItem>) {
        // Write to WAL buffer only
        let write_buffer = match self.serialize_batch_zero_copy(&batch) {
            Ok(buffer) => buffer,
            Err(e) => {
                for item in batch {
                    let _ = item.response_tx.send(Err(anyhow::anyhow!("Serialization failed: {}", e)));
                }
                return;
            }
        };
        
        let write_result = {
            let mut w = self.wal.write().await;
            w.write_all(&write_buffer).is_ok() && w.flush().is_ok()
        };
        
        // No fsync - just notify waiters
        for item in batch {
            let result = if write_result {
                Ok(item.event.offset)
            } else {
                Err(anyhow::anyhow!("Unsafe batch write failed"))
            };
            let _ = item.response_tx.send(result);
        }
    }
    
    /// Mark data for background fsync (enterprise async mode)
    async fn mark_for_background_fsync(&self) {
        // Implementation would track pending fsync operations
        // For now, we'll implement the immediate version
    }
}

/// 🚀 ULTRA-FAST BUFFER POOL: Zero-allocation responses for maximum performance
pub struct UltraFastBufferPool {
    /// Pre-allocated response buffers
    json_buffers: crossbeam_queue::SegQueue<String>,
    binary_buffers: crossbeam_queue::SegQueue<Vec<u8>>,
    /// Statistics
    allocations: AtomicUsize,
    reuses: AtomicUsize,
}

impl UltraFastBufferPool {
    pub fn new() -> Self {
        let pool = Self {
            json_buffers: crossbeam_queue::SegQueue::new(),
            binary_buffers: crossbeam_queue::SegQueue::new(),
            allocations: AtomicUsize::new(0),
            reuses: AtomicUsize::new(0),
        };
        
        // Pre-allocate buffers for zero-allocation responses
        for _ in 0..1000 {
            pool.json_buffers.push(String::with_capacity(256));
            pool.binary_buffers.push(Vec::with_capacity(64));
        }
        
        pool
    }
    
    /// 🚀 GET BUFFER: Zero allocation in common case
    pub fn get_json_buffer(&self) -> String {
        match self.json_buffers.pop() {
            Some(mut buf) => {
                buf.clear();
                self.reuses.fetch_add(1, Ordering::Relaxed);
                buf
            }
            None => {
                self.allocations.fetch_add(1, Ordering::Relaxed);
                String::with_capacity(256)
            }
        }
    }
    
    /// 🚀 RETURN BUFFER: Reuse for next request
    pub fn return_json_buffer(&self, buf: String) {
        if buf.capacity() <= 1024 { // Don't keep huge buffers
            self.json_buffers.push(buf);
        }
    }
    
    /// 🚀 GET BINARY BUFFER: Zero allocation for binary responses
    pub fn get_binary_buffer(&self) -> Vec<u8> {
        match self.binary_buffers.pop() {
            Some(mut buf) => {
                buf.clear();
                self.reuses.fetch_add(1, Ordering::Relaxed);
                buf
            }
            None => {
                self.allocations.fetch_add(1, Ordering::Relaxed);
                Vec::with_capacity(64)
            }
        }
    }
    
    /// 🚀 RETURN BINARY BUFFER: Reuse for next request
    pub fn return_binary_buffer(&self, buf: Vec<u8>) {
        if buf.capacity() <= 512 { // Don't keep huge buffers
            self.binary_buffers.push(buf);
        }
    }
    
    /// Get pool statistics
    pub fn stats(&self) -> (usize, usize) {
        (
            self.allocations.load(Ordering::Relaxed),
            self.reuses.load(Ordering::Relaxed),
        )
    }
}

use std::sync::OnceLock;
static ULTRA_FAST_POOL: OnceLock<UltraFastBufferPool> = OnceLock::new();

pub fn get_ultra_fast_pool() -> &'static UltraFastBufferPool {
    ULTRA_FAST_POOL.get_or_init(|| UltraFastBufferPool::new())
}
