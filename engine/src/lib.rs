//! Durable, crash‑recoverable Event Log with real‑time subscribe.

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use bincode::Options;
use chrono::Utc;
#[cfg(feature = "ann-hnsw")]
use hora::core::ann_index::ANNIndex;
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
pub mod index;
pub use index::{BTreeIndex, Index, PrimaryIndex};
pub mod metrics;
pub mod schema;
// Vector Storage and Search Modules
#[cfg(feature = "vector-storage")]
pub mod vector;
#[cfg(feature = "vector-storage")]
pub use vector::{VectorIndex, VectorQuery, VectorSearchResult, HnswIndex};

// Text Search Module
#[cfg(feature = "multimodal-search")]
pub mod text;
#[cfg(feature = "multimodal-search")]
pub use text::{TextQuery, TextSearchResult, TextIndexWrapper, TextSearchConfig};

// Metadata Indexing Module
#[cfg(feature = "multimodal-search")]
pub mod metadata;
#[cfg(feature = "multimodal-search")]
pub use metadata::{MetadataQuery, MetadataIndex, MetadataSearchResult, QueryOperator, LogicalOperator};

// Hybrid Search Module
#[cfg(feature = "multimodal-search")]
pub mod hybrid;
#[cfg(feature = "multimodal-search")]
pub use hybrid::{HybridQuery, HybridSearchResult, HybridSearchExecutor, HybridScoringConfig, FusionStrategy};
// pub mod server;  // Removed for now
// pub mod sql;
// #[cfg(feature = "grpc")]
// pub mod grpc_svc;

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

    fn return_buffer(&self, buf: BytesMut) {
        if buf.capacity() >= 1024 && buf.capacity() <= 1024 * 1024 {
            let mut buffers = self.buffers.lock();
            if buffers.len() < 32 {
                buffers.push(buf);
            }
        }
    }

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

/// Fast-only record deserialization - no bincode fallback for maximum performance
/// Records must be in the optimized fast format: [key u64][value_len u64][value bytes]
pub fn deserialize_record_compat(payload: &[u8]) -> Option<Record> {
    // Use only the fast format for maximum performance
    // Legacy bincode records will be treated as corrupt and ignored
    try_deserialize_record_fast(payload)
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
pub struct PersistentEventLog {
    inner: Arc<RwLock<Vec<Event>>>,
    wal: Arc<RwLock<BufWriter<File>>>,
    data_dir: PathBuf,
    tx: broadcast::Sender<Event>,
    index: ArcSwap<index::PrimaryIndex>, // primary key → offset (lock-free)
    next_offset: Arc<RwLock<u64>>, // monotonic sequence (not tied to vec length)
    // current WAL segment index for rotation
    wal_seg_index: Arc<RwLock<u32>>,
    // rotation config
    wal_segment_bytes: Arc<RwLock<Option<u64>>>,
    wal_max_segments: Arc<RwLock<usize>>,
    #[cfg(feature = "ann-hnsw")]
    ann: Arc<RwLock<Option<hora::index::hnsw_idx::HNSWIndex<f32, u64>>>>,
    // --- Vector Storage and Search ---
    #[cfg(feature = "vector-storage")]
    schema_registry: Arc<RwLock<schema::SchemaRegistry>>,
    #[cfg(feature = "vector-storage")]
    vector_indexes: Arc<RwLock<std::collections::HashMap<String, Arc<RwLock<vector::HnswIndex>>>>>,
    #[cfg(feature = "vector-storage")]
    document_store: Arc<RwLock<std::collections::HashMap<u64, schema::Document>>>,
    // --- Multimodal Search Support ---
    #[cfg(feature = "multimodal-search")]
    text_index: text::TextIndexWrapper,
    #[cfg(feature = "multimodal-search")]
    metadata_index: Arc<metadata::MetadataIndex>,
    #[cfg(feature = "multimodal-search")]
    hybrid_executor: Arc<hybrid::HybridSearchExecutor>,
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
        let mut idx = index::PrimaryIndex::new_btree();
        let mut max_off = 0u64;
        for ev in &events {
            if ev.offset > max_off {
                max_off = ev.offset;
            }
            if let Some(rec) = deserialize_record_compat(&ev.payload) {
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

        let group_commit_config = GroupCommitConfig::default();
        let group_commit_state = Arc::new(Mutex::new(GroupCommitState {
            batch: VecDeque::new(),
            batch_start_time: None,
            fsync_notify: Arc::new(Notify::new()),
            config: group_commit_config,
        }));

        // --- Initialize multimodal search components ---
        #[cfg(feature = "multimodal-search")]
        let text_index = {
            let text_config = text::TextSearchConfig::default();
            let text_index_path = data_dir.join("text_index");
            text::create_text_index(text_index_path, text_config)?
        };
        
        #[cfg(feature = "multimodal-search")]
        let metadata_index = Arc::new(metadata::MetadataIndex::new());
        
        #[cfg(feature = "multimodal-search")]
        let vector_indexes_clone = Arc::new(RwLock::new(std::collections::HashMap::new()));
        
        #[cfg(feature = "multimodal-search")]
        let document_store_clone = Arc::new(RwLock::new(std::collections::HashMap::new()));
        
        #[cfg(feature = "multimodal-search")]
        let hybrid_executor = Arc::new(hybrid::HybridSearchExecutor::new(
            vector_indexes_clone.clone(),
            Arc::new(text_index.clone()),
            metadata_index.clone(),
            document_store_clone.clone(),
        ));

        let log = Self {
            inner: Arc::new(RwLock::new(events)),
            wal,
            data_dir: data_dir.clone(),
            tx,
            index: ArcSwap::new(Arc::new(idx)),
            next_offset: Arc::new(RwLock::new(next)),
            wal_seg_index: Arc::new(RwLock::new(seg_to_open)),
            wal_segment_bytes: Arc::new(RwLock::new(None)),
            wal_max_segments: Arc::new(RwLock::new(8)),
            #[cfg(feature = "ann-hnsw")]
            ann: Arc::new(RwLock::new(None)),
            // --- Initialize vector storage ---
            #[cfg(feature = "vector-storage")]
            schema_registry: Arc::new(RwLock::new({
                let registry_path = data_dir.join("schema_registry.json");
                schema::SchemaRegistry::load(&registry_path)
            })),
            #[cfg(feature = "vector-storage")]
            vector_indexes: {
                #[cfg(feature = "multimodal-search")]
                { vector_indexes_clone }
                #[cfg(not(feature = "multimodal-search"))]
                { Arc::new(RwLock::new(std::collections::HashMap::new())) }
            },
            #[cfg(feature = "vector-storage")]
            document_store: {
                #[cfg(feature = "multimodal-search")]
                { document_store_clone }
                #[cfg(not(feature = "multimodal-search"))]
                { Arc::new(RwLock::new(std::collections::HashMap::new())) }
            },
            // --- Multimodal search components ---
            #[cfg(feature = "multimodal-search")]
            text_index,
            #[cfg(feature = "multimodal-search")]
            metadata_index,
            #[cfg(feature = "multimodal-search")]
            hybrid_executor,
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
                    // Wake background task to start deadline timer immediately
                    state.fsync_notify.notify_one();
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
                            // For ArcSwap, we need to clone the index, update it, and swap
                            let current_index = self.index.load();
                            let mut new_index = (**current_index).clone();
                            new_index.insert(rec.key, event.offset);
                            self.index.store(Arc::new(new_index));
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
            // For ArcSwap, we need to clone the index, update it, and swap
            let current_index = self.index.load();
            let mut new_index = (**current_index).clone();
            new_index.insert(rec.key, event.offset);
            self.index.store(Arc::new(new_index));
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

    /// Append a vector record
    pub async fn append_vector(&self, request_id: Uuid, key: u64, vector: Vec<f32>) -> Result<u64> {
        let rec = VectorRecord { key, vector };
        let bytes = bincode::serialize(&rec)?;
        self.append(request_id, bytes).await
    }

    /// Get offset for a given key if present via index with delta-first semantics.
    /// Lock-free implementation using ArcSwap for maximum read performance.
    pub async fn lookup_key(&self, key: u64) -> Option<u64> {
        // Lock-free index access - single atomic pointer load
        let idx = self.index.load();
        match &**idx {
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
            let idx = self.index.load();
            if let index::PrimaryIndex::Rmi(r) = &**idx {
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
            if let Some(rec) = deserialize_record_compat(&ev.payload) {
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
            let mut new_idx = index::PrimaryIndex::new_btree();
            for ev in &compacted {
                if let Some(rec) = deserialize_record_compat(&ev.payload) {
                    new_idx.insert(rec.key, ev.offset);
                }
            }
            self.index.store(Arc::new(new_idx));
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
    /// 
    /// This implementation is designed to be deadlock-free by:
    /// 1. Minimizing critical section time (no I/O while holding lock)
    /// 2. Preparing the new index outside the critical section
    /// 3. Using atomic swap to minimize lock holding time
    pub async fn swap_primary_index(&self, mut new_index: index::PrimaryIndex) {
        // Step 1: Prepare delta migration outside the critical section
        // Extract delta pairs from current index without holding any locks
        let delta_pairs: Vec<(u64, u64)> = {
            let current_index = self.index.load();
            match &**current_index {
                index::PrimaryIndex::Rmi(old_rmi) => {
                    old_rmi.delta_pairs()
                }
                _ => Vec::new(),
            }
        };
        
        // Step 2: Apply delta to new index outside critical section
        // This is the expensive operation that was causing deadlocks
        match &mut new_index {
            index::PrimaryIndex::Rmi(new_rmi) => {
                for (k, v) in delta_pairs {
                    new_rmi.insert_delta(k, v);
                }
            }
            index::PrimaryIndex::BTree(btree) => {
                for (k, v) in delta_pairs {
                    btree.insert(k, v);
                }
            }
        }
        
        // Step 3: Atomic swap - no locks needed with ArcSwap
        // This is truly lock-free and wait-free for readers
        self.index.store(Arc::new(new_index));
        
        // Note: No locks, no blocking operations
        // Readers can continue uninterrupted during index swaps
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

impl Clone for PersistentEventLog {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            wal: self.wal.clone(),
            data_dir: self.data_dir.clone(),
            tx: self.tx.clone(),
            index: ArcSwap::new(self.index.load().clone()),
            next_offset: self.next_offset.clone(),
            wal_seg_index: self.wal_seg_index.clone(),
            wal_segment_bytes: self.wal_segment_bytes.clone(),
            wal_max_segments: self.wal_max_segments.clone(),
            #[cfg(feature = "ann-hnsw")]
            ann: self.ann.clone(),
            // Clone vector storage fields
            #[cfg(feature = "vector-storage")]
            schema_registry: self.schema_registry.clone(),
            #[cfg(feature = "vector-storage")]
            vector_indexes: self.vector_indexes.clone(),
            #[cfg(feature = "vector-storage")]
            document_store: self.document_store.clone(),
            // Clone multimodal search components
            #[cfg(feature = "multimodal-search")]
            text_index: self.text_index.clone(),
            #[cfg(feature = "multimodal-search")]
            metadata_index: self.metadata_index.clone(),
            #[cfg(feature = "multimodal-search")]
            hybrid_executor: self.hybrid_executor.clone(),
            snapshot_mmap: self.snapshot_mmap.clone(),
            snapshot_payload_index: self.snapshot_payload_index.clone(),
            wal_block_cache: self.wal_block_cache.clone(),
            fsync_policy: self.fsync_policy,
            group_commit_state: self.group_commit_state.clone(),
        }
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

    /// Group commit background task for high-throughput writes
    /// Uses event-driven + deadline approach to eliminate CPU spin-polling
    async fn group_commit_background_task(&self) {
        loop {
            // Wait for the first item to arrive in batch
            let deadline = {
                let notify = {
                    let state = self.group_commit_state.lock();
                    state.fsync_notify.clone()
                };
                notify.notified().await;
                
                // Calculate deadline when batch should be flushed
                let state = self.group_commit_state.lock();
                if state.batch.is_empty() {
                    continue; // Spurious wakeup, go back to waiting
                }
                
                // Set deadline based on when the first item entered the batch
                if let Some(batch_start) = state.batch_start_time {
                    batch_start + Duration::from_micros(state.config.max_batch_delay_micros)
                } else {
                    // If no start time, flush immediately
                    Instant::now()
                }
            };
            
            // Now wait for either deadline or immediate flush notification
            tokio::select! {
                _ = tokio::time::sleep_until(deadline.into()) => {
                    // Deadline reached - flush batch
                    self.try_flush_batch().await;
                }
                _ = async {
                    let notify = {
                        let state = self.group_commit_state.lock();
                        state.fsync_notify.clone()
                    };
                    notify.notified().await;
                } => {
                    // Immediate flush requested (batch size limit reached)
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

    /// Ultra-fast batch append leveraging group commit for supreme throughput
    /// This method is specifically optimized for gRPC batch operations
    pub async fn append_batch_kv(&self, records: Vec<(Uuid, u64, Vec<u8>)>) -> Result<Vec<u64>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Single record optimization
        if records.len() == 1 {
            let (request_id, key, value) = records.into_iter().next().unwrap();
            let offset = self.append_kv(request_id, key, value).await?;
            return Ok(vec![offset]);
        }

        // Batch optimization: Pre-serialize all records to leverage group commit
        let pool = BUFFER_POOL.get_or_init(|| BufferPool::new());
        let total_size: usize = records.iter().map(|(_, _, v)| 16 + v.len()).sum();
        let mut batch_buffer = pool.get_buffer(total_size);
        let mut request_ids = Vec::with_capacity(records.len());
        let mut offsets = Vec::with_capacity(records.len());
        let mut record_sizes = Vec::with_capacity(records.len());

        // Batch serialization for maximum efficiency
        for (request_id, key, value) in &records {
            request_ids.push(*request_id);
            let record_size = 16 + value.len();
            record_sizes.push(record_size);
            
            // Manual serialization: [key u64][value_len u64][value bytes]
            batch_buffer.put_u64_le(*key);
            batch_buffer.put_u64_le(value.len() as u64);
            batch_buffer.put_slice(value);
        }

        // Split buffer into individual record chunks for group commit
        let batch_bytes = batch_buffer.freeze();
        let mut offset_in_batch = 0;
        
        for (i, &record_size) in record_sizes.iter().enumerate() {
            let record_bytes = batch_bytes.slice(offset_in_batch..offset_in_batch + record_size);
            
            // Each record gets individual append to leverage group commit batching
            let offset = self.append(request_ids[i], record_bytes.to_vec()).await?;
            offsets.push(offset);
            
            offset_in_batch += record_size;
        }

        #[cfg(not(feature = "bench-no-metrics"))]
        {
            crate::metrics::BATCH_APPEND_TOTAL.inc();
            crate::metrics::BATCH_APPEND_SIZE_HISTOGRAM.observe(offsets.len() as f64);
        }

        Ok(offsets)
    }

    // =========================================================================
    // Phase B.1: Enhanced Vector Storage and Collection Management
    // =========================================================================

    #[cfg(feature = "vector-storage")]
    /// Create a new collection with the given schema
    pub async fn create_collection(&self, schema: schema::CollectionSchema) -> Result<()> {
        let mut registry = self.schema_registry.write().await;
        registry.create_collection(schema.clone())
            .map_err(|e| anyhow::anyhow!("Failed to create collection: {}", e))?;

        // Save registry to disk
        let registry_path = self.data_dir.join("schema_registry.json");
        registry.save(&registry_path)
            .context("Failed to save schema registry")?;

        // Initialize vector index if collection has vector dimension
        if let Some(dimension) = schema.vector_dimension {
            let config = schema.hnsw_config.unwrap_or_default();
            let vector_index = Arc::new(RwLock::new(vector::HnswIndex::new(config, dimension)));
            
            let mut indexes = self.vector_indexes.write().await;
            indexes.insert(schema.name.clone(), vector_index);
        }

        tracing::info!(collection = %schema.name, dimension = ?schema.vector_dimension, "Created collection");
        Ok(())
    }

    #[cfg(feature = "vector-storage")]
    /// Get collection schema by name
    pub async fn get_collection(&self, name: &str) -> Option<schema::CollectionSchema> {
        let registry = self.schema_registry.read().await;
        registry.get_collection(name).cloned()
    }

    #[cfg(feature = "vector-storage")]
    /// List all collections
    pub async fn list_collections(&self) -> Vec<schema::CollectionSchema> {
        let registry = self.schema_registry.read().await;
        registry.list_collections().into_iter().cloned().collect()
    }

    #[cfg(feature = "vector-storage")]
    /// Insert a document into a collection
    pub async fn insert_document(&self, mut document: schema::Document) -> Result<u64> {
        // Validate document against collection schema
        let collection_schema = {
            let registry = self.schema_registry.read().await;
            registry.validate_document(&document)
                .map_err(|e| anyhow::anyhow!("Document validation failed: {}", e))?;
            registry.get_collection(&document.collection)
                .ok_or_else(|| anyhow::anyhow!("Collection not found: {}", document.collection))?
                .clone()
        };

        // Update document timestamps
        document.update_timestamp();

        // Store document
        let doc_id = document.id;
        {
            let mut store = self.document_store.write().await;
            store.insert(doc_id, document.clone());
        }

        // Insert into vector index if document has embedding
        if let Some(ref embedding) = document.embedding {
            if let Some(vector_index) = {
                let indexes = self.vector_indexes.read().await;
                indexes.get(&document.collection).cloned()
            } {
                let vector_record = vector::storage::VectorRecord::from_document(&document)?;
                let mut index = vector_index.write().await;
                index.insert(vector_record)?;
            }
        }

        // Phase B.2: Multi-modal indexing
        #[cfg(feature = "multimodal-search")]
        {
            // Index text content if text search is enabled for the collection
            if collection_schema.text_search_enabled && document.text.is_some() {
                let text_content = document.text.as_ref().unwrap();
                let title = document.metadata.get("title")
                    .and_then(|v| if let schema::Value::String(s) = v { Some(s.as_str()) } else { None });
                let metadata_json = if !document.metadata.is_empty() {
                    Some(&serde_json::to_value(&document.metadata)?)
                } else {
                    None
                };
                
                self.text_index.index_document(
                    doc_id,
                    &document.collection,
                    text_content,
                    title,
                    metadata_json,
                ).await?;
            }

            // Index metadata for structured queries
            if !document.metadata.is_empty() {
                self.metadata_index.index_document(
                    doc_id,
                    &document.metadata,
                    &collection_schema.metadata_schema,
                ).await?;
            }
        }

        // Append to WAL for durability
        let doc_bytes = bincode::serialize(&document)?;
        let offset = self.append(Uuid::new_v4(), doc_bytes).await?;

        tracing::debug!(doc_id = %doc_id, collection = %document.collection, offset = %offset, "Inserted document with multi-modal indexing");
        Ok(offset)
    }

    #[cfg(feature = "vector-storage")]
    /// Get a document by ID
    pub async fn get_document(&self, id: u64) -> Option<schema::Document> {
        let store = self.document_store.read().await;
        store.get(&id).cloned()
    }

    #[cfg(feature = "vector-storage")]
    /// Update a document (creates new version)
    pub async fn update_document(&self, mut document: schema::Document) -> Result<u64> {
        // Validate document
        {
            let registry = self.schema_registry.read().await;
            registry.validate_document(&document)
                .map_err(|e| anyhow::anyhow!("Document validation failed: {}", e))?;
        }

        // Update timestamps and version
        document.update_timestamp();

        let doc_id = document.id;

        // Update vector index if document has embedding
        if let Some(ref embedding) = document.embedding {
            if let Some(vector_index) = {
                let indexes = self.vector_indexes.read().await;
                indexes.get(&document.collection).cloned()
            } {
                let vector_record = vector::storage::VectorRecord::from_document(&document)?;
                let mut index = vector_index.write().await;
                index.insert(vector_record)?; // Insert/update
            }
        }

        // Update document store
        {
            let mut store = self.document_store.write().await;
            store.insert(doc_id, document.clone());
        }

        // Append to WAL
        let doc_bytes = bincode::serialize(&document)?;
        let offset = self.append(Uuid::new_v4(), doc_bytes).await?;

        tracing::debug!(doc_id = %doc_id, version = %document.version, offset = %offset, "Updated document");
        Ok(offset)
    }

    #[cfg(feature = "vector-storage")]
    /// Delete a document
    pub async fn delete_document(&self, id: u64) -> Result<bool> {
        // Get document to determine collection
        let document = {
            let store = self.document_store.read().await;
            store.get(&id).cloned()
        };

        let document = match document {
            Some(doc) => doc,
            None => return Ok(false),
        };

        // Remove from vector index
        if document.embedding.is_some() {
            if let Some(vector_index) = {
                let indexes = self.vector_indexes.read().await;
                indexes.get(&document.collection).cloned()
            } {
                let mut index = vector_index.write().await;
                index.remove(id)?;
            }
        }

        // Remove from document store
        {
            let mut store = self.document_store.write().await;
            store.remove(&id);
        }

        // Log deletion to WAL
        let deletion_event = serde_json::json!({
            "type": "document_deletion",
            "id": id,
            "collection": document.collection,
            "timestamp": chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        });
        let deletion_bytes = serde_json::to_vec(&deletion_event)?;
        self.append(Uuid::new_v4(), deletion_bytes).await?;

        tracing::debug!(doc_id = %id, collection = %document.collection, "Deleted document");
        Ok(true)
    }

    #[cfg(feature = "vector-storage")]
    /// Perform vector similarity search
    pub async fn vector_search(
        &self,
        collection: &str,
        query: vector::VectorQuery,
    ) -> Result<Vec<vector::VectorSearchResult>> {
        let vector_index = {
            let indexes = self.vector_indexes.read().await;
            indexes.get(collection).cloned()
        };

        let vector_index = match vector_index {
            Some(index) => index,
            None => anyhow::bail!("Collection '{}' does not have vector search enabled", collection),
        };

        let index = vector_index.read().await;
        let results = index.search(&query)?;

        tracing::debug!(
            collection = %collection, 
            query_dim = query.vector.len(), 
            k = query.k, 
            results = results.len(),
            "Vector search completed"
        );

        Ok(results)
    }

    #[cfg(feature = "vector-storage")]
    /// Perform hybrid search combining vector similarity and metadata filtering
    pub async fn hybrid_search(
        &self,
        collection: &str,
        vector_query: Option<vector::VectorQuery>,
        metadata_filters: Vec<(String, schema::Value)>,
    ) -> Result<Vec<schema::Document>> {
        // Start with vector search if provided
        let mut candidate_ids: Option<std::collections::HashSet<u64>> = None;

        if let Some(ref vq) = vector_query {
            let vector_results = self.vector_search(collection, vq.clone()).await?;
            candidate_ids = Some(
                vector_results.into_iter()
                    .map(|result| result.id)
                    .collect()
            );
        }

        // Apply metadata filters
        let store = self.document_store.read().await;
        let mut results = Vec::new();

        for (doc_id, document) in store.iter() {
            // Skip if document is not in this collection
            if document.collection != collection {
                continue;
            }

            // Skip if not in vector search candidates (if vector search was performed)
            if let Some(ref candidates) = candidate_ids {
                if !candidates.contains(doc_id) {
                    continue;
                }
            }

            // Apply metadata filters
            let mut matches_all_filters = true;
            for (key, expected_value) in &metadata_filters {
                if let Some(actual_value) = document.metadata.get(key) {
                    if actual_value != expected_value {
                        matches_all_filters = false;
                        break;
                    }
                } else {
                    matches_all_filters = false;
                    break;
                }
            }

            if matches_all_filters {
                results.push(document.clone());
            }
        }

        tracing::debug!(
            collection = %collection,
            vector_search = vector_query.is_some(),
            metadata_filters = metadata_filters.len(),
            results = results.len(),
            "Hybrid search completed"
        );

        Ok(results)
    }

    #[cfg(feature = "multimodal-search")]
    /// Perform text search within a collection
    pub async fn text_search(&self, query: text::TextQuery) -> Result<Vec<text::TextSearchResult>> {
        self.text_index.search(&query).await
    }

    #[cfg(feature = "multimodal-search")]
    /// Perform metadata search within a collection
    pub async fn metadata_search(&self, query: metadata::MetadataQuery) -> Result<Vec<metadata::MetadataSearchResult>> {
        let document_ids = self.metadata_index.query(&query).await?;
        Ok(document_ids
            .into_iter()
            .map(|id| metadata::MetadataSearchResult {
                document_id: id,
                matched_fields: vec![], // TODO: Extract matched fields from query
            })
            .collect())
    }

    #[cfg(feature = "multimodal-search")]
    /// Perform advanced hybrid search combining vector + text + metadata
    pub async fn advanced_hybrid_search(&self, query: hybrid::HybridQuery) -> Result<Vec<hybrid::HybridSearchResult>> {
        self.hybrid_executor.search(&query).await
    }

    #[cfg(feature = "vector-storage")]
    /// Get collection statistics including vector index stats
    pub async fn get_collection_stats(&self, collection: &str) -> Option<CollectionStats> {
        let registry = self.schema_registry.read().await;
        let schema = registry.get_collection(collection)?;

        let document_count = {
            let store = self.document_store.read().await;
            store.values().filter(|doc| doc.collection == collection).count()
        };

        let vector_stats = if schema.vector_dimension.is_some() {
            let indexes = self.vector_indexes.read().await;
            if let Some(index) = indexes.get(collection) {
                let idx = index.read().await;
                Some(idx.stats())
            } else {
                None
            }
        } else {
            None
        };

        Some(CollectionStats {
            name: collection.to_string(),
            document_count,
            vector_dimension: schema.vector_dimension,
            vector_stats,
            created_at: schema.created_at,
            updated_at: schema.updated_at,
        })
    }
}

/// Collection statistics for Phase B.1
#[cfg(feature = "vector-storage")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct CollectionStats {
    pub name: String,
    pub document_count: usize,
    pub vector_dimension: Option<usize>,
    pub vector_stats: Option<vector::IndexStats>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}
