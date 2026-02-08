//! HNSW Backend for Cache Integration
//!
//! Provides a clean interface for cache strategies to query HNSW index on cache miss.
//! This bridges the cache layer with the vector search layer.
//!
//! **Persistence**: WAL + snapshots for durability and fast recovery.

use crate::config::DistanceMetric;
use crate::hnsw_index::{HnswVectorIndex, SearchResult};
use crate::metadata_filter;
use crate::metrics::MetricsCollector;
use crate::persistence::{
    FsyncPolicy, Manifest, Snapshot, WalEntry, WalErrorHandler, WalOp, WalReader, WalWriter,
};
use crate::proto::MetadataFilter;
use anyhow::{Context, Result};
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use roaring::RoaringTreemap;
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, instrument, trace, warn};

/// Process-unique base for WAL file IDs when the system clock is invalid.
/// Initialized lazily on first use with (PID << 32 | random_u32) to prevent
/// collisions with WAL files from a previous process that also had a bad clock.
static FALLBACK_WAL_FILE_ID: AtomicU64 = AtomicU64::new(0);
static FALLBACK_WAL_INIT: std::sync::Once = std::sync::Once::new();

/// Returns a process-unique monotonic WAL file ID for the clock-failure path.
/// First call seeds the counter with (pid << 32 | random_u32); subsequent calls
/// increment atomically.  The 32-bit PID prefix partitions the namespace across
/// processes and the random suffix handles PID recycling.
fn next_fallback_wal_file_id() -> u64 {
    FALLBACK_WAL_INIT.call_once(|| {
        use rand::Rng;
        let pid = u64::from(std::process::id());
        let random_suffix: u32 = rand::thread_rng().gen();
        let base = (pid << 32) | u64::from(random_suffix);
        FALLBACK_WAL_FILE_ID.store(base, Ordering::Relaxed);
    });
    FALLBACK_WAL_FILE_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Default)]
struct MetadataInvertedIndex {
    alive: RoaringTreemap,
    by_key_value: HashMap<String, HashMap<String, RoaringTreemap>>,
}

impl MetadataInvertedIndex {
    fn rebuild_from(metadata: &[HashMap<String, String>], alive: &[Option<u64>]) -> Self {
        let mut index = Self::default();
        let len = std::cmp::min(metadata.len(), alive.len());
        for doc_id in 0..len {
            if alive[doc_id].is_none() {
                continue;
            }
            index.insert_doc(doc_id as u64, &metadata[doc_id]);
        }
        index
    }

    fn insert_doc(&mut self, doc_id: u64, metadata: &HashMap<String, String>) {
        // Defensive: if a caller inserts an already-live doc without using `replace_doc`,
        // ensure we don't leave stale postings behind.
        if self.alive.contains(doc_id) {
            let mut keys_to_remove: Vec<String> = Vec::new();
            for (k, values) in self.by_key_value.iter_mut() {
                let mut values_to_remove: Vec<String> = Vec::new();
                for (v, bitmap) in values.iter_mut() {
                    bitmap.remove(doc_id);
                    if bitmap.is_empty() {
                        values_to_remove.push(v.clone());
                    }
                }
                for v in values_to_remove {
                    values.remove(&v);
                }
                if values.is_empty() {
                    keys_to_remove.push(k.clone());
                }
            }
            for k in keys_to_remove {
                self.by_key_value.remove(&k);
            }
        }

        self.alive.insert(doc_id);

        for (k, v) in metadata.iter() {
            self.by_key_value
                .entry(k.clone())
                .or_default()
                .entry(v.clone())
                .or_default()
                .insert(doc_id);
        }
    }

    fn remove_doc(&mut self, doc_id: u64, metadata: &HashMap<String, String>) {
        self.alive.remove(doc_id);

        for (k, v) in metadata.iter() {
            if let Some(values) = self.by_key_value.get_mut(k) {
                if let Some(bitmap) = values.get_mut(v) {
                    bitmap.remove(doc_id);
                    if bitmap.is_empty() {
                        values.remove(v);
                    }
                }
                if values.is_empty() {
                    self.by_key_value.remove(k);
                }
            }
        }
    }

    fn replace_doc(
        &mut self,
        doc_id: u64,
        old: &HashMap<String, String>,
        new: &HashMap<String, String>,
    ) {
        self.remove_doc(doc_id, old);
        self.insert_doc(doc_id, new);
    }

    fn bitmap_for_exact(&self, key: &str, value: &str) -> RoaringTreemap {
        self.by_key_value
            .get(key)
            .and_then(|m| m.get(value))
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Default)]
struct DocumentStore {
    embeddings: Vec<Vec<f32>>,
    metadata: Vec<HashMap<String, String>>,
    external_to_internal: HashMap<u64, usize>,
    internal_to_external: Vec<Option<u64>>,
}

fn compile_filter_to_bitmap(
    filter: &MetadataFilter,
    index: &MetadataInvertedIndex,
) -> Option<RoaringTreemap> {
    use crate::proto::metadata_filter::FilterType;

    match &filter.filter_type {
        None => Some(index.alive.clone()),
        Some(FilterType::Exact(exact)) => Some(index.bitmap_for_exact(&exact.key, &exact.value)),
        Some(FilterType::InMatch(in_match)) => {
            let mut out = RoaringTreemap::new();
            for v in &in_match.values {
                out |= index.bitmap_for_exact(&in_match.key, v);
            }
            Some(out)
        }
        Some(FilterType::AndFilter(and_filter)) => {
            if and_filter.filters.is_empty() {
                return Some(index.alive.clone());
            }
            let mut it = and_filter.filters.iter();
            let first = it.next()?;
            let mut acc = compile_filter_to_bitmap(first, index)?;
            for sub in it {
                let b = compile_filter_to_bitmap(sub, index)?;
                acc &= b;
            }
            Some(acc)
        }
        Some(FilterType::OrFilter(or_filter)) => {
            if or_filter.filters.is_empty() {
                return Some(RoaringTreemap::new());
            }
            let mut acc = RoaringTreemap::new();
            for sub in &or_filter.filters {
                let b = compile_filter_to_bitmap(sub, index)?;
                acc |= b;
            }
            Some(acc)
        }
        Some(FilterType::NotFilter(not_filter)) => {
            let sub = not_filter.filter.as_ref()?;
            let sub_b = compile_filter_to_bitmap(sub, index)?;
            let mut out = index.alive.clone();
            out -= &sub_b;
            Some(out)
        }
        // Range requires evaluating actual values.
        // We fall back to scan for correctness.
        Some(FilterType::Range(_)) => None,
    }
}

/// Disk space warning threshold (alert if free space < 10%)
const DISK_SPACE_WARNING_THRESHOLD: f64 = 0.10;

/// Disk space critical threshold (reject writes if free space < 5%)
const DISK_SPACE_CRITICAL_THRESHOLD: f64 = 0.05;

/// Normalization tolerance for cosine/inner-product vectors (squared L2 norm).
const NORMALIZATION_NORM_SQ_MIN: f32 = 0.98;
const NORMALIZATION_NORM_SQ_MAX: f32 = 1.02;

/// Disk space information
#[derive(Debug, Clone)]
struct DiskSpaceInfo {
    available_bytes: u64,
    available_percent: f64,
}

/// Check available disk space for a given path
///
/// Returns disk space information or error if unable to check.
/// Uses platform-specific APIs: statvfs (Unix) or GetDiskFreeSpaceEx (Windows).
#[instrument(level = "trace", skip(path), fields(path = %path.as_ref().display()))]
fn check_disk_space(path: impl AsRef<Path>) -> Result<DiskSpaceInfo> {
    let path = path.as_ref();

    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        // Use statvfs to get filesystem statistics
        let c_path =
            CString::new(path.as_os_str().as_bytes()).context("Invalid path for statvfs")?;

        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };

        let result = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };

        if result != 0 {
            anyhow::bail!("statvfs failed for path: {}", path.display());
        }

        // Cast to u64 for arithmetic - these are platform-dependent types (u64 on Linux x64, c_ulong elsewhere)
        #[allow(clippy::unnecessary_cast)]
        let total_bytes = stat.f_blocks as u64 * stat.f_frsize;
        #[allow(clippy::unnecessary_cast)]
        let available_bytes = stat.f_bavail as u64 * stat.f_frsize;
        let available_percent = if total_bytes > 0 {
            available_bytes as f64 / total_bytes as f64
        } else {
            0.0
        };

        Ok(DiskSpaceInfo {
            available_bytes,
            available_percent,
        })
    }

    #[cfg(windows)]
    {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;

        // Convert path to wide string for Windows API
        let wide_path: Vec<u16> = OsStr::new(path)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        let mut free_bytes: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut available_bytes: u64 = 0;

        let result = unsafe {
            windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW(
                wide_path.as_ptr(),
                &mut available_bytes,
                &mut total_bytes,
                &mut free_bytes,
            )
        };

        if result == 0 {
            anyhow::bail!("GetDiskFreeSpaceExW failed for path: {}", path.display());
        }

        let available_percent = if total_bytes > 0 {
            available_bytes as f64 / total_bytes as f64
        } else {
            0.0
        };

        Ok(DiskSpaceInfo {
            available_bytes,
            available_percent,
        })
    }

    #[cfg(not(any(unix, windows)))]
    {
        // Fallback for unsupported platforms - assume sufficient space
        warn!("disk space monitoring not supported on this platform");
        Ok(DiskSpaceInfo {
            total_bytes: u64::MAX,
            available_bytes: u64::MAX,
            available_percent: 1.0,
        })
    }
}

/// Check disk space and log warnings/errors if thresholds breached
///
/// Returns:
/// - `Ok(true)` if space is sufficient
/// - `Ok(false)` if space is critically low (< 5%)
/// - `Err` if unable to check disk space
#[instrument(level = "trace", skip(path), fields(path = %path.as_ref().display()))]
fn check_and_warn_disk_space(path: impl AsRef<Path>) -> Result<bool> {
    let info = check_disk_space(&path)?;

    if info.available_percent < DISK_SPACE_CRITICAL_THRESHOLD {
        error!(
            available_gb = info.available_bytes / (1024 * 1024 * 1024),
            available_percent = format!("{:.1}%", info.available_percent * 100.0),
            threshold = format!("{:.0}%", DISK_SPACE_CRITICAL_THRESHOLD * 100.0),
            path = %path.as_ref().display(),
            "CRITICAL: disk space critically low; rejecting writes"
        );
        return Ok(false);
    }

    if info.available_percent < DISK_SPACE_WARNING_THRESHOLD {
        warn!(
            available_gb = info.available_bytes / (1024 * 1024 * 1024),
            available_percent = format!("{:.1}%", info.available_percent * 100.0),
            threshold = format!("{:.0}%", DISK_SPACE_WARNING_THRESHOLD * 100.0),
            path = %path.as_ref().display(),
            "WARNING: disk space running low"
        );
    }

    Ok(true)
}

/// HNSW-backed document store for cache integration
///
/// This wraps HnswVectorIndex and provides:
/// - Document retrieval by ID (exact match, O(1))
/// - k-NN search by embedding (approximate, O(log n))
/// - Thread-safe concurrent access
/// - **Persistence**: WAL for durability + snapshots for fast recovery
///
/// **Usage**: Cache strategies call `fetch_document` on cache miss.
pub struct HnswBackend {
    index: Arc<RwLock<HnswVectorIndex>>,
    /// Document storage and ID mapping (internal contiguous IDs)
    doc_store: Arc<RwLock<DocumentStore>>,
    /// Inverted index for scalable metadata filtering (derived from metadata)
    metadata_index: Arc<RwLock<MetadataInvertedIndex>>,
    /// Persistence components (optional)
    persistence: Option<PersistenceState>,
    /// Set to `true` when WAL rollback fails after a failed HNSW insert.
    /// When set, further writes are rejected to prevent silent data loss.
    /// Reads remain available (the in-memory index may still serve queries).
    /// Operators must inspect the WAL, repair, and restart.
    wal_inconsistent: Arc<AtomicBool>,
}

/// Persistence state for HnswBackend
struct PersistenceState {
    data_dir: PathBuf,
    wal: Arc<RwLock<WalWriter>>,
    wal_error_handler: Arc<WalErrorHandler>,
    wal_metrics: MetricsCollector,
    inserts_since_snapshot: Arc<RwLock<usize>>,
    snapshot_interval: usize,
    next_wal_seq: Arc<AtomicU64>,
    snapshot_lock: Arc<RwLock<()>>,
    manifest_lock: Arc<Mutex<()>>,
    fsync_policy: FsyncPolicy,
    max_wal_size_bytes: u64,
}

impl PersistenceState {
    fn rotate_wal_if_needed(&self, wal_guard: &mut WalWriter) -> Result<bool> {
        if self.max_wal_size_bytes == 0 || wal_guard.bytes_written() < self.max_wal_size_bytes {
            return Ok(false);
        }

        let old_path = wal_guard.path().to_path_buf();

        let new_wal_path = self
            .data_dir
            .join(format!("wal_{}.wal", HnswBackend::file_id()));
        let new_wal_name = new_wal_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        // Create the file first, but do not start writing to it until the MANIFEST references it.
        let new_writer = WalWriter::create_with_error_handler(
            &new_wal_path,
            self.fsync_policy,
            Some(Arc::clone(&self.wal_error_handler)),
        )?;

        let _manifest_guard = self.manifest_lock.lock();
        let manifest_path = self.data_dir.join("MANIFEST");
        if !manifest_path.exists() {
            anyhow::bail!(
                "MANIFEST missing at {}; refusing to rotate WAL to avoid data loss",
                manifest_path.display()
            );
        }
        let mut manifest = Manifest::load(&manifest_path)?;
        manifest.wal_segments.push(new_wal_name);
        manifest.save(&manifest_path)?;

        *wal_guard = new_writer;
        info!(
            old = %old_path.display(),
            new = %new_wal_path.display(),
            max_wal_size_bytes = self.max_wal_size_bytes,
            "rotated WAL segment"
        );
        Ok(true)
    }
}
impl HnswBackend {
    /// Compact tombstones by rebuilding the in-memory HNSW index and document store.
    ///
    /// KyroDB uses soft deletes (tombstones) because graph edge rewiring delete is
    /// intentionally deferred to compaction/rebuild paths.
    /// Over time, tombstones can consume HNSW capacity (`max_elements`). This compaction:
    /// - Drops tombstones from the document store (compacts internal IDs)
    /// - Rebuilds the HNSW graph from live documents only
    /// - Rebuilds the metadata inverted index
    ///
    /// This is a stop-the-world operation for writers (and blocks readers while rebuilding).
    fn compact_tombstones(&self) -> Result<usize> {
        let snapshot_guard = self.persistence.as_ref().map(|p| p.snapshot_lock.write());

        // Capture index construction params before we swap it.
        let (dimension, capacity, distance, m, ef_construction, disable_norm_check) = {
            let idx = self.index.read();
            (
                idx.dimension(),
                idx.capacity(),
                idx.distance_metric(),
                idx.m(),
                idx.ef_construction(),
                idx.normalization_check_disabled(),
            )
        };

        let mut index = self.index.write();
        let mut store = self.doc_store.write();

        let tombstones = store
            .internal_to_external
            .iter()
            .filter(|v| v.is_none())
            .count();
        if tombstones == 0 {
            drop(store);
            drop(index);
            drop(snapshot_guard);
            return Ok(0);
        }

        let live = store.external_to_internal.len();
        info!(
            tombstones,
            live, capacity, "compacting tombstones by rebuilding HNSW index"
        );

        let old_embeddings = std::mem::take(&mut store.embeddings);
        let old_metadata = std::mem::take(&mut store.metadata);
        let old_internal_to_external = std::mem::take(&mut store.internal_to_external);

        store.embeddings = Vec::with_capacity(live);
        store.metadata = Vec::with_capacity(live);
        store.internal_to_external = Vec::with_capacity(live);
        store.external_to_internal.clear();

        for ((embedding, metadata), ext) in old_embeddings
            .into_iter()
            .zip(old_metadata.into_iter())
            .zip(old_internal_to_external.into_iter())
        {
            let Some(external_id) = ext else { continue };
            let internal_id = store.embeddings.len();
            store.embeddings.push(embedding);
            store.metadata.push(metadata);
            store.internal_to_external.push(Some(external_id));
            store.external_to_internal.insert(external_id, internal_id);
        }

        // Rebuild HNSW index from compacted live docs using parallel insertion.
        let mut new_index = HnswVectorIndex::new_with_params(
            dimension,
            capacity,
            distance,
            m,
            ef_construction,
            disable_norm_check,
        )?;
        let batch: Vec<(&[f32], usize)> = store
            .embeddings
            .iter()
            .enumerate()
            .map(|(internal_id, emb)| (emb.as_slice(), internal_id))
            .collect();
        new_index.parallel_insert_batch(&batch)?;
        *index = new_index;

        // Rebuild metadata inverted index to match compacted internal IDs.
        let new_meta_index =
            MetadataInvertedIndex::rebuild_from(&store.metadata, &store.internal_to_external);
        let mut meta_index = self.metadata_index.write();
        *meta_index = new_meta_index;

        drop(store);
        drop(index);
        drop(meta_index);
        drop(snapshot_guard);

        Ok(tombstones)
    }

    /// Create new HNSW backend from pre-loaded embeddings (no persistence)
    ///
    /// # Parameters
    /// - `embeddings`: Pre-loaded document embeddings (indexed by doc_id)
    /// - `metadata`: Pre-loaded document metadata (indexed by doc_id)
    /// - `max_elements`: HNSW index capacity
    ///
    /// # Note
    /// This builds the HNSW index immediately, which may take time for large corpora.
    /// For production with persistence, use `with_persistence` or `recover`.
    #[instrument(level = "debug", skip(embeddings, metadata), fields(num_docs = embeddings.len(), max_elements))]
    pub fn new(
        dimension: usize,
        distance: DistanceMetric,
        embeddings: Vec<Vec<f32>>,
        metadata: Vec<HashMap<String, String>>,
        max_elements: usize,
    ) -> Result<Self> {
        Self::new_with_hnsw_params(
            dimension,
            distance,
            embeddings,
            metadata,
            max_elements,
            HnswVectorIndex::DEFAULT_M,
            HnswVectorIndex::DEFAULT_EF_CONSTRUCTION,
            false,
        )
    }

    /// Create new HNSW backend from pre-loaded embeddings (no persistence) with explicit HNSW params.
    #[instrument(level = "debug", skip(embeddings, metadata), fields(num_docs = embeddings.len(), max_elements, hnsw_m, hnsw_ef_construction))]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_hnsw_params(
        dimension: usize,
        distance: DistanceMetric,
        mut embeddings: Vec<Vec<f32>>,
        metadata: Vec<HashMap<String, String>>,
        max_elements: usize,
        hnsw_m: usize,
        hnsw_ef_construction: usize,
        disable_normalization_check: bool,
    ) -> Result<Self> {
        if dimension == 0 {
            anyhow::bail!("embedding dimension must be > 0");
        }
        if embeddings.len() != metadata.len() {
            anyhow::bail!(
                "embeddings and metadata length mismatch: {} vs {}",
                embeddings.len(),
                metadata.len()
            );
        }

        let mut index = HnswVectorIndex::new_with_params(
            dimension,
            max_elements,
            distance,
            hnsw_m,
            hnsw_ef_construction,
            disable_normalization_check,
        )?;

        // Build HNSW index from embeddings
        info!(
            documents = embeddings.len(),
            dimension, "building HNSW index"
        );

        // Phase 1: Validate and normalize all embeddings
        let mut live_indices: Vec<usize> = Vec::with_capacity(embeddings.len());
        for (doc_id, embedding) in embeddings.iter_mut().enumerate() {
            if embedding.iter().all(|&x| x == 0.0) {
                continue;
            }
            if embedding.len() != dimension {
                anyhow::bail!(
                    "embedding dimension mismatch for doc_id {}: expected {}, got {}",
                    doc_id,
                    dimension,
                    embedding.len()
                );
            }
            normalize_in_place_if_needed(distance, embedding)?;
            live_indices.push(doc_id);
        }

        // Phase 2: Parallel HNSW graph construction
        let batch: Vec<(&[f32], usize)> = live_indices
            .iter()
            .map(|&doc_id| (embeddings[doc_id].as_slice(), doc_id))
            .collect();
        index.parallel_insert_batch(&batch)?;
        info!("HNSW index built");

        let mut doc_store = DocumentStore {
            embeddings,
            metadata,
            ..Default::default()
        };
        doc_store
            .internal_to_external
            .reserve(doc_store.embeddings.len());
        for internal_id in 0..doc_store.embeddings.len() {
            let alive = !doc_store.embeddings[internal_id].iter().all(|&x| x == 0.0);
            if alive {
                let external_id = internal_id as u64;
                doc_store.internal_to_external.push(Some(external_id));
                doc_store
                    .external_to_internal
                    .insert(external_id, internal_id);
            } else {
                doc_store.internal_to_external.push(None);
            }
        }

        let metadata_index = MetadataInvertedIndex::rebuild_from(
            &doc_store.metadata,
            &doc_store.internal_to_external,
        );

        Ok(Self {
            index: Arc::new(RwLock::new(index)),
            doc_store: Arc::new(RwLock::new(doc_store)),
            metadata_index: Arc::new(RwLock::new(metadata_index)),
            persistence: None,
            wal_inconsistent: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Create new HNSW backend with persistence enabled
    ///
    /// # Parameters
    /// - `embeddings`: Initial document embeddings
    /// - `metadata`: Initial document metadata
    /// - `max_elements`: HNSW index capacity
    /// - `data_dir`: Directory for WAL and snapshots
    /// - `fsync_policy`: Durability guarantee (Always, Periodic, Never)
    /// - `snapshot_interval`: Create snapshot every N inserts
    #[instrument(level = "debug", skip(embeddings, metadata, data_dir), fields(num_docs = embeddings.len(), max_elements, snapshot_interval))]
    #[allow(clippy::too_many_arguments)]
    pub fn with_persistence(
        dimension: usize,
        distance: DistanceMetric,
        embeddings: Vec<Vec<f32>>,
        metadata: Vec<HashMap<String, String>>,
        max_elements: usize,
        data_dir: impl AsRef<Path>,
        fsync_policy: FsyncPolicy,
        snapshot_interval: usize,
        max_wal_size_bytes: u64,
    ) -> Result<Self> {
        Self::with_persistence_with_hnsw_params(
            dimension,
            distance,
            embeddings,
            metadata,
            max_elements,
            data_dir,
            fsync_policy,
            snapshot_interval,
            max_wal_size_bytes,
            HnswVectorIndex::DEFAULT_M,
            HnswVectorIndex::DEFAULT_EF_CONSTRUCTION,
            false,
        )
    }

    /// Create new HNSW backend with persistence enabled and explicit HNSW params.
    #[instrument(level = "debug", skip(embeddings, metadata, data_dir), fields(num_docs = embeddings.len(), max_elements, snapshot_interval, hnsw_m, hnsw_ef_construction))]
    #[allow(clippy::too_many_arguments)]
    pub fn with_persistence_with_hnsw_params(
        dimension: usize,
        distance: DistanceMetric,
        mut embeddings: Vec<Vec<f32>>,
        metadata: Vec<HashMap<String, String>>,
        max_elements: usize,
        data_dir: impl AsRef<Path>,
        fsync_policy: FsyncPolicy,
        snapshot_interval: usize,
        max_wal_size_bytes: u64,
        hnsw_m: usize,
        hnsw_ef_construction: usize,
        disable_normalization_check: bool,
    ) -> Result<Self> {
        if dimension == 0 {
            anyhow::bail!("embedding dimension must be > 0");
        }
        if embeddings.len() != metadata.len() {
            anyhow::bail!(
                "embeddings and metadata length mismatch: {} vs {}",
                embeddings.len(),
                metadata.len()
            );
        }

        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).context("Failed to create data directory")?;

        let mut index = HnswVectorIndex::new_with_params(
            dimension,
            max_elements,
            distance,
            hnsw_m,
            hnsw_ef_construction,
            disable_normalization_check,
        )?;

        // Build HNSW index
        info!(
            documents = embeddings.len(),
            dimension, "building HNSW index (persistence)"
        );

        let mut live_indices: Vec<usize> = Vec::with_capacity(embeddings.len());
        for (doc_id, embedding) in embeddings.iter_mut().enumerate() {
            if embedding.iter().all(|&x| x == 0.0) {
                continue;
            }
            if embedding.len() != dimension {
                anyhow::bail!(
                    "embedding dimension mismatch for doc_id {}: expected {}, got {}",
                    doc_id,
                    dimension,
                    embedding.len()
                );
            }
            normalize_in_place_if_needed(distance, embedding)?;
            live_indices.push(doc_id);
        }
        let batch: Vec<(&[f32], usize)> = live_indices
            .iter()
            .map(|&doc_id| (embeddings[doc_id].as_slice(), doc_id))
            .collect();
        index.parallel_insert_batch(&batch)?;
        info!("HNSW index built (persistence)");

        // Create initial WAL
        let wal_path = data_dir.join(format!("wal_{}.wal", Self::file_id()));
        let wal_metrics = MetricsCollector::new();
        let wal_error_handler = Arc::new(WalErrorHandler::with_metrics(wal_metrics.clone()));
        let wal = WalWriter::create_with_error_handler(
            &wal_path,
            fsync_policy,
            Some(Arc::clone(&wal_error_handler)),
        )?;

        // Update manifest
        let mut manifest = Manifest::load_or_create(data_dir.join("MANIFEST"))?;
        manifest
            .wal_segments
            .push(Self::wal_segment_name(&wal_path)?);
        manifest.save(data_dir.join("MANIFEST"))?;

        let persistence = PersistenceState {
            data_dir,
            wal: Arc::new(RwLock::new(wal)),
            wal_error_handler,
            wal_metrics,
            inserts_since_snapshot: Arc::new(RwLock::new(0)),
            snapshot_interval,
            next_wal_seq: Arc::new(AtomicU64::new(1)),
            snapshot_lock: Arc::new(RwLock::new(())),
            manifest_lock: Arc::new(Mutex::new(())),
            fsync_policy,
            max_wal_size_bytes,
        };

        let mut doc_store = DocumentStore {
            embeddings,
            metadata,
            ..Default::default()
        };
        doc_store
            .internal_to_external
            .reserve(doc_store.embeddings.len());
        for internal_id in 0..doc_store.embeddings.len() {
            let alive = !doc_store.embeddings[internal_id].iter().all(|&x| x == 0.0);
            if alive {
                let external_id = internal_id as u64;
                doc_store.internal_to_external.push(Some(external_id));
                doc_store
                    .external_to_internal
                    .insert(external_id, internal_id);
            } else {
                doc_store.internal_to_external.push(None);
            }
        }

        let metadata_index = MetadataInvertedIndex::rebuild_from(
            &doc_store.metadata,
            &doc_store.internal_to_external,
        );

        let backend = Self {
            index: Arc::new(RwLock::new(index)),
            doc_store: Arc::new(RwLock::new(doc_store)),
            metadata_index: Arc::new(RwLock::new(metadata_index)),
            persistence: Some(persistence),
            wal_inconsistent: Arc::new(AtomicBool::new(false)),
        };

        // Durability: initial state must be recoverable even if no user-triggered snapshot
        // occurs before a crash. Persist a baseline snapshot on initialization.
        if !backend.is_empty() {
            backend.create_snapshot()?;
        }

        Ok(backend)
    }

    /// Recover from WAL + snapshot
    ///
    /// # Recovery Flow
    /// 1. Load manifest
    /// 2. Load latest snapshot with fallback recovery (if corrupted)
    /// 3. Replay WAL entries since snapshot
    /// 4. Rebuild HNSW index
    /// 5. Create new active WAL
    #[instrument(level = "info", skip(data_dir, metrics), fields(data_dir = %data_dir.as_ref().to_path_buf().display(), max_elements, snapshot_interval))]
    #[allow(clippy::too_many_arguments)]
    pub fn recover(
        embedding_dimension: usize,
        distance: DistanceMetric,
        data_dir: impl AsRef<Path>,
        max_elements: usize,
        fsync_policy: FsyncPolicy,
        snapshot_interval: usize,
        max_wal_size_bytes: u64,
        metrics: MetricsCollector,
    ) -> Result<Self> {
        Self::recover_with_hnsw_params(
            embedding_dimension,
            distance,
            data_dir,
            max_elements,
            fsync_policy,
            snapshot_interval,
            max_wal_size_bytes,
            metrics,
            HnswVectorIndex::DEFAULT_M,
            HnswVectorIndex::DEFAULT_EF_CONSTRUCTION,
            false,
        )
    }

    #[instrument(level = "info", skip(data_dir, metrics), fields(data_dir = %data_dir.as_ref().to_path_buf().display(), max_elements, snapshot_interval, hnsw_m, hnsw_ef_construction))]
    #[allow(clippy::too_many_arguments)]
    pub fn recover_with_hnsw_params(
        embedding_dimension: usize,
        distance: DistanceMetric,
        data_dir: impl AsRef<Path>,
        max_elements: usize,
        fsync_policy: FsyncPolicy,
        snapshot_interval: usize,
        max_wal_size_bytes: u64,
        metrics: MetricsCollector,
        hnsw_m: usize,
        hnsw_ef_construction: usize,
        disable_normalization_check: bool,
    ) -> Result<Self> {
        if embedding_dimension == 0 {
            anyhow::bail!("embedding dimension must be > 0");
        }
        let data_dir = data_dir.as_ref().to_path_buf();
        info!("recovering HnswBackend");

        // Load manifest
        let manifest_path = data_dir.join("MANIFEST");
        if !manifest_path.exists() {
            anyhow::bail!("No MANIFEST found in {}", data_dir.display());
        }

        let manifest = Manifest::load(&manifest_path)?;

        // Load snapshot with fallback recovery (if exists)
        let mut documents: HashMap<u64, (Vec<f32>, HashMap<String, String>)> = HashMap::new();
        let mut dimension = embedding_dimension;
        let mut snapshot_last_wal_seq = 0u64;
        let mut snapshot_timestamp = 0u64;
        let mut max_wal_seq = 0u64;

        if let Some(snapshot_name) = &manifest.latest_snapshot {
            let snapshot_path = data_dir.join(snapshot_name);
            info!(snapshot = %snapshot_path.display(), "loading snapshot with validation");

            // Use load_with_validation for automatic fallback recovery
            match Snapshot::load_with_validation(&snapshot_path, &metrics) {
                Ok((snapshot, recovered_from_fallback)) => {
                    let snapshot_has_docs =
                        !snapshot.documents.is_empty() || !snapshot.metadata.is_empty();

                    if snapshot_has_docs {
                        if snapshot.dimension == 0 {
                            anyhow::bail!("snapshot dimension is 0 (corrupt snapshot)");
                        }
                        if snapshot.dimension != embedding_dimension {
                            anyhow::bail!(
                                "configured embedding dimension {} does not match snapshot dimension {}",
                                embedding_dimension,
                                snapshot.dimension
                            );
                        }
                    }

                    let snapshot_distance = snapshot.distance.unwrap_or_default();
                    if snapshot_distance != distance {
                        anyhow::bail!(
                            "configured distance metric {:?} does not match snapshot distance metric {:?}",
                            distance,
                            snapshot_distance
                        );
                    }
                    dimension = if snapshot_has_docs {
                        snapshot.dimension
                    } else {
                        embedding_dimension
                    };
                    snapshot_last_wal_seq = snapshot.last_wal_seq;
                    snapshot_timestamp = snapshot.timestamp;
                    if snapshot_last_wal_seq > max_wal_seq {
                        max_wal_seq = snapshot_last_wal_seq;
                    }

                    // Restore documents from snapshot.
                    documents.clear();
                    for (doc_id, embedding) in snapshot.documents {
                        documents.insert(doc_id, (embedding, HashMap::new()));
                    }
                    for (doc_id, meta) in snapshot.metadata {
                        if let Some((_, meta_slot)) = documents.get_mut(&doc_id) {
                            *meta_slot = meta;
                        } else {
                            warn!(
                                doc_id,
                                "snapshot metadata entry missing corresponding document"
                            );
                        }
                    }

                    if recovered_from_fallback {
                        warn!(
                            documents = documents.len(),
                            "snapshot loaded from fallback (primary corrupted)"
                        );
                    } else {
                        info!(documents = documents.len(), "snapshot loaded successfully");
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "failed to load snapshot (primary and all fallbacks corrupted)"
                    );
                    // Continue with empty state - will replay all WAL entries
                    warn!("continuing recovery with empty state from WAL only");
                }
            }
        }

        // Replay WAL segments (skip entries already captured in snapshot)
        for wal_name in &manifest.wal_segments {
            let wal_path = data_dir.join(wal_name);
            if !wal_path.exists() {
                warn!(wal_segment = wal_name, "WAL segment missing; skipping");
                continue;
            }
            info!(wal_segment = wal_name, "replaying WAL");
            let mut reader = WalReader::open(&wal_path)?;
            let entries = reader.read_all()?;

            for entry in entries {
                if entry.seq_no > max_wal_seq {
                    max_wal_seq = entry.seq_no;
                }

                // Skip entries already captured in snapshot (sequence-based)
                if snapshot_last_wal_seq > 0
                    && entry.seq_no > 0
                    && entry.seq_no <= snapshot_last_wal_seq
                {
                    trace!(
                        doc_id = entry.doc_id,
                        entry_seq = entry.seq_no,
                        snapshot_seq = snapshot_last_wal_seq,
                        "skipping entry captured by snapshot"
                    );
                    continue;
                }
                // Legacy WAL entries (seq_no == 0) are skipped based on snapshot timestamp.
                if entry.seq_no == 0
                    && snapshot_timestamp > 0
                    && entry.timestamp > 0
                    && entry.timestamp <= snapshot_timestamp
                {
                    trace!(
                        doc_id = entry.doc_id,
                        entry_ts = entry.timestamp,
                        snapshot_ts = snapshot_timestamp,
                        "skipping legacy entry captured by snapshot timestamp"
                    );
                    continue;
                }

                match entry.op {
                    WalOp::Insert => {
                        if entry.embedding.len() != dimension {
                            anyhow::bail!(
                                "WAL embedding dimension mismatch for doc_id {}: expected {}, got {}",
                                entry.doc_id,
                                dimension,
                                entry.embedding.len()
                            );
                        }
                        documents.insert(entry.doc_id, (entry.embedding, entry.metadata));
                    }
                    WalOp::Delete => {
                        documents.remove(&entry.doc_id);
                    }
                    WalOp::UpdateMetadata => {
                        if let Some((_, meta)) = documents.get_mut(&entry.doc_id) {
                            *meta = entry.metadata;
                        } else {
                            warn!(
                                doc_id = entry.doc_id,
                                "WAL metadata update for missing document"
                            );
                        }
                    }
                }
            }

            info!(
                valid = reader.valid_entries(),
                corrupted = reader.corrupted_entries(),
                wal_segment = wal_name,
                "wal replay complete"
            );
        }

        // Rebuild HNSW index from recovered documents.
        let mut index = HnswVectorIndex::new_with_params(
            dimension,
            max_elements,
            distance,
            hnsw_m,
            hnsw_ef_construction,
            disable_normalization_check,
        )?;

        let mut docs_vec: Vec<(u64, Vec<f32>, HashMap<String, String>)> = documents
            .into_iter()
            .map(|(doc_id, (embedding, metadata))| (doc_id, embedding, metadata))
            .collect();
        docs_vec.sort_unstable_by_key(|(doc_id, _, _)| *doc_id);

        info!(
            documents = docs_vec.len(),
            dimension, "rebuilding HNSW index from recovered state"
        );

        let mut doc_store = DocumentStore::default();
        doc_store.embeddings.reserve(docs_vec.len());
        doc_store.metadata.reserve(docs_vec.len());
        doc_store.internal_to_external.reserve(docs_vec.len());

        // Phase 1: Build the document store sequentially (cheap, deterministic internal IDs).
        // Validate embeddings and normalize in-place before parallel HNSW insertion.
        for (doc_id, embedding, metadata) in docs_vec {
            if embedding.len() != dimension {
                anyhow::bail!(
                    "embedding dimension mismatch for doc_id {}: expected {}, got {}",
                    doc_id,
                    dimension,
                    embedding.len()
                );
            }
            let mut embedding = embedding;
            normalize_in_place_if_needed(distance, &mut embedding)?;
            let internal_id = doc_store.embeddings.len();
            doc_store.embeddings.push(embedding);
            doc_store.metadata.push(metadata);
            doc_store.internal_to_external.push(Some(doc_id));
            doc_store.external_to_internal.insert(doc_id, internal_id);
        }

        // Phase 2: Parallel HNSW graph construction from pre-validated embeddings.
        // Uses backend batch insertion for concurrent graph building where supported.
        let batch: Vec<(&[f32], usize)> = doc_store
            .embeddings
            .iter()
            .enumerate()
            .map(|(internal_id, emb)| (emb.as_slice(), internal_id))
            .collect();
        index.parallel_insert_batch(&batch)?;
        info!("HNSW index rebuilt");

        if doc_store.external_to_internal.is_empty() {
            // It's possible to have an empty DB if it's new, but usually we expect something if recovering.
            // However, allowing empty recovery is safer for new deployments.
            info!("Recovery: no data found (fresh start or empty DB)");
        }

        // Create new active WAL
        let wal_path = data_dir.join(format!("wal_{}.wal", Self::file_id()));
        let wal_error_handler = Arc::new(WalErrorHandler::with_metrics(metrics.clone()));
        let wal = WalWriter::create_with_error_handler(
            &wal_path,
            fsync_policy,
            Some(Arc::clone(&wal_error_handler)),
        )?;

        // Update manifest
        let mut manifest = manifest;
        manifest
            .wal_segments
            .push(Self::wal_segment_name(&wal_path)?);
        manifest.save(&manifest_path)?;

        let persistence = PersistenceState {
            data_dir,
            wal: Arc::new(RwLock::new(wal)),
            wal_error_handler,
            wal_metrics: metrics.clone(),
            inserts_since_snapshot: Arc::new(RwLock::new(0)),
            snapshot_interval,
            next_wal_seq: Arc::new(AtomicU64::new(max_wal_seq.saturating_add(1))),
            snapshot_lock: Arc::new(RwLock::new(())),
            manifest_lock: Arc::new(Mutex::new(())),
            fsync_policy,
            max_wal_size_bytes,
        };

        info!(
            documents = doc_store.external_to_internal.len(),
            "recovery complete"
        );

        let metadata_index = MetadataInvertedIndex::rebuild_from(
            &doc_store.metadata,
            &doc_store.internal_to_external,
        );

        Ok(Self {
            index: Arc::new(RwLock::new(index)),
            doc_store: Arc::new(RwLock::new(doc_store)),
            metadata_index: Arc::new(RwLock::new(metadata_index)),
            persistence: Some(persistence),
            wal_inconsistent: Arc::new(AtomicBool::new(false)),
        })
    }

    fn wal_segment_name(wal_path: &Path) -> Result<String> {
        wal_path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "failed to derive WAL segment name from path: {}",
                    wal_path.display()
                )
            })
    }

    fn timestamp() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(error) => {
                warn!(
                    error = %error,
                    "system clock is before UNIX epoch; using timestamp=0"
                );
                0
            }
        }
    }

    fn file_id() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => u64::try_from(duration.as_micros()).unwrap_or(u64::MAX),
            Err(error) => {
                let fallback = next_fallback_wal_file_id();
                warn!(
                    error = %error,
                    fallback_file_id = fallback,
                    "system clock is before UNIX epoch; using process-unique fallback WAL file id"
                );
                fallback
            }
        }
    }

    /// Get vector dimension
    pub fn dimension(&self) -> usize {
        self.index.read().dimension()
    }

    /// Get number of documents
    pub fn len(&self) -> usize {
        self.doc_store.read().external_to_internal.len()
    }

    /// Check if backend is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the WAL is in an inconsistent state (writes are rejected).
    /// This occurs when a WAL rollback fails after a failed HNSW insert.
    pub fn is_wal_inconsistent(&self) -> bool {
        self.wal_inconsistent.load(Ordering::SeqCst)
    }

    pub fn wal_writes_failed(&self) -> u64 {
        self.persistence
            .as_ref()
            .map(|p| p.wal_metrics.get_wal_writes_failed())
            .unwrap_or(0)
    }

    /// Lightweight existence check that avoids cloning embeddings
    pub fn exists(&self, doc_id: u64) -> bool {
        self.doc_store
            .read()
            .external_to_internal
            .contains_key(&doc_id)
    }

    /// Insert new document (with WAL logging if persistence enabled)
    ///
    /// # Parameters
    /// - `doc_id`: Document identifier
    /// - `embedding`: Document embedding vector
    /// - `metadata`: Document metadata
    ///
    /// # Durability
    /// - If persistence enabled: Append to WAL → HNSW insert → Update in-memory → fsync (per policy)
    /// - Triggers snapshot creation if `inserts_since_snapshot >= snapshot_interval`
    #[instrument(level = "trace", skip(self, embedding, metadata), fields(doc_id, dim = embedding.len()))]
    pub fn insert(
        &self,
        doc_id: u64,
        embedding: Vec<f32>,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        // Reject writes if WAL is in an inconsistent state (unrecoverable rollback failure).
        if self.wal_inconsistent.load(Ordering::SeqCst) {
            anyhow::bail!(
                "Insert rejected: WAL is in an inconsistent state. \
                 Manual inspection and restart required before writes can resume."
            );
        }

        // Check disk space before write (if persistence enabled)
        if let Some(ref persistence) = self.persistence {
            if !check_and_warn_disk_space(&persistence.data_dir)? {
                anyhow::bail!(
                    "Insert rejected: disk space critically low (< {}%)",
                    DISK_SPACE_CRITICAL_THRESHOLD * 100.0
                );
            }
        }

        // Validate embedding dimension against backend dimension
        let current_dim = self.dimension();
        if current_dim != 0 && embedding.len() != current_dim {
            anyhow::bail!(
                "embedding dimension mismatch: expected {} found {}",
                current_dim,
                embedding.len()
            );
        }

        let mut embedding = embedding;
        let distance = self.index.read().distance_metric();
        normalize_in_place_if_needed(distance, &mut embedding)?;

        let mut attempted_compaction = false;
        loop {
            let snapshot_guard = self.persistence.as_ref().map(|p| p.snapshot_lock.read());

            // Serialize WAL append and in-memory mutation in one write critical section.
            // This guarantees WAL replay order matches acknowledged commit order.
            let mut index = self.index.write();
            let mut store = self.doc_store.write();
            if index.is_full() {
                let tombstones = store
                    .internal_to_external
                    .iter()
                    .filter(|v| v.is_none())
                    .count();
                drop(index);
                drop(store);
                drop(snapshot_guard);

                if !attempted_compaction && tombstones > 0 {
                    attempted_compaction = true;
                    let reclaimed = self.compact_tombstones()?;
                    if reclaimed > 0 {
                        continue;
                    }
                }

                let index = self.index.read();
                anyhow::bail!(
                    "HNSW index full: {} elements (max {})",
                    index.len(),
                    index.capacity()
                );
            }

            let embedding_for_wal = embedding.clone();
            let metadata_for_wal = metadata.clone();
            let metadata_for_index = metadata.clone();

            // Write-ahead: WAL must be durable before mutating in-memory state.
            if let Some(ref persistence) = self.persistence {
                let manifest_path = persistence.data_dir.join("MANIFEST");
                if !manifest_path.exists() {
                    anyhow::bail!(
                        "MANIFEST missing at {}; refusing to append WAL to avoid unrecoverable data loss",
                        manifest_path.display()
                    );
                }

                let seq_no = persistence.next_wal_seq.fetch_add(1, Ordering::SeqCst);
                let entry = WalEntry {
                    op: WalOp::Insert,
                    doc_id,
                    embedding: embedding_for_wal,
                    metadata: metadata_for_wal,
                    seq_no,
                    timestamp: Self::timestamp(),
                };

                let mut wal = persistence.wal.write();
                wal.append(&entry)?;
                if let Err(e) = persistence.rotate_wal_if_needed(&mut wal) {
                    error!(
                        error = %e,
                        doc_id,
                        wal_seq_no = seq_no,
                        "failed to rotate WAL segment; continuing with current WAL"
                    );
                }
            }

            let old_internal_id = store.external_to_internal.get(&doc_id).copied();
            let old_metadata = old_internal_id.map(|id| store.metadata[id].clone());
            let internal_id = store.embeddings.len();

            if let Err(e) = index.add_vector(internal_id as u64, &embedding) {
                // WAL already contains the insert, but the in-memory index update failed.
                // This should be extremely rare (we pre-check capacity/dim), but we must
                // prevent "phantom" inserts on recovery for an operation we are failing.
                if let Some(ref persistence) = self.persistence {
                    let rollback_seq_no = persistence.next_wal_seq.fetch_add(1, Ordering::SeqCst);
                    let rollback = WalEntry {
                        op: WalOp::Delete,
                        doc_id,
                        embedding: Vec::new(),
                        metadata: HashMap::new(),
                        seq_no: rollback_seq_no,
                        timestamp: Self::timestamp(),
                    };

                    let mut wal = persistence.wal.write();
                    if let Err(rollback_err) = wal.append(&rollback) {
                        error!(
                            error = %rollback_err,
                            doc_id,
                            rollback_seq_no,
                            "CRITICAL: WAL rollback failed after failed HNSW insert; \
                             marking backend write-degraded to prevent silent data loss. \
                             Manual WAL inspection and restart required."
                        );
                        self.wal_inconsistent.store(true, Ordering::SeqCst);
                        return Err(anyhow::anyhow!(
                            "CRITICAL: WAL inconsistency detected for doc_id {}. \
                             WAL contains an insert with no matching rollback (rollback seq={}). \
                             Backend is now write-degraded. Reads still available. \
                             Repair WAL and restart.",
                            doc_id,
                            rollback_seq_no
                        ));
                    }
                    if let Err(rotate_err) = persistence.rotate_wal_if_needed(&mut wal) {
                        error!(
                            error = %rotate_err,
                            doc_id,
                            rollback_seq_no,
                            "failed to rotate WAL after rollback; continuing with current WAL"
                        );
                    }
                }

                return Err(e).context("HNSW insert failed after WAL append");
            }

            // Ensure read-optimized backend state stays active for sequential insert flows.
            index.complete_sequential_inserts();

            store.embeddings.push(std::mem::take(&mut embedding));
            store.metadata.push(metadata);
            store.internal_to_external.push(Some(doc_id));
            store.external_to_internal.insert(doc_id, internal_id);

            // Upsert semantics: keep the newest internal id live and tombstone the previous one.
            if let Some(old_internal_id) = old_internal_id {
                store.internal_to_external[old_internal_id] = None;
                store.metadata[old_internal_id].clear();
            }

            let mut should_create_snapshot = false;
            if let Some(ref persistence) = self.persistence {
                // Track inserts for snapshot trigger (only after WAL append succeeds).
                let mut inserts = persistence.inserts_since_snapshot.write();
                *inserts += 1;

                if persistence.snapshot_interval > 0 && *inserts >= persistence.snapshot_interval {
                    debug!(
                        inserts = *inserts,
                        interval = persistence.snapshot_interval,
                        "snapshot interval reached; creating snapshot"
                    );
                    should_create_snapshot = true;
                }
            }

            // Keep metadata inverted index in sync with metadata/tombstones.
            // Lock order is doc_store -> metadata_index to avoid deadlocks.
            {
                let mut meta_index = self.metadata_index.write();
                meta_index.insert_doc(internal_id as u64, &metadata_for_index);
                if let (Some(old_internal_id), Some(old_metadata)) = (old_internal_id, old_metadata)
                {
                    meta_index.remove_doc(old_internal_id as u64, &old_metadata);
                }
            }

            // Important: release the in-memory write locks before snapshot creation.
            // Snapshot creation needs to acquire read locks on the document store, and
            // performing disk I/O while holding write locks would block writers.
            drop(index);
            drop(store);
            drop(snapshot_guard);

            if should_create_snapshot {
                // Snapshotting is best-effort after a committed insert. The WAL already contains
                // the mutation, so we do not fail the user operation if a snapshot write fails.
                if let Err(e) = self.create_snapshot() {
                    error!(error = %e, "failed to create snapshot after insert");
                }
            }

            return Ok(());
        }
    }

    /// Bulk insert documents directly into HNSW index.
    ///
    /// This method is optimized for loading large batches of vectors quickly:
    /// - Acquires write locks once (not per-vector)
    /// - Pre-allocates capacity for document store arrays
    /// - Validates all dimensions upfront before inserting
    /// - Logs progress for long-running operations
    ///
    /// # Parameters
    /// - `documents`: Vector of (doc_id, embedding, metadata) tuples
    ///
    /// # Returns
    /// - `Ok((loaded, failed))`: Tuple of successfully loaded and failed counts
    ///
    /// # Note
    /// This bypasses the hot tier and WAL for maximum speed. Use for:
    /// - Benchmarks
    /// - Data migrations
    /// - Initial data loading
    ///
    /// For production streaming inserts, use `insert()` instead.
    #[instrument(level = "info", skip(self, documents), fields(batch_size = documents.len()))]
    pub fn bulk_insert(
        &self,
        documents: Vec<(u64, Vec<f32>, HashMap<String, String>)>,
    ) -> Result<(u64, u64)> {
        use std::time::Instant;
        use tracing::{info, warn};

        if documents.is_empty() {
            return Ok((0, 0));
        }

        if self.wal_inconsistent.load(Ordering::SeqCst) {
            anyhow::bail!(
                "Bulk insert rejected: WAL is in an inconsistent state. \
                 Manual inspection and restart required."
            );
        }

        if self.persistence.is_some() {
            warn!(
                batch_size = documents.len(),
                "bulk_insert bypasses the WAL; data may be lost on crash before the next snapshot"
            );
        }

        let start = Instant::now();
        let total = documents.len();

        // Get expected dimension from first vector or from index
        let expected_dim = if let Some((_, ref emb, _)) = documents.first() {
            emb.len()
        } else {
            self.dimension()
        };

        let backend_dim = self.dimension();
        if backend_dim != 0 && expected_dim != backend_dim {
            anyhow::bail!(
                "bulk_insert: batch dimension {} does not match backend dimension {}",
                expected_dim,
                backend_dim
            );
        }

        let _snapshot_guard = self.persistence.as_ref().map(|p| p.snapshot_lock.read());

        // Validate all dimensions upfront (fail fast)
        for (doc_id, embedding, _) in &documents {
            if embedding.len() != expected_dim {
                anyhow::bail!(
                    "bulk_insert: embedding dimension mismatch for doc_id={}: expected {} found {}",
                    doc_id,
                    expected_dim,
                    embedding.len()
                );
            }
        }

        // Acquire all write locks once
        let mut index = self.index.write();
        let mut store = self.doc_store.write();
        let distance = index.distance_metric();

        // Pre-allocate capacity for new inserts
        store.embeddings.reserve(documents.len());
        store.metadata.reserve(documents.len());
        store.internal_to_external.reserve(documents.len());

        let mut loaded = 0u64;
        let mut failed = 0u64;
        let log_interval = std::cmp::max(total / 20, 10000); // Log every 5% or 10K
        let mut seen_ids = std::collections::HashSet::with_capacity(documents.len());
        let mut meta_updates: Vec<(u64, HashMap<String, String>)> = Vec::new();

        for (i, (doc_id, embedding, metadata)) in documents.into_iter().enumerate() {
            if !seen_ids.insert(doc_id) {
                if failed < 10 {
                    tracing::warn!(doc_id, "bulk_insert: duplicate doc_id within batch");
                }
                failed += 1;
                continue;
            }

            if store.external_to_internal.contains_key(&doc_id) {
                if failed < 10 {
                    tracing::warn!(doc_id, "bulk_insert: doc_id already exists");
                }
                failed += 1;
                continue;
            }

            let internal_id = store.embeddings.len();
            let mut embedding = embedding;
            if let Err(e) = normalize_in_place_if_needed(distance, &mut embedding) {
                if failed < 10 {
                    tracing::warn!(doc_id, error = %e, "bulk_insert: failed to normalize embedding");
                }
                failed += 1;
                continue;
            }
            match index.add_vector(internal_id as u64, &embedding) {
                Ok(_) => {
                    let metadata_for_index = metadata.clone();
                    store.embeddings.push(embedding);
                    store.metadata.push(metadata);
                    store.internal_to_external.push(Some(doc_id));
                    store.external_to_internal.insert(doc_id, internal_id);
                    meta_updates.push((internal_id as u64, metadata_for_index));
                    loaded += 1;
                }
                Err(e) => {
                    if failed < 10 {
                        tracing::warn!(doc_id, error = %e, "bulk_insert: failed to add vector");
                    }
                    failed += 1;
                }
            }

            // Progress logging
            if (i + 1) % log_interval == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = (i + 1) as f64 / elapsed;
                info!(
                    progress = i + 1,
                    total,
                    rate_per_sec = rate as u64,
                    "bulk_insert progress"
                );
            }
        }

        if loaded > 0 {
            index.complete_sequential_inserts();
        }

        drop(store);
        drop(index);

        if !meta_updates.is_empty() {
            let mut meta_index = self.metadata_index.write();
            for (internal_id, meta) in meta_updates {
                meta_index.insert_doc(internal_id, &meta);
            }
        }

        let elapsed = start.elapsed();
        let rate = if elapsed.as_secs_f64() > 0.0 {
            loaded as f64 / elapsed.as_secs_f64()
        } else {
            loaded as f64
        };

        info!(
            loaded,
            failed,
            elapsed_ms = elapsed.as_millis() as u64,
            rate_per_sec = rate as u64,
            "bulk_insert complete"
        );

        Ok((loaded, failed))
    }

    /// Create snapshot (atomic file operation)
    ///
    /// This is called automatically when `snapshot_interval` is reached,
    /// or can be called manually for backup purposes.
    #[instrument(level = "debug", skip(self), ret)]
    pub fn create_snapshot(&self) -> Result<()> {
        let persistence = self
            .persistence
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Persistence not enabled"))?;

        // Check disk space before creating snapshot
        if !check_and_warn_disk_space(&persistence.data_dir)? {
            anyhow::bail!(
                "Snapshot creation rejected: disk space critically low (< {}%)",
                DISK_SPACE_CRITICAL_THRESHOLD * 100.0
            );
        }

        let snapshot_guard = persistence.snapshot_lock.write();
        let last_wal_seq = persistence
            .next_wal_seq
            .load(Ordering::SeqCst)
            .saturating_sub(1);

        let store = self.doc_store.read();

        // Create snapshot object
        // Collect a consistent view of documents. This clones embeddings/metadata while holding
        // the read lock to ensure snapshot consistency. We drop the read lock before
        // performing the potentially slow file I/O below to avoid blocking writers
        // for the duration of the save.
        let documents: Vec<(u64, Vec<f32>)> = store
            .internal_to_external
            .iter()
            .enumerate()
            .filter_map(|(internal_id, external_id)| {
                external_id.map(|doc_id| (doc_id, store.embeddings[internal_id].clone()))
            })
            .collect();

        let metadata_vec: Vec<(u64, HashMap<String, String>)> = store
            .internal_to_external
            .iter()
            .enumerate()
            .filter_map(|(internal_id, external_id)| {
                external_id.map(|doc_id| (doc_id, store.metadata[internal_id].clone()))
            })
            .collect();

        // Release locks before heavy I/O (snapshot.save)
        drop(store);
        drop(snapshot_guard);

        let dimension = if documents.is_empty() {
            0
        } else {
            documents[0].1.len()
        };

        let doc_count = documents.len();
        let distance = self.index.read().distance_metric();
        let snapshot = Snapshot::new(dimension, distance, documents, metadata_vec, last_wal_seq)?;

        // Save snapshot with timestamp
        let snapshot_timestamp = Self::file_id();
        let snapshot_name = format!("snapshot_{}.snap", snapshot_timestamp);
        let snapshot_path = persistence.data_dir.join(&snapshot_name);
        snapshot.save(&snapshot_path)?;
        info!(path = %snapshot_path.display(), docs = doc_count, timestamp = snapshot_timestamp, "snapshot saved");

        // Update manifest and compact WAL segments.
        //
        // Important: serialize MANIFEST read/modify/write with WAL rotation to avoid lost updates.
        let _manifest_guard = persistence.manifest_lock.lock();
        let manifest_path = persistence.data_dir.join("MANIFEST");
        if !manifest_path.exists() {
            anyhow::bail!(
                "MANIFEST missing at {}; cannot safely update snapshot metadata",
                manifest_path.display()
            );
        }
        let mut manifest = Manifest::load(&manifest_path)?;
        manifest.latest_snapshot = Some(snapshot_name.clone());

        // WAL Compaction: Delete old WAL segments that are fully captured in the snapshot.
        // This prevents unbounded disk usage growth when WAL rotation is enabled.
        let compacted = self.compact_old_wal_segments(
            &persistence.data_dir,
            last_wal_seq,
            snapshot.timestamp,
            &mut manifest,
        )?;
        if compacted > 0 {
            info!(
                compacted_segments = compacted,
                snapshot_seq = last_wal_seq,
                "WAL compaction complete"
            );
        }
        manifest.save(&manifest_path)?;

        // Reset insert counter
        *persistence.inserts_since_snapshot.write() = 0;

        Ok(())
    }

    /// Fetch document embedding by ID (O(1) lookup)
    ///
    /// # Parameters
    /// - `doc_id`: Document identifier
    ///
    /// # Returns
    /// - `Some(embedding)` if doc_id exists
    /// - `None` if doc_id out of range
    ///
    /// **Called by**: Cache strategies on cache miss
    #[instrument(level = "trace", skip(self), fields(doc_id))]
    pub fn fetch_document(&self, doc_id: u64) -> Option<Vec<f32>> {
        let store = self.doc_store.read();
        let internal_id = store.external_to_internal.get(&doc_id)?;
        store.embeddings.get(*internal_id).cloned()
    }

    /// Fetch document metadata by ID (O(1) lookup)
    #[instrument(level = "trace", skip(self), fields(doc_id))]
    pub fn fetch_metadata(&self, doc_id: u64) -> Option<HashMap<String, String>> {
        let store = self.doc_store.read();
        let internal_id = store.external_to_internal.get(&doc_id)?;
        store.metadata.get(*internal_id).cloned()
    }

    /// Bulk fetch documents by ID (O(1) lookup)
    ///
    /// Returns a vector of Option<(embedding, metadata)> corresponding to input IDs.
    /// Amortizes lock acquisition cost.
    #[instrument(level = "trace", skip(self), fields(count = doc_ids.len()))]
    #[allow(clippy::type_complexity)]
    pub fn bulk_fetch(&self, doc_ids: &[u64]) -> Vec<Option<(Vec<f32>, HashMap<String, String>)>> {
        let store = self.doc_store.read();

        doc_ids
            .iter()
            .map(|&id| {
                let internal_id = store.external_to_internal.get(&id)?;
                let emb = store.embeddings.get(*internal_id)?;
                let meta = store.metadata.get(*internal_id)?;
                Some((emb.clone(), meta.clone()))
            })
            .collect()
    }

    /// Update document metadata without changing embedding
    ///
    /// # Parameters
    /// - `doc_id`: Document ID to update
    /// - `metadata`: New metadata
    /// - `merge`: true = merge with existing metadata, false = replace all
    ///
    /// # Returns
    /// - `Ok(true)` if document existed and was updated
    /// - `Ok(false)` if document does not exist
    ///
    /// # WAL Logging
    /// Logs the metadata update to WAL for durability before updating in-memory state
    #[instrument(level = "debug", skip(self, metadata), fields(doc_id, merge))]
    pub fn update_metadata(
        &self,
        doc_id: u64,
        metadata: HashMap<String, String>,
        merge: bool,
    ) -> Result<bool> {
        if self.wal_inconsistent.load(Ordering::SeqCst) {
            anyhow::bail!("Metadata update rejected: WAL is in an inconsistent state.");
        }

        let snapshot_guard = self.persistence.as_ref().map(|p| p.snapshot_lock.read());

        let mut store = self.doc_store.write();
        let internal_id = match store.external_to_internal.get(&doc_id) {
            Some(id) => *id,
            None => return Ok(false),
        };

        let old_metadata = store.metadata[internal_id].clone();

        // Apply update
        let updated_metadata = if merge {
            let mut merged = old_metadata.clone();
            merged.extend(metadata);
            merged
        } else {
            metadata
        };

        let mut should_snapshot = false;

        // Log to WAL before updating in-memory state
        if let Some(ref persistence) = self.persistence {
            // Check disk space
            if !check_and_warn_disk_space(&persistence.data_dir)? {
                anyhow::bail!(
                    "Metadata update rejected: disk space critically low (< {}%)",
                    DISK_SPACE_CRITICAL_THRESHOLD * 100.0
                );
            }

            let manifest_path = persistence.data_dir.join("MANIFEST");
            if !manifest_path.exists() {
                anyhow::bail!(
                    "MANIFEST missing at {}; refusing to append WAL to avoid unrecoverable data loss",
                    manifest_path.display()
                );
            }

            let seq_no = persistence.next_wal_seq.fetch_add(1, Ordering::SeqCst);
            let entry = WalEntry {
                op: WalOp::UpdateMetadata,
                doc_id,
                embedding: vec![], // No embedding for metadata-only update
                metadata: updated_metadata.clone(),
                seq_no,
                timestamp: Self::timestamp(),
            };
            let mut wal = persistence.wal.write();
            wal.append(&entry)?;
            if let Err(e) = persistence.rotate_wal_if_needed(&mut wal) {
                error!(
                    error = %e,
                    doc_id,
                    wal_seq_no = seq_no,
                    "failed to rotate WAL segment; continuing with current WAL"
                );
            }

            // Track inserts/updates for snapshot trigger
            // We count metadata updates towards snapshot interval to ensure WAL doesn't grow unbounded
            let mut inserts = persistence.inserts_since_snapshot.write();
            *inserts += 1;

            if persistence.snapshot_interval > 0 && *inserts >= persistence.snapshot_interval {
                should_snapshot = true;
                debug!(
                    inserts = *inserts,
                    interval = persistence.snapshot_interval,
                    "snapshot interval reached (metadata update); creating snapshot"
                );
            }
        }

        // Update in-memory metadata
        store.metadata[internal_id] = updated_metadata.clone();
        drop(store);

        // Keep metadata inverted index in sync.
        {
            let mut meta_index = self.metadata_index.write();
            meta_index.replace_doc(internal_id as u64, &old_metadata, &updated_metadata);
        }

        drop(snapshot_guard);

        if should_snapshot {
            // Note: We ignore snapshot errors here to avoid failing the update
            if let Err(e) = self.create_snapshot() {
                error!(error = %e, "failed to create snapshot after metadata update");
            }
        }

        Ok(true)
    }

    /// Delete document (soft delete with WAL logging)
    ///
    /// # Parameters
    /// - `doc_id`: Document ID to delete
    ///
    /// # Returns
    /// - `Ok(true)` if document was found and marked deleted
    /// - `Ok(false)` if document was not found (already deleted or never existed)
    ///
    /// # Behavior
    /// - Logs Delete op to WAL
    /// - Clears metadata
    /// - Removes external ID mapping (tombstone)
    /// - Does NOT remove from HNSW index immediately (soft delete)
    /// - Filtered out during search
    #[instrument(level = "debug", skip(self), fields(doc_id))]
    pub fn delete(&self, doc_id: u64) -> Result<bool> {
        if self.wal_inconsistent.load(Ordering::SeqCst) {
            anyhow::bail!("Delete rejected: WAL is in an inconsistent state.");
        }

        // Check existence first to avoid logging unnecessary WAL entries
        let snapshot_guard = self.persistence.as_ref().map(|p| p.snapshot_lock.read());

        let mut store = self.doc_store.write();
        let internal_id = match store.external_to_internal.get(&doc_id) {
            Some(id) => *id,
            None => return Ok(false),
        };

        if store
            .internal_to_external
            .get(internal_id)
            .and_then(|v| *v)
            .is_none()
        {
            return Ok(false);
        }

        let mut should_snapshot = false;

        // Log to WAL
        if let Some(ref persistence) = self.persistence {
            if !check_and_warn_disk_space(&persistence.data_dir)? {
                anyhow::bail!(
                    "Delete rejected: disk space critically low (< {}%)",
                    DISK_SPACE_CRITICAL_THRESHOLD * 100.0
                );
            }

            let manifest_path = persistence.data_dir.join("MANIFEST");
            if !manifest_path.exists() {
                anyhow::bail!(
                    "MANIFEST missing at {}; refusing to append WAL to avoid unrecoverable data loss",
                    manifest_path.display()
                );
            }

            let seq_no = persistence.next_wal_seq.fetch_add(1, Ordering::SeqCst);
            let entry = WalEntry {
                op: WalOp::Delete,
                doc_id,
                embedding: vec![],
                metadata: HashMap::new(),
                seq_no,
                timestamp: Self::timestamp(),
            };
            let mut wal = persistence.wal.write();
            wal.append(&entry)?;
            if let Err(e) = persistence.rotate_wal_if_needed(&mut wal) {
                error!(
                    error = %e,
                    doc_id,
                    wal_seq_no = seq_no,
                    "failed to rotate WAL segment; continuing with current WAL"
                );
            }

            // Count deletes towards snapshot interval to bound WAL growth.
            let mut inserts = persistence.inserts_since_snapshot.write();
            *inserts += 1;
            if persistence.snapshot_interval > 0 && *inserts >= persistence.snapshot_interval {
                should_snapshot = true;
                debug!(
                    inserts = *inserts,
                    interval = persistence.snapshot_interval,
                    "snapshot interval reached (delete); creating snapshot"
                );
            }
        }

        // Update in-memory state (Soft Delete)
        let old_metadata = store.metadata[internal_id].clone();
        store.internal_to_external[internal_id] = None;
        store.external_to_internal.remove(&doc_id);
        store.metadata[internal_id].clear();
        drop(store);

        let mut meta_index = self.metadata_index.write();
        meta_index.remove_doc(internal_id as u64, &old_metadata);

        drop(snapshot_guard);

        if should_snapshot {
            if let Err(e) = self.create_snapshot() {
                error!(error = %e, "failed to create snapshot after delete");
            }
        }

        Ok(true)
    }

    /// Batch delete documents by ID
    ///
    /// Efficiently deletes multiple documents with a single lock acquisition and batched WAL write.
    #[instrument(skip(self, doc_ids), fields(count = doc_ids.len()))]
    pub fn batch_delete(&self, doc_ids: &[u64]) -> Result<u64> {
        if self.wal_inconsistent.load(Ordering::SeqCst) {
            anyhow::bail!("Batch delete rejected: WAL is in an inconsistent state.");
        }

        // Acquire write lock for document store
        let snapshot_guard = self.persistence.as_ref().map(|p| p.snapshot_lock.read());

        let mut store = self.doc_store.write();
        let mut deleted_count = 0;
        let mut wal_entries = Vec::with_capacity(doc_ids.len());
        let mut deletes: Vec<(u64, usize, HashMap<String, String>)> = Vec::new();
        let timestamp = Self::timestamp();

        // First pass: identify valid deletes and prepare WAL entries
        for &doc_id in doc_ids {
            let internal_id = match store.external_to_internal.get(&doc_id) {
                Some(id) => *id,
                None => continue,
            };

            if store
                .internal_to_external
                .get(internal_id)
                .and_then(|v| *v)
                .is_none()
            {
                continue;
            }

            wal_entries.push(WalEntry {
                op: WalOp::Delete,
                doc_id,
                embedding: Vec::new(),
                metadata: HashMap::new(),
                seq_no: 0,
                timestamp,
            });
            deletes.push((doc_id, internal_id, store.metadata[internal_id].clone()));
        }

        if wal_entries.is_empty() {
            return Ok(0);
        }

        let mut should_snapshot = false;

        // Log to WAL (batched)
        if let Some(ref persistence) = self.persistence {
            if !check_and_warn_disk_space(&persistence.data_dir)? {
                anyhow::bail!(
                    "Batch delete rejected: disk space critically low (< {}%)",
                    DISK_SPACE_CRITICAL_THRESHOLD * 100.0
                );
            }

            let manifest_path = persistence.data_dir.join("MANIFEST");
            if !manifest_path.exists() {
                anyhow::bail!(
                    "MANIFEST missing at {}; refusing to append WAL to avoid unrecoverable data loss",
                    manifest_path.display()
                );
            }

            let base_seq = persistence
                .next_wal_seq
                .fetch_add(wal_entries.len() as u64, Ordering::SeqCst);
            for (idx, entry) in wal_entries.iter_mut().enumerate() {
                entry.seq_no = base_seq + idx as u64;
            }

            let mut wal = persistence.wal.write();
            wal.append_batch(&wal_entries)?;
            if let Err(e) = persistence.rotate_wal_if_needed(&mut wal) {
                error!(
                    error = %e,
                    docs = wal_entries.len(),
                    wal_seq_no_base = base_seq,
                    "failed to rotate WAL segment after batch delete; continuing with current WAL"
                );
            }

            // Update snapshot counter
            let mut inserts = persistence.inserts_since_snapshot.write();
            *inserts += wal_entries.len();

            if persistence.snapshot_interval > 0 && *inserts >= persistence.snapshot_interval {
                should_snapshot = true;
                debug!(
                    inserts = *inserts,
                    interval = persistence.snapshot_interval,
                    "snapshot interval reached (batch delete); will create snapshot"
                );
            }
        }

        let mut removed_meta: Vec<(u64, HashMap<String, String>)> = Vec::new();

        // Apply changes to memory
        for (doc_id, internal_id, old_meta) in deletes {
            if store.internal_to_external.get(internal_id).and_then(|v| *v) == Some(doc_id) {
                store.internal_to_external[internal_id] = None;
                store.external_to_internal.remove(&doc_id);
                store.metadata[internal_id].clear();
                removed_meta.push((internal_id as u64, old_meta));
                deleted_count += 1;
            }
        }

        // Release locks before snapshot
        drop(store);

        if !removed_meta.is_empty() {
            let mut meta_index = self.metadata_index.write();
            for (internal_id, old_meta) in removed_meta {
                meta_index.remove_doc(internal_id, &old_meta);
            }
        }

        drop(snapshot_guard);

        if should_snapshot {
            if let Err(e) = self.create_snapshot() {
                error!(error = %e, "failed to create snapshot after batch delete");
            }
        }

        Ok(deleted_count)
    }

    /// k-NN search using HNSW index
    ///
    /// # Parameters
    /// - `query`: Query embedding
    /// - `k`: Number of nearest neighbors
    ///
    /// # Returns
    /// Vector of (doc_id, distance) pairs, sorted by distance (closest first)
    ///
    /// **Performance**: <1ms P99 on 10M vectors (target)
    ///
    /// **Note on Deletions**:
    /// This method uses a 2x oversampling heuristic to handle soft-deleted documents (tombstones).
    /// If the deletion rate is very high (>50%), it may return fewer than `k` results.
    #[instrument(level = "trace", skip(self, query), fields(k, dim = query.len()))]
    pub fn knn_search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.knn_search_with_ef(query, k, None)
    }

    #[instrument(level = "trace", skip(self, query), fields(k, dim = query.len(), ef_search_override))]
    pub fn knn_search_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef_search_override: Option<usize>,
    ) -> Result<Vec<SearchResult>> {
        if query.is_empty() {
            anyhow::bail!("query embedding cannot be empty");
        }

        if k == 0 {
            anyhow::bail!("k must be greater than 0");
        }

        if k > 10_000 {
            anyhow::bail!("k must be <= 10,000 (requested: {})", k);
        }

        let backend_dim = self.dimension();
        if backend_dim != 0 && query.len() != backend_dim {
            anyhow::bail!(
                "query dimension mismatch: expected {} found {}",
                backend_dim,
                query.len()
            );
        }

        let index = self.index.read();
        let distance = index.distance_metric();
        let normalized_query = normalize_query_if_needed(distance, query)?;
        let store = self.doc_store.read();

        // Oversample to account for tombstones (deleted documents)
        // We ask for more results, then filter out deleted ones
        // Heuristic: fetch 2x, capped at max allowed to avoid index errors
        let search_k = std::cmp::min(k * 2, 10_000);
        let mut results =
            index.knn_search_with_ef(normalized_query.as_ref(), search_k, ef_search_override)?;

        // Filter out tombstones and map internal IDs to external IDs
        results.retain(|r| {
            let internal_id = r.doc_id as usize;
            store
                .internal_to_external
                .get(internal_id)
                .and_then(|v| *v)
                .is_some()
        });

        // Ensure deterministic ordering after filtering.
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Truncate to requested k
        results.truncate(k);

        let mut mapped = Vec::with_capacity(results.len());
        for r in results {
            let internal_id = r.doc_id as usize;
            if let Some(external_id) = store.internal_to_external.get(internal_id).and_then(|v| *v)
            {
                mapped.push(SearchResult {
                    doc_id: external_id,
                    distance: r.distance,
                });
            }
        }

        Ok(mapped)
    }

    /// Batch k-NN search for multiple queries in parallel.
    ///
    /// This method amortizes lock acquisition overhead by:
    /// 1. Acquiring read locks once for all queries
    /// 2. Processing queries in parallel via Rayon
    ///
    /// **Performance**: ~50-100% faster than calling `knn_search` in a loop
    /// for batches of 10+ queries.
    ///
    /// # Parameters
    /// - `queries`: Slice of query embedding vectors
    /// - `k`: Number of nearest neighbors to return per query
    /// - `ef_search_override`: Optional ef_search parameter override
    ///
    /// # Returns
    /// Vector of search results, one per query (same order as input)
    #[instrument(level = "debug", skip(self, queries), fields(batch_size = queries.len(), k))]
    pub fn knn_search_batch(
        &self,
        queries: &[Vec<f32>],
        k: usize,
        ef_search_override: Option<usize>,
    ) -> Result<Vec<Vec<SearchResult>>> {
        if queries.is_empty() {
            return Ok(vec![]);
        }

        if k == 0 {
            anyhow::bail!("k must be greater than 0");
        }

        if k > 10_000 {
            anyhow::bail!("k must be <= 10,000 (requested: {})", k);
        }

        let backend_dim = self.dimension();
        if backend_dim != 0 {
            for (i, q) in queries.iter().enumerate() {
                if q.len() != backend_dim {
                    anyhow::bail!(
                        "query {} dimension mismatch: expected {} found {}",
                        i,
                        backend_dim,
                        q.len()
                    );
                }
            }
        }

        let search_k = std::cmp::min(k * 2, 10_000);
        let index = self.index.read();
        let distance = index.distance_metric();

        // Process query ANN traversals in parallel while holding only the index read lock.
        let raw_results: Vec<Result<Vec<SearchResult>>> = queries
            .par_iter()
            .map(|query| {
                let normalized_query = normalize_query_if_needed(distance, query)?;
                index.knn_search_with_ef(normalized_query.as_ref(), search_k, ef_search_override)
            })
            .collect();
        let raw_results: Vec<Vec<SearchResult>> = raw_results.into_iter().collect::<Result<_>>()?;
        drop(index);

        // Map internal IDs to external IDs in a single doc_store read pass (no O(N) clone).
        let store = self.doc_store.read();
        let mapped = raw_results
            .into_iter()
            .map(|mut results| {
                // Results are already sorted by ascending distance from the index backend;
                // preserve that order while filtering tombstones.
                let mut out = Vec::with_capacity(k.min(results.len()));
                for r in results.drain(..) {
                    let internal_id = r.doc_id as usize;
                    let external_id = match store.internal_to_external.get(internal_id) {
                        Some(Some(doc_id)) => *doc_id,
                        _ => continue,
                    };
                    out.push(SearchResult {
                        doc_id: external_id,
                        distance: r.distance,
                    });
                    if out.len() >= k {
                        break;
                    }
                }
                out
            })
            .collect();
        Ok(mapped)
    }

    /// Return document IDs matching a structured metadata filter.
    ///
    /// Fast path uses the inverted index for `Exact`, `InMatch`, `AndFilter`, `OrFilter`, and `NotFilter`.
    /// Falls back to `scan()` for `Range` (and any shape we cannot compile) to preserve correctness.
    pub fn ids_for_metadata_filter(&self, filter: &MetadataFilter) -> Vec<u64> {
        let store = self.doc_store.read();
        let meta_index = self.metadata_index.read();
        if let Some(mut bitmap) = compile_filter_to_bitmap(filter, &meta_index) {
            // Defensive: ensure tombstones are excluded even if a value bitmap is stale.
            bitmap &= meta_index.alive.clone();
            return bitmap
                .iter()
                .filter_map(|internal_id| {
                    store
                        .internal_to_external
                        .get(internal_id as usize)
                        .and_then(|v| *v)
                })
                .collect();
        }

        self.scan(|meta| metadata_filter::matches(filter, meta))
    }

    /// Scan all documents and return IDs that match the predicate
    ///
    /// **Performance Warning**:
    /// This performs a full linear scan of all metadata. For large datasets,
    /// this is O(N) and holds read locks. Use with caution.
    pub fn scan<F>(&self, predicate: F) -> Vec<u64>
    where
        F: Fn(&HashMap<String, String>) -> bool,
    {
        let store = self.doc_store.read();
        let total_len = std::cmp::min(store.metadata.len(), store.internal_to_external.len());

        let mut out = Vec::new();
        for id in 0..total_len {
            let external_id = match store.internal_to_external[id] {
                Some(doc_id) => doc_id,
                None => continue,
            };
            if predicate(&store.metadata[id]) {
                out.push(external_id);
            }
        }

        out
    }

    /// Force fsync on WAL (for periodic fsync policy)
    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            persistence.wal.write().sync()?;
        }
        Ok(())
    }

    /// Compact old WAL segments that are fully captured in the snapshot
    ///
    /// Deletes WAL files where all entries are covered by the snapshot, using
    /// sequence numbers when available and falling back to timestamps for legacy entries.
    /// This prevents unbounded disk usage from accumulating WAL segments.
    ///
    /// # Parameters
    /// - `data_dir`: Directory containing WAL files
    /// - `snapshot_last_wal_seq`: Last WAL sequence number included in the snapshot
    /// - `snapshot_timestamp`: Snapshot timestamp (for legacy WAL entries)
    /// - `manifest`: Manifest to update (removes deleted WAL segments)
    ///
    /// # Returns
    /// Number of WAL segments deleted
    ///
    /// # Safety
    /// - Only deletes WAL segments listed in manifest (controlled cleanup)
    /// - Always keeps the current active WAL segment (last in list)
    /// - Updates manifest atomically after successful deletion
    #[instrument(level = "debug", skip(self, data_dir, manifest), fields(snapshot_seq = snapshot_last_wal_seq, snapshot_ts = snapshot_timestamp))]
    fn compact_old_wal_segments(
        &self,
        data_dir: &Path,
        snapshot_last_wal_seq: u64,
        snapshot_timestamp: u64,
        manifest: &mut Manifest,
    ) -> Result<usize> {
        let mut deleted_count = 0;
        let mut segments_to_keep = Vec::new();

        if snapshot_last_wal_seq == 0 && snapshot_timestamp == 0 {
            warn!("snapshot has no sequence or timestamp; skipping WAL compaction for safety");
            return Ok(0);
        }

        // Always keep the last WAL segment (active WAL)
        let active_wal_index = manifest.wal_segments.len().saturating_sub(1);

        for (idx, wal_name) in manifest.wal_segments.iter().enumerate() {
            // Never delete active WAL
            if idx == active_wal_index {
                segments_to_keep.push(wal_name.clone());
                continue;
            }

            let wal_path = data_dir.join(wal_name);
            if !wal_path.exists() {
                warn!(wal_segment = wal_name, "WAL segment missing; skipping");
                continue;
            }

            let mut reader = match WalReader::open(&wal_path) {
                Ok(reader) => reader,
                Err(e) => {
                    warn!(wal_segment = wal_name, error = %e, "failed to open WAL segment; keeping");
                    segments_to_keep.push(wal_name.clone());
                    continue;
                }
            };

            let entries = match reader.read_all() {
                Ok(entries) => entries,
                Err(e) => {
                    warn!(wal_segment = wal_name, error = %e, "failed to read WAL segment; keeping");
                    segments_to_keep.push(wal_name.clone());
                    continue;
                }
            };

            if reader.corrupted_entries() > 0 {
                warn!(
                    wal_segment = wal_name,
                    corrupted = reader.corrupted_entries(),
                    "WAL segment has corrupted entries; keeping for safety"
                );
                segments_to_keep.push(wal_name.clone());
                continue;
            }

            let mut max_seq = 0u64;
            let mut max_timestamp = 0u64;
            let mut has_unknown_timestamp = false;
            let mut has_legacy = false;
            let mut all_entries_covered = true;
            for entry in &entries {
                if entry.seq_no == 0 {
                    has_legacy = true;
                    if entry.timestamp == 0 {
                        has_unknown_timestamp = true;
                    } else if entry.timestamp > max_timestamp {
                        max_timestamp = entry.timestamp;
                    }
                } else if entry.seq_no > max_seq {
                    max_seq = entry.seq_no;
                }

                let covered = if entry.seq_no > 0 && snapshot_last_wal_seq > 0 {
                    entry.seq_no <= snapshot_last_wal_seq
                } else if entry.seq_no == 0 && snapshot_timestamp > 0 && entry.timestamp > 0 {
                    entry.timestamp <= snapshot_timestamp
                } else {
                    false
                };

                if !covered {
                    all_entries_covered = false;
                }
            }

            if has_legacy && has_unknown_timestamp {
                warn!(
                    wal_segment = wal_name,
                    "WAL segment contains legacy entries without timestamps; keeping"
                );
                segments_to_keep.push(wal_name.clone());
                continue;
            }

            if all_entries_covered {
                match std::fs::remove_file(&wal_path) {
                    Ok(()) => {
                        debug!(
                            wal_segment = wal_name,
                            wal_max_seq = max_seq,
                            wal_max_ts = max_timestamp,
                            snapshot_seq = snapshot_last_wal_seq,
                            snapshot_ts = snapshot_timestamp,
                            "deleted old WAL segment",
                        );
                        deleted_count += 1;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        warn!(
                            wal_segment = wal_name,
                            "WAL segment already missing (skipping)"
                        );
                    }
                    Err(e) => {
                        error!(
                            wal_segment = wal_name,
                            error = %e,
                            "failed to delete old WAL segment",
                        );
                        segments_to_keep.push(wal_name.clone());
                    }
                }
            } else {
                segments_to_keep.push(wal_name.clone());
            }
        }

        // Update manifest with remaining segments
        manifest.wal_segments = segments_to_keep;

        Ok(deleted_count)
    }
}

fn normalize_in_place_if_needed(distance: DistanceMetric, embedding: &mut [f32]) -> Result<()> {
    if !matches!(
        distance,
        DistanceMetric::Cosine | DistanceMetric::InnerProduct
    ) {
        return Ok(());
    }

    let norm_sq = crate::simd::sum_squares_f32(embedding);
    if norm_sq <= f32::EPSILON {
        anyhow::bail!(
            "embedding norm is zero; cannot normalize for {:?}",
            distance
        );
    }
    if (NORMALIZATION_NORM_SQ_MIN..=NORMALIZATION_NORM_SQ_MAX).contains(&norm_sq) {
        return Ok(());
    }

    let inv_norm = 1.0 / norm_sq.sqrt();
    for v in embedding {
        *v *= inv_norm;
    }

    Ok(())
}

fn normalize_query_if_needed<'a>(
    distance: DistanceMetric,
    query: &'a [f32],
) -> Result<Cow<'a, [f32]>> {
    if !matches!(
        distance,
        DistanceMetric::Cosine | DistanceMetric::InnerProduct
    ) {
        return Ok(Cow::Borrowed(query));
    }

    let norm_sq = crate::simd::sum_squares_f32(query);
    if norm_sq <= f32::EPSILON {
        anyhow::bail!("query embedding norm is zero; cannot normalize");
    }
    if (NORMALIZATION_NORM_SQ_MIN..=NORMALIZATION_NORM_SQ_MAX).contains(&norm_sq) {
        return Ok(Cow::Borrowed(query));
    }

    let mut normalized = query.to_vec();
    let inv_norm = 1.0 / norm_sq.sqrt();
    for v in &mut normalized {
        *v *= inv_norm;
    }

    Ok(Cow::Owned(normalized))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::metadata_filter::FilterType;
    use crate::proto::{ExactMatch, MetadataFilter, NotFilter};
    use std::collections::HashMap;

    fn exact(key: &str, value: &str) -> MetadataFilter {
        MetadataFilter {
            filter_type: Some(FilterType::Exact(ExactMatch {
                key: key.to_string(),
                value: value.to_string(),
            })),
        }
    }

    fn normalize(mut v: Vec<f32>) -> Vec<f32> {
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            v.iter_mut().for_each(|x| *x /= norm);
        }
        v
    }

    #[test]
    fn metadata_index_tracks_insert_update_delete() -> Result<()> {
        let backend = HnswBackend::new(3, DistanceMetric::Euclidean, Vec::new(), Vec::new(), 128)?;

        let mut meta = HashMap::new();
        meta.insert("tag".to_string(), "a".to_string());
        backend.insert(1, vec![1.0, 0.0, 0.0], meta)?;

        assert_eq!(backend.ids_for_metadata_filter(&exact("tag", "a")), vec![1]);

        let mut updated = HashMap::new();
        updated.insert("tag".to_string(), "b".to_string());
        assert!(backend.update_metadata(1, updated, false)?);

        assert!(backend
            .ids_for_metadata_filter(&exact("tag", "a"))
            .is_empty());
        assert_eq!(backend.ids_for_metadata_filter(&exact("tag", "b")), vec![1]);

        assert!(backend.delete(1)?);
        assert!(backend
            .ids_for_metadata_filter(&exact("tag", "b"))
            .is_empty());
        Ok(())
    }

    #[test]
    fn metadata_index_not_filter_uses_alive_complement() -> Result<()> {
        let backend = HnswBackend::new(3, DistanceMetric::Euclidean, Vec::new(), Vec::new(), 128)?;

        let mut meta_a = HashMap::new();
        meta_a.insert("tag".to_string(), "a".to_string());
        backend.insert(1, vec![1.0, 0.0, 0.0], meta_a)?;

        let mut meta_b = HashMap::new();
        meta_b.insert("tag".to_string(), "b".to_string());
        backend.insert(2, vec![0.0, 1.0, 0.0], meta_b)?;

        let not_a = MetadataFilter {
            filter_type: Some(FilterType::NotFilter(Box::new(NotFilter {
                filter: Some(Box::new(exact("tag", "a"))),
            }))),
        };

        assert_eq!(backend.ids_for_metadata_filter(&not_a), vec![2]);
        Ok(())
    }

    #[test]
    fn test_hnsw_backend_basic_operations() {
        // Create test embeddings
        let embeddings = vec![
            normalize(vec![1.0, 0.0, 0.0, 0.0]),
            normalize(vec![0.0, 1.0, 0.0, 0.0]),
            normalize(vec![0.0, 0.0, 1.0, 0.0]),
        ];

        let metadata = vec![
            HashMap::from([("id".to_string(), "0".to_string())]),
            HashMap::from([("id".to_string(), "1".to_string())]),
            HashMap::from([("id".to_string(), "2".to_string())]),
        ];

        let backend =
            HnswBackend::new(4, DistanceMetric::Cosine, embeddings, metadata, 100).unwrap();

        // Test fetch_document
        let doc0 = backend.fetch_document(0).unwrap();
        assert_eq!(doc0, vec![1.0, 0.0, 0.0, 0.0]);

        let doc1 = backend.fetch_document(1).unwrap();
        assert_eq!(doc1, vec![0.0, 1.0, 0.0, 0.0]);

        // Test fetch_metadata
        let meta0 = backend.fetch_metadata(0).unwrap();
        assert_eq!(meta0.get("id").unwrap(), "0");

        // Test out of range
        assert!(backend.fetch_document(100).is_none());
    }

    #[test]
    fn test_hnsw_backend_knn_search() {
        let embeddings = vec![
            normalize(vec![1.0, 0.0, 0.0, 0.0]),
            normalize(vec![0.9, 0.1, 0.0, 0.0]), // Similar to doc 0
            normalize(vec![0.0, 0.0, 1.0, 0.0]), // Different
        ];

        let metadata = vec![HashMap::new(); 3];

        let backend =
            HnswBackend::new(4, DistanceMetric::Cosine, embeddings, metadata, 100).unwrap();

        // Query closest to doc 0
        let query = normalize(vec![1.0, 0.0, 0.0, 0.0]);
        // Pin ef_search for deterministic high-recall behavior in this tiny synthetic graph.
        let results = backend.knn_search_with_ef(&query, 2, Some(128)).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, 0); // Exact match
        assert_eq!(results[1].doc_id, 1); // Similar
    }

    #[test]
    fn test_hnsw_backend_knn_search_batch_matches_single_query_path() {
        let embeddings = vec![
            normalize(vec![1.0, 0.0, 0.0, 0.0]),
            normalize(vec![0.9, 0.1, 0.0, 0.0]),
            normalize(vec![0.0, 1.0, 0.0, 0.0]),
            normalize(vec![0.0, 0.0, 1.0, 0.0]),
        ];

        let metadata = vec![HashMap::new(); embeddings.len()];
        let backend =
            HnswBackend::new(4, DistanceMetric::Cosine, embeddings, metadata, 100).unwrap();

        // Tombstone one entry to verify filtering logic in the batch mapping path.
        assert!(backend.delete(1).unwrap());

        let queries = vec![
            normalize(vec![1.0, 0.0, 0.0, 0.0]),
            normalize(vec![0.0, 1.0, 0.0, 0.0]),
        ];
        let batch = backend.knn_search_batch(&queries, 3, Some(128)).unwrap();
        assert_eq!(batch.len(), queries.len());

        for results in &batch {
            assert!(results.len() <= 3);
            assert!(
                results.windows(2).all(|w| w[0].distance <= w[1].distance),
                "batch results must stay distance-sorted"
            );
            assert!(
                results.iter().all(|r| r.doc_id != 1),
                "tombstoned document must not appear in batch results"
            );
        }

        // Batch behavior must match per-query path for correctness.
        let single0 = backend
            .knn_search_with_ef(&queries[0], 3, Some(128))
            .unwrap();
        let single1 = backend
            .knn_search_with_ef(&queries[1], 3, Some(128))
            .unwrap();
        assert_eq!(batch[0], single0);
        assert_eq!(batch[1], single1);
    }

    #[test]
    fn test_hnsw_backend_empty_check() {
        let embeddings = vec![normalize(vec![1.0, 0.0])];
        let metadata = vec![HashMap::new()];
        let backend =
            HnswBackend::new(2, DistanceMetric::Cosine, embeddings, metadata, 100).unwrap();
        assert!(!backend.is_empty());
        assert_eq!(backend.len(), 1);
        assert_eq!(backend.dimension(), 2);
    }

    #[test]
    fn test_batch_delete_rejected_when_wal_inconsistent() {
        let backend = HnswBackend::new(
            2,
            DistanceMetric::Cosine,
            vec![normalize(vec![1.0, 0.0])],
            vec![HashMap::new()],
            100,
        )
        .unwrap();

        backend.wal_inconsistent.store(true, Ordering::SeqCst);
        let err = backend
            .batch_delete(&[0])
            .expect_err("batch_delete must be rejected when WAL is inconsistent");
        assert!(
            err.to_string().contains("WAL is in an inconsistent state"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_wal_compaction_after_snapshot() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        // Create backend with persistence
        let initial_embeddings = vec![normalize(vec![1.0, 0.0]), normalize(vec![0.0, 1.0])];
        let initial_metadata = vec![HashMap::new(), HashMap::new()];

        let backend = HnswBackend::with_persistence(
            2,
            DistanceMetric::Cosine,
            initial_embeddings,
            initial_metadata,
            100,
            data_dir,
            FsyncPolicy::Never,
            5,    // Snapshot every 5 inserts
            1024, // Rotate WAL aggressively to exercise compaction
        )
        .unwrap();

        // Insert documents to create multiple WAL segments
        for i in 2..7 {
            let mut meta = HashMap::new();
            meta.insert("idx".to_string(), i.to_string());
            backend
                .insert(i as u64, normalize(vec![i as f32, 1.0]), meta)
                .unwrap();
        }

        // Verify snapshot created (triggered at 5 inserts)
        let snapshot_files: Vec<_> = std::fs::read_dir(data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
            .collect();

        assert!(
            !snapshot_files.is_empty(),
            "Snapshot should have been created"
        );

        // Verify WAL compaction occurred (old WAL segments should be deleted)
        let manifest_path = data_dir.join("MANIFEST");
        let manifest = Manifest::load(&manifest_path).unwrap();

        // Should only have the active WAL segment remaining after compaction
        // (old WAL segments captured in snapshot are deleted)
        assert!(
            manifest.wal_segments.len() <= 2,
            "Old WAL segments should be compacted; found {} segments",
            manifest.wal_segments.len()
        );

        // Verify metadata persisted
        let meta2 = backend.fetch_metadata(2).unwrap();
        assert_eq!(meta2.get("idx").unwrap(), "2");
    }

    #[test]
    fn test_disk_space_check() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Check disk space for temp directory (should succeed on any system with >5% free)
        let space_check = check_and_warn_disk_space(temp_dir.path());

        // Should succeed unless disk is critically full
        assert!(space_check.is_ok(), "Disk space check failed unexpectedly");

        // Get space info
        let info = check_disk_space(temp_dir.path()).unwrap();

        // Basic sanity checks
        assert!(info.available_bytes > 0, "Available bytes should be > 0");
        assert!(
            info.available_percent >= 0.0 && info.available_percent <= 1.0,
            "Available percent should be in [0, 1]"
        );
    }

    #[test]
    fn test_insert_rejected_on_critical_disk_space() {
        // This test cannot reliably fill disk to critical level
        // Manual testing required for disk-full scenarios
        // Documented in operational runbook
    }

    #[test]
    fn test_hnsw_backend_update_metadata() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();

        let embeddings = vec![vec![1.0, 0.0]];
        let metadata = vec![HashMap::from([("key".to_string(), "val1".to_string())])];

        let backend = HnswBackend::with_persistence(
            2,
            DistanceMetric::Cosine,
            embeddings,
            metadata,
            100,
            temp_dir.path(),
            FsyncPolicy::Never,
            100,
            1024 * 1024,
        )
        .unwrap();

        // Test merge=true
        let update1 = HashMap::from([("new_key".to_string(), "new_val".to_string())]);
        backend.update_metadata(0, update1, true).unwrap();

        let meta = backend.fetch_metadata(0).unwrap();
        assert_eq!(meta.get("key").unwrap(), "val1");
        assert_eq!(meta.get("new_key").unwrap(), "new_val");

        // Test merge=false (replace)
        let update2 = HashMap::from([("replaced".to_string(), "yes".to_string())]);
        backend.update_metadata(0, update2, false).unwrap();

        let meta = backend.fetch_metadata(0).unwrap();
        assert!(!meta.contains_key("key"));
        assert_eq!(meta.get("replaced").unwrap(), "yes");

        // Test out of range
        assert!(!backend.update_metadata(999, HashMap::new(), true).unwrap());
    }
}
