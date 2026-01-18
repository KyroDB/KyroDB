//! HNSW Backend for Cache Integration
//!
//! Provides a clean interface for cache strategies to query HNSW index on cache miss.
//! This bridges the cache layer with the vector search layer.
//!
//! **Persistence**: WAL + snapshots for durability and fast recovery.

use crate::hnsw_index::{HnswVectorIndex, SearchResult};
use crate::metrics::MetricsCollector;
use crate::persistence::{FsyncPolicy, Manifest, Snapshot, WalEntry, WalOp, WalReader, WalWriter};
use anyhow::{Context, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, instrument, trace, warn};

/// Disk space warning threshold (alert if free space < 10%)
const DISK_SPACE_WARNING_THRESHOLD: f64 = 0.10;

/// Disk space critical threshold (reject writes if free space < 5%)
const DISK_SPACE_CRITICAL_THRESHOLD: f64 = 0.05;

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
    /// Pre-loaded embeddings for O(1) fetch by doc_id
    /// This avoids storing embeddings in HNSW graph (memory optimization)
    embeddings: Arc<RwLock<Vec<Vec<f32>>>>,
    /// Metadata storage for O(1) fetch by doc_id
    metadata: Arc<RwLock<Vec<HashMap<String, String>>>>,
    /// Persistence components (optional)
    persistence: Option<PersistenceState>,
}

/// Persistence state for HnswBackend
struct PersistenceState {
    data_dir: PathBuf,
    wal: Arc<RwLock<WalWriter>>,
    inserts_since_snapshot: Arc<RwLock<usize>>,
    snapshot_interval: usize,
}
impl HnswBackend {
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
        embeddings: Vec<Vec<f32>>,
        metadata: Vec<HashMap<String, String>>,
        max_elements: usize,
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
        let mut index = HnswVectorIndex::new(dimension, max_elements)?;

        // Build HNSW index from embeddings
        info!(
            documents = embeddings.len(),
            dimension, "building HNSW index"
        );
        for (doc_id, embedding) in embeddings.iter().enumerate() {
            if embedding.len() != dimension {
                anyhow::bail!(
                    "embedding dimension mismatch for doc_id {}: expected {}, got {}",
                    doc_id,
                    dimension,
                    embedding.len()
                );
            }
            index.add_vector(doc_id as u64, embedding)?;
        }
        info!("HNSW index built");

        Ok(Self {
            index: Arc::new(RwLock::new(index)),
            embeddings: Arc::new(RwLock::new(embeddings)),
            metadata: Arc::new(RwLock::new(metadata)),
            persistence: None,
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
    pub fn with_persistence(
        dimension: usize,
        embeddings: Vec<Vec<f32>>,
        metadata: Vec<HashMap<String, String>>,
        max_elements: usize,
        data_dir: impl AsRef<Path>,
        fsync_policy: FsyncPolicy,
        snapshot_interval: usize,
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

        let mut index = HnswVectorIndex::new(dimension, max_elements)?;

        // Build HNSW index
        info!(
            documents = embeddings.len(),
            dimension, "building HNSW index (persistence)"
        );
        for (doc_id, embedding) in embeddings.iter().enumerate() {
            if embedding.len() != dimension {
                anyhow::bail!(
                    "embedding dimension mismatch for doc_id {}: expected {}, got {}",
                    doc_id,
                    dimension,
                    embedding.len()
                );
            }
            index.add_vector(doc_id as u64, embedding)?;
        }
        info!("HNSW index built (persistence)");

        // Create initial WAL
        let wal_path = data_dir.join(format!("wal_{}.wal", Self::timestamp()));
        let wal = WalWriter::create(&wal_path, fsync_policy)?;

        // Update manifest
        let mut manifest = Manifest::load_or_create(data_dir.join("MANIFEST"))?;
        manifest
            .wal_segments
            .push(wal_path.file_name().unwrap().to_string_lossy().to_string());
        manifest.save(data_dir.join("MANIFEST"))?;

        let persistence = PersistenceState {
            data_dir,
            wal: Arc::new(RwLock::new(wal)),
            inserts_since_snapshot: Arc::new(RwLock::new(0)),
            snapshot_interval,
        };

        Ok(Self {
            index: Arc::new(RwLock::new(index)),
            embeddings: Arc::new(RwLock::new(embeddings)),
            metadata: Arc::new(RwLock::new(metadata)),
            persistence: Some(persistence),
        })
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
    /// Recover from WAL + snapshot
    ///
    /// # Recovery Flow
    /// 1. Load manifest
    /// 2. Load latest snapshot with fallback recovery (if corrupted)
    /// 3. Replay WAL entries since snapshot
    /// 4. Rebuild HNSW index
    /// 5. Create new active WAL
    #[instrument(level = "info", skip(data_dir, metrics), fields(data_dir = %data_dir.as_ref().to_path_buf().display(), max_elements, snapshot_interval))]
    pub fn recover(
        embedding_dimension: usize,
        data_dir: impl AsRef<Path>,
        max_elements: usize,
        fsync_policy: FsyncPolicy,
        snapshot_interval: usize,
        metrics: MetricsCollector,
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
        let mut embeddings: Vec<Vec<f32>> = Vec::new();
        let mut metadata: Vec<HashMap<String, String>> = Vec::new();
        let mut dimension = embedding_dimension;
        let mut snapshot_timestamp = 0u64;

        if let Some(snapshot_name) = &manifest.latest_snapshot {
            let snapshot_path = data_dir.join(snapshot_name);
            info!(snapshot = %snapshot_path.display(), "loading snapshot with validation");

            // Use load_with_validation for automatic fallback recovery
            match Snapshot::load_with_validation(&snapshot_path, &metrics) {
                Ok((snapshot, recovered_from_fallback)) => {
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
                    dimension = snapshot.dimension;
                    snapshot_timestamp = snapshot.timestamp;

                    // Restore embeddings and metadata preserving doc_id → index mapping
                    // Find max doc_id to size the vector correctly
                    let max_doc_id = snapshot
                        .documents
                        .iter()
                        .map(|(id, _)| *id)
                        .max()
                        .unwrap_or(0) as usize;

                    // Ensure max_doc_id covers metadata too
                    let max_meta_id = snapshot
                        .metadata
                        .iter()
                        .map(|(id, _)| *id)
                        .max()
                        .unwrap_or(0) as usize;

                    let max_id = std::cmp::max(max_doc_id, max_meta_id);

                    embeddings = vec![vec![0.0; dimension]; max_id + 1];
                    metadata = vec![HashMap::new(); max_id + 1];

                    for (doc_id, embedding) in snapshot.documents {
                        embeddings[doc_id as usize] = embedding;
                    }

                    for (doc_id, meta) in snapshot.metadata {
                        metadata[doc_id as usize] = meta;
                    }

                    if recovered_from_fallback {
                        warn!(
                            documents = embeddings.len(),
                            "snapshot loaded from fallback (primary corrupted)"
                        );
                    } else {
                        info!(documents = embeddings.len(), "snapshot loaded successfully");
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
                // Skip entries older than snapshot timestamp (already included in snapshot)
                if snapshot_timestamp > 0 && entry.timestamp <= snapshot_timestamp {
                    trace!(
                        doc_id = entry.doc_id,
                        entry_ts = entry.timestamp,
                        snapshot_ts = snapshot_timestamp,
                        "skipping entry older than snapshot"
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

                        let doc_id = entry.doc_id as usize;
                        if doc_id >= embeddings.len() {
                            embeddings.resize(doc_id + 1, vec![0.0; dimension]);
                            metadata.resize(doc_id + 1, HashMap::new());
                        }
                        embeddings[doc_id] = entry.embedding;
                        metadata[doc_id] = entry.metadata;
                    }
                    WalOp::Delete => {
                        let doc_id = entry.doc_id as usize;
                        if doc_id < embeddings.len() {
                            embeddings[doc_id] = vec![0.0; dimension];
                            metadata[doc_id].clear();
                        }
                    }
                    WalOp::UpdateMetadata => {
                        let doc_id = entry.doc_id as usize;
                        if doc_id < metadata.len() {
                            metadata[doc_id] = entry.metadata;
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

        if embeddings.is_empty() {
            // It's possible to have an empty DB if it's new, but usually we expect something if recovering.
            // However, allowing empty recovery is safer for new deployments.
            info!("Recovery: no data found (fresh start or empty DB)");
        }

        // Rebuild HNSW index
        let mut index = HnswVectorIndex::new(dimension, max_elements)?;
        info!(
            documents = embeddings.len(),
            dimension, "rebuilding HNSW index after recovery"
        );

        for (doc_id, embedding) in embeddings.iter().enumerate() {
            // Skip tombstones (all zeros)
            if embedding.iter().all(|&x| x == 0.0) {
                continue;
            }
            index.add_vector(doc_id as u64, embedding)?;
        }

        info!("HNSW index rebuilt");

        // Create new active WAL
        let wal_path = data_dir.join(format!("wal_{}.wal", Self::timestamp()));
        let wal = WalWriter::create(&wal_path, fsync_policy)?;

        // Update manifest
        let mut manifest = manifest;
        manifest
            .wal_segments
            .push(wal_path.file_name().unwrap().to_string_lossy().to_string());
        manifest.save(&manifest_path)?;

        let persistence = PersistenceState {
            data_dir,
            wal: Arc::new(RwLock::new(wal)),
            inserts_since_snapshot: Arc::new(RwLock::new(0)),
            snapshot_interval,
        };

        info!(documents = embeddings.len(), "recovery complete");

        Ok(Self {
            index: Arc::new(RwLock::new(index)),
            embeddings: Arc::new(RwLock::new(embeddings)),
            metadata: Arc::new(RwLock::new(metadata)),
            persistence: Some(persistence),
        })
    }

    fn timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is set before Unix epoch (1970-01-01) - this should never happen on a properly configured system")
            .as_secs()
    }

    /// Get vector dimension
    pub fn dimension(&self) -> usize {
        self.index.read().dimension()
    }

    /// Get number of documents
    pub fn len(&self) -> usize {
        self.embeddings.read().len()
    }

    /// Check if backend is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Lightweight existence check that avoids cloning embeddings
    pub fn exists(&self, doc_id: u64) -> bool {
        let embeddings = self.embeddings.read();
        embeddings
            .get(doc_id as usize)
            .map(|embedding| embedding.iter().any(|&x| x != 0.0))
            .unwrap_or(false)
    }

    /// Insert new document (with WAL logging if persistence enabled)
    ///
    /// # Parameters
    /// - `doc_id`: Document identifier
    /// - `embedding`: Document embedding vector
    /// - `metadata`: Document metadata
    ///
    /// # Durability
    /// - If persistence enabled: Append to WAL → Update in-memory → fsync (if Always)
    /// - Triggers snapshot creation if `inserts_since_snapshot >= snapshot_interval`
    #[instrument(level = "trace", skip(self, embedding, metadata), fields(doc_id, dim = embedding.len()))]
    pub fn insert(
        &self,
        doc_id: u64,
        embedding: Vec<f32>,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        // Check disk space before write (if persistence enabled)
        if let Some(ref persistence) = self.persistence {
            if !check_and_warn_disk_space(&persistence.data_dir)? {
                anyhow::bail!(
                    "Insert rejected: disk space critically low (< {}%)",
                    DISK_SPACE_CRITICAL_THRESHOLD * 100.0
                );
            }

            let entry = WalEntry {
                op: WalOp::Insert,
                doc_id,
                embedding: embedding.clone(),
                metadata: metadata.clone(),
                timestamp: Self::timestamp(),
            };

            persistence.wal.write().append(&entry)?;

            // Track inserts for snapshot trigger
            let mut inserts = persistence.inserts_since_snapshot.write();
            *inserts += 1;

            if *inserts >= persistence.snapshot_interval {
                debug!(
                    inserts = *inserts,
                    interval = persistence.snapshot_interval,
                    "snapshot interval reached; creating snapshot"
                );
                drop(inserts); // Release lock before snapshot
                self.create_snapshot()?;
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

        // Update in-memory index and embeddings
        let mut index = self.index.write();
        let mut embeddings = self.embeddings.write();
        let mut metas = self.metadata.write();

        let doc_id_usize = doc_id as usize;
        if doc_id_usize >= embeddings.len() {
            embeddings.resize(doc_id_usize + 1, vec![0.0; embedding.len()]);
            metas.resize(doc_id_usize + 1, HashMap::new());
        }

        embeddings[doc_id_usize] = embedding.clone();
        metas[doc_id_usize] = metadata;
        index.add_vector(doc_id, &embedding)?;

        Ok(())
    }

    /// Bulk insert documents directly into HNSW index.
    ///
    /// This method is optimized for loading large batches of vectors quickly:
    /// - Acquires write locks once (not per-vector)
    /// - Pre-allocates capacity for embeddings/metadata arrays
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
        use tracing::info;

        if documents.is_empty() {
            return Ok((0, 0));
        }

        let start = Instant::now();
        let total = documents.len();
        
        // Get expected dimension from first vector or from index
        let expected_dim = if let Some((_, ref emb, _)) = documents.first() {
            emb.len()
        } else {
            self.dimension()
        };

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

        // Find max doc_id for pre-allocation
        let max_doc_id = documents.iter().map(|(id, _, _)| *id).max().unwrap_or(0) as usize;

        // Acquire all write locks once
        let mut index = self.index.write();
        let mut embeddings = self.embeddings.write();
        let mut metas = self.metadata.write();

        // Pre-allocate capacity
        if max_doc_id >= embeddings.len() {
            embeddings.resize(max_doc_id + 1, vec![0.0; expected_dim]);
            metas.resize(max_doc_id + 1, HashMap::new());
        }

        let mut loaded = 0u64;
        let mut failed = 0u64;
        let log_interval = std::cmp::max(total / 20, 10000); // Log every 5% or 10K

        for (i, (doc_id, embedding, metadata)) in documents.into_iter().enumerate() {
            let doc_id_usize = doc_id as usize;

            // Ensure capacity for this specific doc_id
            if doc_id_usize >= embeddings.len() {
                embeddings.resize(doc_id_usize + 1, vec![0.0; expected_dim]);
                metas.resize(doc_id_usize + 1, HashMap::new());
            }

            // Store embedding and metadata
            embeddings[doc_id_usize] = embedding.clone();
            metas[doc_id_usize] = metadata;

            // Add to HNSW index
            match index.add_vector(doc_id, &embedding) {
                Ok(_) => loaded += 1,
                Err(e) => {
                    // Log first few failures, then throttle
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

        let embeddings = self.embeddings.read();
        let metas = self.metadata.read();

        // Create snapshot object
        // Collect a consistent view of documents. This clones embeddings while holding
        // the read lock to ensure snapshot consistency. We drop the read lock before
        // performing the potentially slow file I/O below to avoid blocking writers
        // for the duration of the save.
        let documents: Vec<(u64, Vec<f32>)> = embeddings
            .iter()
            .enumerate()
            .filter(|(_, emb)| !emb.iter().all(|&x| x == 0.0)) // Skip tombstones
            .map(|(id, emb)| (id as u64, emb.clone()))
            .collect();

        let metadata_vec: Vec<(u64, HashMap<String, String>)> = metas
            .iter()
            .enumerate()
            .filter(|(id, _)| *id < embeddings.len() && !embeddings[*id].iter().all(|&x| x == 0.0)) // Match documents
            .map(|(id, meta)| (id as u64, meta.clone()))
            .collect();

        // Release locks before heavy I/O (snapshot.save)
        drop(embeddings);
        drop(metas);

        let dimension = if documents.is_empty() {
            0
        } else {
            documents[0].1.len()
        };

        let doc_count = documents.len();
        let snapshot = Snapshot::new(dimension, documents, metadata_vec)?;

        // Save snapshot with timestamp
        let snapshot_timestamp = Self::timestamp();
        let snapshot_name = format!("snapshot_{}.snap", snapshot_timestamp);
        let snapshot_path = persistence.data_dir.join(&snapshot_name);
        snapshot.save(&snapshot_path)?;
        info!(path = %snapshot_path.display(), docs = doc_count, timestamp = snapshot_timestamp, "snapshot saved");

        // Update manifest
        let manifest_path = persistence.data_dir.join("MANIFEST");
        let mut manifest = Manifest::load_or_create(&manifest_path)?;
        manifest.latest_snapshot = Some(snapshot_name.clone());
        manifest.save(&manifest_path)?;

        // WAL Compaction: Delete old WAL segments that are fully captured in the snapshot
        // This prevents unbounded disk usage growth
        let compacted = self.compact_old_wal_segments(
            &persistence.data_dir,
            snapshot_timestamp,
            &mut manifest,
        )?;
        if compacted > 0 {
            info!(
                compacted_segments = compacted,
                snapshot_ts = snapshot_timestamp,
                "WAL compaction complete"
            );
            // Save updated manifest after compaction
            manifest.save(&manifest_path)?;
        }

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
        self.embeddings.read().get(doc_id as usize).cloned()
    }

    /// Fetch document metadata by ID (O(1) lookup)
    #[instrument(level = "trace", skip(self), fields(doc_id))]
    pub fn fetch_metadata(&self, doc_id: u64) -> Option<HashMap<String, String>> {
        self.metadata.read().get(doc_id as usize).cloned()
    }

    /// Bulk fetch documents by ID (O(1) lookup)
    ///
    /// Returns a vector of Option<(embedding, metadata)> corresponding to input IDs.
    /// Amortizes lock acquisition cost.
    #[instrument(level = "trace", skip(self), fields(count = doc_ids.len()))]
    #[allow(clippy::type_complexity)]
    pub fn bulk_fetch(&self, doc_ids: &[u64]) -> Vec<Option<(Vec<f32>, HashMap<String, String>)>> {
        let embeddings = self.embeddings.read();
        let metadata = self.metadata.read();

        doc_ids
            .iter()
            .map(|&id| {
                let id_usize = id as usize;
                if id_usize < embeddings.len() && id_usize < metadata.len() {
                    let emb = &embeddings[id_usize];
                    // Check for tombstone (all zeros)
                    if emb.iter().all(|&x| x == 0.0) {
                        None
                    } else {
                        Some((emb.clone(), metadata[id_usize].clone()))
                    }
                } else {
                    None
                }
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
        let mut meta_guard = self.metadata.write();

        if doc_id as usize >= meta_guard.len() {
            return Ok(false);
        }

        // Apply update
        let updated_metadata = if merge {
            let mut merged = meta_guard[doc_id as usize].clone();
            merged.extend(metadata);
            merged
        } else {
            metadata
        };

        // Log to WAL before updating in-memory state
        if let Some(ref persistence) = self.persistence {
            // Check disk space
            if !check_and_warn_disk_space(&persistence.data_dir)? {
                anyhow::bail!(
                    "Metadata update rejected: disk space critically low (< {}%)",
                    DISK_SPACE_CRITICAL_THRESHOLD * 100.0
                );
            }

            let entry = WalEntry {
                op: WalOp::UpdateMetadata,
                doc_id,
                embedding: vec![], // No embedding for metadata-only update
                metadata: updated_metadata.clone(),
                timestamp: Self::timestamp(),
            };
            persistence.wal.write().append(&entry)?;

            // Track inserts/updates for snapshot trigger
            // We count metadata updates towards snapshot interval to ensure WAL doesn't grow unbounded
            let mut inserts = persistence.inserts_since_snapshot.write();
            *inserts += 1;

            if *inserts >= persistence.snapshot_interval {
                debug!(
                    inserts = *inserts,
                    interval = persistence.snapshot_interval,
                    "snapshot interval reached (metadata update); creating snapshot"
                );
                drop(inserts); // Release lock before snapshot
                               // Note: We ignore snapshot errors here to avoid failing the update
                if let Err(e) = self.create_snapshot() {
                    error!(error = %e, "failed to create snapshot after metadata update");
                }
            }
        }

        // Update in-memory metadata
        meta_guard[doc_id as usize] = updated_metadata;

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
    /// - Sets embedding to all-zeros (tombstone)
    /// - Does NOT remove from HNSW index immediately (soft delete)
    /// - Filtered out during search
    #[instrument(level = "debug", skip(self), fields(doc_id))]
    pub fn delete(&self, doc_id: u64) -> Result<bool> {
        // Check existence first to avoid logging unnecessary WAL entries
        let doc_id_usize = doc_id as usize;
        let exists = {
            let embeddings = self.embeddings.read();
            doc_id_usize < embeddings.len() && !embeddings[doc_id_usize].iter().all(|&x| x == 0.0)
        };

        if !exists {
            return Ok(false);
        }

        // Log to WAL
        if let Some(ref persistence) = self.persistence {
            if !check_and_warn_disk_space(&persistence.data_dir)? {
                anyhow::bail!(
                    "Delete rejected: disk space critically low (< {}%)",
                    DISK_SPACE_CRITICAL_THRESHOLD * 100.0
                );
            }

            let entry = WalEntry {
                op: WalOp::Delete,
                doc_id,
                embedding: vec![],
                metadata: HashMap::new(),
                timestamp: Self::timestamp(),
            };
            persistence.wal.write().append(&entry)?;
        }

        // Update in-memory state (Soft Delete)
        let mut embeddings = self.embeddings.write();
        let mut metas = self.metadata.write();

        if doc_id_usize < embeddings.len() {
            // Set to zero-vector (tombstone)
            let dim = embeddings[doc_id_usize].len();
            embeddings[doc_id_usize] = vec![0.0; dim];

            // Clear metadata
            metas[doc_id_usize].clear();

            Ok(true)
        } else {
            // Should be covered by initial check, but safe fallback
            Ok(false)
        }
    }

    /// Batch delete documents by ID
    ///
    /// Efficiently deletes multiple documents with a single lock acquisition and batched WAL write.
    #[instrument(skip(self, doc_ids), fields(count = doc_ids.len()))]
    pub fn batch_delete(&self, doc_ids: &[u64]) -> Result<u64> {
        // Acquire write locks
        let mut embeddings = self.embeddings.write();
        let mut metadata = self.metadata.write();

        let mut deleted_count = 0;
        let mut wal_entries = Vec::with_capacity(doc_ids.len());
        let timestamp = Self::timestamp();

        // First pass: identify valid deletes and prepare WAL entries
        for &doc_id in doc_ids {
            let id_usize = doc_id as usize;
            if id_usize < embeddings.len() {
                let emb = &embeddings[id_usize];
                // Check if not already tombstone
                if !emb.iter().all(|&x| x == 0.0) {
                    // Valid delete
                    wal_entries.push(WalEntry {
                        op: WalOp::Delete,
                        doc_id,
                        embedding: Vec::new(),
                        metadata: HashMap::new(),
                        timestamp,
                    });
                }
            }
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

            persistence.wal.write().append_batch(&wal_entries)?;

            // Update snapshot counter
            let mut inserts = persistence.inserts_since_snapshot.write();
            *inserts += wal_entries.len();

            if *inserts >= persistence.snapshot_interval {
                should_snapshot = true;
                debug!(
                    inserts = *inserts,
                    interval = persistence.snapshot_interval,
                    "snapshot interval reached (batch delete); will create snapshot"
                );
            }
        }

        // Apply changes to memory
        for entry in &wal_entries {
            let id_usize = entry.doc_id as usize;
            // Mark as tombstone
            if let Some(emb) = embeddings.get_mut(id_usize) {
                let dim = emb.len();
                *emb = vec![0.0; dim];
            }
            if let Some(meta) = metadata.get_mut(id_usize) {
                meta.clear();
            }
            deleted_count += 1;
        }

        // Release locks before snapshot
        drop(embeddings);
        drop(metadata);

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
        let embeddings = self.embeddings.read();

        // Oversample to account for tombstones (deleted documents)
        // We ask for more results, then filter out deleted ones
        // Heuristic: fetch 2x, capped at max allowed to avoid index errors
        let search_k = std::cmp::min(k * 2, 10_000);
        let mut results = index.knn_search_with_ef(query, search_k, ef_search_override)?;

        // Filter out tombstones (all-zero embeddings)
        results.retain(|r| {
            let id = r.doc_id as usize;
            if id < embeddings.len() {
                // Check if embedding is all zeros (tombstone)
                !embeddings[id].iter().all(|&x| x == 0.0)
            } else {
                false // Should not happen if index is consistent
            }
        });

        // Truncate to requested k
        results.truncate(k);

        Ok(results)
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
        let metas = self.metadata.read();
        let embeddings = self.embeddings.read();

        metas
            .iter()
            .enumerate()
            .filter(|(id, meta)| {
                // Skip tombstones
                if *id < embeddings.len() && embeddings[*id].iter().all(|&x| x == 0.0) {
                    return false;
                }
                predicate(meta)
            })
            .map(|(id, _)| id as u64)
            .collect()
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
    /// Deletes WAL files where all entries have timestamp <= snapshot_timestamp.
    /// This prevents unbounded disk usage from accumulating WAL segments.
    ///
    /// # Parameters
    /// - `data_dir`: Directory containing WAL files
    /// - `snapshot_timestamp`: Timestamp of the just-created snapshot
    /// - `manifest`: Manifest to update (removes deleted WAL segments)
    ///
    /// # Returns
    /// Number of WAL segments deleted
    ///
    /// # Safety
    /// - Only deletes WAL segments listed in manifest (controlled cleanup)
    /// - Always keeps the current active WAL segment (last in list)
    /// - Updates manifest atomically after successful deletion
    #[instrument(level = "debug", skip(self, data_dir, manifest), fields(snapshot_ts = snapshot_timestamp))]
    fn compact_old_wal_segments(
        &self,
        data_dir: &Path,
        snapshot_timestamp: u64,
        manifest: &mut Manifest,
    ) -> Result<usize> {
        let mut deleted_count = 0;
        let mut segments_to_keep = Vec::new();

        // Always keep the last WAL segment (active WAL)
        let active_wal_index = manifest.wal_segments.len().saturating_sub(1);

        for (idx, wal_name) in manifest.wal_segments.iter().enumerate() {
            // Never delete active WAL
            if idx == active_wal_index {
                segments_to_keep.push(wal_name.clone());
                continue;
            }

            // Extract timestamp from WAL filename (format: "wal_{timestamp}.wal")
            let wal_timestamp = wal_name
                .strip_prefix("wal_")
                .and_then(|s| s.strip_suffix(".wal"))
                .and_then(|s| s.parse::<u64>().ok());

            match wal_timestamp {
                Some(ts) if ts <= snapshot_timestamp => {
                    // This WAL segment is fully captured in snapshot - safe to delete
                    let wal_path = data_dir.join(wal_name);

                    match std::fs::remove_file(&wal_path) {
                        Ok(()) => {
                            debug!(
                                wal_segment = wal_name,
                                wal_ts = ts,
                                snapshot_ts = snapshot_timestamp,
                                "deleted old WAL segment",
                            );
                            deleted_count += 1;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // Already deleted or never existed - not an error
                            warn!(
                                wal_segment = wal_name,
                                "WAL segment already missing (skipping)"
                            );
                            deleted_count += 1; // Still count as "compacted"
                        }
                        Err(e) => {
                            // Log error but continue - don't fail compaction for one bad file
                            error!(
                                wal_segment = wal_name,
                                error = %e,
                                "failed to delete old WAL segment",
                            );
                            segments_to_keep.push(wal_name.clone()); // Keep in manifest
                        }
                    }
                }
                Some(_) => {
                    // WAL segment newer than snapshot - keep it
                    segments_to_keep.push(wal_name.clone());
                }
                None => {
                    // Failed to parse timestamp - keep segment for safety
                    warn!(
                        wal_segment = wal_name,
                        "failed to parse WAL timestamp; keeping segment"
                    );
                    segments_to_keep.push(wal_name.clone());
                }
            }
        }

        // Update manifest with remaining segments
        manifest.wal_segments = segments_to_keep;

        Ok(deleted_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_backend_basic_operations() {
        // Create test embeddings
        let embeddings = vec![
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0, 0.0],
            vec![0.0, 0.0, 1.0, 0.0],
        ];

        let metadata = vec![
            HashMap::from([("id".to_string(), "0".to_string())]),
            HashMap::from([("id".to_string(), "1".to_string())]),
            HashMap::from([("id".to_string(), "2".to_string())]),
        ];

        let backend = HnswBackend::new(4, embeddings, metadata, 100).unwrap();

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
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.9, 0.1, 0.0, 0.0], // Similar to doc 0
            vec![0.0, 0.0, 1.0, 0.0], // Different
        ];

        let metadata = vec![HashMap::new(); 3];

        let backend = HnswBackend::new(4, embeddings, metadata, 100).unwrap();

        // Query closest to doc 0
        let query = vec![1.0, 0.0, 0.0, 0.0];
        let results = backend.knn_search(&query, 2).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, 0); // Exact match
        assert_eq!(results[1].doc_id, 1); // Similar
    }

    #[test]
    fn test_hnsw_backend_empty_check() {
        let embeddings = vec![vec![1.0, 0.0]];
        let metadata = vec![HashMap::new()];
        let backend = HnswBackend::new(2, embeddings, metadata, 100).unwrap();
        assert!(!backend.is_empty());
        assert_eq!(backend.len(), 1);
        assert_eq!(backend.dimension(), 2);
    }

    #[test]
    fn test_wal_compaction_after_snapshot() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        // Create backend with persistence
        let initial_embeddings = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let initial_metadata = vec![HashMap::new(), HashMap::new()];

        let backend = HnswBackend::with_persistence(
            2,
            initial_embeddings,
            initial_metadata,
            100,
            data_dir,
            FsyncPolicy::Never,
            5, // Snapshot every 5 inserts
        )
        .unwrap();

        // Insert documents to create multiple WAL segments
        for i in 2..7 {
            let mut meta = HashMap::new();
            meta.insert("idx".to_string(), i.to_string());
            backend.insert(i as u64, vec![i as f32, 0.0], meta).unwrap();
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
            embeddings,
            metadata,
            100,
            temp_dir.path(),
            FsyncPolicy::Never,
            100,
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
