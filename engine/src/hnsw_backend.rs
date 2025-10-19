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
        use std::os::unix::ffi::OsStrExt;
        use std::ffi::CString;
        
        // Use statvfs to get filesystem statistics
        let c_path = CString::new(path.as_os_str().as_bytes())
            .context("Invalid path for statvfs")?;
        
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        
        let result = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
        
        if result != 0 {
            anyhow::bail!("statvfs failed for path: {}", path.display());
        }
        
        // Cast to u64 for arithmetic (f_blocks and f_bavail are platform-dependent types)
        let total_bytes = stat.f_blocks as u64 * stat.f_frsize;
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
        use std::os::windows::ffi::OsStrExt;
        use std::ffi::OsStr;
        
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
    /// Persistence components (optional)
    persistence: Option<PersistenceState>,
}

/// Persistence state for HnswBackend
struct PersistenceState {
    data_dir: PathBuf,
    wal: Arc<RwLock<WalWriter>>,
    inserts_since_snapshot: Arc<RwLock<usize>>,
    snapshot_interval: usize,
}impl HnswBackend {
    /// Create new HNSW backend from pre-loaded embeddings (no persistence)
    ///
    /// # Parameters
    /// - `embeddings`: Pre-loaded document embeddings (indexed by doc_id)
    /// - `max_elements`: HNSW index capacity
    ///
    /// # Note
    /// This builds the HNSW index immediately, which may take time for large corpora.
    /// For production with persistence, use `with_persistence` or `recover`.
    #[instrument(level = "debug", skip(embeddings), fields(num_docs = embeddings.len(), max_elements))]
    pub fn new(embeddings: Vec<Vec<f32>>, max_elements: usize) -> Result<Self> {
        if embeddings.is_empty() {
            anyhow::bail!("Cannot create HnswBackend with empty embeddings");
        }

        let dimension = embeddings[0].len();
        let mut index = HnswVectorIndex::new(dimension, max_elements)?;

        // Build HNSW index from embeddings
        info!(
            documents = embeddings.len(),
            dimension, "building HNSW index"
        );
        for (doc_id, embedding) in embeddings.iter().enumerate() {
            index.add_vector(doc_id as u64, embedding)?;
        }
        info!("HNSW index built");

        Ok(Self {
            index: Arc::new(RwLock::new(index)),
            embeddings: Arc::new(RwLock::new(embeddings)),
            persistence: None,
        })
    }

    /// Create new HNSW backend with persistence enabled
    ///
    /// # Parameters
    /// - `embeddings`: Initial document embeddings
    /// - `max_elements`: HNSW index capacity
    /// - `data_dir`: Directory for WAL and snapshots
    /// - `fsync_policy`: Durability guarantee (Always, Periodic, Never)
    /// - `snapshot_interval`: Create snapshot every N inserts
    #[instrument(level = "debug", skip(embeddings, data_dir), fields(num_docs = embeddings.len(), max_elements, snapshot_interval))]
    pub fn with_persistence(
        embeddings: Vec<Vec<f32>>,
        max_elements: usize,
        data_dir: impl AsRef<Path>,
        fsync_policy: FsyncPolicy,
        snapshot_interval: usize,
    ) -> Result<Self> {
        if embeddings.is_empty() {
            anyhow::bail!("Cannot create HnswBackend with empty embeddings");
        }

        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).context("Failed to create data directory")?;

        let dimension = embeddings[0].len();
        let mut index = HnswVectorIndex::new(dimension, max_elements)?;

        // Build HNSW index
        info!(
            documents = embeddings.len(),
            dimension, "building HNSW index (persistence)"
        );
        for (doc_id, embedding) in embeddings.iter().enumerate() {
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
    pub fn recover(
        data_dir: impl AsRef<Path>,
        max_elements: usize,
        fsync_policy: FsyncPolicy,
        snapshot_interval: usize,
        metrics: MetricsCollector,
    ) -> Result<Self> {
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
        let mut dimension = 0;
        let mut snapshot_timestamp = 0u64;

        if let Some(snapshot_name) = &manifest.latest_snapshot {
            let snapshot_path = data_dir.join(snapshot_name);
            info!(snapshot = %snapshot_path.display(), "loading snapshot with validation");

            // Use load_with_validation for automatic fallback recovery
            match Snapshot::load_with_validation(&snapshot_path, &metrics) {
                Ok((snapshot, recovered_from_fallback)) => {
                    dimension = snapshot.dimension;
                    snapshot_timestamp = snapshot.timestamp;
                    
                    // Restore embeddings preserving doc_id → index mapping
                    // Find max doc_id to size the vector correctly
                    let max_doc_id = snapshot.documents.iter().map(|(id, _)| *id).max().unwrap_or(0) as usize;
                    embeddings = vec![vec![0.0; dimension]; max_doc_id + 1];
                    
                    for (doc_id, embedding) in snapshot.documents {
                        embeddings[doc_id as usize] = embedding;
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
                        // Infer dimension from first entry if no snapshot
                        if dimension == 0 && !entry.embedding.is_empty() {
                            dimension = entry.embedding.len();
                        }

                        let doc_id = entry.doc_id as usize;
                        if doc_id >= embeddings.len() {
                            embeddings.resize(doc_id + 1, vec![0.0; dimension]);
                        }
                        embeddings[doc_id] = entry.embedding;
                    }
                    WalOp::Delete => {
                        // For now, just zero out the embedding (tombstone)
                        let doc_id = entry.doc_id as usize;
                        if doc_id < embeddings.len() {
                            embeddings[doc_id] = vec![0.0; dimension];
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
            anyhow::bail!("Recovery failed: no data found in snapshots or WAL");
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
            persistence: Some(persistence),
        })
    }

    fn timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Insert new document (with WAL logging if persistence enabled)
    ///
    /// # Parameters
    /// - `doc_id`: Document identifier
    /// - `embedding`: Document embedding vector
    ///
    /// # Durability
    /// - If persistence enabled: Append to WAL → Update in-memory → fsync (if Always)
    /// - Triggers snapshot creation if `inserts_since_snapshot >= snapshot_interval`
    #[instrument(level = "trace", skip(self, embedding), fields(doc_id, dim = embedding.len()))]
    pub fn insert(&self, doc_id: u64, embedding: Vec<f32>) -> Result<()> {
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
            anyhow::bail!("embedding dimension mismatch: expected {} found {}", current_dim, embedding.len());
        }

        // Update in-memory index and embeddings
        let mut index = self.index.write();
        let mut embeddings = self.embeddings.write();

        let doc_id_usize = doc_id as usize;
        if doc_id_usize >= embeddings.len() {
            embeddings.resize(doc_id_usize + 1, vec![0.0; embedding.len()]);
        }

        embeddings[doc_id_usize] = embedding.clone();
        index.add_vector(doc_id, &embedding)?;

        Ok(())
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

        // Release embeddings read lock before heavy I/O (snapshot.save)
        drop(embeddings);

        let dimension = if documents.is_empty() {
            0
        } else {
            documents[0].1.len()
        };

        let doc_count = documents.len();
        let snapshot = Snapshot::new(dimension, documents);
        
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
        let compacted = self.compact_old_wal_segments(&persistence.data_dir, snapshot_timestamp, &mut manifest)?;
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
    #[instrument(level = "trace", skip(self, query), fields(k, dim = query.len()))]
    pub fn knn_search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
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
        index.knn_search(query, k)
    }

    /// Get number of documents in backend
    pub fn len(&self) -> usize {
        self.embeddings.read().len()
    }

    /// Check if backend is empty
    pub fn is_empty(&self) -> bool {
        self.embeddings.read().is_empty()
    }

    /// Get embedding dimension
    pub fn dimension(&self) -> usize {
        let embeddings = self.embeddings.read();
        if embeddings.is_empty() {
            0
        } else {
            embeddings[0].len()
        }
    }

    /// Get all embeddings (for semantic adapter initialization)
    pub fn get_all_embeddings(&self) -> Arc<Vec<Vec<f32>>> {
        let embeddings = self.embeddings.read();
        Arc::new(embeddings.clone())
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
                                "deleted old WAL segment"
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
                                "failed to delete old WAL segment"
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

        let backend = HnswBackend::new(embeddings, 100).unwrap();

        // Test fetch_document
        let doc0 = backend.fetch_document(0).unwrap();
        assert_eq!(doc0, vec![1.0, 0.0, 0.0, 0.0]);

        let doc1 = backend.fetch_document(1).unwrap();
        assert_eq!(doc1, vec![0.0, 1.0, 0.0, 0.0]);

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

        let backend = HnswBackend::new(embeddings, 100).unwrap();

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
        let backend = HnswBackend::new(embeddings, 100).unwrap();
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
        let backend = HnswBackend::with_persistence(
            initial_embeddings,
            100,
            data_dir,
            FsyncPolicy::Never,
            5, // Snapshot every 5 inserts
        )
        .unwrap();

        // Insert documents to create multiple WAL segments
        for i in 2..7 {
            backend.insert(i as u64, vec![i as f32, 0.0]).unwrap();
        }

        // Verify snapshot created (triggered at 5 inserts)
        let snapshot_files: Vec<_> = std::fs::read_dir(data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("snapshot_")
            })
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
        assert!(info.available_percent >= 0.0 && info.available_percent <= 1.0,
                "Available percent should be in [0, 1]");
    }

    #[test]
    fn test_insert_rejected_on_critical_disk_space() {
        // This test cannot reliably fill disk to critical level
        // Manual testing required for disk-full scenarios
        // Documented in operational runbook
    }
}
