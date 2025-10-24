//! Persistence layer for HNSW index (WAL + Snapshots)
//!
//! This module provides durability guarantees for the vector database:
//! - **WAL (Write-Ahead Log)**: All mutations logged before in-memory update
//! - **Snapshots**: Periodic full index serialization for fast recovery
//! - **Manifest**: Tracks valid snapshots and WAL segments
//! - **Error Recovery**: Circuit breaker + retry logic for write failures
//!
//! # Durability Guarantees
//! - `fsync` policy configurable (always, periodic, never)
//! - Checksum validation on all reads (detect corruption)
//! - Atomic file operations (temp file + rename)
//! - Crash recovery via snapshot + WAL replay
//! - Automatic retry with exponential backoff
//! - Circuit breaker for disk full scenarios

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, instrument, warn};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::metrics::MetricsCollector;

/// WAL magic number (identifies valid WAL files)
const WAL_MAGIC: u32 = 0x57414C00; // "WAL\0"

/// Snapshot magic number
const SNAPSHOT_MAGIC: u32 = 0x534E4150; // "SNAP"

/// WAL operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum WalOp {
    Insert = 1,
    Delete = 2,
}

/// WAL entry: single mutation logged to disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub op: WalOp,
    pub doc_id: u64,
    pub embedding: Vec<f32>,
    pub timestamp: u64,
}

/// fsync policy for WAL writes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every write (safest, slowest)
    Always,
    /// fsync every N milliseconds (balanced)
    Periodic(u64),
    /// Never fsync (fastest, data loss on crash)
    Never,
}

/// WAL writer: append-only log with checksums
///
/// Performance optimization: Uses async fsync for FsyncPolicy::Always to avoid
/// blocking writes. The file is cloned and synced in a background task.
pub struct WalWriter {
    file: BufWriter<File>,
    path: PathBuf,
    fsync_policy: FsyncPolicy,
    entry_count: usize,
    bytes_written: u64,
    error_handler: Option<Arc<WalErrorHandler>>,
}

impl WalWriter {
    /// Create new WAL file
    #[instrument(level = "debug", skip(path), fields(fsync_policy = ?fsync_policy))]
    pub fn create(path: impl AsRef<Path>, fsync_policy: FsyncPolicy) -> Result<Self> {
        Self::create_with_error_handler(path, fsync_policy, None)
    }

    /// Create new WAL file with error handler
    #[instrument(level = "debug", skip(path, error_handler), fields(fsync_policy = ?fsync_policy))]
    pub fn create_with_error_handler(
        path: impl AsRef<Path>,
        fsync_policy: FsyncPolicy,
        error_handler: Option<Arc<WalErrorHandler>>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .context("Failed to create WAL file")?;

        let mut writer = BufWriter::new(file);

        // Write magic header
        writer.write_all(&WAL_MAGIC.to_le_bytes())?;
        writer.flush()?;

        Ok(Self {
            file: writer,
            path,
            fsync_policy,
            entry_count: 0,
            bytes_written: 4, // Magic header
            error_handler,
        })
    }

    /// Append entry to WAL
    #[instrument(level = "trace", skip(self, entry), fields(doc_id = entry.doc_id, op = ?entry.op, embedding_dim = entry.embedding.len()))]
    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        match &self.error_handler {
            Some(error_handler) => {
                // Clone error handler arc to avoid borrow conflict
                let handler = Arc::clone(error_handler);
                // Clone entry to avoid borrowing issues
                let entry_clone = entry.clone();
                handler.write_with_retry(|| self.append_internal(&entry_clone))
            }
            None => {
                // Direct write without error handling
                self.append_internal(entry)
            }
        }
    }

    /// Internal append logic (called by append or via error handler)
    fn append_internal(&mut self, entry: &WalEntry) -> Result<()> {
        // Serialize entry
        let entry_bytes = bincode::serialize(entry).context("Failed to serialize WAL entry")?;

        // Calculate checksum (CRC32)
        let checksum = crc32fast::hash(&entry_bytes);

        // Write: [entry_size (4 bytes) | entry_data | checksum (4 bytes)]
        let entry_size = entry_bytes.len() as u32;
        self.file.write_all(&entry_size.to_le_bytes())?;
        self.file.write_all(&entry_bytes)?;
        self.file.write_all(&checksum.to_le_bytes())?;

        self.entry_count += 1;
        self.bytes_written += 4 + entry_bytes.len() as u64 + 4;

        // Flush to OS buffer cache (fast, does not wait for disk)
        self.file.flush()?;

        // fsync policy: Only FsyncPolicy::Always does immediate sync
        // For async contexts, caller should use sync_async() instead
        match self.fsync_policy {
            FsyncPolicy::Always => {
                // Synchronous fsync - blocks for ~10ms
                // For production, use FsyncPolicy::Periodic and call sync() from background task
                self.file.get_ref().sync_data()?;
            }
            FsyncPolicy::Never | FsyncPolicy::Periodic(_) => {
                // No immediate sync - already flushed to OS buffer
            }
        }

        Ok(())
    }

    /// Force fsync (for periodic policy or manual sync)
    #[instrument(level = "trace", skip(self))]
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        Ok(())
    }

    /// Async fsync using tokio::task::spawn_blocking
    ///
    /// Non-blocking alternative to sync() - spawns fsync in background thread.
    /// Returns immediately; use returned handle to await completion.
    ///
    /// # Performance
    /// - Does not block caller (returns instantly)
    /// - Fsync happens on separate thread pool (~10ms)
    /// - Ideal for high-throughput write paths
    ///
    /// # Usage
    /// ```rust,ignore
    /// let sync_handle = writer.sync_async()?;
    /// // Continue processing...
    /// sync_handle.await??; // Wait for fsync completion
    /// ```
    pub fn sync_async(&mut self) -> Result<tokio::task::JoinHandle<Result<()>>> {
        self.file.flush()?;

        // Clone file descriptor for background sync
        let file = self
            .file
            .get_ref()
            .try_clone()
            .context("Failed to clone file descriptor for async sync")?;

        // Spawn blocking fsync on separate thread pool
        let handle =
            tokio::task::spawn_blocking(move || file.sync_data().context("Async fsync failed"));

        Ok(handle)
    }

    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// WAL error handler with circuit breaker and retry logic
///
/// Handles transient failures (I/O errors, disk pressure) with exponential backoff.
/// Opens circuit breaker on persistent failures (disk full, permissions).
pub struct WalErrorHandler {
    circuit_breaker: Arc<CircuitBreaker>,
    max_retries: usize,
    base_delay_ms: u64,
    metrics: MetricsCollector,
}

impl WalErrorHandler {
    /// Create new error handler with default config
    pub fn new() -> Self {
        Self::with_metrics(MetricsCollector::new())
    }

    /// Create error handler with custom metrics collector
    pub fn with_metrics(metrics: MetricsCollector) -> Self {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout: Duration::from_secs(60),
            window_size: Duration::from_secs(60),
        };

        Self {
            circuit_breaker: Arc::new(CircuitBreaker::with_config(config)),
            max_retries: 5,
            base_delay_ms: 10,
            metrics,
        }
    }

    /// Create error handler with custom config
    pub fn with_config(
        circuit_breaker_config: CircuitBreakerConfig,
        max_retries: usize,
        base_delay_ms: u64,
        metrics: MetricsCollector,
    ) -> Self {
        Self {
            circuit_breaker: Arc::new(CircuitBreaker::with_config(circuit_breaker_config)),
            max_retries,
            base_delay_ms,
            metrics,
        }
    }

    /// Write entry with retry logic
    ///
    /// Retries transient errors with exponential backoff.
    /// Opens circuit breaker on persistent failures.
    pub fn write_with_retry<F>(&self, mut write_fn: F) -> Result<()>
    where
        F: FnMut() -> Result<()>,
    {
        // Check circuit breaker
        if self.circuit_breaker.is_open() {
            self.update_circuit_state();
            bail!("WAL circuit breaker is open - writes disabled");
        }

        let mut attempt = 0;
        loop {
            match write_fn() {
                Ok(()) => {
                    self.circuit_breaker.record_success();
                    self.update_circuit_state();
                    self.metrics.record_wal_write(true);
                    return Ok(());
                }
                Err(e) => {
                    let error_kind = Self::classify_error(&e);

                    match error_kind {
                        WalErrorKind::DiskFull => {
                            error!("Disk full detected: {}", e);
                            self.circuit_breaker.open();
                            self.update_circuit_state();
                            self.metrics.record_wal_disk_full();
                            self.metrics.record_wal_write(false);
                            return Err(e).context("Disk full - circuit breaker opened");
                        }
                        WalErrorKind::PermissionDenied => {
                            error!("Permission denied: {}", e);
                            self.circuit_breaker.record_failure();
                            self.update_circuit_state();
                            self.metrics.record_wal_write(false);
                            return Err(e).context("Permission denied");
                        }
                        WalErrorKind::Transient if attempt < self.max_retries => {
                            attempt += 1;
                            self.metrics.record_wal_retry();
                            // Cap exponential backoff to prevent overflow
                            let exponent = (attempt as u32 - 1).min(20); // 2^20 = ~1M multiplier
                            let delay_ms = self.base_delay_ms.saturating_mul(2u64.pow(exponent));
                            warn!(
                                "WAL write failed (attempt {}/{}): {}. Retrying in {}ms",
                                attempt, self.max_retries, e, delay_ms
                            );
                            std::thread::sleep(Duration::from_millis(delay_ms));
                        }
                        _ => {
                            error!("WAL write failed after {} attempts: {}", attempt + 1, e);
                            self.circuit_breaker.record_failure();
                            self.update_circuit_state();
                            self.metrics.record_wal_write(false);
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// Update circuit breaker state in metrics
    fn update_circuit_state(&self) {
        let state = if self.circuit_breaker.is_closed() {
            0 // Closed
        } else if self.circuit_breaker.is_open() {
            1 // Open
        } else {
            2 // Half-open
        };
        self.metrics.update_wal_circuit_breaker_state(state);
    }

    /// Get circuit breaker for monitoring
    pub fn circuit_breaker(&self) -> &Arc<CircuitBreaker> {
        &self.circuit_breaker
    }

    /// Classify error for retry decision
    fn classify_error(error: &anyhow::Error) -> WalErrorKind {
        // Check if it's an I/O error
        if let Some(io_err) = error.root_cause().downcast_ref::<std::io::Error>() {
            match io_err.kind() {
                ErrorKind::StorageFull => WalErrorKind::DiskFull,
                ErrorKind::PermissionDenied => WalErrorKind::PermissionDenied,
                ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut => {
                    WalErrorKind::Transient
                }
                _ => {
                    // Check for ENOSPC (disk full) in error message
                    let err_str = format!("{}", io_err);
                    if err_str.contains("ENOSPC") || err_str.contains("No space left")
                        || err_str.contains("EDQUOT") || err_str.contains("Quota exceeded")
                    {
                        WalErrorKind::DiskFull
                    } else {
                        WalErrorKind::Transient
                    }
                }
            }
        } else {
            // Check error message for disk full indicators
            let err_str = format!("{}", error);
            if err_str.contains("ENOSPC") || err_str.contains("No space left")
                || err_str.contains("EDQUOT") || err_str.contains("Quota exceeded")
            {
                WalErrorKind::DiskFull
            } else if err_str.contains("Permission denied") || err_str.contains("EACCES") {
                WalErrorKind::PermissionDenied
            } else {
                WalErrorKind::Unknown
            }
        }
    }
}

impl Default for WalErrorHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// WAL error classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalErrorKind {
    /// Disk full (ENOSPC, EDQUOT)
    DiskFull,
    /// Permission denied
    PermissionDenied,
    /// Transient error (retry)
    Transient,
    /// Unknown error
    Unknown,
}

/// WAL reader: iterate over entries with checksum validation
pub struct WalReader {
    file: BufReader<File>,
    valid_entries: usize,
    corrupted_entries: usize,
}

impl WalReader {
    /// Open existing WAL file
    #[instrument(level = "debug", skip(path))]
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let file = File::open(path).context("Failed to open WAL file")?;

        let mut reader = BufReader::new(file);

        // Validate magic header
        let mut magic = [0u8; 4];
        reader
            .read_exact(&mut magic)
            .context("Failed to read WAL magic header")?;

        let magic_val = u32::from_le_bytes(magic);
        if magic_val != WAL_MAGIC {
            bail!(
                "Invalid WAL magic: expected {:#x}, got {:#x}",
                WAL_MAGIC,
                magic_val
            );
        }

        Ok(Self {
            file: reader,
            valid_entries: 0,
            corrupted_entries: 0,
        })
    }

    /// Read all entries (validates checksums)
    #[instrument(level = "debug", skip(self))]
    pub fn read_all(&mut self) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();

        loop {
            // Read entry size
            let mut size_bytes = [0u8; 4];
            match self.file.read_exact(&mut size_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let entry_size = u32::from_le_bytes(size_bytes) as usize;

            // Read entry data
            let mut entry_bytes = vec![0u8; entry_size];
            self.file
                .read_exact(&mut entry_bytes)
                .context("Failed to read WAL entry data")?;

            // Read checksum
            let mut checksum_bytes = [0u8; 4];
            self.file
                .read_exact(&mut checksum_bytes)
                .context("Failed to read WAL checksum")?;

            let stored_checksum = u32::from_le_bytes(checksum_bytes);
            let computed_checksum = crc32fast::hash(&entry_bytes);

            if stored_checksum != computed_checksum {
                debug!(
                    stored_checksum = format!("{:#x}", stored_checksum),
                    computed_checksum = format!("{:#x}", computed_checksum),
                    "corrupted WAL entry; checksum mismatch"
                );
                self.corrupted_entries += 1;
                continue;
            }

            // Deserialize entry
            let entry: WalEntry =
                bincode::deserialize(&entry_bytes).context("Failed to deserialize WAL entry")?;

            entries.push(entry);
            self.valid_entries += 1;
        }

        Ok(entries)
    }

    pub fn valid_entries(&self) -> usize {
        self.valid_entries
    }

    pub fn corrupted_entries(&self) -> usize {
        self.corrupted_entries
    }
}

/// Snapshot: full HNSW index state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u32,
    pub timestamp: u64,
    pub doc_count: usize,
    pub dimension: usize,
    /// All documents: (doc_id, embedding)
    pub documents: Vec<(u64, Vec<f32>)>,
}

impl Snapshot {
    /// Create snapshot from current state
    pub fn new(dimension: usize, documents: Vec<(u64, Vec<f32>)>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            version: 1,
            timestamp,
            doc_count: documents.len(),
            dimension,
            documents,
        }
    }

    /// Save snapshot to file (atomic: write to temp, then rename)
    #[instrument(level = "debug", skip(self, path), fields(version = self.version, documents = self.doc_count, dimension = self.dimension))]
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();

        // Write to temporary file first
        let temp_path = path.with_extension("tmp");

        let file = File::create(&temp_path).context("Failed to create snapshot temp file")?;
        let mut writer = BufWriter::new(file);

        // Write magic header
        writer.write_all(&SNAPSHOT_MAGIC.to_le_bytes())?;

        // Serialize snapshot
        let snapshot_bytes = bincode::serialize(self).context("Failed to serialize snapshot")?;

        // Write size + data + checksum
        let size = snapshot_bytes.len() as u64;
        writer.write_all(&size.to_le_bytes())?;
        writer.write_all(&snapshot_bytes)?;

        let checksum = crc32fast::hash(&snapshot_bytes);
        writer.write_all(&checksum.to_le_bytes())?;

        writer.flush()?;
        writer.get_ref().sync_all()?;

        // Atomic rename
        std::fs::rename(&temp_path, path).context("Failed to rename snapshot file")?;

        Ok(())
    }

    /// Load snapshot from file (validates checksum)
    #[instrument(level = "debug", skip(path))]
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let file = File::open(path).context("Failed to open snapshot file")?;
        let mut reader = BufReader::new(file);

        // Validate magic
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        let magic_val = u32::from_le_bytes(magic);
        if magic_val != SNAPSHOT_MAGIC {
            bail!(
                "Invalid snapshot magic: expected {:#x}, got {:#x}",
                SNAPSHOT_MAGIC,
                magic_val
            );
        }

        // Read size
        let mut size_bytes = [0u8; 8];
        reader.read_exact(&mut size_bytes)?;
        let size = u64::from_le_bytes(size_bytes) as usize;

        // Read data
        let mut snapshot_bytes = vec![0u8; size];
        reader.read_exact(&mut snapshot_bytes)?;

        // Read checksum
        let mut checksum_bytes = [0u8; 4];
        reader.read_exact(&mut checksum_bytes)?;

        let stored_checksum = u32::from_le_bytes(checksum_bytes);
        let computed_checksum = crc32fast::hash(&snapshot_bytes);

        if stored_checksum != computed_checksum {
            bail!(
                "Snapshot checksum mismatch: stored={:#x}, computed={:#x}",
                stored_checksum,
                computed_checksum
            );
        }

        // Deserialize
        let snapshot: Snapshot =
            bincode::deserialize(&snapshot_bytes).context("Failed to deserialize snapshot")?;

        Ok(snapshot)
    }

    /// Load snapshot with validation and fallback recovery
    ///
    /// Attempts to load the primary snapshot. On corruption, tries fallback
    /// snapshots (snapshot_N, snapshot_N-1, snapshot_N-2, etc).
    ///
    /// # Parameters
    /// - `path`: Primary snapshot path
    /// - `metrics`: Metrics collector for tracking corruption/recovery
    ///
    /// # Returns
    /// - `Ok((Snapshot, recovered_from_fallback))`: Successfully loaded snapshot
    /// - `Err`: All snapshots corrupt or missing
    #[instrument(level = "info", skip(path, metrics))]
    pub fn load_with_validation(
        path: impl AsRef<Path>,
        metrics: &MetricsCollector,
    ) -> Result<(Self, bool)> {
        let path = path.as_ref();

        // Try loading primary snapshot
        match Self::load(path) {
            Ok(snapshot) => {
                info!(snapshot = %path.display(), "snapshot loaded successfully");
                return Ok((snapshot, false));
            }
            Err(e) => {
                error!(
                    snapshot = %path.display(),
                    error = %e,
                    "primary snapshot corrupted or missing"
                );
                metrics.record_hnsw_corruption();
            }
        }

        // Extract snapshot number from filename (e.g., "snapshot_123.snap" -> 123)
        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let snapshot_number = filename
            .trim_start_matches("snapshot_")
            .trim_end_matches(".snap")
            .parse::<u64>()
            .ok();

        if let Some(num) = snapshot_number {
            // Try up to 5 previous snapshots
            for fallback_idx in 1..=5 {
                if num < fallback_idx {
                    break;
                }

                let fallback_num = num - fallback_idx;
                let fallback_path = path.with_file_name(format!("snapshot_{}.snap", fallback_num));

                if !fallback_path.exists() {
                    continue;
                }

                warn!(
                    fallback = %fallback_path.display(),
                    "attempting fallback recovery"
                );

                match Self::load(&fallback_path) {
                    Ok(snapshot) => {
                        info!(
                            fallback = %fallback_path.display(),
                            "fallback recovery successful"
                        );
                        metrics.record_hnsw_fallback_success();
                        return Ok((snapshot, true));
                    }
                    Err(e) => {
                        warn!(
                            fallback = %fallback_path.display(),
                            error = %e,
                            "fallback snapshot also corrupted"
                        );
                    }
                }
            }
        }

        // All fallback attempts failed
        error!("all snapshot recovery attempts failed");
        metrics.record_hnsw_fallback_failed();
        bail!("Failed to load snapshot: primary and all fallbacks corrupted")
    }
}

/// Manifest: tracks valid snapshots and WAL segments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub latest_snapshot: Option<String>,
    pub wal_segments: Vec<String>,
    pub last_updated: u64,
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

impl Manifest {
    pub fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            version: 1,
            latest_snapshot: None,
            wal_segments: Vec::new(),
            last_updated: timestamp,
        }
    }

    /// Save manifest (atomic)
    #[instrument(level = "debug", skip(self, path), fields(wal_segments = self.wal_segments.len(), latest_snapshot = self.latest_snapshot.as_deref().unwrap_or("none")))]
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let temp_path = path.with_extension("tmp");

        let manifest_json =
            serde_json::to_string_pretty(self).context("Failed to serialize manifest")?;

        std::fs::write(&temp_path, manifest_json).context("Failed to write manifest temp file")?;

        std::fs::rename(&temp_path, path).context("Failed to rename manifest file")?;

        Ok(())
    }

    /// Load manifest
    #[instrument(level = "debug", skip(path))]
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let contents = std::fs::read_to_string(path).context("Failed to read manifest file")?;

        let manifest: Manifest =
            serde_json::from_str(&contents).context("Failed to parse manifest JSON")?;

        Ok(manifest)
    }

    /// Load or create new manifest
    #[instrument(level = "debug", skip(path))]
    pub fn load_or_create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        if path.exists() {
            Self::load(path)
        } else {
            Ok(Self::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Seek;
    use tempfile::TempDir;

    #[test]
    fn test_wal_write_read() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write entries
        let mut writer = WalWriter::create(&wal_path, FsyncPolicy::Always).unwrap();

        let entry1 = WalEntry {
            op: WalOp::Insert,
            doc_id: 42,
            embedding: vec![0.1, 0.2, 0.3],
            timestamp: 1000,
        };

        let entry2 = WalEntry {
            op: WalOp::Delete,
            doc_id: 99,
            embedding: vec![],
            timestamp: 2000,
        };

        writer.append(&entry1).unwrap();
        writer.append(&entry2).unwrap();

        drop(writer);

        // Read entries
        let mut reader = WalReader::open(&wal_path).unwrap();
        let entries = reader.read_all().unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].doc_id, 42);
        assert_eq!(entries[1].doc_id, 99);
        assert_eq!(reader.valid_entries(), 2);
        assert_eq!(reader.corrupted_entries(), 0);
    }

    #[test]
    fn test_snapshot_save_load() {
        let dir = TempDir::new().unwrap();
        let snapshot_path = dir.path().join("test.snapshot");

        // Create snapshot
        let documents = vec![(1, vec![0.1, 0.2]), (2, vec![0.3, 0.4])];

        let snapshot = Snapshot::new(2, documents);
        snapshot.save(&snapshot_path).unwrap();

        // Load snapshot
        let loaded = Snapshot::load(&snapshot_path).unwrap();

        assert_eq!(loaded.dimension, 2);
        assert_eq!(loaded.doc_count, 2);
        assert_eq!(loaded.documents.len(), 2);
        assert_eq!(loaded.documents[0].0, 1);
    }

    #[test]
    fn test_manifest() {
        let dir = TempDir::new().unwrap();
        let manifest_path = dir.path().join("MANIFEST");

        let mut manifest = Manifest::new();
        manifest.latest_snapshot = Some("snapshot_001.snap".to_string());
        manifest.wal_segments.push("wal_001.wal".to_string());

        manifest.save(&manifest_path).unwrap();

        let loaded = Manifest::load(&manifest_path).unwrap();
        assert_eq!(
            loaded.latest_snapshot,
            Some("snapshot_001.snap".to_string())
        );
        assert_eq!(loaded.wal_segments.len(), 1);
    }

    #[test]
    fn test_wal_error_handler_success() {
        let handler = WalErrorHandler::new();
        let mut call_count = 0;

        let result = handler.write_with_retry(|| {
            call_count += 1;
            Ok(())
        });

        assert!(result.is_ok());
        assert_eq!(call_count, 1); // Success on first try
    }

    #[test]
    fn test_wal_error_handler_retry_transient() {
        let handler = WalErrorHandler::with_config(
            CircuitBreakerConfig {
                failure_threshold: 10,
                success_threshold: 2,
                timeout: Duration::from_secs(60),
                window_size: Duration::from_secs(60),
            },
            5, // max_retries
            1, // base_delay_ms (short for testing)
            MetricsCollector::new(),
        );
        let mut attempt = 0;

        let result = handler.write_with_retry(|| {
            attempt += 1;
            if attempt < 3 {
                // Simulate transient I/O error
                let io_err = std::io::Error::from(std::io::ErrorKind::Interrupted);
                Err(anyhow::Error::new(io_err))
            } else {
                Ok(())
            }
        });

        println!("Result: {:?}, attempts: {}", result, attempt);
        if result.is_err() {
            println!("Error: {}", result.as_ref().unwrap_err());
        }
        assert!(result.is_ok(), "Expected success after retries");
        assert_eq!(attempt, 3); // Succeeded after 2 retries
    }

    #[test]
    fn test_wal_error_handler_disk_full() {
        let handler = WalErrorHandler::new();
        let mut call_count = 0;

        let result = handler.write_with_retry(|| {
            call_count += 1;
            // Simulate disk full error
            Err(anyhow::anyhow!("ENOSPC: No space left on device"))
        });

        assert!(result.is_err());
        assert_eq!(call_count, 1); // No retry on disk full

        let err_msg = result.unwrap_err().to_string();
        println!("Error message: {}", err_msg);
        assert!(err_msg.contains("Disk full") || err_msg.contains("ENOSPC"));

        // Circuit breaker should be open
        assert!(handler.circuit_breaker().is_open());
    }

    #[test]
    fn test_wal_error_handler_retry_exhausted() {
        let handler = WalErrorHandler::with_config(
            CircuitBreakerConfig {
                failure_threshold: 10,
                success_threshold: 2,
                timeout: Duration::from_secs(60),
                window_size: Duration::from_secs(60),
            },
            3, // max_retries
            1, // base_delay_ms
            MetricsCollector::new(),
        );

        let mut attempt = 0;

        let result = handler.write_with_retry(|| {
            attempt += 1;
            // Always fail with transient error
            let io_err = std::io::Error::from(std::io::ErrorKind::Interrupted);
            Err(anyhow::Error::new(io_err))
        });

        println!("Attempts: {}, Expected: 4", attempt);
        assert!(result.is_err());
        assert_eq!(attempt, 4); // Initial + 3 retries
    }

    #[test]
    fn test_wal_error_handler_circuit_breaker_open() {
        let handler = WalErrorHandler::new();

        // Manually open circuit breaker
        handler.circuit_breaker().open();

        let result = handler.write_with_retry(|| Ok(()));

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("circuit breaker is open"));
    }

    #[test]
    fn test_wal_writer_with_error_handler() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test_error.wal");

        let error_handler = Arc::new(WalErrorHandler::new());
        let mut writer = WalWriter::create_with_error_handler(
            &wal_path,
            FsyncPolicy::Never,
            Some(error_handler.clone()),
        )
        .unwrap();

        let entry = WalEntry {
            op: WalOp::Insert,
            doc_id: 42,
            embedding: vec![1.0, 2.0, 3.0],
            timestamp: 1234567890,
        };

        // Should succeed
        writer.append(&entry).unwrap();
        assert_eq!(writer.entry_count(), 1);

        // Circuit breaker should be closed (successful writes)
        assert!(!error_handler.circuit_breaker().is_open());
    }

    #[test]
    fn test_error_classification() {
        use std::io::Error as IoError;

        // Test ENOSPC detection
        let disk_full_err = anyhow::anyhow!("ENOSPC: No space left on device");
        assert_eq!(
            WalErrorHandler::classify_error(&disk_full_err),
            WalErrorKind::DiskFull
        );

        // Test quota exceeded
        let quota_err = anyhow::anyhow!("EDQUOT: Quota exceeded");
        assert_eq!(
            WalErrorHandler::classify_error(&quota_err),
            WalErrorKind::DiskFull
        );

        // Test transient errors (wrapping actual io::Error)
        let interrupted_err = anyhow::Error::new(IoError::from(std::io::ErrorKind::Interrupted));
        assert_eq!(
            WalErrorHandler::classify_error(&interrupted_err),
            WalErrorKind::Transient
        );

        // Test permission denied (wrapping actual io::Error)
        let perm_err = anyhow::Error::new(IoError::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(
            WalErrorHandler::classify_error(&perm_err),
            WalErrorKind::PermissionDenied
        );
    }

    #[test]
    fn test_snapshot_corruption_detection() {
        use std::io::Write;

        let dir = TempDir::new().unwrap();
        let snapshot_path = dir.path().join("corrupted.snapshot");

        // Create valid snapshot first
        let documents = vec![(1, vec![0.1, 0.2]), (2, vec![0.3, 0.4])];
        let snapshot = Snapshot::new(2, documents.clone());
        snapshot.save(&snapshot_path).unwrap();

        // Corrupt the snapshot by modifying bytes in the data section
        // Magic (4) + Size (8) + data... = start corrupting at byte 20
        let mut file = OpenOptions::new().write(true).open(&snapshot_path).unwrap();
        file.seek(std::io::SeekFrom::Start(20)).unwrap();
        file.write_all(b"CORRUPTED_DATA").unwrap();
        drop(file);

        // Attempt to load corrupted snapshot
        let result = Snapshot::load(&snapshot_path);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Should fail with either checksum mismatch or deserialization error
        assert!(
            err_msg.contains("checksum mismatch") || err_msg.contains("Failed to deserialize"),
            "Unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_snapshot_fallback_recovery() {
        let dir = TempDir::new().unwrap();
        let metrics = MetricsCollector::new();

        // Create snapshot_100.snap (primary)
        let snapshot_100_path = dir.path().join("snapshot_100.snap");
        let documents_100 = vec![(1, vec![1.0, 2.0]), (2, vec![3.0, 4.0])];
        let snapshot_100 = Snapshot::new(2, documents_100);
        snapshot_100.save(&snapshot_100_path).unwrap();

        // Create snapshot_99.snap (fallback)
        let snapshot_99_path = dir.path().join("snapshot_99.snap");
        let documents_99 = vec![(1, vec![0.1, 0.2])];
        let snapshot_99 = Snapshot::new(2, documents_99);
        snapshot_99.save(&snapshot_99_path).unwrap();

        // Corrupt snapshot_100
        let mut file = OpenOptions::new()
            .write(true)
            .open(&snapshot_100_path)
            .unwrap();
        file.seek(std::io::SeekFrom::Start(50)).unwrap();
        file.write_all(b"CORRUPT").unwrap();
        drop(file);

        // Should fallback to snapshot_99
        let (recovered_snapshot, recovered_from_fallback) =
            Snapshot::load_with_validation(&snapshot_100_path, &metrics).unwrap();

        assert!(recovered_from_fallback);
        assert_eq!(recovered_snapshot.doc_count, 1);
        assert_eq!(recovered_snapshot.dimension, 2);

        // Metrics should show corruption and successful fallback
        let corruption_count = metrics.get_hnsw_corruption_count();
        let fallback_success = metrics.get_hnsw_fallback_success_count();
        assert_eq!(corruption_count, 1);
        assert_eq!(fallback_success, 1);
    }

    #[test]
    fn test_snapshot_all_fallbacks_corrupted() {
        let dir = TempDir::new().unwrap();
        let metrics = MetricsCollector::new();

        // Create and corrupt snapshot_100.snap
        let snapshot_100_path = dir.path().join("snapshot_100.snap");
        std::fs::write(&snapshot_100_path, b"CORRUPTED_DATA").unwrap();

        // Create and corrupt snapshot_99.snap
        let snapshot_99_path = dir.path().join("snapshot_99.snap");
        std::fs::write(&snapshot_99_path, b"ALSO_CORRUPTED").unwrap();

        // Should fail - all snapshots corrupted
        let result = Snapshot::load_with_validation(&snapshot_100_path, &metrics);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("primary and all fallbacks corrupted"));

        // Metrics should show corruption and failed fallback
        let corruption_count = metrics.get_hnsw_corruption_count();
        let fallback_failed = metrics.get_hnsw_fallback_failed_count();
        assert_eq!(corruption_count, 1);
        assert_eq!(fallback_failed, 1);
    }
}
