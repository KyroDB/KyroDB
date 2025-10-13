//! Persistence layer for HNSW index (WAL + Snapshots)
//!
//! This module provides durability guarantees for the vector database:
//! - **WAL (Write-Ahead Log)**: All mutations logged before in-memory update
//! - **Snapshots**: Periodic full index serialization for fast recovery
//! - **Manifest**: Tracks valid snapshots and WAL segments
//!
//! # Durability Guarantees
//! - `fsync` policy configurable (always, periodic, never)
//! - Checksum validation on all reads (detect corruption)
//! - Atomic file operations (temp file + rename)
//! - Crash recovery via snapshot + WAL replay

use anyhow::{Context, Result, bail};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

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
pub struct WalWriter {
    file: BufWriter<File>,
    path: PathBuf,
    fsync_policy: FsyncPolicy,
    entry_count: usize,
    bytes_written: u64,
}

impl WalWriter {
    /// Create new WAL file
    pub fn create(path: impl AsRef<Path>, fsync_policy: FsyncPolicy) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
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
        })
    }
    
    /// Append entry to WAL
    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        // Serialize entry
        let entry_bytes = bincode::serialize(entry)
            .context("Failed to serialize WAL entry")?;
        
        // Calculate checksum (CRC32)
        let checksum = crc32fast::hash(&entry_bytes);
        
        // Write: [entry_size (4 bytes) | entry_data | checksum (4 bytes)]
        let entry_size = entry_bytes.len() as u32;
        self.file.write_all(&entry_size.to_le_bytes())?;
        self.file.write_all(&entry_bytes)?;
        self.file.write_all(&checksum.to_le_bytes())?;
        
        self.entry_count += 1;
        self.bytes_written += 4 + entry_bytes.len() as u64 + 4;
        
        // fsync if needed
        match self.fsync_policy {
            FsyncPolicy::Always => {
                self.file.flush()?;
                self.file.get_ref().sync_data()?;
            }
            FsyncPolicy::Never => {
                // Just flush to OS buffer
                self.file.flush()?;
            }
            FsyncPolicy::Periodic(_) => {
                // Caller responsible for periodic fsync
                self.file.flush()?;
            }
        }
        
        Ok(())
    }
    
    /// Force fsync (for periodic policy)
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        Ok(())
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

/// WAL reader: iterate over entries with checksum validation
pub struct WalReader {
    file: BufReader<File>,
    path: PathBuf,
    valid_entries: usize,
    corrupted_entries: usize,
}

impl WalReader {
    /// Open existing WAL file
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        let file = File::open(&path)
            .context("Failed to open WAL file")?;
        
        let mut reader = BufReader::new(file);
        
        // Validate magic header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)
            .context("Failed to read WAL magic header")?;
        
        let magic_val = u32::from_le_bytes(magic);
        if magic_val != WAL_MAGIC {
            bail!("Invalid WAL magic: expected {:#x}, got {:#x}", WAL_MAGIC, magic_val);
        }
        
        Ok(Self {
            file: reader,
            path,
            valid_entries: 0,
            corrupted_entries: 0,
        })
    }
    
    /// Read all entries (validates checksums)
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
            self.file.read_exact(&mut entry_bytes)
                .context("Failed to read WAL entry data")?;
            
            // Read checksum
            let mut checksum_bytes = [0u8; 4];
            self.file.read_exact(&mut checksum_bytes)
                .context("Failed to read WAL checksum")?;
            
            let stored_checksum = u32::from_le_bytes(checksum_bytes);
            let computed_checksum = crc32fast::hash(&entry_bytes);
            
            if stored_checksum != computed_checksum {
                eprintln!(
                    "WARNING: Corrupted WAL entry (checksum mismatch: stored={:#x}, computed={:#x})",
                    stored_checksum, computed_checksum
                );
                self.corrupted_entries += 1;
                continue;
            }
            
            // Deserialize entry
            let entry: WalEntry = bincode::deserialize(&entry_bytes)
                .context("Failed to deserialize WAL entry")?;
            
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
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        
        // Write to temporary file first
        let temp_path = path.with_extension("tmp");
        
        let file = File::create(&temp_path)
            .context("Failed to create snapshot temp file")?;
        let mut writer = BufWriter::new(file);
        
        // Write magic header
        writer.write_all(&SNAPSHOT_MAGIC.to_le_bytes())?;
        
        // Serialize snapshot
        let snapshot_bytes = bincode::serialize(self)
            .context("Failed to serialize snapshot")?;
        
        // Write size + data + checksum
        let size = snapshot_bytes.len() as u64;
        writer.write_all(&size.to_le_bytes())?;
        writer.write_all(&snapshot_bytes)?;
        
        let checksum = crc32fast::hash(&snapshot_bytes);
        writer.write_all(&checksum.to_le_bytes())?;
        
        writer.flush()?;
        writer.get_ref().sync_all()?;
        
        // Atomic rename
        std::fs::rename(&temp_path, path)
            .context("Failed to rename snapshot file")?;
        
        Ok(())
    }
    
    /// Load snapshot from file (validates checksum)
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        
        let file = File::open(path)
            .context("Failed to open snapshot file")?;
        let mut reader = BufReader::new(file);
        
        // Validate magic
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        
        let magic_val = u32::from_le_bytes(magic);
        if magic_val != SNAPSHOT_MAGIC {
            bail!("Invalid snapshot magic: expected {:#x}, got {:#x}", SNAPSHOT_MAGIC, magic_val);
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
        let snapshot: Snapshot = bincode::deserialize(&snapshot_bytes)
            .context("Failed to deserialize snapshot")?;
        
        Ok(snapshot)
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
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let temp_path = path.with_extension("tmp");
        
        let manifest_json = serde_json::to_string_pretty(self)
            .context("Failed to serialize manifest")?;
        
        std::fs::write(&temp_path, manifest_json)
            .context("Failed to write manifest temp file")?;
        
        std::fs::rename(&temp_path, path)
            .context("Failed to rename manifest file")?;
        
        Ok(())
    }
    
    /// Load manifest
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .context("Failed to read manifest file")?;
        
        let manifest: Manifest = serde_json::from_str(&contents)
            .context("Failed to parse manifest JSON")?;
        
        Ok(manifest)
    }
    
    /// Load or create new manifest
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
        let documents = vec![
            (1, vec![0.1, 0.2]),
            (2, vec![0.3, 0.4]),
        ];
        
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
        assert_eq!(loaded.latest_snapshot, Some("snapshot_001.snap".to_string()));
        assert_eq!(loaded.wal_segments.len(), 1);
    }
}
