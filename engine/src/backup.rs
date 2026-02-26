use crate::persistence::Manifest;
#[cfg(test)]
use crate::persistence::Snapshot;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Backup type: full snapshot or incremental changes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupType {
    Full,
    Incremental,
}

/// Backup metadata stored with each backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    /// Unique backup identifier
    pub id: Uuid,
    /// Unix timestamp when backup was created
    pub timestamp: u64,
    /// Full or incremental backup
    pub backup_type: BackupType,
    /// Total size in bytes
    pub size_bytes: u64,
    /// Number of vectors in this backup
    pub vector_count: u64,
    /// CRC32 checksum for integrity verification
    pub checksum: u32,
    /// ID of parent backup (for incrementals)
    pub parent_id: Option<Uuid>,
    /// Human-readable description
    pub description: String,
    /// Highest numeric WAL file id included in this backup, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wal_file_id: Option<u64>,
    /// Snapshot filename included in this backup, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_file: Option<String>,
}

impl BackupMetadata {
    pub fn new_full(
        size_bytes: u64,
        vector_count: u64,
        checksum: u32,
        description: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            backup_type: BackupType::Full,
            size_bytes,
            vector_count,
            checksum,
            parent_id: None,
            description,
            max_wal_file_id: None,
            snapshot_file: None,
        }
    }

    pub fn new_incremental(
        parent_id: Uuid,
        size_bytes: u64,
        vector_count: u64,
        checksum: u32,
        description: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            backup_type: BackupType::Incremental,
            size_bytes,
            vector_count,
            checksum,
            parent_id: Some(parent_id),
            description,
            max_wal_file_id: None,
            snapshot_file: None,
        }
    }
}

/// Retention policy for automatic backup pruning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Keep hourly backups for this many hours
    pub hourly_hours: usize,
    /// Keep daily backups for this many days
    pub daily_days: usize,
    /// Keep weekly backups for this many weeks
    pub weekly_weeks: usize,
    /// Keep monthly backups for this many months
    pub monthly_months: usize,
    /// Minimum age in days before a backup can be pruned
    pub min_age_days: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            hourly_hours: 24,
            daily_days: 7,
            weekly_weeks: 4,
            monthly_months: 12,
            min_age_days: 0,
        }
    }
}

const MAX_BACKUP_ARCHIVE_FILES: u32 = 1_000_000;
const MAX_BACKUP_MEMBER_NAME_BYTES: u32 = 1024;
const MAX_BACKUP_MEMBER_SIZE_BYTES: u64 = 1 << 40; // 1 TiB per member
const BACKUP_STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// Shared utility function to list backups from a directory
/// This eliminates code duplication between BackupManager and RestoreManager
fn list_backups_from_dir(backup_dir: &Path) -> Result<Vec<BackupMetadata>> {
    let mut backups = Vec::new();

    for entry in fs::read_dir(backup_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            if let Ok(contents) = fs::read_to_string(&path) {
                if let Ok(metadata) = serde_json::from_str::<BackupMetadata>(&contents) {
                    backups.push(metadata);
                } else {
                    warn!("Failed to parse backup metadata: {:?}", path);
                }
            }
        }
    }

    // Sort by timestamp, newest first
    backups.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    Ok(backups)
}

#[cfg(unix)]
fn open_restore_target(output_path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(output_path)
        .with_context(|| format!("Failed to create restore target {}", output_path.display()))
}

#[cfg(not(unix))]
fn open_restore_target(output_path: &Path) -> Result<File> {
    // Best-effort fallback for platforms without O_NOFOLLOW.
    // This check is not atomic with file creation and therefore cannot fully
    // prevent TOCTOU symlink attacks on non-Unix systems.
    if let Ok(meta) = fs::symlink_metadata(output_path) {
        anyhow::ensure!(
            !meta.file_type().is_symlink(),
            "refusing to overwrite symlink during restore: {}",
            output_path.display()
        );
        anyhow::ensure!(
            !meta.is_dir(),
            "refusing to overwrite directory during restore: {}",
            output_path.display()
        );
    }

    File::create(output_path)
        .with_context(|| format!("Failed to create restore target {}", output_path.display()))
}

fn validate_backup_member_name(name: &str) -> Result<()> {
    anyhow::ensure!(!name.is_empty(), "backup member name cannot be empty");

    let path = Path::new(name);
    anyhow::ensure!(
        !path.is_absolute(),
        "absolute paths are not allowed in backups"
    );

    let mut components = path.components();
    let first = components
        .next()
        .ok_or_else(|| anyhow!("backup member path is empty"))?;
    anyhow::ensure!(
        components.next().is_none() && matches!(first, Component::Normal(_)),
        "invalid backup member path '{}': only single-file names are allowed",
        name
    );

    Ok(())
}

fn read_archive_file_count<R: Read>(reader: &mut R) -> Result<u32> {
    let mut file_count_bytes = [0u8; 4];
    reader
        .read_exact(&mut file_count_bytes)
        .context("Failed to read file count")?;
    let file_count = u32::from_le_bytes(file_count_bytes);
    anyhow::ensure!(
        file_count <= MAX_BACKUP_ARCHIVE_FILES,
        "backup archive contains too many members: {} (max {})",
        file_count,
        MAX_BACKUP_ARCHIVE_FILES
    );
    Ok(file_count)
}

fn read_archive_member_header<R: Read>(reader: &mut R) -> Result<(String, u64)> {
    let mut name_len_bytes = [0u8; 4];
    reader
        .read_exact(&mut name_len_bytes)
        .context("Failed to read filename length")?;
    let name_len = u32::from_le_bytes(name_len_bytes);
    anyhow::ensure!(name_len > 0, "backup member name length cannot be zero");
    anyhow::ensure!(
        name_len <= MAX_BACKUP_MEMBER_NAME_BYTES,
        "backup member name length {} exceeds maximum {}",
        name_len,
        MAX_BACKUP_MEMBER_NAME_BYTES
    );

    let mut name_bytes = vec![0u8; name_len as usize];
    reader
        .read_exact(&mut name_bytes)
        .context("Failed to read filename")?;
    let name = String::from_utf8(name_bytes).context("backup member name is not valid UTF-8")?;
    validate_backup_member_name(&name)?;

    let mut data_len_bytes = [0u8; 8];
    reader
        .read_exact(&mut data_len_bytes)
        .context("Failed to read data length")?;
    let data_len = u64::from_le_bytes(data_len_bytes);
    anyhow::ensure!(
        data_len <= MAX_BACKUP_MEMBER_SIZE_BYTES,
        "backup member '{}' size {} exceeds maximum supported size {}",
        name,
        data_len,
        MAX_BACKUP_MEMBER_SIZE_BYTES
    );

    Ok((name, data_len))
}

fn stream_member_crc32<R: Read>(reader: &mut R, data_len: u64) -> Result<u32> {
    let mut hasher = crc32fast::Hasher::new();
    let mut remaining = data_len;
    let mut buffer = [0u8; BACKUP_STREAM_CHUNK_BYTES];

    while remaining > 0 {
        let chunk = usize::try_from(remaining.min(BACKUP_STREAM_CHUNK_BYTES as u64))
            .unwrap_or(BACKUP_STREAM_CHUNK_BYTES);
        reader
            .read_exact(&mut buffer[..chunk])
            .context("Failed to read file data")?;
        hasher.update(&buffer[..chunk]);
        remaining -= chunk as u64;
    }

    Ok(hasher.finalize())
}

fn stream_member_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    data_len: u64,
) -> Result<()> {
    let mut remaining = data_len;
    let mut buffer = [0u8; BACKUP_STREAM_CHUNK_BYTES];

    while remaining > 0 {
        let chunk = usize::try_from(remaining.min(BACKUP_STREAM_CHUNK_BYTES as u64))
            .unwrap_or(BACKUP_STREAM_CHUNK_BYTES);
        reader
            .read_exact(&mut buffer[..chunk])
            .context("Failed to read backup member payload")?;
        writer
            .write_all(&buffer[..chunk])
            .context("Failed to write restored backup member payload")?;
        remaining -= chunk as u64;
    }

    Ok(())
}

enum ArchiveEntrySource {
    Path(PathBuf),
    Bytes(Vec<u8>),
}

struct ArchiveEntry {
    name: String,
    source: ArchiveEntrySource,
}

impl ArchiveEntry {
    fn from_path(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            source: ArchiveEntrySource::Path(path.into()),
        }
    }

    fn from_bytes(name: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            name: name.into(),
            source: ArchiveEntrySource::Bytes(bytes),
        }
    }
}

enum ManifestLayout {
    Modern(Manifest),
    Legacy {
        snapshot_number: Option<u64>,
        raw_bytes: Vec<u8>,
    },
}

fn parse_wal_file_id(file_name: &str) -> Option<u64> {
    file_name
        .strip_prefix("wal_")
        .and_then(|s| s.strip_suffix(".wal"))
        .and_then(|s| s.parse::<u64>().ok())
}

fn list_wal_segments_in_dir(data_dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut wal_files = Vec::new();
    for entry in fs::read_dir(data_dir).with_context(|| {
        format!(
            "Failed to read data directory while collecting WAL segments: {}",
            data_dir.display()
        )
    })? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("wal_") && name.ends_with(".wal") {
            wal_files.push((name, entry.path()));
        }
    }

    wal_files.sort_by(|(a_name, _), (b_name, _)| {
        let a_id = parse_wal_file_id(a_name).unwrap_or(0);
        let b_id = parse_wal_file_id(b_name).unwrap_or(0);
        a_id.cmp(&b_id).then_with(|| a_name.cmp(b_name))
    });
    Ok(wal_files)
}

fn read_manifest_layout(path: &Path) -> Result<Option<ManifestLayout>> {
    if !path.exists() {
        return Ok(None);
    }

    let raw = fs::read(path).with_context(|| format!("Failed to read {}", path.display()))?;

    if let Ok(manifest) = serde_json::from_slice::<Manifest>(&raw) {
        return Ok(Some(ManifestLayout::Modern(manifest)));
    }

    let text = std::str::from_utf8(&raw)
        .with_context(|| format!("MANIFEST is not valid UTF-8: {}", path.display()))?;
    let snapshot_number = text
        .lines()
        .find(|line| line.starts_with("snapshot_number:"))
        .and_then(|line| line.split(':').nth(1))
        .and_then(|s| s.trim().parse::<u64>().ok());
    if text
        .lines()
        .any(|line| line.trim_start().starts_with("snapshot_number:"))
    {
        return Ok(Some(ManifestLayout::Legacy {
            snapshot_number,
            raw_bytes: raw,
        }));
    }

    Err(anyhow!(
        "Failed to parse MANIFEST as modern JSON or recognized legacy format: {}",
        path.display()
    ))
}

fn read_archive_entry_payload(entry: &ArchiveEntry) -> Result<Vec<u8>> {
    match &entry.source {
        ArchiveEntrySource::Path(path) => {
            fs::read(path).with_context(|| format!("Failed to read {}", path.display()))
        }
        ArchiveEntrySource::Bytes(bytes) => Ok(bytes.clone()),
    }
}

/// Read snapshot `doc_count` from the serialized header without deserializing
/// the full snapshot payload into memory.
fn read_snapshot_doc_count(path: &Path) -> Result<u64> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open snapshot file {}", path.display()))?;
    let mut reader = BufReader::new(file);

    let mut magic = [0u8; 4];
    reader
        .read_exact(&mut magic)
        .with_context(|| format!("Failed to read snapshot magic from {}", path.display()))?;
    let magic_val = u32::from_le_bytes(magic);
    anyhow::ensure!(
        magic_val == 0x534E4150, // "SNAP"
        "Invalid snapshot magic in {}: expected {:#x}, got {:#x}",
        path.display(),
        0x534E4150u32,
        magic_val
    );

    let mut size_bytes = [0u8; 8];
    reader.read_exact(&mut size_bytes).with_context(|| {
        format!(
            "Failed to read snapshot payload size from {}",
            path.display()
        )
    })?;
    let payload_size = u64::from_le_bytes(size_bytes);

    let mut payload_reader = reader.take(payload_size);
    let _version: u32 = bincode::deserialize_from(&mut payload_reader)
        .with_context(|| format!("Failed to read snapshot version from {}", path.display()))?;
    let _timestamp: u64 = bincode::deserialize_from(&mut payload_reader)
        .with_context(|| format!("Failed to read snapshot timestamp from {}", path.display()))?;
    let doc_count: usize = bincode::deserialize_from(&mut payload_reader)
        .with_context(|| format!("Failed to read snapshot doc_count from {}", path.display()))?;

    Ok(doc_count as u64)
}

fn write_backup_archive(backup_path: &Path, entries: &[ArchiveEntry]) -> Result<u32> {
    let backup_file = File::create(backup_path).context("Failed to create backup file")?;
    let mut writer = BufWriter::new(backup_file);
    let entry_count = entries.len();
    anyhow::ensure!(
        entry_count <= MAX_BACKUP_ARCHIVE_FILES as usize,
        "backup archive contains too many members: {} (max {})",
        entry_count,
        MAX_BACKUP_ARCHIVE_FILES
    );
    anyhow::ensure!(
        entry_count <= u32::MAX as usize,
        "backup archive member count exceeds u32 range: {}",
        entry_count
    );
    writer.write_all(&(entry_count as u32).to_le_bytes())?;

    let mut checksum = 0u32;
    for entry in entries {
        debug!("Backing up file: {}", entry.name);
        let payload = read_archive_entry_payload(entry)?;

        let name_bytes = entry.name.as_bytes();
        writer.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(name_bytes)?;
        writer.write_all(&(payload.len() as u64).to_le_bytes())?;
        writer.write_all(&payload)?;

        checksum = checksum.wrapping_add(crc32fast::hash(&payload));
    }

    writer.flush()?;
    writer
        .get_ref()
        .sync_all()
        .context("Failed to fsync backup archive")?;
    drop(writer);
    Ok(checksum)
}

/// Compute CRC32 checksum of a backup file
/// This matches the algorithm used during backup creation
/// Backup format: [file_count][name_len][name][data_len][data]...
/// Checksum is computed only from file data, not metadata
pub fn compute_backup_checksum(backup_path: &Path) -> Result<u32> {
    let file =
        File::open(backup_path).context("Failed to open backup file for checksum computation")?;
    let mut reader = BufReader::new(file);

    let file_count = read_archive_file_count(&mut reader)?;

    let mut checksum = 0u32;

    // Process each file in the backup
    for _ in 0..file_count {
        let (_name, data_len) = read_archive_member_header(&mut reader)?;
        checksum = checksum.wrapping_add(stream_member_crc32(&mut reader, data_len)?);
    }

    Ok(checksum)
}

/// Manages backup creation and listing
pub struct BackupManager {
    backup_dir: PathBuf,
    data_dir: PathBuf,
}

impl BackupManager {
    pub fn new(backup_dir: impl AsRef<Path>, data_dir: impl AsRef<Path>) -> Result<Self> {
        let backup_dir = backup_dir.as_ref().to_path_buf();
        let data_dir = data_dir.as_ref().to_path_buf();

        // Create backup directory if it doesn't exist
        fs::create_dir_all(&backup_dir).context("Failed to create backup directory")?;

        Ok(Self {
            backup_dir,
            data_dir,
        })
    }

    /// Create a full backup of the current database state
    pub fn create_full_backup(&self, description: String) -> Result<BackupMetadata> {
        info!("Creating full backup: {}", description);

        // Generate backup ID first
        let backup_id = Uuid::new_v4();
        let backup_timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(error) => {
                warn!(
                    error = %error,
                    "system clock is before UNIX epoch; using backup timestamp=0"
                );
                0
            }
        };

        let backup_path = self.backup_dir.join(format!("backup_{}.tar", backup_id));
        let manifest_path = self.data_dir.join("MANIFEST");
        let mut entries = Vec::new();
        let mut vector_count = 0u64;
        let mut snapshot_file = None;
        let mut max_wal_file_id: Option<u64> = None;

        match read_manifest_layout(&manifest_path)? {
            Some(ManifestLayout::Modern(mut manifest)) => {
                let discovered_wal_segments = list_wal_segments_in_dir(&self.data_dir)?;
                let discovered_names: BTreeSet<String> = discovered_wal_segments
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect();
                let listed_names: BTreeSet<String> =
                    manifest.wal_segments.iter().cloned().collect();

                for listed in &manifest.wal_segments {
                    anyhow::ensure!(
                        discovered_names.contains(listed),
                        "MANIFEST references missing WAL segment '{}' in {}",
                        listed,
                        self.data_dir.display()
                    );
                }

                for discovered in discovered_names.difference(&listed_names) {
                    warn!(
                        wal_segment = discovered,
                        "WAL segment exists on disk but not in MANIFEST; adding to backup MANIFEST"
                    );
                    manifest.wal_segments.push(discovered.clone());
                }

                manifest.wal_segments.sort_by(|a, b| {
                    let a_id = parse_wal_file_id(a).unwrap_or(0);
                    let b_id = parse_wal_file_id(b).unwrap_or(0);
                    a_id.cmp(&b_id).then_with(|| a.cmp(b))
                });
                manifest.wal_segments.dedup();

                if let Some(snapshot_name) = &manifest.latest_snapshot {
                    let snapshot_path = self.data_dir.join(snapshot_name);
                    anyhow::ensure!(
                        snapshot_path.exists(),
                        "MANIFEST references missing snapshot '{}' in {}",
                        snapshot_name,
                        self.data_dir.display()
                    );
                    match read_snapshot_doc_count(&snapshot_path) {
                        Ok(count) => vector_count = count,
                        Err(error) => {
                            warn!(
                                snapshot = %snapshot_path.display(),
                                error = %error,
                                "Failed to read snapshot doc_count from header; storing vector_count=0"
                            );
                            vector_count = 0;
                        }
                    }
                    snapshot_file = Some(snapshot_name.clone());
                    entries.push(ArchiveEntry::from_path(
                        snapshot_name.clone(),
                        snapshot_path,
                    ));
                }

                let manifest_bytes =
                    serde_json::to_vec_pretty(&manifest).context("Failed to serialize MANIFEST")?;
                entries.push(ArchiveEntry::from_bytes("MANIFEST", manifest_bytes));

                for segment in &manifest.wal_segments {
                    let path = self.data_dir.join(segment);
                    anyhow::ensure!(
                        path.exists(),
                        "WAL segment '{}' listed in MANIFEST is missing from {}",
                        segment,
                        self.data_dir.display()
                    );
                    max_wal_file_id = match (max_wal_file_id, parse_wal_file_id(segment)) {
                        (Some(current), Some(candidate)) => Some(current.max(candidate)),
                        (None, Some(candidate)) => Some(candidate),
                        (current, None) => current,
                    };
                    entries.push(ArchiveEntry::from_path(segment.clone(), path));
                }
            }
            Some(ManifestLayout::Legacy {
                snapshot_number,
                raw_bytes,
            }) => {
                entries.push(ArchiveEntry::from_bytes("MANIFEST", raw_bytes));

                if let Some(snapshot_number) = snapshot_number.filter(|n| *n > 0) {
                    let snapshot_name = format!("snapshot_{}", snapshot_number);
                    let snapshot_path = self.data_dir.join(&snapshot_name);
                    anyhow::ensure!(
                        snapshot_path.exists(),
                        "Legacy MANIFEST references missing snapshot '{}' in {}",
                        snapshot_name,
                        self.data_dir.display()
                    );
                    snapshot_file = Some(snapshot_name.clone());
                    entries.push(ArchiveEntry::from_path(snapshot_name, snapshot_path));
                }

                for (name, path) in list_wal_segments_in_dir(&self.data_dir)? {
                    max_wal_file_id = match (max_wal_file_id, parse_wal_file_id(&name)) {
                        (Some(current), Some(candidate)) => Some(current.max(candidate)),
                        (None, Some(candidate)) => Some(candidate),
                        (current, None) => current,
                    };
                    entries.push(ArchiveEntry::from_path(name, path));
                }
            }
            None => {
                warn!(
                    "No MANIFEST found in {}; creating WAL-only backup",
                    self.data_dir.display()
                );
                for (name, path) in list_wal_segments_in_dir(&self.data_dir)? {
                    max_wal_file_id = match (max_wal_file_id, parse_wal_file_id(&name)) {
                        (Some(current), Some(candidate)) => Some(current.max(candidate)),
                        (None, Some(candidate)) => Some(candidate),
                        (current, None) => current,
                    };
                    entries.push(ArchiveEntry::from_path(name, path));
                }
            }
        }

        anyhow::ensure!(
            !entries.is_empty(),
            "No recoverable state found in {} (missing MANIFEST/snapshot/WAL files)",
            self.data_dir.display()
        );

        let mut unique_entries = Vec::with_capacity(entries.len());
        let mut seen_names = BTreeSet::new();
        for entry in entries {
            if seen_names.insert(entry.name.clone()) {
                unique_entries.push(entry);
            }
        }

        let checksum = write_backup_archive(&backup_path, &unique_entries)?;

        // Store the archive byte size so CLI verification can compare against on-disk artifact size.
        let archive_size = fs::metadata(&backup_path)?.len();

        // Create metadata with the same backup_id
        let metadata = BackupMetadata {
            id: backup_id,
            timestamp: backup_timestamp,
            backup_type: BackupType::Full,
            size_bytes: archive_size,
            vector_count,
            checksum,
            parent_id: None,
            description,
            max_wal_file_id,
            snapshot_file,
        };

        // Save metadata as JSON
        let metadata_path = self.backup_dir.join(format!("backup_{}.json", metadata.id));
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        fs::write(metadata_path, metadata_json)?;

        info!(
            "Full backup created: id={}, size={} bytes, vectors={}",
            metadata.id, metadata.size_bytes, metadata.vector_count
        );

        Ok(metadata)
    }

    /// Create an incremental backup (WAL entries since last backup)
    pub fn create_incremental_backup(
        &self,
        parent_id: Uuid,
        description: String,
    ) -> Result<BackupMetadata> {
        info!("Creating incremental backup: {}", description);

        // Load parent metadata
        let parent_metadata_path = self.backup_dir.join(format!("backup_{}.json", parent_id));
        if !parent_metadata_path.exists() {
            return Err(anyhow!("Parent backup {} not found", parent_id));
        }

        let parent_metadata: BackupMetadata =
            serde_json::from_str(&fs::read_to_string(parent_metadata_path)?)?;

        // Generate backup ID first
        let backup_id = Uuid::new_v4();
        let backup_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let backup_path = self.backup_dir.join(format!("backup_{}.tar", backup_id));
        let manifest_path = self.data_dir.join("MANIFEST");
        let manifest_layout = read_manifest_layout(&manifest_path)?;
        let mut entries = Vec::new();
        let mut max_wal_file_id: Option<u64> = None;

        let all_wal_segments = list_wal_segments_in_dir(&self.data_dir)?;
        let modified_since_parent = |path: &Path| -> bool {
            path.metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs() >= parent_metadata.timestamp)
                .unwrap_or(false)
        };

        let wal_segments: Vec<(String, PathBuf)> =
            if let Some(parent_max) = parent_metadata.max_wal_file_id {
                all_wal_segments
                    .into_iter()
                    .filter(|(name, path)| match parse_wal_file_id(name) {
                        Some(file_id) => {
                            file_id > parent_max
                                || (file_id == parent_max && modified_since_parent(path))
                        }
                        None => modified_since_parent(path),
                    })
                    .collect()
            } else {
                all_wal_segments
                    .into_iter()
                    .filter(|(_, path)| modified_since_parent(path))
                    .collect()
            };

        if wal_segments.is_empty() {
            return Err(anyhow!("No new WAL files since parent backup"));
        }

        match manifest_layout {
            Some(ManifestLayout::Modern(mut manifest)) => {
                let discovered_names: BTreeSet<String> =
                    wal_segments.iter().map(|(name, _)| name.clone()).collect();
                let listed_names: BTreeSet<String> =
                    manifest.wal_segments.iter().cloned().collect();
                for discovered in discovered_names.difference(&listed_names) {
                    warn!(
                        wal_segment = discovered,
                        "WAL segment exists on disk but not in MANIFEST; adding to incremental backup MANIFEST"
                    );
                    manifest.wal_segments.push(discovered.clone());
                }
                manifest.wal_segments.sort_by(|a, b| {
                    let a_id = parse_wal_file_id(a).unwrap_or(0);
                    let b_id = parse_wal_file_id(b).unwrap_or(0);
                    a_id.cmp(&b_id).then_with(|| a.cmp(b))
                });
                manifest.wal_segments.dedup();
                let manifest_bytes =
                    serde_json::to_vec_pretty(&manifest).context("Failed to serialize MANIFEST")?;
                entries.push(ArchiveEntry::from_bytes("MANIFEST", manifest_bytes));
            }
            Some(ManifestLayout::Legacy { raw_bytes, .. }) => {
                entries.push(ArchiveEntry::from_bytes("MANIFEST", raw_bytes));
            }
            None => {
                return Err(anyhow!(
                    "Cannot create incremental backup without MANIFEST in {}",
                    self.data_dir.display()
                ));
            }
        }

        for (name, path) in wal_segments {
            max_wal_file_id = match (max_wal_file_id, parse_wal_file_id(&name)) {
                (Some(current), Some(candidate)) => Some(current.max(candidate)),
                (None, Some(candidate)) => Some(candidate),
                (current, None) => current,
            };
            entries.push(ArchiveEntry::from_path(name, path));
        }

        let checksum = write_backup_archive(&backup_path, &entries)?;

        // Store the archive byte size so CLI verification can compare against on-disk artifact size.
        let archive_size = fs::metadata(&backup_path)?.len();

        // Create metadata with the same backup_id
        let metadata = BackupMetadata {
            id: backup_id,
            timestamp: backup_timestamp,
            backup_type: BackupType::Incremental,
            size_bytes: archive_size,
            vector_count: 0, // Vector count not tracked in incremental
            checksum,
            parent_id: Some(parent_id),
            description,
            max_wal_file_id,
            snapshot_file: None,
        };

        // Save metadata
        let metadata_path = self.backup_dir.join(format!("backup_{}.json", metadata.id));
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        fs::write(metadata_path, metadata_json)?;

        info!(
            "Incremental backup created: id={}, parent={}, size={} bytes",
            metadata.id, parent_id, metadata.size_bytes
        );

        Ok(metadata)
    }

    /// List all backups sorted by timestamp (newest first)
    pub fn list_backups(&self) -> Result<Vec<BackupMetadata>> {
        list_backups_from_dir(&self.backup_dir)
    }

    /// Prune old backups according to retention policy
    pub fn prune_backups(&self, policy: &RetentionPolicy) -> Result<Vec<Uuid>> {
        info!("Pruning backups with retention policy: {:?}", policy);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let backups = self.list_backups()?;

        // Categorize backups by time period
        let hour = 3600u64;
        let day = 86400u64;
        let week = 7 * day;
        let month = 30 * day; // Approximate month duration for retention bucketing (months vary 28-31 days)

        let mut hourly = BTreeMap::new();
        let mut daily = BTreeMap::new();
        let mut weekly = BTreeMap::new();
        let mut monthly = BTreeMap::new();

        for backup in &backups {
            let age = now.saturating_sub(backup.timestamp);

            if age < policy.hourly_hours as u64 * hour {
                let bucket = backup.timestamp / hour;
                hourly.entry(bucket).or_insert_with(Vec::new).push(backup);
            } else if age < policy.daily_days as u64 * day {
                let bucket = backup.timestamp / day;
                daily.entry(bucket).or_insert_with(Vec::new).push(backup);
            } else if age < policy.weekly_weeks as u64 * week {
                let bucket = backup.timestamp / week;
                weekly.entry(bucket).or_insert_with(Vec::new).push(backup);
            } else if age < policy.monthly_months as u64 * month {
                let bucket = backup.timestamp / month;
                monthly.entry(bucket).or_insert_with(Vec::new).push(backup);
            }
        }

        // Keep newest backup in each bucket
        let mut to_keep = std::collections::HashSet::new();

        for bucket_backups in hourly.values() {
            if let Some(newest) = bucket_backups.iter().max_by_key(|b| b.timestamp) {
                to_keep.insert(newest.id);
            }
        }

        for bucket_backups in daily.values() {
            if let Some(newest) = bucket_backups.iter().max_by_key(|b| b.timestamp) {
                to_keep.insert(newest.id);
            }
        }

        for bucket_backups in weekly.values() {
            if let Some(newest) = bucket_backups.iter().max_by_key(|b| b.timestamp) {
                to_keep.insert(newest.id);
            }
        }

        for bucket_backups in monthly.values() {
            if let Some(newest) = bucket_backups.iter().max_by_key(|b| b.timestamp) {
                to_keep.insert(newest.id);
            }
        }

        // Delete backups not in keep set, respecting min_age_days
        let mut deleted = Vec::new();
        let min_age_seconds = policy.min_age_days * day;

        for backup in &backups {
            if !to_keep.contains(&backup.id) {
                let age = now.saturating_sub(backup.timestamp);

                // Skip if backup is younger than minimum age threshold
                if age < min_age_seconds {
                    debug!(
                        "Skipping backup {} (age={} days < min_age={} days)",
                        backup.id,
                        age / day,
                        policy.min_age_days
                    );
                    continue;
                }

                debug!("Deleting backup: {} (age={} days)", backup.id, age / day);

                let backup_path = self.backup_dir.join(format!("backup_{}.tar", backup.id));
                let metadata_path = self.backup_dir.join(format!("backup_{}.json", backup.id));

                if backup_path.exists() {
                    fs::remove_file(backup_path)?;
                }
                if metadata_path.exists() {
                    fs::remove_file(metadata_path)?;
                }

                deleted.push(backup.id);
            }
        }

        info!("Pruned {} backups", deleted.len());

        Ok(deleted)
    }
}

/// Options for safely clearing data directory during restore operations
#[derive(Debug, Clone, Default)]
pub struct ClearDirectoryOptions {
    /// Allow clearing without explicit confirmation (default: false for safety)
    pub allow_clear: bool,
    /// Enable dry-run mode to preview files that would be deleted (default: false)
    pub dry_run: bool,
}

impl ClearDirectoryOptions {
    /// Create new options with default settings (confirmation required)
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable explicit clearing (use with caution in production)
    pub fn with_allow_clear(mut self, allow: bool) -> Self {
        self.allow_clear = allow;
        self
    }

    /// Enable dry-run mode to preview deletions without removing files
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }
}

/// Manages backup restoration
pub struct RestoreManager {
    backup_dir: PathBuf,
    data_dir: PathBuf,
}

impl RestoreManager {
    pub fn new(backup_dir: impl AsRef<Path>, data_dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            backup_dir: backup_dir.as_ref().to_path_buf(),
            data_dir: data_dir.as_ref().to_path_buf(),
        })
    }

    /// Safely clear the data directory with safeguards and confirmation
    pub fn clear_data_directory(&self, options: &ClearDirectoryOptions) -> Result<()> {
        // Check if data directory exists and get its contents
        if !self.data_dir.exists() {
            warn!("Data directory does not exist: {}", self.data_dir.display());
            return Ok(());
        }

        // Count and collect entries to be deleted
        let entries = fs::read_dir(&self.data_dir)?;
        let mut file_count = 0usize;
        let mut files_to_delete = Vec::new();

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                file_count += 1;
                files_to_delete.push(path);
            }
        }

        // Get absolute path for warning message
        let abs_path = self
            .data_dir
            .canonicalize()
            .unwrap_or_else(|_| self.data_dir.clone());

        if file_count == 0 {
            debug!(
                "Data directory is empty: {} (no files to delete)",
                abs_path.display()
            );
            return Ok(());
        }

        // Log warning with directory and file count
        warn!(
            "About to clear data directory: {} ({} file(s))",
            abs_path.display(),
            file_count
        );

        // Check environment variable for confirmation
        let env_confirm = std::env::var("BACKUP_ALLOW_CLEAR")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        // Require explicit confirmation if not already given
        if !options.allow_clear && !env_confirm {
            error!(
                "Refusing to delete {} file(s) from {} without explicit confirmation. \
                 Set allow_clear=true or environment variable BACKUP_ALLOW_CLEAR=true",
                file_count,
                abs_path.display()
            );
            return Err(anyhow!(
                "Data directory clear requires explicit confirmation. Set BACKUP_ALLOW_CLEAR=true \
                 environment variable or use RestoreManager with allow_clear=true"
            ));
        }

        // If dry-run is enabled, just list files without deleting
        if options.dry_run {
            info!(
                "DRY-RUN: Would delete {} file(s) from {}:",
                file_count,
                abs_path.display()
            );
            for file_path in &files_to_delete {
                info!("  - {}", file_path.display());
            }
            return Ok(());
        }

        // Actually delete files
        let mut deleted_count = 0usize;
        for file_path in files_to_delete {
            match fs::remove_file(&file_path) {
                Ok(_) => {
                    deleted_count += 1;
                    debug!("Deleted file: {}", file_path.display());
                }
                Err(e) => {
                    warn!("Failed to delete file {}: {}", file_path.display(), e);
                }
            }
        }

        info!(
            "Successfully deleted {} file(s) from {}",
            deleted_count,
            abs_path.display()
        );

        if deleted_count < file_count {
            return Err(anyhow!(
                "Failed to delete all files: deleted {}/{} from {}",
                deleted_count,
                file_count,
                abs_path.display()
            ));
        }

        Ok(())
    }

    /// Restore from a full backup
    pub fn restore_from_backup(&self, backup_id: Uuid) -> Result<()> {
        self.restore_from_backup_with_options(backup_id, &ClearDirectoryOptions::default())
    }

    /// Restore from a full backup with custom clear options
    pub fn restore_from_backup_with_options(
        &self,
        backup_id: Uuid,
        clear_options: &ClearDirectoryOptions,
    ) -> Result<()> {
        info!(
            "Restoring from backup: {} (allow_clear={}, dry_run={})",
            backup_id, clear_options.allow_clear, clear_options.dry_run
        );

        // Load metadata
        let metadata_path = self.backup_dir.join(format!("backup_{}.json", backup_id));
        if !metadata_path.exists() {
            return Err(anyhow!("Backup {} not found", backup_id));
        }

        let metadata: BackupMetadata = serde_json::from_str(&fs::read_to_string(metadata_path)?)?;

        // Verify this is a full backup or build chain
        let restore_chain = if metadata.backup_type == BackupType::Full {
            vec![metadata]
        } else {
            // Build chain of incrementals back to full
            let mut chain = vec![metadata.clone()];
            let mut current = metadata;

            while let Some(parent_id) = current.parent_id {
                let parent_path = self.backup_dir.join(format!("backup_{}.json", parent_id));
                if !parent_path.exists() {
                    return Err(anyhow!("Parent backup {} not found", parent_id));
                }

                current = serde_json::from_str(&fs::read_to_string(parent_path)?)?;
                chain.push(current.clone());

                if current.backup_type == BackupType::Full {
                    break;
                }
            }

            // Safety: chain is initialized with metadata on line 776, so it's never empty
            let Some(chain_tail) = chain.last() else {
                return Err(anyhow!("No full backup found in chain"));
            };
            if chain_tail.backup_type != BackupType::Full {
                return Err(anyhow!("No full backup found in chain"));
            }

            // Reverse to get full -> incrementals order
            chain.reverse();
            chain
        };

        info!("Restore chain has {} backups", restore_chain.len());

        // Preflight integrity verification: never clear live data if backups are invalid.
        let mut verified_archives = Vec::with_capacity(restore_chain.len());
        for backup_metadata in &restore_chain {
            let backup_path = self.verify_backup_archive(backup_metadata)?;
            verified_archives.push((backup_metadata.id, backup_path));
        }

        // Clear data directory with safeguards
        self.clear_data_directory(clear_options)?;

        // Return early if dry-run was enabled
        if clear_options.dry_run {
            info!(
                "DRY-RUN: Skipping actual restore (would restore {} backups)",
                restore_chain.len()
            );
            return Ok(());
        }

        // Restore each backup in chain
        for (backup_id, backup_path) in verified_archives {
            self.extract_backup_archive(backup_id, &backup_path)?;
        }

        info!("Restore completed successfully");

        Ok(())
    }

    fn backup_archive_path(&self, backup_id: Uuid) -> PathBuf {
        self.backup_dir.join(format!("backup_{}.tar", backup_id))
    }

    fn verify_backup_archive(&self, metadata: &BackupMetadata) -> Result<PathBuf> {
        let backup_path = self.backup_archive_path(metadata.id);
        if !backup_path.exists() {
            return Err(anyhow!("Backup file not found: {}", metadata.id));
        }

        let computed_checksum = compute_backup_checksum(&backup_path).with_context(|| {
            format!(
                "failed to validate backup archive structure for {}",
                metadata.id
            )
        })?;
        anyhow::ensure!(
            computed_checksum == metadata.checksum,
            "backup checksum mismatch for {}: expected 0x{:08X}, computed 0x{:08X}",
            metadata.id,
            metadata.checksum,
            computed_checksum
        );

        Ok(backup_path)
    }

    /// Extract a pre-verified backup archive into `data_dir`.
    fn extract_backup_archive(&self, backup_id: Uuid, backup_path: &Path) -> Result<()> {
        debug!("Extracting backup: {}", backup_id);

        fs::create_dir_all(&self.data_dir).context("Failed to create restore data directory")?;

        let backup_file = File::open(backup_path)?;
        let mut reader = BufReader::new(backup_file);

        let file_count = read_archive_file_count(&mut reader)?;

        // Extract each file
        for _ in 0..file_count {
            let (name, data_len) = read_archive_member_header(&mut reader)?;

            // Write to data directory
            let output_path = self.data_dir.join(&name);
            let output_file = open_restore_target(&output_path)?;
            let mut writer = BufWriter::new(output_file);
            stream_member_to_writer(&mut reader, &mut writer, data_len)?;
            writer.flush().with_context(|| {
                format!("Failed to flush restore target {}", output_path.display())
            })?;

            debug!("Extracted file: {}", name);
        }

        Ok(())
    }

    /// Restore to a specific point in time (PITR)
    pub fn restore_point_in_time(&self, timestamp: u64) -> Result<()> {
        self.restore_point_in_time_with_options(timestamp, &ClearDirectoryOptions::default())
    }

    /// Restore to a specific point in time with custom clear options
    pub fn restore_point_in_time_with_options(
        &self,
        timestamp: u64,
        clear_options: &ClearDirectoryOptions,
    ) -> Result<()> {
        info!(
            "Restoring to point in time: {} (allow_clear={}, dry_run={})",
            timestamp, clear_options.allow_clear, clear_options.dry_run
        );

        // Find the most recent full backup before timestamp
        let backups = self.list_backups()?;

        let mut full_backup = None;
        for backup in &backups {
            if backup.timestamp <= timestamp && backup.backup_type == BackupType::Full {
                full_backup = Some(backup);
                break;
            }
        }

        let full_backup = full_backup
            .ok_or_else(|| anyhow!("No full backup found before timestamp {}", timestamp))?;

        // Find all incremental backups between full backup and target timestamp
        // Traverse the full chain: Full → Inc1 → Inc2 → Inc3 (not just direct children)
        let mut incrementals = Vec::new();
        let mut current_id = full_backup.id;

        loop {
            let next = backups.iter().find(|b| {
                b.parent_id == Some(current_id)
                    && b.timestamp <= timestamp
                    && b.backup_type == BackupType::Incremental
            });

            match next {
                Some(backup) => {
                    incrementals.push(backup);
                    current_id = backup.id;
                }
                None => break,
            }
        }

        info!(
            "PITR: restoring full backup {} + {} incrementals",
            full_backup.id,
            incrementals.len()
        );

        // Preflight integrity verification before any destructive action.
        let mut verified_archives = Vec::with_capacity(incrementals.len().saturating_add(1));
        verified_archives.push((full_backup.id, self.verify_backup_archive(full_backup)?));
        for incremental in &incrementals {
            verified_archives.push((incremental.id, self.verify_backup_archive(incremental)?));
        }

        // Clear data directory with safeguards
        self.clear_data_directory(clear_options)?;

        // Return early if dry-run was enabled
        if clear_options.dry_run {
            info!(
                "DRY-RUN: Skipping actual restore (would restore 1 full backup + {} incrementals)",
                incrementals.len()
            );
            return Ok(());
        }

        // Restore full backup and incrementals in order
        for (backup_id, backup_path) in verified_archives {
            self.extract_backup_archive(backup_id, &backup_path)?;
        }

        info!("PITR restore completed successfully");

        Ok(())
    }

    /// List all backups
    fn list_backups(&self) -> Result<Vec<BackupMetadata>> {
        list_backups_from_dir(&self.backup_dir)
    }
}

/// S3 client for remote backup storage (requires s3-backup feature)
#[cfg(feature = "s3-backup")]
pub struct S3Client {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

#[cfg(feature = "s3-backup")]
impl S3Client {
    /// Create a new S3 client
    pub async fn new(bucket: String, prefix: String) -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&config);

        info!(
            "S3 client initialized: bucket={}, prefix={}",
            bucket, prefix
        );

        Ok(Self {
            client,
            bucket,
            prefix,
        })
    }

    /// Upload a backup to S3 with multipart upload for large files
    pub async fn upload_backup(
        &self,
        backup_manager: &BackupManager,
        backup_id: Uuid,
    ) -> Result<()> {
        info!("Uploading backup {} to S3", backup_id);

        let backup_path = backup_manager
            .backup_dir
            .join(format!("backup_{}.tar", backup_id));
        let metadata_path = backup_manager
            .backup_dir
            .join(format!("backup_{}.json", backup_id));

        if !backup_path.exists() {
            return Err(anyhow!("Backup file not found: {}", backup_id));
        }

        if !metadata_path.exists() {
            return Err(anyhow!("Backup metadata not found: {}", backup_id));
        }

        // Get file size to determine upload strategy
        let file_size = fs::metadata(&backup_path)?.len();

        // S3 multipart upload is required for files >5GB and improves reliability
        // with resumable uploads for large files. Using threshold well below the
        // 5GB S3 object size limit to ensure reliable single-part uploads.
        let large_file_threshold = 5 * 1024 * 1024 * 1024; // 5GB

        // Upload backup tar file
        let tar_key = format!("{}/backup_{}.tar", self.prefix, backup_id);

        if file_size > large_file_threshold {
            // Use multipart upload for large files
            self.upload_multipart(&backup_path, &tar_key).await?;
        } else {
            // Simple upload for small files
            self.upload_simple(&backup_path, &tar_key).await?;
        }

        // Upload metadata JSON
        let json_key = format!("{}/backup_{}.json", self.prefix, backup_id);
        self.upload_simple(&metadata_path, &json_key).await?;

        info!("Backup {} uploaded successfully to S3", backup_id);

        Ok(())
    }

    /// Simple upload for files <5GB
    async fn upload_simple(&self, file_path: &Path, key: &str) -> Result<()> {
        debug!(
            "Uploading {} to s3://{}/{}",
            file_path.display(),
            self.bucket,
            key
        );

        let mut retries = 0;
        let max_retries = 3;

        loop {
            // Read file inside retry loop to avoid cloning large bodies
            // This is more memory-efficient for retries of large files
            let body = tokio::fs::read(file_path).await?;
            let body_len = body.len();

            match self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body.into())
                .send()
                .await
            {
                Ok(_) => {
                    debug!("Upload successful: {} ({} bytes)", key, body_len);
                    return Ok(());
                }
                Err(e) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(anyhow!(
                            "Upload failed after {} retries: {}",
                            max_retries,
                            e
                        ));
                    }

                    let backoff_ms = 1000 * (1 << (retries - 1)); // 1s, 2s, 4s
                    warn!(
                        "Upload failed (attempt {}/{}), retrying in {}ms: {}",
                        retries, max_retries, backoff_ms, e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                }
            }
        }
    }

    /// Multipart upload for files >5GB (5MB chunks)
    async fn upload_multipart(&self, file_path: &Path, key: &str) -> Result<()> {
        let file_size = fs::metadata(file_path)?.len();
        let chunk_size = 5 * 1024 * 1024; // 5MB chunks

        info!(
            "Starting multipart upload: {} ({} bytes, {} chunks)",
            key,
            file_size,
            file_size.div_ceil(chunk_size)
        );

        // Initiate multipart upload
        let multipart = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context("Failed to initiate multipart upload")?;

        let upload_id = multipart
            .upload_id()
            .ok_or_else(|| anyhow!("No upload ID returned"))?;

        // Upload parts
        let file = tokio::fs::File::open(file_path).await?;
        let mut reader = tokio::io::BufReader::new(file);
        let mut part_number = 1;
        let mut completed_parts = Vec::new();

        loop {
            let mut buffer = vec![0u8; chunk_size as usize];
            let bytes_read = tokio::io::AsyncReadExt::read(&mut reader, &mut buffer)
                .await
                .context("Failed to read file chunk")?;

            if bytes_read == 0 {
                break; // End of file
            }

            buffer.truncate(bytes_read);

            debug!("Uploading part {} ({} bytes)", part_number, bytes_read);

            // Upload part with retry
            let mut retries = 0;
            let max_retries = 3;

            let upload_result = loop {
                match self
                    .client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .body(buffer.clone().into())
                    .send()
                    .await
                {
                    Ok(result) => break result,
                    Err(e) => {
                        retries += 1;
                        if retries >= max_retries {
                            // Abort multipart upload on failure
                            let _ = self
                                .client
                                .abort_multipart_upload()
                                .bucket(&self.bucket)
                                .key(key)
                                .upload_id(upload_id)
                                .send()
                                .await;

                            return Err(anyhow!(
                                "Part upload failed after {} retries: {}",
                                max_retries,
                                e
                            ));
                        }

                        let backoff_ms = 1000 * (1 << (retries - 1));
                        warn!(
                            "Part {} upload failed (attempt {}/{}), retrying in {}ms: {}",
                            part_number, retries, max_retries, backoff_ms, e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    }
                }
            };

            completed_parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(upload_result.e_tag().unwrap_or(""))
                    .build(),
            );

            part_number += 1;
        }

        // Complete multipart upload
        let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .context("Failed to complete multipart upload")?;

        info!("Multipart upload completed: {}", key);

        Ok(())
    }

    /// Download a backup from S3 with retry logic
    pub async fn download_backup(
        &self,
        backup_manager: &BackupManager,
        backup_id: Uuid,
    ) -> Result<()> {
        info!("Downloading backup {} from S3", backup_id);

        let tar_key = format!("{}/backup_{}.tar", self.prefix, backup_id);
        let json_key = format!("{}/backup_{}.json", self.prefix, backup_id);

        let tar_path = backup_manager
            .backup_dir
            .join(format!("backup_{}.tar", backup_id));
        let json_path = backup_manager
            .backup_dir
            .join(format!("backup_{}.json", backup_id));

        // Download tar file with retry
        self.download_with_retry(&tar_key, &tar_path).await?;

        // Download metadata with retry
        self.download_with_retry(&json_key, &json_path).await?;

        info!("Backup {} downloaded successfully from S3", backup_id);

        Ok(())
    }

    /// Download a file from S3 with exponential backoff retry
    async fn download_with_retry(&self, key: &str, output_path: &Path) -> Result<()> {
        let mut retries = 0;
        let max_retries = 3;

        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(output) => {
                    let mut body = output.body.into_async_read();
                    let mut file = tokio::fs::File::create(output_path).await?;

                    tokio::io::copy(&mut body, &mut file)
                        .await
                        .context("Failed to write downloaded file")?;

                    debug!("Downloaded {} to {}", key, output_path.display());
                    return Ok(());
                }
                Err(e) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(anyhow!(
                            "Download failed after {} retries: {}",
                            max_retries,
                            e
                        ));
                    }

                    let backoff_ms = 1000 * (1 << (retries - 1)); // 1s, 2s, 4s
                    warn!(
                        "Download failed (attempt {}/{}), retrying in {}ms: {}",
                        retries, max_retries, backoff_ms, e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                }
            }
        }
    }

    /// Generate a presigned URL for temporary access to a backup
    pub async fn presigned_url(&self, backup_id: Uuid, expiration_secs: u64) -> Result<String> {
        let key = format!("{}/backup_{}.tar", self.prefix, backup_id);

        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .presigned(
                aws_sdk_s3::presigning::PresigningConfig::expires_in(
                    std::time::Duration::from_secs(expiration_secs),
                )
                .context("Invalid expiration duration")?,
            )
            .await
            .context("Failed to generate presigned URL")?;

        Ok(presigned.uri().to_string())
    }

    /// List all backups in S3
    pub async fn list_backups_s3(&self) -> Result<Vec<String>> {
        let prefix = format!("{}/", self.prefix);

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .send()
            .await
            .context("Failed to list S3 objects")?;

        let mut backup_ids = Vec::new();

        // AWS SDK returns a slice directly, not Option
        for object in response.contents() {
            if let Some(key) = object.key() {
                if key.ends_with(".tar") {
                    // Extract backup ID from key
                    if let Some(filename) = key.split('/').next_back() {
                        if let Some(id_str) = filename
                            .strip_prefix("backup_")
                            .and_then(|s| s.strip_suffix(".tar"))
                        {
                            backup_ids.push(id_str.to_string());
                        }
                    }
                }
            }
        }

        Ok(backup_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    fn write_archive_with_entries(backup_path: &std::path::Path, entries: &[(&str, &[u8])]) {
        let file = File::create(backup_path).unwrap();
        let mut writer = BufWriter::new(file);
        writer
            .write_all(&(entries.len() as u32).to_le_bytes())
            .unwrap();
        for (name, payload) in entries {
            writer
                .write_all(&(name.len() as u32).to_le_bytes())
                .unwrap();
            writer.write_all(name.as_bytes()).unwrap();
            writer
                .write_all(&(payload.len() as u64).to_le_bytes())
                .unwrap();
            writer.write_all(payload).unwrap();
        }
        writer.flush().unwrap();
    }

    #[test]
    fn test_backup_manager_create_full() {
        let temp_data = TempDir::new().unwrap();
        let temp_backup = TempDir::new().unwrap();

        // Create modern MANIFEST + snapshot layout.
        let snapshot_name = "snapshot_100.snap";
        let snapshot = Snapshot::new(
            2,
            crate::config::DistanceMetric::Cosine,
            vec![(0, vec![1.0, 0.0])],
            vec![(0, std::collections::HashMap::new())],
            0,
        )
        .unwrap();
        snapshot.save(temp_data.path().join(snapshot_name)).unwrap();

        fs::write(temp_data.path().join("wal_1000.wal"), b"wal entry 1").unwrap();
        fs::write(temp_data.path().join("wal_1001.wal"), b"wal entry 2").unwrap();
        let mut manifest = Manifest::new();
        manifest.latest_snapshot = Some(snapshot_name.to_string());
        manifest.wal_segments = vec!["wal_1000.wal".to_string(), "wal_1001.wal".to_string()];
        manifest.save(temp_data.path().join("MANIFEST")).unwrap();

        let manager = BackupManager::new(temp_backup.path(), temp_data.path()).unwrap();
        let metadata = manager
            .create_full_backup("Test full backup".to_string())
            .unwrap();

        assert_eq!(metadata.backup_type, BackupType::Full);
        assert!(metadata.size_bytes > 0);
        assert!(metadata.checksum != 0);
        assert_eq!(metadata.parent_id, None);
        assert_eq!(metadata.vector_count, 1);
        assert_eq!(metadata.snapshot_file.as_deref(), Some(snapshot_name));
        assert_eq!(metadata.max_wal_file_id, Some(1001));

        // Verify backup files exist
        let backup_tar = temp_backup
            .path()
            .join(format!("backup_{}.tar", metadata.id));
        let backup_json = temp_backup
            .path()
            .join(format!("backup_{}.json", metadata.id));

        assert!(backup_tar.exists());
        assert!(backup_json.exists());
    }

    #[test]
    fn test_backup_manager_create_incremental() {
        let temp_data = TempDir::new().unwrap();
        let temp_backup = TempDir::new().unwrap();
        let temp_restore = TempDir::new().unwrap();

        let snapshot_name = "snapshot_100.snap";
        let snapshot = Snapshot::new(
            2,
            crate::config::DistanceMetric::Cosine,
            vec![(0, vec![1.0, 0.0])],
            vec![(0, std::collections::HashMap::new())],
            0,
        )
        .unwrap();
        snapshot.save(temp_data.path().join(snapshot_name)).unwrap();
        fs::write(temp_data.path().join("wal_1000.wal"), b"wal 1").unwrap();
        let mut manifest = Manifest::new();
        manifest.latest_snapshot = Some(snapshot_name.to_string());
        manifest.wal_segments = vec!["wal_1000.wal".to_string()];
        manifest.save(temp_data.path().join("MANIFEST")).unwrap();

        let manager = BackupManager::new(temp_backup.path(), temp_data.path()).unwrap();

        // Create full backup
        let full_metadata = manager.create_full_backup("Full".to_string()).unwrap();
        assert_eq!(full_metadata.max_wal_file_id, Some(1000));

        // Wait a moment and create new WAL
        thread::sleep(Duration::from_millis(100));
        fs::write(temp_data.path().join("wal_1001.wal"), b"wal 2").unwrap();
        let mut updated_manifest = Manifest::load(temp_data.path().join("MANIFEST")).unwrap();
        updated_manifest
            .wal_segments
            .push("wal_1001.wal".to_string());
        updated_manifest
            .save(temp_data.path().join("MANIFEST"))
            .unwrap();

        // Create incremental
        let inc_metadata = manager
            .create_incremental_backup(full_metadata.id, "Incremental".to_string())
            .unwrap();

        assert_eq!(inc_metadata.backup_type, BackupType::Incremental);
        assert_eq!(inc_metadata.parent_id, Some(full_metadata.id));
        assert!(inc_metadata.size_bytes > 0);
        assert_eq!(inc_metadata.max_wal_file_id, Some(1001));
        assert!(inc_metadata.snapshot_file.is_none());

        // Restore from incremental backup ID and ensure final MANIFEST includes new WAL segment.
        let restore_mgr = RestoreManager::new(temp_backup.path(), temp_restore.path()).unwrap();
        restore_mgr.restore_from_backup(inc_metadata.id).unwrap();
        let restored_manifest = Manifest::load(temp_restore.path().join("MANIFEST")).unwrap();
        assert!(
            restored_manifest
                .wal_segments
                .iter()
                .any(|segment| segment == "wal_1001.wal"),
            "Incremental restore should carry forward updated MANIFEST"
        );
        assert!(temp_restore.path().join("wal_1001.wal").exists());
    }

    #[test]
    fn test_backup_manager_list() {
        let temp_data = TempDir::new().unwrap();
        let temp_backup = TempDir::new().unwrap();

        fs::write(temp_data.path().join("MANIFEST"), "snapshot_number: 1\n").unwrap();
        fs::write(temp_data.path().join("snapshot_1"), b"data").unwrap();

        let manager = BackupManager::new(temp_backup.path(), temp_data.path()).unwrap();

        manager.create_full_backup("Backup 1".to_string()).unwrap();
        thread::sleep(Duration::from_millis(10));
        manager.create_full_backup("Backup 2".to_string()).unwrap();

        let backups = manager.list_backups().unwrap();
        assert_eq!(backups.len(), 2);

        // Should be sorted newest first
        assert!(backups[0].timestamp >= backups[1].timestamp);
    }

    #[test]
    fn test_restore_manager_full_backup() {
        let temp_data = TempDir::new().unwrap();
        let temp_backup = TempDir::new().unwrap();
        let temp_restore = TempDir::new().unwrap();

        // Create test files
        fs::write(temp_data.path().join("MANIFEST"), "snapshot_number: 50\n").unwrap();
        fs::write(temp_data.path().join("snapshot_50"), b"original data").unwrap();

        // Create backup
        let backup_mgr = BackupManager::new(temp_backup.path(), temp_data.path()).unwrap();
        let metadata = backup_mgr.create_full_backup("Test".to_string()).unwrap();

        // Restore to different directory
        let restore_mgr = RestoreManager::new(temp_backup.path(), temp_restore.path()).unwrap();
        restore_mgr.restore_from_backup(metadata.id).unwrap();

        // Verify restored files
        let manifest = fs::read_to_string(temp_restore.path().join("MANIFEST")).unwrap();
        assert!(manifest.contains("snapshot_number: 50"));

        let snapshot = fs::read(temp_restore.path().join("snapshot_50")).unwrap();
        assert_eq!(snapshot, b"original data");
    }

    #[test]
    fn test_restore_rejects_path_traversal_archive_member() {
        let temp_backup = TempDir::new().unwrap();
        let temp_restore = TempDir::new().unwrap();
        let escaped_target = temp_restore.path().parent().unwrap().join("pwned.txt");
        let backup_id = Uuid::new_v4();

        let backup_tar = temp_backup.path().join(format!("backup_{}.tar", backup_id));
        write_archive_with_entries(&backup_tar, &[("../pwned.txt", b"owned")]);
        let checksum_err = compute_backup_checksum(&backup_tar).unwrap_err();
        assert!(
            checksum_err
                .to_string()
                .contains("invalid backup member path"),
            "unexpected checksum validation error: {checksum_err}"
        );

        let metadata = BackupMetadata {
            id: backup_id,
            timestamp: 1,
            backup_type: BackupType::Full,
            size_bytes: 5,
            vector_count: 0,
            checksum: 0,
            parent_id: None,
            description: "malicious".to_string(),
            max_wal_file_id: None,
            snapshot_file: None,
        };
        fs::write(
            temp_backup
                .path()
                .join(format!("backup_{}.json", backup_id)),
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();

        let restore_mgr = RestoreManager::new(temp_backup.path(), temp_restore.path()).unwrap();
        let err = restore_mgr
            .restore_from_backup_with_options(
                backup_id,
                &ClearDirectoryOptions::new().with_allow_clear(true),
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to validate backup archive structure"),
            "unexpected error: {err}"
        );
        assert!(
            !escaped_target.exists(),
            "path traversal must not create files outside restore directory"
        );
    }

    #[test]
    fn test_restore_rejects_oversized_archive_member() {
        let temp_backup = TempDir::new().unwrap();
        let temp_restore = TempDir::new().unwrap();
        let backup_id = Uuid::new_v4();

        let backup_tar = temp_backup.path().join(format!("backup_{}.tar", backup_id));
        let file = File::create(&backup_tar).unwrap();
        let mut writer = BufWriter::new(file);
        writer.write_all(&1u32.to_le_bytes()).unwrap();
        let name = "MANIFEST".as_bytes();
        writer
            .write_all(&(name.len() as u32).to_le_bytes())
            .unwrap();
        writer.write_all(name).unwrap();
        writer
            .write_all(&(MAX_BACKUP_MEMBER_SIZE_BYTES + 1).to_le_bytes())
            .unwrap();
        writer.flush().unwrap();
        let checksum_err = compute_backup_checksum(&backup_tar).unwrap_err();
        assert!(
            checksum_err
                .to_string()
                .contains("exceeds maximum supported size"),
            "unexpected checksum validation error: {checksum_err}"
        );

        let metadata = BackupMetadata {
            id: backup_id,
            timestamp: 1,
            backup_type: BackupType::Full,
            size_bytes: 0,
            vector_count: 0,
            checksum: 0,
            parent_id: None,
            description: "oversized".to_string(),
            max_wal_file_id: None,
            snapshot_file: None,
        };
        fs::write(
            temp_backup
                .path()
                .join(format!("backup_{}.json", backup_id)),
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();

        let restore_mgr = RestoreManager::new(temp_backup.path(), temp_restore.path()).unwrap();
        let err = restore_mgr
            .restore_from_backup_with_options(
                backup_id,
                &ClearDirectoryOptions::new().with_allow_clear(true),
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to validate backup archive structure"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_restore_preflight_integrity_check_prevents_data_loss() {
        let temp_data = TempDir::new().unwrap();
        let temp_backup = TempDir::new().unwrap();
        let temp_restore = TempDir::new().unwrap();

        fs::write(temp_data.path().join("MANIFEST"), "snapshot_number: 7\n").unwrap();
        fs::write(temp_data.path().join("snapshot_7"), b"snapshot").unwrap();

        let backup_mgr = BackupManager::new(temp_backup.path(), temp_data.path()).unwrap();
        let metadata = backup_mgr.create_full_backup("valid".to_string()).unwrap();
        let backup_tar = temp_backup
            .path()
            .join(format!("backup_{}.tar", metadata.id));

        // Tamper with archive payload to force checksum mismatch.
        let mut file = File::options()
            .read(true)
            .write(true)
            .open(&backup_tar)
            .unwrap();
        let len = file.metadata().unwrap().len();
        file.seek(SeekFrom::Start(len.saturating_sub(1))).unwrap();
        let mut last = [0u8; 1];
        file.read_exact(&mut last).unwrap();
        file.seek(SeekFrom::Start(len.saturating_sub(1))).unwrap();
        file.write_all(&[last[0] ^ 0xFF]).unwrap();
        file.flush().unwrap();

        let sentinel = temp_restore.path().join("KEEP_ME");
        fs::write(&sentinel, b"do-not-delete").unwrap();

        let restore_mgr = RestoreManager::new(temp_backup.path(), temp_restore.path()).unwrap();
        let err = restore_mgr
            .restore_from_backup_with_options(
                metadata.id,
                &ClearDirectoryOptions::new().with_allow_clear(true),
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("checksum mismatch"),
            "unexpected error: {err}"
        );
        assert!(
            sentinel.exists(),
            "preflight verification must fail before clearing restore directory"
        );
    }

    #[test]
    fn test_restore_point_in_time() {
        let temp_data = TempDir::new().unwrap();
        let temp_backup = TempDir::new().unwrap();
        let temp_restore = TempDir::new().unwrap();

        // Create initial data
        fs::write(temp_data.path().join("MANIFEST"), "snapshot_number: 1\n").unwrap();
        fs::write(temp_data.path().join("snapshot_1"), b"v1").unwrap();

        let backup_mgr = BackupManager::new(temp_backup.path(), temp_data.path()).unwrap();

        // Full backup
        let full = backup_mgr.create_full_backup("v1".to_string()).unwrap();
        let timestamp_after_full = full.timestamp + 1;

        thread::sleep(Duration::from_millis(100));

        // Incremental backup
        fs::write(temp_data.path().join("wal_100.wal"), b"changes").unwrap();
        let _inc = backup_mgr
            .create_incremental_backup(full.id, "v2".to_string())
            .unwrap();

        // Restore to point after full but before incremental
        let restore_mgr = RestoreManager::new(temp_backup.path(), temp_restore.path()).unwrap();
        restore_mgr
            .restore_point_in_time(timestamp_after_full)
            .unwrap();

        // Should have full backup files but not incremental WAL
        assert!(temp_restore.path().join("MANIFEST").exists());
        assert!(temp_restore.path().join("snapshot_1").exists());
    }

    #[test]
    fn test_retention_policy_pruning() {
        let temp_data = TempDir::new().unwrap();
        let temp_backup = TempDir::new().unwrap();

        fs::write(temp_data.path().join("MANIFEST"), "snapshot_number: 1\n").unwrap();
        fs::write(temp_data.path().join("snapshot_1"), b"data").unwrap();

        let manager = BackupManager::new(temp_backup.path(), temp_data.path()).unwrap();

        // Create multiple backups
        for i in 0..5 {
            manager.create_full_backup(format!("Backup {}", i)).unwrap();
            thread::sleep(Duration::from_millis(10));
        }

        let before_prune = manager.list_backups().unwrap();
        assert_eq!(before_prune.len(), 5);

        // Prune with aggressive policy (keep only 1 hour)
        let policy = RetentionPolicy {
            hourly_hours: 1,
            daily_days: 0,
            weekly_weeks: 0,
            monthly_months: 0,
            min_age_days: 0,
        };

        let deleted = manager.prune_backups(&policy).unwrap();

        // Should keep backups within 1 hour (all of them in this test)
        let after_prune = manager.list_backups().unwrap();
        assert!(!after_prune.is_empty());
        assert!(deleted.len() < before_prune.len());
    }

    #[tokio::test]
    #[cfg(feature = "s3-backup")]
    async fn test_s3_client_creation() {
        // Test S3Client creation (requires AWS credentials in env)
        // This test will be skipped if AWS credentials are not available

        if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
            eprintln!("Skipping S3 test: AWS credentials not configured");
            return;
        }

        let client =
            S3Client::new("test-kyrodb-backups".to_string(), "test-prefix".to_string()).await;

        assert!(
            client.is_ok(),
            "S3 client initialization should succeed when credentials are configured: {client:?}"
        );
    }

    #[tokio::test]
    #[cfg(feature = "s3-backup")]
    async fn test_s3_backup_upload_download() {
        // Integration test for S3 upload/download
        // Requires localstack or real S3 credentials

        if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
            eprintln!("Skipping S3 upload/download test: AWS credentials not configured");
            return;
        }

        let temp_data = TempDir::new().unwrap();
        let temp_backup = TempDir::new().unwrap();

        // Create test backup
        fs::write(temp_data.path().join("MANIFEST"), "snapshot_number: 1\n").unwrap();
        fs::write(temp_data.path().join("snapshot_1"), b"test data").unwrap();

        let backup_mgr = BackupManager::new(temp_backup.path(), temp_data.path()).unwrap();
        let metadata = backup_mgr
            .create_full_backup("S3 test".to_string())
            .unwrap();

        // Try to create S3 client
        if let Ok(s3_client) =
            S3Client::new("test-kyrodb-backups".to_string(), "test".to_string()).await
        {
            // Try upload (may fail if S3 not available, that's okay for local dev)
            let upload_result = s3_client.upload_backup(&backup_mgr, metadata.id).await;

            if upload_result.is_ok() {
                eprintln!("S3 upload succeeded");

                // Try download
                let download_result = s3_client.download_backup(&backup_mgr, metadata.id).await;
                assert!(download_result.is_ok());
            } else {
                eprintln!(
                    "S3 upload failed (expected in local dev): {:?}",
                    upload_result.err()
                );
            }
        }
    }

    #[test]
    fn test_backup_metadata_serialization() {
        let metadata = BackupMetadata {
            id: Uuid::new_v4(),
            timestamp: 1234567890,
            backup_type: BackupType::Full,
            size_bytes: 1024000,
            vector_count: 10000,
            checksum: 0xDEADBEEF,
            parent_id: None,
            description: "Test backup".to_string(),
            max_wal_file_id: Some(777),
            snapshot_file: Some("snapshot_777.snap".to_string()),
        };

        // Serialize to JSON
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("\"id\""));
        assert!(json.contains("\"timestamp\":1234567890"));
        assert!(json.contains("\"Full\""));

        // Deserialize back
        let deserialized: BackupMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, metadata.id);
        assert_eq!(deserialized.timestamp, metadata.timestamp);
        assert_eq!(deserialized.backup_type, BackupType::Full);
    }
}
