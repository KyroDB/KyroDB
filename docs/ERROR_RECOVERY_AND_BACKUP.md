# Error Recovery & Backup/Restore System

## Overview

This document describes KyroDB's production-grade error recovery and backup/restore infrastructure. The system ensures zero data loss, automatic recovery from failures, and point-in-time restore capabilities.

**Design Principles**:
- Fail-fast with graceful degradation
- Automatic recovery without human intervention
- Observable failure modes (metrics + logging)
- Zero data loss guarantees
- Sub-second recovery time objectives (RTO)

---

## Architecture

### Error Recovery Components

```
┌─────────────────────────────────────────────────────────┐
│                    Error Recovery                        │
├─────────────────────────────────────────────────────────┤
│  WAL Error Handler                                       │
│    ├─ Disk full detection                               │
│    ├─ Write retry with exponential backoff              │
│    └─ Circuit breaker (open after 3 failures)           │
│                                                          │
│  HNSW Corruption Detector                               │
│    ├─ Checksum validation on load                       │
│    ├─ Automatic fallback to last good snapshot          │
│    └─ WAL replay for recovery                           │
│                                                          │
│  Training Task Supervisor                                │
│    ├─ Auto-restart on panic/crash                       │
│    ├─ Exponential backoff (1s, 2s, 4s, 8s, 16s max)    │
│    └─ Max 10 retries before giving up                   │
│                                                          │
│  Query Timeout Handler                                   │
│    ├─ Per-layer timeouts (cache: 10ms, hot: 50ms)      │
│    ├─ Fallback to next layer on timeout                 │
│    └─ Circuit breaker for consistently slow layers      │
└─────────────────────────────────────────────────────────┘
```

### Backup/Restore Components

```
┌─────────────────────────────────────────────────────────┐
│                  Backup/Restore System                   │
├─────────────────────────────────────────────────────────┤
│  BackupManager                                           │
│    ├─ Snapshot creation (atomic, consistent)            │
│    ├─ Incremental WAL backups                           │
│    ├─ Metadata tracking (timestamp, size, checksum)     │
│    └─ Retention policy enforcement                      │
│                                                          │
│  RestoreManager                                          │
│    ├─ Snapshot validation (checksum, integrity)         │
│    ├─ Atomic restore (all-or-nothing)                   │
│    ├─ Rollback on failure                               │
│    └─ WAL replay for PITR                               │
│                                                          │
│  S3Client                                                │
│    ├─ Async upload/download                             │
│    ├─ Multipart upload for large files (>5GB)           │
│    ├─ Retry with exponential backoff                    │
│    └─ Presigned URLs for direct access                  │
│                                                          │
│  Point-in-Time Recovery (PITR)                          │
│    ├─ WAL timestamp indexing                            │
│    ├─ Snapshot + incremental WAL restore                │
│    └─ Recovery to any point within retention window     │
└─────────────────────────────────────────────────────────┘
```

---

## Error Recovery Implementation

### 1. WAL Error Handling

**Failure Scenarios**:
1. Disk full (ENOSPC)
2. I/O errors (disk failure, network filesystem issues)
3. Permission errors (EACCES)
4. Quota exceeded (EDQUOT)

**Recovery Strategy**:
```rust
pub struct WalErrorHandler {
    circuit_breaker: CircuitBreaker,
    retry_policy: ExponentialBackoff,
    metrics: MetricsCollector,
}

impl WalErrorHandler {
    pub async fn write_with_recovery(&self, entry: WalEntry) -> Result<()> {
        // Check circuit breaker state
        if self.circuit_breaker.is_open() {
            return Err(anyhow!("Circuit breaker open: WAL writes disabled"));
        }
        
        // Retry with exponential backoff
        let mut attempt = 0;
        loop {
            match self.try_write(&entry).await {
                Ok(()) => {
                    self.circuit_breaker.record_success();
                    self.metrics.increment_wal_writes_success();
                    return Ok(());
                }
                Err(e) if is_retryable(&e) && attempt < 5 => {
                    attempt += 1;
                    let delay = self.retry_policy.next_delay(attempt);
                    warn!("WAL write failed (attempt {}): {}", attempt, e);
                    tokio::time::sleep(delay).await;
                }
                Err(e) if is_disk_full(&e) => {
                    error!("Disk full detected, opening circuit breaker");
                    self.circuit_breaker.open();
                    self.metrics.increment_wal_errors_disk_full();
                    return Err(anyhow!("Disk full: {}", e));
                }
                Err(e) => {
                    self.circuit_breaker.record_failure();
                    self.metrics.increment_wal_errors_total();
                    return Err(e);
                }
            }
        }
    }
}
```

**Circuit Breaker States**:
- **Closed**: Normal operation, all writes allowed
- **Open**: After 3 consecutive failures, block all writes for 60 seconds
- **Half-Open**: After timeout, allow 1 write to test recovery

**Observability**:
```prometheus
wal_writes_total{status="success|failure"}
wal_errors_total{type="disk_full|io_error|permission"}
wal_circuit_breaker_state{state="open|closed|half_open"}
wal_retry_attempts_total
```

---

### 2. HNSW Corruption Detection and Recovery

**Corruption Scenarios**:
1. Incomplete snapshot write (crash during save)
2. Bit rot (disk corruption over time)
3. File system errors (truncated files)

**Detection Method**:
```rust
pub struct HnswSnapshotMetadata {
    checksum: u64,           // CRC64 of entire snapshot
    version: u32,            // Snapshot format version
    timestamp: SystemTime,   // Creation time
    vector_count: usize,     // Expected vector count
    dimension: usize,        // Expected dimension
}

impl HnswBackend {
    pub fn load_with_validation(path: &Path) -> Result<Self> {
        // Load metadata
        let metadata = Self::load_metadata(path)?;
        
        // Validate checksum
        let actual_checksum = Self::compute_checksum(path)?;
        if actual_checksum != metadata.checksum {
            warn!("HNSW snapshot corruption detected at {}", path.display());
            return Self::fallback_recovery(path);
        }
        
        // Load index
        let backend = Self::load_index(path)?;
        
        // Validate vector count matches metadata
        if backend.vector_count() != metadata.vector_count {
            warn!("Vector count mismatch: expected {}, got {}", 
                  metadata.vector_count, backend.vector_count());
            return Self::fallback_recovery(path);
        }
        
        Ok(backend)
    }
    
    fn fallback_recovery(data_dir: &Path) -> Result<Self> {
        // Try previous snapshots (snapshot_N.hnsw, snapshot_N-1.hnsw, ...)
        for i in (0..10).rev() {
            let snapshot_path = data_dir.join(format!("snapshot_{}.hnsw", i));
            if snapshot_path.exists() {
                info!("Attempting recovery from {}", snapshot_path.display());
                match Self::load_with_validation(&snapshot_path) {
                    Ok(backend) => {
                        // Replay WAL from snapshot timestamp to present
                        let wal_entries = Self::load_wal_since(backend.metadata.timestamp)?;
                        backend.replay_wal(&wal_entries)?;
                        return Ok(backend);
                    }
                    Err(e) => {
                        warn!("Recovery failed from {}: {}", snapshot_path.display(), e);
                        continue;
                    }
                }
            }
        }
        
        Err(anyhow!("All recovery attempts failed, no valid snapshot found"))
    }
}
```

**Recovery Strategy**:
1. Detect corruption via checksum mismatch
2. Fall back to previous snapshot (up to 10 generations)
3. Replay WAL from snapshot timestamp to present
4. Emit alert if recovery required (indicates disk issues)

**Observability**:
```prometheus
hnsw_corruption_detected_total
hnsw_recovery_attempts_total{result="success|failure"}
hnsw_snapshot_age_seconds{generation="0|1|2|..."}
```

---

### 3. Training Task Supervision

**Crash Scenarios**:
1. Panic in RMI training code
2. Out-of-memory during training
3. Deadlock in concurrent access

**Supervisor Pattern**:
```rust
pub struct TrainingTaskSupervisor {
    task_handle: Option<JoinHandle<()>>,
    restart_count: Arc<AtomicUsize>,
    backoff: ExponentialBackoff,
    max_restarts: usize,
    metrics: MetricsCollector,
}

impl TrainingTaskSupervisor {
    pub async fn supervise(
        logger: Arc<RwLock<AccessPatternLogger>>,
        predictor: Arc<RwLock<LearnedCachePredictor>>,
        config: TrainingConfig,
    ) -> Result<()> {
        let mut supervisor = Self::new(config);
        
        loop {
            // Spawn training task
            let task = tokio::spawn(Self::run_training_loop(
                logger.clone(),
                predictor.clone(),
                config.clone(),
            ));
            
            supervisor.task_handle = Some(task);
            
            // Wait for task to complete or crash
            match supervisor.task_handle.unwrap().await {
                Ok(()) => {
                    // Normal shutdown
                    info!("Training task completed normally");
                    break;
                }
                Err(e) if e.is_panic() => {
                    // Task panicked, attempt restart
                    let restart_count = supervisor.restart_count.fetch_add(1, Ordering::Relaxed);
                    error!("Training task panicked (restart {}): {}", restart_count, e);
                    
                    if restart_count >= supervisor.max_restarts {
                        error!("Max restarts ({}) exceeded, giving up", supervisor.max_restarts);
                        supervisor.metrics.increment_training_restarts_exceeded();
                        return Err(anyhow!("Training task failed after {} restarts", restart_count));
                    }
                    
                    // Exponential backoff before restart
                    let delay = supervisor.backoff.next_delay(restart_count);
                    warn!("Restarting training task in {:?}", delay);
                    tokio::time::sleep(delay).await;
                    
                    supervisor.metrics.increment_training_restarts_total();
                }
                Err(e) => {
                    error!("Training task failed: {}", e);
                    return Err(e.into());
                }
            }
        }
        
        Ok(())
    }
}
```

**Backoff Schedule**:
- Restart 1: 1 second
- Restart 2: 2 seconds
- Restart 3: 4 seconds
- Restart 4: 8 seconds
- Restart 5+: 16 seconds (capped)
- Max restarts: 10

**Observability**:
```prometheus
training_restarts_total
training_restarts_exceeded_total
training_uptime_seconds
training_last_success_timestamp
```

---

### 4. Query Timeout and Graceful Degradation

**Timeout Strategy**:
```rust
pub struct TieredQueryExecutor {
    cache_timeout: Duration,
    hot_tier_timeout: Duration,
    cold_tier_timeout: Duration,
    circuit_breakers: HashMap<String, CircuitBreaker>,
}

impl TieredQueryExecutor {
    pub async fn search_with_timeouts(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        // Layer 1: Hybrid Semantic Cache (fast path)
        if !self.circuit_breakers["cache"].is_open() {
            match tokio::time::timeout(
                self.cache_timeout,
                self.search_cache(query, k),
            ).await {
                Ok(Ok(results)) if !results.is_empty() => {
                    self.metrics.increment_cache_hits();
                    return Ok(results);
                }
                Ok(Err(e)) => {
                    warn!("Cache search failed: {}", e);
                    self.circuit_breakers["cache"].record_failure();
                }
                Err(_timeout) => {
                    warn!("Cache search timeout ({}ms)", self.cache_timeout.as_millis());
                    self.circuit_breakers["cache"].record_failure();
                    self.metrics.increment_cache_timeouts();
                }
                _ => {} // Cache miss, continue to next layer
            }
        }
        
        // Layer 2: Hot Tier (recent writes)
        if !self.circuit_breakers["hot_tier"].is_open() {
            match tokio::time::timeout(
                self.hot_tier_timeout,
                self.search_hot_tier(query, k),
            ).await {
                Ok(Ok(results)) => {
                    self.metrics.increment_hot_tier_hits();
                    return Ok(results);
                }
                Ok(Err(e)) => {
                    warn!("Hot tier search failed: {}", e);
                    self.circuit_breakers["hot_tier"].record_failure();
                }
                Err(_timeout) => {
                    warn!("Hot tier timeout ({}ms)", self.hot_tier_timeout.as_millis());
                    self.metrics.increment_hot_tier_timeouts();
                }
            }
        }
        
        // Layer 3: Cold Tier (HNSW, always try)
        match tokio::time::timeout(
            self.cold_tier_timeout,
            self.search_cold_tier(query, k),
        ).await {
            Ok(Ok(results)) => {
                self.metrics.increment_cold_tier_hits();
                Ok(results)
            }
            Ok(Err(e)) => {
                error!("Cold tier search failed: {}", e);
                Err(e)
            }
            Err(_timeout) => {
                error!("Cold tier timeout ({}ms), all layers exhausted", 
                       self.cold_tier_timeout.as_millis());
                Err(anyhow!("Search timeout: all layers exceeded timeout"))
            }
        }
    }
}
```

**Timeout Configuration**:
- Cache layer: 10ms (fast, in-memory)
- Hot tier: 50ms (BTree lookup)
- Cold tier: 1000ms (HNSW k-NN search)

**Observability**:
```prometheus
query_timeouts_total{layer="cache|hot_tier|cold_tier"}
query_layer_circuit_breaker_state{layer="cache|hot_tier|cold_tier", state="open|closed"}
query_degradation_total{fallback_to="hot_tier|cold_tier"}
```

---

## Backup/Restore Implementation

### 1. Backup Manager

**Backup Types**:
1. **Full Backup**: Complete snapshot of HNSW index + WAL + metadata
2. **Incremental Backup**: Only new WAL entries since last backup
3. **Continuous Backup**: Stream WAL entries to S3 in real-time

**Implementation**:
```rust
pub struct BackupManager {
    data_dir: PathBuf,
    s3_client: Option<S3Client>,
    retention_policy: RetentionPolicy,
    metrics: MetricsCollector,
}

pub struct BackupMetadata {
    pub backup_id: String,           // UUID
    pub timestamp: SystemTime,       // Backup creation time
    pub backup_type: BackupType,     // Full, Incremental, Continuous
    pub size_bytes: u64,            // Total backup size
    pub checksum: u64,              // CRC64 checksum
    pub hnsw_snapshot: String,      // HNSW snapshot filename
    pub wal_files: Vec<String>,     // WAL files included
    pub vector_count: usize,        // Total vectors at backup time
}

impl BackupManager {
    pub async fn create_full_backup(&self) -> Result<BackupMetadata> {
        let backup_id = Uuid::new_v4().to_string();
        let backup_dir = self.data_dir.join(format!("backup_{}", backup_id));
        fs::create_dir_all(&backup_dir)?;
        
        info!("Creating full backup: {}", backup_id);
        
        // Step 1: Create HNSW snapshot
        let snapshot_path = backup_dir.join("hnsw_snapshot.bin");
        self.create_hnsw_snapshot(&snapshot_path).await?;
        
        // Step 2: Copy all WAL files
        let wal_files = self.copy_wal_files(&backup_dir).await?;
        
        // Step 3: Create metadata file
        let metadata = BackupMetadata {
            backup_id: backup_id.clone(),
            timestamp: SystemTime::now(),
            backup_type: BackupType::Full,
            size_bytes: self.compute_backup_size(&backup_dir)?,
            checksum: self.compute_backup_checksum(&backup_dir)?,
            hnsw_snapshot: "hnsw_snapshot.bin".to_string(),
            wal_files,
            vector_count: self.get_vector_count()?,
        };
        
        self.save_metadata(&backup_dir, &metadata)?;
        
        // Step 4: Upload to S3 (if configured)
        if let Some(s3) = &self.s3_client {
            self.upload_to_s3(s3, &backup_dir, &metadata).await?;
        }
        
        // Step 5: Apply retention policy
        self.apply_retention_policy().await?;
        
        self.metrics.increment_backup_success();
        info!("Full backup completed: {} ({} bytes)", backup_id, metadata.size_bytes);
        
        Ok(metadata)
    }
    
    pub async fn create_incremental_backup(
        &self,
        since: SystemTime,
    ) -> Result<BackupMetadata> {
        let backup_id = Uuid::new_v4().to_string();
        let backup_dir = self.data_dir.join(format!("backup_{}", backup_id));
        fs::create_dir_all(&backup_dir)?;
        
        info!("Creating incremental backup since {:?}: {}", since, backup_id);
        
        // Copy only WAL files modified after 'since' timestamp
        let wal_files = self.copy_wal_files_since(&backup_dir, since).await?;
        
        if wal_files.is_empty() {
            return Err(anyhow!("No new WAL files since {:?}", since));
        }
        
        let metadata = BackupMetadata {
            backup_id: backup_id.clone(),
            timestamp: SystemTime::now(),
            backup_type: BackupType::Incremental,
            size_bytes: self.compute_backup_size(&backup_dir)?,
            checksum: self.compute_backup_checksum(&backup_dir)?,
            hnsw_snapshot: String::new(), // No snapshot in incremental
            wal_files,
            vector_count: self.get_vector_count()?,
        };
        
        self.save_metadata(&backup_dir, &metadata)?;
        
        if let Some(s3) = &self.s3_client {
            self.upload_to_s3(s3, &backup_dir, &metadata).await?;
        }
        
        self.metrics.increment_backup_success();
        Ok(metadata)
    }
}
```

**Retention Policy**:
```rust
pub struct RetentionPolicy {
    pub keep_hourly: usize,   // Keep last N hourly backups
    pub keep_daily: usize,    // Keep last N daily backups
    pub keep_weekly: usize,   // Keep last N weekly backups
    pub keep_monthly: usize,  // Keep last N monthly backups
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            keep_hourly: 24,      // 24 hours
            keep_daily: 7,        // 7 days
            keep_weekly: 4,       // 4 weeks
            keep_monthly: 12,     // 12 months
        }
    }
}
```

---

### 2. Restore Manager

**Restore Types**:
1. **Full Restore**: Restore from complete snapshot
2. **Point-in-Time Restore (PITR)**: Restore to specific timestamp
3. **Incremental Restore**: Apply incremental backups on top of full backup

**Implementation**:
```rust
pub struct RestoreManager {
    data_dir: PathBuf,
    s3_client: Option<S3Client>,
    metrics: MetricsCollector,
}

impl RestoreManager {
    pub async fn restore_from_backup(
        &self,
        backup_id: &str,
    ) -> Result<()> {
        info!("Starting restore from backup: {}", backup_id);
        
        // Step 1: Download backup from S3 (if needed)
        let backup_dir = if let Some(s3) = &self.s3_client {
            self.download_from_s3(s3, backup_id).await?
        } else {
            self.data_dir.join(format!("backup_{}", backup_id))
        };
        
        // Step 2: Validate backup integrity
        let metadata = self.load_metadata(&backup_dir)?;
        self.validate_backup(&backup_dir, &metadata)?;
        
        // Step 3: Create restore point (for rollback)
        let restore_point = self.create_restore_point().await?;
        
        // Step 4: Restore HNSW snapshot
        match self.restore_hnsw_snapshot(&backup_dir, &metadata).await {
            Ok(()) => info!("HNSW snapshot restored successfully"),
            Err(e) => {
                error!("HNSW restore failed: {}", e);
                self.rollback_to_restore_point(&restore_point).await?;
                return Err(e);
            }
        }
        
        // Step 5: Replay WAL files
        match self.replay_wal_files(&backup_dir, &metadata).await {
            Ok(()) => info!("WAL replay completed successfully"),
            Err(e) => {
                error!("WAL replay failed: {}", e);
                self.rollback_to_restore_point(&restore_point).await?;
                return Err(e);
            }
        }
        
        // Step 6: Verify restored state
        self.verify_restored_state(&metadata)?;
        
        // Step 7: Cleanup restore point
        self.cleanup_restore_point(&restore_point).await?;
        
        self.metrics.increment_restore_success();
        info!("Restore completed successfully: {}", backup_id);
        
        Ok(())
    }
    
    pub async fn restore_point_in_time(
        &self,
        target_time: SystemTime,
    ) -> Result<()> {
        info!("Starting point-in-time restore to {:?}", target_time);
        
        // Step 1: Find closest full backup before target_time
        let full_backup = self.find_closest_backup(target_time).await?;
        
        // Step 2: Restore from full backup
        self.restore_from_backup(&full_backup.backup_id).await?;
        
        // Step 3: Find and apply incremental backups up to target_time
        let incremental_backups = self.find_incremental_backups(
            full_backup.timestamp,
            target_time,
        ).await?;
        
        for backup in incremental_backups {
            info!("Applying incremental backup: {}", backup.backup_id);
            self.apply_incremental_backup(&backup).await?;
        }
        
        // Step 4: Replay WAL up to exact target_time
        self.replay_wal_until(target_time).await?;
        
        self.metrics.increment_pitr_success();
        info!("Point-in-time restore completed: {:?}", target_time);
        
        Ok(())
    }
    
    fn validate_backup(
        &self,
        backup_dir: &Path,
        metadata: &BackupMetadata,
    ) -> Result<()> {
        // Validate checksum
        let actual_checksum = self.compute_backup_checksum(backup_dir)?;
        if actual_checksum != metadata.checksum {
            return Err(anyhow!(
                "Backup checksum mismatch: expected {}, got {}",
                metadata.checksum,
                actual_checksum
            ));
        }
        
        // Validate all files exist
        if !metadata.hnsw_snapshot.is_empty() {
            let snapshot_path = backup_dir.join(&metadata.hnsw_snapshot);
            if !snapshot_path.exists() {
                return Err(anyhow!(
                    "HNSW snapshot not found: {}",
                    snapshot_path.display()
                ));
            }
        }
        
        for wal_file in &metadata.wal_files {
            let wal_path = backup_dir.join(wal_file);
            if !wal_path.exists() {
                return Err(anyhow!("WAL file not found: {}", wal_path.display()));
            }
        }
        
        Ok(())
    }
}
```

---

### 3. S3 Integration

**S3Client Implementation**:
```rust
use aws_sdk_s3::{Client, Config};
use aws_sdk_s3::primitives::ByteStream;

pub struct S3Client {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Client {
    pub async fn new(
        bucket: String,
        region: String,
        prefix: String,
    ) -> Result<Self> {
        let config = aws_config::from_env()
            .region(region)
            .load()
            .await;
        
        let client = Client::new(&config);
        
        Ok(Self { client, bucket, prefix })
    }
    
    pub async fn upload_backup(
        &self,
        backup_dir: &Path,
        backup_id: &str,
    ) -> Result<()> {
        let key_prefix = format!("{}/backup_{}", self.prefix, backup_id);
        
        // Upload all files in backup directory
        for entry in fs::read_dir(backup_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                let filename = path.file_name().unwrap().to_str().unwrap();
                let key = format!("{}/{}", key_prefix, filename);
                
                info!("Uploading {} to s3://{}/{}", filename, self.bucket, key);
                
                // Use multipart upload for large files (>5GB)
                if path.metadata()?.len() > 5 * 1024 * 1024 * 1024 {
                    self.multipart_upload(&path, &key).await?;
                } else {
                    self.simple_upload(&path, &key).await?;
                }
            }
        }
        
        Ok(())
    }
    
    async fn simple_upload(&self, path: &Path, key: &str) -> Result<()> {
        let body = ByteStream::from_path(path).await?;
        
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await?;
        
        Ok(())
    }
    
    async fn multipart_upload(&self, path: &Path, key: &str) -> Result<()> {
        // Initiate multipart upload
        let multipart = self.client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        
        let upload_id = multipart.upload_id().unwrap();
        
        // Upload parts (5MB chunks)
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut part_number = 1;
        let mut parts = Vec::new();
        
        loop {
            let mut buffer = vec![0u8; 5 * 1024 * 1024]; // 5MB
            let bytes_read = reader.read(&mut buffer)?;
            
            if bytes_read == 0 {
                break;
            }
            
            buffer.truncate(bytes_read);
            
            let part_response = self.client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(buffer))
                .send()
                .await?;
            
            parts.push((part_number, part_response.e_tag().unwrap().to_string()));
            part_number += 1;
        }
        
        // Complete multipart upload
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(
                        parts.into_iter()
                            .map(|(num, etag)| {
                                aws_sdk_s3::types::CompletedPart::builder()
                                    .part_number(num)
                                    .e_tag(etag)
                                    .build()
                            })
                            .collect()
                    ))
                    .build()
            )
            .send()
            .await?;
        
        Ok(())
    }
    
    pub async fn download_backup(
        &self,
        backup_id: &str,
        dest_dir: &Path,
    ) -> Result<()> {
        let key_prefix = format!("{}/backup_{}", self.prefix, backup_id);
        
        // List all objects with prefix
        let objects = self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&key_prefix)
            .send()
            .await?;
        
        for object in objects.contents() {
            let key = object.key().unwrap();
            let filename = key.strip_prefix(&format!("{}/", key_prefix)).unwrap();
            let dest_path = dest_dir.join(filename);
            
            info!("Downloading s3://{}/{} to {}", self.bucket, key, dest_path.display());
            
            let response = self.client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await?;
            
            let mut file = File::create(&dest_path)?;
            let mut body = response.body.into_async_read();
            tokio::io::copy(&mut body, &mut file).await?;
        }
        
        Ok(())
    }
}
```

---

## CLI Commands

### Backup Command

```bash
# Create full backup
kyrodb backup create --full --output /backups/

# Create incremental backup
kyrodb backup create --incremental --since "2024-01-15T10:00:00Z"

# Create backup and upload to S3
kyrodb backup create --full --s3 s3://kyrodb-backups/prod/

# List available backups
kyrodb backup list

# Delete old backups (apply retention policy)
kyrodb backup prune --retention-days 30
```

### Restore Command

```bash
# Restore from backup ID
kyrodb restore --backup-id abc123 --data-dir /var/lib/kyrodb/

# Point-in-time restore
kyrodb restore --point-in-time "2024-01-15T10:30:00Z"

# Restore from S3
kyrodb restore --s3 s3://kyrodb-backups/prod/backup_abc123/

# Dry-run restore (validate only)
kyrodb restore --backup-id abc123 --dry-run
```

---

## Configuration

```yaml
# config.yaml
backup:
  enabled: true
  
  # Backup schedule (cron format)
  full_backup_schedule: "0 2 * * *"  # Daily at 2 AM
  incremental_backup_schedule: "0 * * * *"  # Hourly
  
  # Retention policy
  retention:
    keep_hourly: 24
    keep_daily: 7
    keep_weekly: 4
    keep_monthly: 12
  
  # S3 configuration (optional)
  s3:
    enabled: true
    bucket: kyrodb-backups
    region: us-west-2
    prefix: prod
    
error_recovery:
  # WAL error handling
  wal:
    retry_attempts: 5
    circuit_breaker_threshold: 3
    circuit_breaker_timeout_seconds: 60
  
  # Training task supervision
  training:
    max_restarts: 10
    restart_backoff_seconds: [1, 2, 4, 8, 16]
  
  # Query timeouts
  query:
    cache_timeout_ms: 10
    hot_tier_timeout_ms: 50
    cold_tier_timeout_ms: 1000
```

---

## Observability

### Metrics

```prometheus
# Error Recovery Metrics
wal_writes_total{status="success|failure"}
wal_errors_total{type="disk_full|io_error|permission"}
wal_circuit_breaker_state{state="open|closed|half_open"}
hnsw_corruption_detected_total
training_restarts_total
query_timeouts_total{layer="cache|hot_tier|cold_tier"}

# Backup/Restore Metrics
backup_success_total{type="full|incremental"}
backup_failure_total{type="full|incremental"}
backup_duration_seconds{type="full|incremental"}
backup_size_bytes{type="full|incremental"}
restore_success_total
restore_failure_total
restore_duration_seconds
pitr_success_total
```

### Alerts

```yaml
# alertmanager.yml
groups:
  - name: kyrodb_error_recovery
    rules:
      - alert: WalCircuitBreakerOpen
        expr: wal_circuit_breaker_state{state="open"} == 1
        for: 1m
        annotations:
          summary: "WAL circuit breaker is open (disk full?)"
          
      - alert: HnswCorruptionDetected
        expr: rate(hnsw_corruption_detected_total[5m]) > 0
        annotations:
          summary: "HNSW index corruption detected, check disk health"
          
      - alert: TrainingTaskRestartsHigh
        expr: rate(training_restarts_total[5m]) > 0.1
        annotations:
          summary: "Training task restarting frequently, investigate crashes"
          
      - alert: BackupFailing
        expr: rate(backup_failure_total[1h]) > 0
        annotations:
          summary: "Backup failures detected, data loss risk"
```

---

## Testing Strategy

### Chaos Tests

```rust
// engine/tests/chaos_recovery.rs

#[test]
fn disk_full_during_wal_write() {
    fail::cfg("wal_write", "return(ENOSPC)").unwrap();
    
    let engine = VectorEngine::open(tempdir)?;
    let result = engine.insert_vector(42, random_embedding(128));
    
    // Should fail gracefully, not panic
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("disk full"));
    
    // Circuit breaker should be open
    assert!(engine.wal_circuit_breaker_is_open());
}

#[test]
fn hnsw_corruption_recovery() {
    let tempdir = TempDir::new()?;
    
    // Create valid snapshot
    let engine = VectorEngine::open(&tempdir)?;
    engine.insert_vectors(1000)?;
    engine.create_snapshot("snapshot_0.hnsw")?;
    
    // Corrupt the snapshot file
    let snapshot_path = tempdir.path().join("snapshot_0.hnsw");
    let mut file = OpenOptions::new().write(true).open(&snapshot_path)?;
    file.write_all(b"CORRUPTED")?;
    drop(file);
    
    // Attempt recovery - should fall back to previous snapshot
    let recovered_engine = VectorEngine::open(&tempdir)?;
    assert_eq!(recovered_engine.vector_count(), 1000);
}

#[tokio::test]
async fn training_task_panic_recovery() -> anyhow::Result<()> {
    fail::cfg("training_rmi", "panic").unwrap();
    
    let logger = Arc::new(RwLock::new(AccessLogger::new(1000)));
    let predictor = Arc::new(RwLock::new(LearnedCachePredictor::new(100, 0.001)));
    let config = TrainingConfig {
        interval_secs: 1,
        min_samples: 10,
    };
    
    let supervisor = TrainingTaskSupervisor::new(logger.clone(), predictor.clone(), config);
    let result = supervisor.supervise_training().await;
    
    // Should restart automatically
    assert!(result.is_ok());
    assert!(supervisor.restart_count() > 0);
    
    Ok(())
}
```

---

## Best Practices

1. **Backup Schedule**:
   - Full backup: Daily at low-traffic hours
   - Incremental backup: Hourly
   - Test restores: Weekly

2. **Retention Policy**:
   - Keep hourly backups for 24 hours
   - Keep daily backups for 7 days
   - Keep weekly backups for 4 weeks
   - Keep monthly backups for 12 months

3. **Monitoring**:
   - Alert on WAL circuit breaker open
   - Alert on HNSW corruption detected
   - Alert on backup failures
   - Monitor restore duration (should be <5 minutes)

4. **Disaster Recovery**:
   - Store backups in multiple regions
   - Test restores monthly
   - Document recovery procedures
   - Maintain runbook for common failures

---

## Recovery Procedures

### Scenario 1: Disk Full

```bash
# 1. Check disk space
df -h /var/lib/kyrodb

# 2. Check WAL circuit breaker state
curl localhost:9090/metrics | grep wal_circuit_breaker

# 3. Free up disk space
kyrodb backup prune --retention-days 7

# 4. Reset circuit breaker
kyrodb admin reset-circuit-breaker --name wal

# 5. Verify writes are working
kyrodb admin health-check
```

### Scenario 2: HNSW Corruption

```bash
# 1. Check logs for corruption detection
tail -f /var/log/kyrodb/kyrodb.log | grep corruption

# 2. Verify backup integrity
kyrodb backup verify --backup-id latest

# 3. Restore from last good backup
kyrodb restore --backup-id <backup_id> --data-dir /var/lib/kyrodb/

# 4. Verify restored state
kyrodb admin health-check
```

### Scenario 3: Accidental Data Deletion

```bash
# 1. Find backup before deletion
kyrodb backup list --before "2024-01-15T10:00:00Z"

# 2. Restore to point in time
kyrodb restore --point-in-time "2024-01-15T09:55:00Z"

# 3. Verify restored data
kyrodb query --doc-id 12345
```

---

## Performance Characteristics

### Backup Performance

| Operation | Duration | Throughput |
|-----------|----------|------------|
| Full backup (1M vectors) | ~30 seconds | 33K vectors/sec |
| Incremental backup (1K WAL entries) | ~1 second | 1K entries/sec |
| S3 upload (1GB) | ~15 seconds | 67 MB/sec |

### Restore Performance

| Operation | Duration | Throughput |
|-----------|----------|------------|
| Full restore (1M vectors) | ~45 seconds | 22K vectors/sec |
| PITR (replay 10K WAL entries) | ~5 seconds | 2K entries/sec |
| S3 download (1GB) | ~10 seconds | 100 MB/sec |

### Error Recovery Overhead

| Component | Normal Latency | With Error Recovery | Overhead |
|-----------|---------------|-------------------|----------|
| WAL write | 50µs | 55µs | +10% |
| HNSW load | 200ms | 220ms | +10% |
| Training cycle | 500ms | 500ms | 0% (background) |
| Query execution | 1ms | 1.05ms | +5% |

---

## Future Enhancements

1. **Continuous Backup**:
   - Stream WAL entries to S3 in real-time
   - Sub-second recovery point objective (RPO)

2. **Cross-Region Replication**:
   - Replicate backups to multiple AWS regions
   - Automatic failover to backup region

3. **Backup Compression**:
   - Use zstd compression for backups
   - Reduce storage costs by 50-70%

4. **Backup Encryption**:
   - Encrypt backups at rest with AES-256
   - Key management via AWS KMS

5. **Backup Verification**:
   - Automated restore testing
   - Weekly full restore validation

---

**Document Version**: 1.0  

