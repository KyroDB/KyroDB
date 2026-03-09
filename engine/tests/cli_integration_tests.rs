#![cfg(feature = "cli-tools")]

use anyhow::Result;
use kyrodb_engine::backup::{BackupManager, BackupType};
use kyrodb_engine::hnsw_backend::HnswBackend;
use kyrodb_engine::metrics::MetricsCollector;
use kyrodb_engine::persistence::{FsyncPolicy, Manifest, WalReader};
use kyrodb_engine::DistanceMetric;
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn empty_metadata(n: usize) -> Vec<HashMap<String, String>> {
    vec![HashMap::new(); n]
}

fn recover_backend(
    data_dir: &Path,
    dimension: usize,
    distance: DistanceMetric,
) -> Result<HnswBackend> {
    let metrics = MetricsCollector::new();
    HnswBackend::recover(
        dimension,
        distance,
        data_dir,
        100,
        FsyncPolicy::Always,
        10,
        100 * 1024 * 1024,
        metrics,
    )
}

#[test]
fn test_cli_backup_create_full() -> Result<()> {
    let data_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    let embeddings = vec![
        vec![1.0, 0.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0, 0.0],
        vec![0.0, 0.0, 1.0, 0.0],
    ];

    let data_path = data_dir.path();

    let metadata = empty_metadata(embeddings.len());
    let backend = HnswBackend::with_persistence(
        4,
        DistanceMetric::Euclidean,
        embeddings.clone(),
        metadata,
        100,
        data_path,
        FsyncPolicy::Always,
        10,
        100 * 1024 * 1024,
    )?;
    backend.create_snapshot()?;
    let mut wal_only_metadata = HashMap::new();
    wal_only_metadata.insert("source".to_string(), "wal_only".to_string());
    backend.insert(10, vec![9.0, 9.0, 9.0, 9.0], wal_only_metadata)?;
    backend.sync_wal()?;
    let mut wal_entries_while_live = 0usize;
    let mut wal_diag_live = Vec::new();
    for entry in std::fs::read_dir(data_path)? {
        let entry = entry?;
        let wal_name = entry.file_name().to_string_lossy().to_string();
        if !wal_name.starts_with("wal_") || !wal_name.ends_with(".wal") {
            continue;
        }
        let mut reader = WalReader::open(entry.path())?;
        let entries = reader.read_all()?;
        wal_entries_while_live += entries.len();
        wal_diag_live.push((wal_name, entries.len(), reader.corrupted_entries()));
    }
    assert!(
        wal_entries_while_live > 0,
        "Insert should append at least one WAL entry before backup: {wal_diag_live:?}"
    );
    drop(backend);

    let output = Command::new(env!("CARGO_BIN_EXE_kyrodb_backup"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--backup-dir")
        .arg(backup_dir.path())
        .arg("create")
        .arg("--description")
        .arg("Test full backup")
        .output()?;

    println!("Output: {}", String::from_utf8_lossy(&output.stdout));
    println!("Error: {}", String::from_utf8_lossy(&output.stderr));

    assert!(output.status.success(), "CLI backup create should succeed");

    let backup_manager = BackupManager::new(backup_dir.path(), data_dir.path())?;
    let backups = backup_manager.list_backups()?;

    assert_eq!(backups.len(), 1, "Should have created 1 backup");
    assert!(
        matches!(backups[0].backup_type, BackupType::Full),
        "Should be full backup"
    );
    assert_eq!(
        backups[0].vector_count,
        embeddings.len() as u64,
        "Full backup should capture snapshot document count"
    );
    assert!(
        backups[0].snapshot_file.is_some(),
        "Full backup metadata should include snapshot filename"
    );
    assert!(
        backups[0].max_wal_file_id.is_some(),
        "Full backup metadata should include WAL high watermark"
    );

    Ok(())
}

#[test]
fn test_cli_backup_list() -> Result<()> {
    let data_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    let embeddings = vec![vec![1.0, 2.0, 3.0, 4.0]];
    let data_path = data_dir.path();

    let metadata = empty_metadata(embeddings.len());
    let backend = HnswBackend::with_persistence(
        4,
        DistanceMetric::Euclidean,
        embeddings,
        metadata,
        100,
        data_path,
        FsyncPolicy::Always,
        10,
        100 * 1024 * 1024,
    )?;
    backend.create_snapshot()?;
    let mut wal_only_metadata = HashMap::new();
    wal_only_metadata.insert("source".to_string(), "wal_only".to_string());
    backend.insert(10, vec![9.0, 9.0, 9.0, 9.0], wal_only_metadata)?;
    backend.sync_wal()?;
    let mut wal_entries_while_live = 0usize;
    let mut wal_diag_live = Vec::new();
    for entry in std::fs::read_dir(data_path)? {
        let entry = entry?;
        let wal_name = entry.file_name().to_string_lossy().to_string();
        if !wal_name.starts_with("wal_") || !wal_name.ends_with(".wal") {
            continue;
        }
        let mut reader = WalReader::open(entry.path())?;
        let entries = reader.read_all()?;
        wal_entries_while_live += entries.len();
        wal_diag_live.push((wal_name, entries.len(), reader.corrupted_entries()));
    }
    assert!(
        wal_entries_while_live > 0,
        "Insert should append at least one WAL entry before backup: {wal_diag_live:?}"
    );
    drop(backend);

    let backup_manager = BackupManager::new(backup_dir.path(), data_dir.path())?;
    let _metadata = backup_manager.create_full_backup("Test backup".to_string())?;

    let output = Command::new(env!("CARGO_BIN_EXE_kyrodb_backup"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--backup-dir")
        .arg(backup_dir.path())
        .arg("list")
        .arg("--format")
        .arg("table")
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Output: {}", stdout);

    assert!(output.status.success(), "CLI list should succeed");
    assert!(stdout.contains("Backup ID"), "Should contain table headers");
    assert!(stdout.contains("Full"), "Should show backup type");
    assert!(stdout.contains("Test backup"), "Should show description");

    Ok(())
}

#[test]
fn test_cli_backup_list_json() -> Result<()> {
    let data_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    let embeddings = vec![vec![1.0, 2.0, 3.0, 4.0]];
    let data_path = data_dir.path();

    let metadata = empty_metadata(embeddings.len());
    let backend = HnswBackend::with_persistence(
        4,
        DistanceMetric::Euclidean,
        embeddings,
        metadata,
        100,
        data_path,
        FsyncPolicy::Always,
        10,
        100 * 1024 * 1024,
    )?;
    backend.create_snapshot()?;
    drop(backend);

    let backup_manager = BackupManager::new(backup_dir.path(), data_dir.path())?;
    let metadata = backup_manager.create_full_backup("JSON test".to_string())?;

    let output = Command::new(env!("CARGO_BIN_EXE_kyrodb_backup"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--backup-dir")
        .arg(backup_dir.path())
        .arg("list")
        .arg("--format")
        .arg("json")
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Output: {}", stdout);

    assert!(output.status.success(), "CLI list JSON should succeed");

    let json_backups: Vec<kyrodb_engine::backup::BackupMetadata> = serde_json::from_str(&stdout)?;
    assert_eq!(json_backups.len(), 1);
    assert_eq!(json_backups[0].id, metadata.id);
    assert_eq!(json_backups[0].description, "JSON test");

    Ok(())
}

#[test]
fn test_cli_restore_from_backup() -> Result<()> {
    let data_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    let embeddings = vec![vec![1.0, 2.0, 3.0, 4.0], vec![5.0, 6.0, 7.0, 8.0]];
    let data_path = data_dir.path();

    let metadata = empty_metadata(embeddings.len());
    let backend = HnswBackend::with_persistence(
        4,
        DistanceMetric::Euclidean,
        embeddings.clone(),
        metadata,
        100,
        data_path,
        FsyncPolicy::Always,
        10,
        100 * 1024 * 1024,
    )?;
    backend.create_snapshot()?;
    let mut wal_only_metadata = HashMap::new();
    wal_only_metadata.insert("source".to_string(), "wal_only".to_string());
    backend.insert(10, vec![9.0, 9.0, 9.0, 9.0], wal_only_metadata)?;
    backend.sync_wal()?;
    let mut wal_entries_while_live = 0usize;
    let mut wal_diag_live = Vec::new();
    for entry in std::fs::read_dir(data_path)? {
        let entry = entry?;
        let wal_name = entry.file_name().to_string_lossy().to_string();
        if !wal_name.starts_with("wal_") || !wal_name.ends_with(".wal") {
            continue;
        }
        let mut reader = WalReader::open(entry.path())?;
        let entries = reader.read_all()?;
        wal_entries_while_live += entries.len();
        wal_diag_live.push((wal_name, entries.len(), reader.corrupted_entries()));
    }
    assert!(
        wal_entries_while_live > 0,
        "Insert should append at least one WAL entry before backup: {wal_diag_live:?}"
    );
    drop(backend);

    let backup_manager = BackupManager::new(backup_dir.path(), data_dir.path())?;
    let metadata = backup_manager.create_full_backup("Pre-restore backup".to_string())?;
    let mut wal_entries_before_restore = 0usize;
    let mut wal_diag_before = Vec::new();
    for entry in std::fs::read_dir(data_dir.path())? {
        let entry = entry?;
        let wal_name = entry.file_name().to_string_lossy().to_string();
        if !wal_name.starts_with("wal_") || !wal_name.ends_with(".wal") {
            continue;
        }
        let wal_path = entry.path();
        let mut reader = WalReader::open(&wal_path)?;
        let entries = reader.read_all()?;
        wal_entries_before_restore += entries.len();
        wal_diag_before.push((wal_name, entries.len(), reader.corrupted_entries()));
    }

    std::fs::remove_dir_all(data_dir.path())?;
    std::fs::create_dir_all(data_dir.path())?;

    let output = Command::new(env!("CARGO_BIN_EXE_kyrodb_backup"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--backup-dir")
        .arg(backup_dir.path())
        .arg("restore")
        .arg("--backup-id")
        .arg(metadata.id.to_string())
        .output()?;

    println!("Output: {}", String::from_utf8_lossy(&output.stdout));
    println!("Error: {}", String::from_utf8_lossy(&output.stderr));

    assert!(output.status.success(), "CLI restore should succeed");
    let manifest_after_restore = Manifest::load(data_dir.path().join("MANIFEST"))?;
    let mut wal_entries_after_restore = 0usize;
    let mut wal_diag_after = Vec::new();
    for wal_segment in &manifest_after_restore.wal_segments {
        let wal_path = data_dir.path().join(wal_segment);
        let mut reader = WalReader::open(&wal_path)?;
        let entries = reader.read_all()?;
        wal_entries_after_restore += entries.len();
        wal_diag_after.push((
            wal_segment.clone(),
            entries.len(),
            reader.corrupted_entries(),
        ));
    }
    assert!(
        wal_entries_after_restore > 0,
        "Restored data should include replayable WAL entries: live={wal_diag_live:?} before={wal_diag_before:?} after={wal_diag_after:?}",
    );
    assert!(
        wal_entries_after_restore >= wal_entries_before_restore,
        "Restored WAL entries should not regress: live={wal_diag_live:?} before={wal_diag_before:?} after={wal_diag_after:?}",
    );
    let recovered = recover_backend(data_dir.path(), 4, DistanceMetric::Euclidean)?;
    assert_eq!(
        recovered.len(),
        3,
        "Recovered engine should include snapshot and WAL-only inserts; live={wal_diag_live:?} before={wal_diag_before:?} after={wal_diag_after:?}"
    );
    assert_eq!(
        recovered.fetch_document(10),
        Some(vec![9.0, 9.0, 9.0, 9.0]),
        "WAL-only insert should be replayed after restore"
    );

    let nearest = recovered.knn_search(&[9.0, 9.0, 9.0, 9.0], 1)?;
    assert_eq!(nearest[0].doc_id, 10, "Restored index should be searchable");

    Ok(())
}

#[test]
fn test_cli_prune_backups() -> Result<()> {
    let data_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    let embeddings = vec![vec![1.0, 2.0, 3.0, 4.0]];
    let data_path = data_dir.path();

    let metadata = empty_metadata(embeddings.len());
    let backend = HnswBackend::with_persistence(
        4,
        DistanceMetric::Euclidean,
        embeddings,
        metadata,
        100,
        data_path,
        FsyncPolicy::Always,
        10,
        100 * 1024 * 1024,
    )?;
    backend.create_snapshot()?;
    drop(backend);

    let backup_manager = BackupManager::new(backup_dir.path(), data_dir.path())?;

    // Create backups with timestamps spanning multiple days to test retention policy
    // This ensures backups fall into different daily/weekly/monthly buckets
    for i in 0..10 {
        let metadata = backup_manager.create_full_backup(format!("Backup {}", i))?;

        // Adjust timestamps to simulate backups from different days
        // i=0: today, i=1-2: yesterday, i=3-4: last week, i=5+: last month
        let days_ago = match i {
            0 => 0,     // today
            1..=2 => 1, // yesterday
            3..=4 => 7, // last week
            _ => 30,    // last month
        };
        let adjusted_timestamp = metadata.timestamp - (days_ago * 86400); // Subtract days

        // Update the metadata file with adjusted timestamp
        let metadata_path = backup_dir
            .path()
            .join(format!("backup_{}.json", metadata.id));
        let mut metadata_content: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&metadata_path)?)?;
        metadata_content["timestamp"] = adjusted_timestamp.into();

        std::fs::write(
            metadata_path,
            serde_json::to_string_pretty(&metadata_content)?,
        )?;

        // Small delay to ensure different IDs
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let output = Command::new(env!("CARGO_BIN_EXE_kyrodb_backup"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--backup-dir")
        .arg(backup_dir.path())
        .arg("prune")
        .arg("--keep-daily")
        .arg("3")
        .arg("--keep-weekly")
        .arg("2")
        .arg("--keep-monthly")
        .arg("1")
        .output()?;

    println!("Output: {}", String::from_utf8_lossy(&output.stdout));

    assert!(output.status.success(), "CLI prune should succeed");

    let backups = backup_manager.list_backups()?;

    // Verify retention policy is correctly applied
    // With keep-daily=3, keep-weekly=2, keep-monthly=1, we expect at most 6 backups (3+2+1)
    // though the actual number depends on the time distribution of backups
    assert!(
        backups.len() <= 6,
        "Should respect retention policy (found {} backups, expected <= 6)",
        backups.len()
    );

    assert!(
        backups.len() >= 3,
        "Should keep at least daily backups (found {} backups, expected >= 3)",
        backups.len()
    );

    assert!(
        backups.len() < 10,
        "Should have pruned some backups (found {} backups, started with 10)",
        backups.len()
    );

    Ok(())
}

#[test]
fn test_cli_verify_backup() -> Result<()> {
    let data_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    let embeddings = vec![vec![1.0, 2.0, 3.0, 4.0]];
    let data_path = data_dir.path();

    let metadata = empty_metadata(embeddings.len());
    let backend = HnswBackend::with_persistence(
        4,
        DistanceMetric::Euclidean,
        embeddings,
        metadata,
        100,
        data_path,
        FsyncPolicy::Always,
        10,
        100 * 1024 * 1024,
    )?;
    backend.create_snapshot()?;
    drop(backend);

    let backup_manager = BackupManager::new(backup_dir.path(), data_dir.path())?;
    let metadata = backup_manager.create_full_backup("Verification test".to_string())?;

    let output = Command::new(env!("CARGO_BIN_EXE_kyrodb_backup"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--backup-dir")
        .arg(backup_dir.path())
        .arg("verify")
        .arg(metadata.id.to_string())
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("Output: {}", stdout);
    println!("Error: {}", stderr);

    assert!(output.status.success(), "CLI verify should succeed");
    assert!(
        stdout.contains("is valid"),
        "Verify output should confirm backup validity"
    );
    assert!(
        stdout.contains("Checksum"),
        "Verify output should include checksum details"
    );

    Ok(())
}

#[test]
fn test_cli_incremental_backup() -> Result<()> {
    let data_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    let embeddings = vec![vec![1.0, 2.0, 3.0, 4.0]];
    let data_path = data_dir.path();

    let metadata = empty_metadata(embeddings.len());
    let backend = HnswBackend::with_persistence(
        4,
        DistanceMetric::Euclidean,
        embeddings,
        metadata,
        100,
        data_path,
        FsyncPolicy::Always,
        10,
        100 * 1024 * 1024,
    )?;
    backend.sync_wal()?;

    let output_full = Command::new(env!("CARGO_BIN_EXE_kyrodb_backup"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--backup-dir")
        .arg(backup_dir.path())
        .arg("create")
        .arg("--description")
        .arg("Full backup")
        .output()?;

    assert!(
        output_full.status.success(),
        "Full backup creation should succeed"
    );

    let backup_manager = BackupManager::new(backup_dir.path(), data_dir.path())?;
    let backups = backup_manager.list_backups()?;
    let reference_id = backups[0].id;

    backend.insert(1, vec![5.0, 6.0, 7.0, 8.0], HashMap::new())?;
    backend.sync_wal()?;

    let output_inc = Command::new(env!("CARGO_BIN_EXE_kyrodb_backup"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--backup-dir")
        .arg(backup_dir.path())
        .arg("create")
        .arg("--incremental")
        .arg("--reference")
        .arg(reference_id.to_string())
        .arg("--description")
        .arg("Incremental backup")
        .output()?;

    let stdout = String::from_utf8_lossy(&output_inc.stdout);
    println!("Incremental output: {}", stdout);

    assert!(
        output_inc.status.success(),
        "Incremental backup creation should succeed"
    );
    assert!(
        stdout.contains("Incremental"),
        "Should confirm incremental backup"
    );

    let backups = backup_manager.list_backups()?;
    assert_eq!(backups.len(), 2, "Should have 2 backups now");
    let full_backup = backups
        .iter()
        .find(|b| b.backup_type == BackupType::Full)
        .expect("full backup should exist");
    let incremental_backup = backups
        .iter()
        .find(|b| b.backup_type == BackupType::Incremental)
        .expect("incremental backup should exist");
    assert_eq!(
        incremental_backup.parent_id,
        Some(reference_id),
        "Incremental backup should reference full backup"
    );
    assert!(
        incremental_backup.max_wal_file_id.is_some(),
        "Incremental metadata should capture WAL high watermark"
    );
    assert!(
        incremental_backup.max_wal_file_id.unwrap_or(0) >= full_backup.max_wal_file_id.unwrap_or(0),
        "Incremental WAL watermark should not regress"
    );

    Ok(())
}

#[test]
fn test_cli_point_in_time_restore() -> Result<()> {
    let data_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    let embeddings = vec![vec![1.0, 2.0, 3.0, 4.0]];
    let data_path = data_dir.path();

    let metadata = empty_metadata(embeddings.len());
    let backend = HnswBackend::with_persistence(
        4,
        DistanceMetric::Euclidean,
        embeddings,
        metadata,
        100,
        data_path,
        FsyncPolicy::Always,
        10,
        100 * 1024 * 1024,
    )?;
    backend.sync_wal()?;

    let backup_manager = BackupManager::new(backup_dir.path(), data_dir.path())?;
    let metadata = backup_manager.create_full_backup("PITR test".to_string())?;
    let pitr_timestamp = metadata.timestamp;

    std::thread::sleep(std::time::Duration::from_secs(1));

    backend.insert(1, vec![5.0, 6.0, 7.0, 8.0], HashMap::new())?;
    backend.sync_wal()?;
    drop(backend);

    std::fs::remove_dir_all(data_dir.path())?;
    std::fs::create_dir_all(data_dir.path())?;

    let output = Command::new(env!("CARGO_BIN_EXE_kyrodb_backup"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--backup-dir")
        .arg(backup_dir.path())
        .arg("restore")
        .arg("--point-in-time")
        .arg(pitr_timestamp.to_string())
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("PITR output: {}", stdout);

    assert!(
        output.status.success(),
        "Point-in-time restore should succeed"
    );
    assert!(
        stdout.contains("point-in-time"),
        "Should confirm PITR restore"
    );

    let recovered = recover_backend(data_dir.path(), 4, DistanceMetric::Euclidean)?;
    assert_eq!(
        recovered.len(),
        1,
        "PITR restore should only include data at or before target timestamp"
    );
    assert!(
        recovered.fetch_document(1).is_none(),
        "Post-cutoff write should not appear after PITR restore"
    );
    assert_eq!(
        recovered.fetch_document(0),
        Some(vec![1.0, 2.0, 3.0, 4.0]),
        "Base snapshot document should remain after PITR restore"
    );

    Ok(())
}
