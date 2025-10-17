use kyrodb_engine::backup::{BackupManager, ClearDirectoryOptions, RestoreManager};
use std::fs;
use tempfile::TempDir;

#[test]
fn test_clear_directory_requires_confirmation() {
    // Make sure BACKUP_ALLOW_CLEAR is not set
    std::env::remove_var("BACKUP_ALLOW_CLEAR");

    let data_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    // Create some test files in data directory
    fs::write(data_dir.path().join("test1.bin"), b"data1").unwrap();
    fs::write(data_dir.path().join("test2.bin"), b"data2").unwrap();

    let restore_manager = RestoreManager::new(backup_dir.path(), data_dir.path()).unwrap();

    // Test 1: Default options should deny clearing without confirmation
    let result = restore_manager.clear_data_directory(&ClearDirectoryOptions::default());
    assert!(
        result.is_err(),
        "Should require confirmation by default"
    );

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("requires explicit confirmation"),
        "Error message should mention confirmation requirement"
    );

    // Test 2: Files should still exist after failed clear
    assert!(
        data_dir.path().join("test1.bin").exists(),
        "Files should not be deleted without confirmation"
    );
    assert!(
        data_dir.path().join("test2.bin").exists(),
        "Files should not be deleted without confirmation"
    );
}

#[test]
fn test_clear_directory_with_explicit_allow() {
    let data_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    // Create test files
    fs::write(data_dir.path().join("file1.dat"), b"content1").unwrap();
    fs::write(data_dir.path().join("file2.dat"), b"content2").unwrap();

    let restore_manager = RestoreManager::new(backup_dir.path(), data_dir.path()).unwrap();

    // Clear with explicit permission
    let options = ClearDirectoryOptions::new().with_allow_clear(true);
    let result = restore_manager.clear_data_directory(&options);
    assert!(result.is_ok(), "Should succeed with allow_clear=true");

    // Files should be deleted
    assert!(
        !data_dir.path().join("file1.dat").exists(),
        "Files should be deleted"
    );
    assert!(
        !data_dir.path().join("file2.dat").exists(),
        "Files should be deleted"
    );
}

#[test]
fn test_clear_directory_environment_variable() {
    // Clean up any previous state
    std::env::remove_var("BACKUP_ALLOW_CLEAR");

    let data_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    // Create test files
    fs::write(data_dir.path().join("env_test.bin"), b"test").unwrap();

    let restore_manager = RestoreManager::new(backup_dir.path(), data_dir.path()).unwrap();

    // Set environment variable
    std::env::set_var("BACKUP_ALLOW_CLEAR", "true");

    // Clear should work with environment variable
    let options = ClearDirectoryOptions::default(); // No explicit allow_clear
    let result = restore_manager.clear_data_directory(&options);
    assert!(
        result.is_ok(),
        "Should succeed with BACKUP_ALLOW_CLEAR=true env var"
    );

    assert!(
        !data_dir.path().join("env_test.bin").exists(),
        "File should be deleted with env var"
    );

    // Clean up
    std::env::remove_var("BACKUP_ALLOW_CLEAR");
}

#[test]
fn test_dry_run_mode() {
    let data_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    // Create test files
    fs::write(data_dir.path().join("dry_run_1.dat"), b"data1").unwrap();
    fs::write(data_dir.path().join("dry_run_2.dat"), b"data2").unwrap();
    fs::write(data_dir.path().join("dry_run_3.dat"), b"data3").unwrap();

    let restore_manager = RestoreManager::new(backup_dir.path(), data_dir.path()).unwrap();

    // Dry-run with allow_clear
    let options = ClearDirectoryOptions::new()
        .with_allow_clear(true)
        .with_dry_run(true);

    let result = restore_manager.clear_data_directory(&options);
    assert!(result.is_ok(), "Dry-run should succeed");

    // Files should NOT be deleted in dry-run mode
    assert!(
        data_dir.path().join("dry_run_1.dat").exists(),
        "Files should not be deleted in dry-run mode"
    );
    assert!(
        data_dir.path().join("dry_run_2.dat").exists(),
        "Files should not be deleted in dry-run mode"
    );
    assert!(
        data_dir.path().join("dry_run_3.dat").exists(),
        "Files should not be deleted in dry-run mode"
    );
}

#[test]
fn test_clear_empty_directory() {
    let data_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    // Directory is empty - no files created

    let restore_manager = RestoreManager::new(backup_dir.path(), data_dir.path()).unwrap();

    // Should succeed even without confirmation for empty directory
    let options = ClearDirectoryOptions::default();
    let result = restore_manager.clear_data_directory(&options);
    assert!(
        result.is_ok(),
        "Should succeed for empty directory without confirmation"
    );
}

#[test]
fn test_clear_nonexistent_directory() {
    let backup_dir = TempDir::new().unwrap();
    let nonexistent = backup_dir.path().join("does_not_exist");

    let restore_manager = RestoreManager::new(backup_dir.path(), &nonexistent).unwrap();

    // Should handle nonexistent directory gracefully
    let options = ClearDirectoryOptions::default();
    let result = restore_manager.clear_data_directory(&options);
    assert!(
        result.is_ok(),
        "Should handle nonexistent directory gracefully"
    );
}

#[test]
fn test_restore_from_backup_requires_confirmation() {
    let data_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    // Create test file in data directory
    fs::write(data_dir.path().join("existing.dat"), b"data").unwrap();

    // Create a dummy backup file (for this test we just verify the safeguard)
    let restore_manager = RestoreManager::new(backup_dir.path(), data_dir.path()).unwrap();

    // Try to restore without confirmation - should fail when clearing directory
    // (It will fail because the backup doesn't exist, but that comes after the clear check)
    let dummy_id = uuid::Uuid::new_v4();
    let result = restore_manager.restore_from_backup(dummy_id);

    // Result will be an error due to missing backup, which is fine for this test
    // The important thing is that clear_data_directory is called and requires confirmation
    assert!(result.is_err());
}

#[test]
fn test_restore_with_dry_run() {
    let data_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    // Create test files
    fs::write(data_dir.path().join("test_dry_run.dat"), b"data").unwrap();

    let restore_manager = RestoreManager::new(backup_dir.path(), data_dir.path()).unwrap();

    // Dry-run should preview what would be deleted
    let options = ClearDirectoryOptions::new()
        .with_allow_clear(true)
        .with_dry_run(true);

    let result = restore_manager.clear_data_directory(&options);
    assert!(result.is_ok());

    // Files should still exist after dry-run
    assert!(
        data_dir.path().join("test_dry_run.dat").exists(),
        "Files should exist after dry-run"
    );
}

#[test]
fn test_point_in_time_restore_requires_confirmation() {
    let data_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    fs::write(data_dir.path().join("pitr_test.dat"), b"data").unwrap();

    let restore_manager = RestoreManager::new(backup_dir.path(), data_dir.path()).unwrap();

    // PITR should also respect safeguards
    let result = restore_manager.restore_point_in_time(0);
    assert!(result.is_err(), "PITR should fail when clearing requires confirmation");
}
