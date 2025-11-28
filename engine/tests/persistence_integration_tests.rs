//! Integration tests for WAL + snapshot persistence
//!
//! Tests:
//! - WAL write/read cycle
//! - Snapshot creation and loading
//! - Crash recovery (snapshot + WAL replay)
//! - Durability guarantees with different fsync policies

use kyrodb_engine::{
    FsyncPolicy, HnswBackend, Manifest, Snapshot, WalEntry, WalOp, WalReader, WalWriter,
};
use std::collections::HashMap;
use tempfile::TempDir;

#[test]
fn test_wal_basic_operations() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("test.wal");

    // Write entries
    let mut writer = WalWriter::create(&wal_path, FsyncPolicy::Always).unwrap();

    let entry1 = WalEntry {
        op: WalOp::Insert,
        doc_id: 42,
        embedding: vec![0.1, 0.2, 0.3, 0.4],
        timestamp: 1000,
        metadata: HashMap::new(),
    };

    let entry2 = WalEntry {
        op: WalOp::Insert,
        doc_id: 99,
        embedding: vec![0.5, 0.6, 0.7, 0.8],
        timestamp: 2000,
        metadata: HashMap::new(),
    };

    writer.append(&entry1).unwrap();
    writer.append(&entry2).unwrap();

    assert_eq!(writer.entry_count(), 2);

    drop(writer);

    // Read entries
    let mut reader = WalReader::open(&wal_path).unwrap();
    let entries = reader.read_all().unwrap();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].doc_id, 42);
    assert_eq!(entries[0].embedding, vec![0.1, 0.2, 0.3, 0.4]);
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
        (0, vec![1.0, 0.0, 0.0, 0.0]),
        (1, vec![0.0, 1.0, 0.0, 0.0]),
        (2, vec![0.0, 0.0, 1.0, 0.0]),
    ];

    let metadata = documents
        .iter()
        .map(|(id, _)| (*id, HashMap::new()))
        .collect();
    let snapshot = Snapshot::new(4, documents, metadata).unwrap();
    snapshot.save(&snapshot_path).unwrap();

    // Load snapshot
    let loaded = Snapshot::load(&snapshot_path).unwrap();

    assert_eq!(loaded.dimension, 4);
    assert_eq!(loaded.doc_count, 3);
    assert_eq!(loaded.documents.len(), 3);
    assert_eq!(loaded.metadata.len(), 3);
    // Metadata is a Vec, check length matches
    assert!(loaded.metadata.len() >= 3);
    assert_eq!(loaded.documents[0].0, 0);
    assert_eq!(loaded.documents[0].1, vec![1.0, 0.0, 0.0, 0.0]);
}

#[test]
fn test_hnsw_backend_persistence() {
    let dir = TempDir::new().unwrap();

    // Create backend with persistence
    let initial_embeddings = vec![vec![1.0, 0.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]];

    let backend = HnswBackend::with_persistence(
        initial_embeddings.clone(),
        vec![HashMap::new(); 2],
        100,
        dir.path(),
        FsyncPolicy::Always,
        10, // Snapshot every 10 inserts
    )
    .unwrap();

    // Insert new documents
    backend
        .insert(2, vec![0.0, 0.0, 1.0, 0.0], HashMap::new())
        .unwrap();
    backend
        .insert(3, vec![0.0, 0.0, 0.0, 1.0], HashMap::new())
        .unwrap();

    // Verify documents exist
    let doc2 = backend.fetch_document(2).unwrap();
    assert_eq!(doc2, vec![0.0, 0.0, 1.0, 0.0]);

    // Create snapshot manually
    backend.create_snapshot().unwrap();

    drop(backend);

    // Recover from persistence with metrics
    let metrics = kyrodb_engine::metrics::MetricsCollector::new();
    let recovered =
        HnswBackend::recover(dir.path(), 100, FsyncPolicy::Always, 10, metrics.clone()).unwrap();

    // Verify all documents recovered
    assert_eq!(recovered.len(), 4);

    let doc0 = recovered.fetch_document(0).unwrap();
    assert_eq!(doc0, vec![1.0, 0.0, 0.0, 0.0]);

    let doc2 = recovered.fetch_document(2).unwrap();
    assert_eq!(doc2, vec![0.0, 0.0, 1.0, 0.0]);

    let doc3 = recovered.fetch_document(3).unwrap();
    assert_eq!(doc3, vec![0.0, 0.0, 0.0, 1.0]);

    // Verify metrics were collected during recovery
    // Recovery should not record errors if successful
    assert_eq!(
        metrics.get_hnsw_corruption_count(),
        0,
        "No corruption should be detected during clean recovery"
    );
}

#[test]
fn test_crash_recovery_wal_only() {
    let dir = TempDir::new().unwrap();

    // Initial setup: No snapshot, only WAL
    let initial_embeddings = vec![vec![1.0, 0.0], vec![0.0, 1.0]];

    {
        let backend = HnswBackend::with_persistence(
            initial_embeddings.clone(),
            vec![HashMap::new(); 2],
            100,
            dir.path(),
            FsyncPolicy::Always,
            1000, // High threshold to avoid snapshot
        )
        .unwrap();

        // Insert without snapshot
        backend.insert(2, vec![0.5, 0.5], HashMap::new()).unwrap();

        // Simulate crash (drop without clean shutdown)
    }

    // Recover from WAL
    let metrics = kyrodb_engine::metrics::MetricsCollector::new();
    let recovered =
        HnswBackend::recover(dir.path(), 100, FsyncPolicy::Always, 1000, metrics).unwrap();

    // Verify data recovered from WAL
    assert_eq!(recovered.len(), 3);
    let doc2 = recovered.fetch_document(2).unwrap();
    assert_eq!(doc2, vec![0.5, 0.5]);
}

#[test]
fn test_automatic_snapshot_creation() {
    let dir = TempDir::new().unwrap();

    let initial_embeddings = vec![vec![1.0, 0.0]];

    let backend = HnswBackend::with_persistence(
        initial_embeddings,
        vec![HashMap::new(); 1],
        100,
        dir.path(),
        FsyncPolicy::Always,
        3, // Snapshot every 3 inserts
    )
    .unwrap();

    // Insert 3 documents (should trigger snapshot)
    backend.insert(1, vec![0.9, 0.1], HashMap::new()).unwrap();
    backend.insert(2, vec![0.8, 0.2], HashMap::new()).unwrap();
    backend.insert(3, vec![0.7, 0.3], HashMap::new()).unwrap();

    // Check that snapshot was created
    let manifest_path = dir.path().join("MANIFEST");
    let manifest = Manifest::load(&manifest_path).unwrap();

    assert!(manifest.latest_snapshot.is_some());

    let snapshot_name = manifest.latest_snapshot.unwrap();
    let snapshot_path = dir.path().join(&snapshot_name);
    assert!(snapshot_path.exists());
}

#[test]
fn test_knn_search_after_recovery() {
    let dir = TempDir::new().unwrap();

    {
        let initial_embeddings = vec![
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.9, 0.1, 0.0, 0.0],
            vec![0.0, 0.0, 1.0, 0.0],
        ];

        let backend = HnswBackend::with_persistence(
            initial_embeddings,
            vec![HashMap::new(); 3],
            100,
            dir.path(),
            FsyncPolicy::Always,
            10,
        )
        .unwrap();

        backend.create_snapshot().unwrap();
    }

    // Recover and test k-NN search
    let metrics = kyrodb_engine::metrics::MetricsCollector::new();
    let recovered =
        HnswBackend::recover(dir.path(), 100, FsyncPolicy::Always, 10, metrics).unwrap();

    // Query closest to doc 0
    let query = vec![1.0, 0.0, 0.0, 0.0];
    let results = recovered.knn_search(&query, 2).unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].doc_id, 0); // Exact match
    assert_eq!(results[1].doc_id, 1); // Similar (0.9, 0.1, ...)
}

#[test]
fn test_fsync_policy_never() {
    let dir = TempDir::new().unwrap();

    // Should work even without fsync (but no durability guarantee)
    let initial_embeddings = vec![vec![1.0, 0.0]];

    let backend = HnswBackend::with_persistence(
        initial_embeddings,
        vec![HashMap::new(); 1],
        100,
        dir.path(),
        FsyncPolicy::Never,
        5,
    )
    .unwrap();

    backend.insert(1, vec![0.9, 0.1], HashMap::new()).unwrap();
    backend.create_snapshot().unwrap();

    // Recovery should still work
    drop(backend);

    let metrics = kyrodb_engine::metrics::MetricsCollector::new();
    let recovered = HnswBackend::recover(dir.path(), 100, FsyncPolicy::Never, 5, metrics).unwrap();

    assert_eq!(recovered.len(), 2);
}
