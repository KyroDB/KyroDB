//! HNSW Backend for Cache Integration
//!
//! Provides a clean interface for cache strategies to query HNSW index on cache miss.
//! This bridges the cache layer with the vector search layer.
//!
//! **Persistence**: WAL + snapshots for durability and fast recovery.

use crate::hnsw_index::{HnswVectorIndex, SearchResult};
use crate::persistence::{FsyncPolicy, Manifest, Snapshot, WalEntry, WalOp, WalReader, WalWriter};
use anyhow::{Context, Result};
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    fsync_policy: FsyncPolicy,
    inserts_since_snapshot: Arc<RwLock<usize>>,
    snapshot_interval: usize, // Create snapshot every N inserts
}

impl HnswBackend {
    /// Create new HNSW backend from pre-loaded embeddings (no persistence)
    ///
    /// # Parameters
    /// - `embeddings`: Pre-loaded document embeddings (indexed by doc_id)
    /// - `max_elements`: HNSW index capacity
    ///
    /// # Note
    /// This builds the HNSW index immediately, which may take time for large corpora.
    /// For production with persistence, use `with_persistence` or `recover`.
    pub fn new(embeddings: Vec<Vec<f32>>, max_elements: usize) -> Result<Self> {
        if embeddings.is_empty() {
            anyhow::bail!("Cannot create HnswBackend with empty embeddings");
        }

        let dimension = embeddings[0].len();
        let mut index = HnswVectorIndex::new(dimension, max_elements)?;

        // Build HNSW index from embeddings
        println!("Building HNSW index for {} documents...", embeddings.len());
        for (doc_id, embedding) in embeddings.iter().enumerate() {
            index.add_vector(doc_id as u64, embedding)?;
        }
        println!("HNSW index built successfully");

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
        std::fs::create_dir_all(&data_dir)
            .context("Failed to create data directory")?;

        let dimension = embeddings[0].len();
        let mut index = HnswVectorIndex::new(dimension, max_elements)?;

        // Build HNSW index
        println!("Building HNSW index for {} documents...", embeddings.len());
        for (doc_id, embedding) in embeddings.iter().enumerate() {
            index.add_vector(doc_id as u64, embedding)?;
        }
        println!("HNSW index built successfully");

        // Create initial WAL
        let wal_path = data_dir.join(format!("wal_{}.wal", Self::timestamp()));
        let wal = WalWriter::create(&wal_path, fsync_policy)?;
        
        // Update manifest
        let mut manifest = Manifest::load_or_create(data_dir.join("MANIFEST"))?;
        manifest.wal_segments.push(wal_path.file_name().unwrap().to_string_lossy().to_string());
        manifest.save(data_dir.join("MANIFEST"))?;

        let persistence = PersistenceState {
            data_dir,
            wal: Arc::new(RwLock::new(wal)),
            fsync_policy,
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
    /// 2. Load latest snapshot (if exists)
    /// 3. Replay WAL entries since snapshot
    /// 4. Rebuild HNSW index
    /// 5. Create new active WAL
    pub fn recover(
        data_dir: impl AsRef<Path>,
        max_elements: usize,
        fsync_policy: FsyncPolicy,
        snapshot_interval: usize,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        
        println!("Recovering HnswBackend from {}...", data_dir.display());
        
        // Load manifest
        let manifest_path = data_dir.join("MANIFEST");
        if !manifest_path.exists() {
            anyhow::bail!("No MANIFEST found in {}", data_dir.display());
        }
        
        let manifest = Manifest::load(&manifest_path)?;
        
        // Load snapshot (if exists)
        let mut embeddings: Vec<Vec<f32>> = Vec::new();
        let mut dimension = 0;
        
        if let Some(snapshot_name) = &manifest.latest_snapshot {
            let snapshot_path = data_dir.join(snapshot_name);
            println!("Loading snapshot from {}...", snapshot_path.display());
            
            let snapshot = Snapshot::load(&snapshot_path)?;
            dimension = snapshot.dimension;
            embeddings = snapshot.documents.into_iter().map(|(_, emb)| emb).collect();
            
            println!("Loaded {} documents from snapshot", embeddings.len());
        }
        
        // Replay WAL segments
        for wal_name in &manifest.wal_segments {
            let wal_path = data_dir.join(wal_name);
            if !wal_path.exists() {
                eprintln!("WARNING: WAL segment {} not found, skipping", wal_name);
                continue;
            }
            
            println!("Replaying WAL {}...", wal_name);
            let mut reader = WalReader::open(&wal_path)?;
            let entries = reader.read_all()?;
            
            for entry in entries {
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
            
            println!("Replayed {} valid entries ({} corrupted)", 
                     reader.valid_entries(), reader.corrupted_entries());
        }
        
        if embeddings.is_empty() {
            anyhow::bail!("Recovery failed: no data found in snapshots or WAL");
        }
        
        // Rebuild HNSW index
        let mut index = HnswVectorIndex::new(dimension, max_elements)?;
        println!("Rebuilding HNSW index from {} documents...", embeddings.len());
        
        for (doc_id, embedding) in embeddings.iter().enumerate() {
            // Skip tombstones (all zeros)
            if embedding.iter().all(|&x| x == 0.0) {
                continue;
            }
            index.add_vector(doc_id as u64, embedding)?;
        }
        
        println!("HNSW index rebuilt successfully");
        
        // Create new active WAL
        let wal_path = data_dir.join(format!("wal_{}.wal", Self::timestamp()));
        let wal = WalWriter::create(&wal_path, fsync_policy)?;
        
        // Update manifest
        let mut manifest = manifest;
        manifest.wal_segments.push(wal_path.file_name().unwrap().to_string_lossy().to_string());
        manifest.save(&manifest_path)?;
        
        let persistence = PersistenceState {
            data_dir,
            wal: Arc::new(RwLock::new(wal)),
            fsync_policy,
            inserts_since_snapshot: Arc::new(RwLock::new(0)),
            snapshot_interval,
        };
        
        println!("Recovery complete: {} documents", embeddings.len());
        
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
    pub fn insert(&self, doc_id: u64, embedding: Vec<f32>) -> Result<()> {
        // Log to WAL (if persistence enabled)
        if let Some(ref persistence) = self.persistence {
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
                println!("Snapshot interval reached ({} inserts), creating snapshot...", *inserts);
                drop(inserts); // Release lock before snapshot
                self.create_snapshot()?;
            }
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
    pub fn create_snapshot(&self) -> Result<()> {
        let persistence = self.persistence.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Persistence not enabled"))?;
        
        let embeddings = self.embeddings.read();
        
        // Create snapshot object
        let documents: Vec<(u64, Vec<f32>)> = embeddings
            .iter()
            .enumerate()
            .filter(|(_, emb)| !emb.iter().all(|&x| x == 0.0)) // Skip tombstones
            .map(|(id, emb)| (id as u64, emb.clone()))
            .collect();
        
        let dimension = if documents.is_empty() {
            0
        } else {
            documents[0].1.len()
        };
        
        let snapshot = Snapshot::new(dimension, documents);
        
        // Save snapshot
        let snapshot_name = format!("snapshot_{}.snap", Self::timestamp());
        let snapshot_path = persistence.data_dir.join(&snapshot_name);
        
        snapshot.save(&snapshot_path)?;
        println!("Snapshot saved to {}", snapshot_path.display());
        
        // Update manifest
        let manifest_path = persistence.data_dir.join("MANIFEST");
        let mut manifest = Manifest::load_or_create(&manifest_path)?;
        manifest.latest_snapshot = Some(snapshot_name);
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
    pub fn knn_search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
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
}
