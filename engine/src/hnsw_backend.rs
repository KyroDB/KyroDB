//! HNSW Backend for Cache Integration
//!
//! Provides a clean interface for cache strategies to query HNSW index on cache miss.
//! This bridges the cache layer with the vector search layer.

use crate::hnsw_index::{HnswVectorIndex, SearchResult};
use anyhow::Result;
use parking_lot::RwLock;
use std::sync::Arc;

/// HNSW-backed document store for cache integration
///
/// This wraps HnswVectorIndex and provides:
/// - Document retrieval by ID (exact match, O(1))
/// - k-NN search by embedding (approximate, O(log n))
/// - Thread-safe concurrent access
///
/// **Usage**: Cache strategies call `fetch_document` on cache miss.
pub struct HnswBackend {
    index: Arc<RwLock<HnswVectorIndex>>,
    /// Pre-loaded embeddings for O(1) fetch by doc_id
    /// This avoids storing embeddings in HNSW graph (memory optimization)
    embeddings: Arc<Vec<Vec<f32>>>,
}

impl HnswBackend {
    /// Create new HNSW backend from pre-loaded embeddings
    ///
    /// # Parameters
    /// - `embeddings`: Pre-loaded document embeddings (indexed by doc_id)
    /// - `max_elements`: HNSW index capacity
    ///
    /// # Note
    /// This builds the HNSW index immediately, which may take time for large corpora.
    /// For production, consider lazy loading or async initialization.
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
            embeddings: Arc::new(embeddings),
        })
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
        self.embeddings.get(doc_id as usize).cloned()
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
        self.embeddings.len()
    }

    /// Check if backend is empty
    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }

    /// Get embedding dimension
    pub fn dimension(&self) -> usize {
        if self.embeddings.is_empty() {
            0
        } else {
            self.embeddings[0].len()
        }
    }

    /// Get all embeddings (for semantic adapter initialization)
    pub fn get_all_embeddings(&self) -> Arc<Vec<Vec<f32>>> {
        Arc::clone(&self.embeddings)
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
