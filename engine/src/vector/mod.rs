//! Phase B.1: Native Vector Storage Module
//! 
//! This module provides high-performance vector storage and similarity search
//! using a native HNSW (Hierarchical Navigable Small World) implementation
//! with SIMD-optimized distance computation.

pub mod distance;
pub mod hnsw;
pub mod storage;

pub use distance::{DistanceFunction, SIMDDistance};
pub use hnsw::{HnswIndex, SearchResult};
pub use storage::{VectorStorage, VectorRecord};

use crate::schema::{DistanceMetric, Document};
use anyhow::Result;
use std::sync::Arc;

/// Vector query parameters for similarity search
#[derive(Debug, Clone)]
pub struct VectorQuery {
    /// Query vector
    pub vector: Vec<f32>,
    /// Number of nearest neighbors to return
    pub k: usize,
    /// Distance metric to use
    pub distance_metric: DistanceMetric,
    /// Search parameter (higher = more accurate, slower)
    pub ef: Option<usize>,
    /// Similarity threshold filter
    pub similarity_threshold: Option<f32>,
}

/// Vector search result
#[derive(Debug, Clone, serde::Serialize)]
pub struct VectorSearchResult {
    /// Document ID
    pub id: u64,
    /// Distance/similarity score
    pub score: f32,
    /// Optional document metadata
    pub document: Option<Document>,
}

/// High-level vector index interface for Phase B.1
pub trait VectorIndex: Send + Sync {
    /// Insert a vector with associated document ID
    fn insert(&mut self, id: u64, vector: Vec<f32>) -> Result<()>;
    
    /// Remove a vector by document ID
    fn remove(&mut self, id: u64) -> Result<bool>;
    
    /// Search for k nearest neighbors
    fn search(&self, query: &VectorQuery) -> Result<Vec<VectorSearchResult>>;
    
    /// Get the number of vectors in the index
    fn len(&self) -> usize;
    
    /// Check if the index is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Get index statistics
    fn stats(&self) -> IndexStats;
}

/// Index performance and memory statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexStats {
    /// Number of vectors indexed
    pub vector_count: usize,
    /// Vector dimension
    pub dimension: usize,
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// Number of levels in HNSW graph
    pub levels: usize,
    /// Average connections per node
    pub avg_connections: f32,
    /// Last search statistics
    pub last_search: Option<SearchStats>,
}

/// Search performance statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchStats {
    /// Search duration in microseconds
    pub duration_micros: u64,
    /// Number of distance calculations performed
    pub distance_calculations: usize,
    /// Number of nodes visited
    pub nodes_visited: usize,
    /// Effective search parameter used
    pub ef_used: usize,
}
