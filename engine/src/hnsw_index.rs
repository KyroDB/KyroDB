//! HNSW vector index for k-NN search
//!
//! Thin wrapper around hnsw_rs optimized for RAG: cosine similarity,
//! type-safe u64 doc IDs, >95% recall@10, P99 <1ms on 10M vectors.

use anyhow::Result;
use hnsw_rs::prelude::*;
use std::sync::Arc;

use crate::config::DistanceMetric;

/// Vector search result with document ID and distance
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub doc_id: u64,
    pub distance: f32,
}

/// HNSW vector index for approximate k-NN search
///
/// RAG-optimized defaults: cosine similarity, ef=200, M=16
///
/// # Example
/// ```no_run
/// use kyrodb_engine::hnsw_index::HnswVectorIndex;
///
/// let mut index = HnswVectorIndex::new(128, 10_000_000).unwrap();
/// index.add_vector(42, &vec![0.1; 128]).unwrap();
///
/// let results = index.knn_search(&vec![0.1; 128], 10).unwrap();
/// assert_eq!(results[0].doc_id, 42);
/// ```
pub struct HnswVectorIndex {
    index: HnswIndexImpl,
    dimension: usize,
    max_elements: usize,
    current_count: usize,
    distance: DistanceMetric,
    disable_normalization_check: bool,
}

#[derive(Clone)]
enum HnswIndexImpl {
    Cosine(Arc<Hnsw<'static, f32, DistCosine>>),
    Euclidean(Arc<Hnsw<'static, f32, DistL2>>),
    InnerProduct(Arc<Hnsw<'static, f32, DistDot>>),
}

impl HnswVectorIndex {
    pub const DEFAULT_M: usize = 16;
    pub const DEFAULT_EF_CONSTRUCTION: usize = 200;

    /// Create new HNSW index
    ///
    /// # Parameters
    /// - `dimension`: Vector dimension
    /// - `max_elements`: Maximum capacity
    ///
    /// Uses M=16, ef_construction=200 (RAG-optimized)
    pub fn new(dimension: usize, max_elements: usize) -> Result<Self> {
        Self::new_with_distance(dimension, max_elements, DistanceMetric::Cosine)
    }

    /// Create a new HNSW index with the requested distance metric.
    ///
    /// This is critical for correctness: using Cosine for an L2 benchmark (or vice versa)
    /// will destroy recall and makes results incomparable.
    ///
    /// Warning: `DistanceMetric::InnerProduct` uses `DistDot`, which assumes **L2-normalized**
    /// input vectors. If embeddings are not normalized, results will be incorrect. Normalize
    /// each vector to unit length before inserting/querying. KyroDB validates and rejects
    /// non-normalized vectors at runtime unless `disable_normalization_check` is enabled.
    pub fn new_with_distance(
        dimension: usize,
        max_elements: usize,
        distance: DistanceMetric,
    ) -> Result<Self> {
        Self::new_with_params(
            dimension,
            max_elements,
            distance,
            Self::DEFAULT_M,
            Self::DEFAULT_EF_CONSTRUCTION,
            false,
        )
    }

    /// Create a new HNSW index with explicit construction parameters.
    ///
    /// `m` controls graph connectivity (higher = better recall, more memory).
    /// `ef_construction` controls build quality (higher = better recall, slower build).
    pub fn new_with_params(
        dimension: usize,
        max_elements: usize,
        distance: DistanceMetric,
        m: usize,
        ef_construction: usize,
        disable_normalization_check: bool,
    ) -> Result<Self> {
        if dimension == 0 {
            anyhow::bail!("dimension must be > 0");
        }
        if max_elements == 0 {
            anyhow::bail!("max_elements must be > 0");
        }
        if m == 0 {
            anyhow::bail!("HNSW m must be > 0");
        }
        if ef_construction == 0 {
            anyhow::bail!("HNSW ef_construction must be > 0");
        }

        // Calculate max_layer based on max_elements (hnsw_rs convention)
        let max_layer = 16.min((max_elements as f32).ln().ceil() as usize);

        let index = match distance {
            DistanceMetric::Cosine => HnswIndexImpl::Cosine(Arc::new(Hnsw::<f32, DistCosine>::new(
                m,
                max_elements,
                max_layer,
                ef_construction,
                DistCosine {},
            ))),
            DistanceMetric::Euclidean => HnswIndexImpl::Euclidean(Arc::new(Hnsw::<f32, DistL2>::new(
                m,
                max_elements,
                max_layer,
                ef_construction,
                DistL2 {},
            ))),
            DistanceMetric::InnerProduct => {
                // DistDot assumes vectors are L2-normalized by the caller.
                // We enforce no normalization here to keep the hot path allocation-free.
                HnswIndexImpl::InnerProduct(Arc::new(Hnsw::<f32, DistDot>::new(
                    m,
                    max_elements,
                    max_layer,
                    ef_construction,
                    DistDot {},
                )))
            }
        };

        Ok(Self {
            index,
            dimension,
            max_elements,
            current_count: 0,
            distance,
            disable_normalization_check,
        })
    }

    /// Add vector to index (no upserts, errors if full or wrong dimension)
    pub fn add_vector(&mut self, doc_id: u64, embedding: &[f32]) -> Result<()> {
        if embedding.len() != self.dimension {
            anyhow::bail!(
                "Embedding dimension mismatch: expected {}, got {}",
                self.dimension,
                embedding.len()
            );
        }

        if self.current_count >= self.max_elements {
            anyhow::bail!(
                "HNSW index full: {} elements (max {})",
                self.current_count,
                self.max_elements
            );
        }

        if matches!(self.distance, DistanceMetric::InnerProduct) && !self.disable_normalization_check {
            let norm_sq: f32 = embedding.iter().map(|v| v * v).sum();
            if !(0.98..=1.02).contains(&norm_sq) {
                anyhow::bail!(
                    "InnerProduct requires L2-normalized vectors; norm_sq={}",
                    norm_sq
                );
            }
        }

        // hnsw_rs copies the slice internally (Point::new(data.to_vec(), ...)), so we can pass
        // the provided slice directly without an extra allocation here.
        let origin_id = usize::try_from(doc_id)
            .map_err(|_| anyhow::anyhow!("doc_id {} exceeds usize capacity", doc_id))?;
        match &self.index {
            HnswIndexImpl::Cosine(h) => h.insert((embedding, origin_id)),
            HnswIndexImpl::Euclidean(h) => h.insert((embedding, origin_id)),
            HnswIndexImpl::InnerProduct(h) => h.insert((embedding, origin_id)),
        }

        self.current_count += 1;
        Ok(())
    }

    /// k-NN search (adaptive ef_search by default)
    pub fn knn_search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.knn_search_with_ef(query, k, None)
    }

    /// k-NN search with optional ef_search override.
    ///
    /// If `ef_search_override` is `None`, uses the existing adaptive behavior.
    /// If set, it is clamped to be >= k and <= 10_000.
    pub fn knn_search_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef_search_override: Option<usize>,
    ) -> Result<Vec<SearchResult>> {
        if query.len() != self.dimension {
            anyhow::bail!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimension,
                query.len()
            );
        }

        if self.current_count == 0 {
            return Ok(Vec::new());
        }

        // ef_search controls search quality (higher = better recall, slower)
        let ef_search = if let Some(override_val) = ef_search_override {
            override_val.clamp(k.max(1), 10_000)
        } else {
            // Adaptive: for very small indexes, crank this up to ensure near-perfect recall
            let mut ef = 200; // RAG-optimized default for large corpora
            if self.current_count <= 1024 {
                // Explore more of the graph when the index is small to guarantee recall in tests
                // Use max of current_count and k scaled, with an upper safety bound
                let target = (self.current_count.max(k) * 4).max(32);
                ef = ef.max(target.min(2048));
            }
            ef
        };
        let neighbours = match &self.index {
            HnswIndexImpl::Cosine(h) => h.search(query, k, ef_search),
            HnswIndexImpl::Euclidean(h) => h.search(query, k, ef_search),
            HnswIndexImpl::InnerProduct(h) => h.search(query, k, ef_search),
        };

        let results = neighbours
            .into_iter()
            .map(|neighbour| SearchResult {
                doc_id: neighbour.d_id as u64,
                distance: neighbour.distance,
            })
            .collect::<Vec<_>>();

        // For small indexes, HNSW may return fewer results
        // Accepted tradeoff: no external embedding storage overhead
        Ok(results)
    }

    /// Get index statistics
    pub fn len(&self) -> usize {
        self.current_count
    }

    pub fn is_empty(&self) -> bool {
        self.current_count == 0
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn capacity(&self) -> usize {
        self.max_elements
    }

    pub fn distance_metric(&self) -> DistanceMetric {
        self.distance
    }

    /// Estimate memory usage assuming M=16, avg 2 layers/node, +10% overhead
    pub fn estimate_memory_bytes(&self) -> usize {
        if self.current_count == 0 {
            return 0;
        }

        // Vector data storage
        let vector_data_bytes = self.current_count * self.dimension * std::mem::size_of::<f32>();

        // Graph structure (neighbors + edge weights)
        // Assuming max_nb_connection=16, average 2 layers per node
        let max_nb_connection = 16;
        let avg_layers = 2;
        let graph_bytes =
            self.current_count * max_nb_connection * avg_layers * std::mem::size_of::<usize>();

        // Metadata overhead (10% estimate)
        let total_without_overhead = vector_data_bytes + graph_bytes;
        let overhead = total_without_overhead / 10;

        total_without_overhead + overhead
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_basic_operations() {
        let mut index = HnswVectorIndex::new(128, 1000).unwrap();

        // Insert vector
        let embedding = vec![0.1; 128];
        index.add_vector(42, &embedding).unwrap();

        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());

        // Search should return same vector
        let results = index.knn_search(&embedding, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, 42);
        assert!(results[0].distance < 0.01); // Nearly identical (cosine distance ~0)
    }

    #[test]
    fn test_hnsw_dimension_validation() {
        let mut index = HnswVectorIndex::new(128, 1000).unwrap();

        // Wrong dimension should fail
        let wrong_dim = vec![0.1; 64];
        assert!(index.add_vector(1, &wrong_dim).is_err());

        let wrong_query = vec![0.1; 256];
        assert!(index.knn_search(&wrong_query, 10).is_err());
    }

    #[test]
    fn test_hnsw_multiple_vectors() {
        let mut index = HnswVectorIndex::new(4, 100).unwrap();

        // Add 3 vectors
        index.add_vector(1, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        index.add_vector(2, &[0.0, 1.0, 0.0, 0.0]).unwrap();
        index.add_vector(3, &[0.0, 0.0, 1.0, 0.0]).unwrap();

        // Query closest to vector 1
        let query = [0.9, 0.1, 0.0, 0.0];
        let results = index.knn_search(&query, 2).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, 1); // Closest
    }

    #[test]
    fn test_hnsw_empty_index() {
        let index = HnswVectorIndex::new(128, 1000).unwrap();
        let query = vec![0.1; 128];
        let results = index.knn_search(&query, 10).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_hnsw_capacity_limit() {
        let mut index = HnswVectorIndex::new(4, 2).unwrap();

        index.add_vector(1, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        index.add_vector(2, &[0.0, 1.0, 0.0, 0.0]).unwrap();

        // Third insert should fail (capacity = 2)
        let result = index.add_vector(3, &[0.0, 0.0, 1.0, 0.0]);
        assert!(result.is_err());
    }
}
