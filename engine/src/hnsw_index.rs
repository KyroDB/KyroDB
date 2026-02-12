//! HNSW-style vector index for k-NN search.
//!
//! Thin wrapper around KyroDB's internal ANN backend optimized for RAG:
//! cosine similarity, type-safe u64 doc IDs, and strict runtime validation.

use anyhow::Result;

use crate::ann_backend::{create_default_backend, AnnBackend};
use crate::config::{AnnSearchMode, DistanceMetric};

const NORMALIZATION_NORM_SQ_MIN: f32 = 0.98;
const NORMALIZATION_NORM_SQ_MAX: f32 = 1.02;

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
    backend: Box<dyn AnnBackend>,
    backend_name: &'static str,
    dimension: usize,
    max_elements: usize,
    current_count: usize,
    distance: DistanceMetric,
    m: usize,
    ef_construction: usize,
    disable_normalization_check: bool,
    ann_search_mode: AnnSearchMode,
    quantized_rerank_multiplier: usize,
}

impl HnswVectorIndex {
    pub const DEFAULT_M: usize = 16;
    pub const DEFAULT_EF_CONSTRUCTION: usize = 200;
    pub const DEFAULT_QUANTIZED_RERANK_MULTIPLIER: usize = 8;

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
    /// Warning: `DistanceMetric::Cosine` and `DistanceMetric::InnerProduct` rely on fast
    /// dot-product distance implementations that assume **L2-normalized** input vectors.
    /// If embeddings are not normalized, results will be incorrect. Normalize each vector to
    /// unit length before inserting/querying. KyroDB validates and rejects non-normalized
    /// vectors at runtime unless `disable_normalization_check` is enabled.
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
        Self::new_with_params_and_search_mode(
            dimension,
            max_elements,
            distance,
            m,
            ef_construction,
            disable_normalization_check,
            AnnSearchMode::Fp32Strict,
            Self::DEFAULT_QUANTIZED_RERANK_MULTIPLIER,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_params_and_search_mode(
        dimension: usize,
        max_elements: usize,
        distance: DistanceMetric,
        m: usize,
        ef_construction: usize,
        disable_normalization_check: bool,
        ann_search_mode: AnnSearchMode,
        quantized_rerank_multiplier: usize,
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
        if !(1..=64).contains(&quantized_rerank_multiplier) {
            anyhow::bail!(
                "quantized_rerank_multiplier must be in [1, 64], got {}",
                quantized_rerank_multiplier
            );
        }

        // Calculate max_layer based on max_elements
        let max_layer = 16.min(((max_elements as f32).ln().ceil() as usize).max(1));

        // KyroDB default ANN backend. Isolated behind a trait so we can replace
        // the implementation without changing higher-level engine behavior.
        let backend = create_default_backend(
            distance,
            m,
            max_elements,
            max_layer,
            ef_construction,
            ann_search_mode,
            quantized_rerank_multiplier,
        );
        let backend_name = backend.name();

        Ok(Self {
            backend,
            backend_name,
            dimension,
            max_elements,
            current_count: 0,
            distance,
            m,
            ef_construction,
            disable_normalization_check,
            ann_search_mode,
            quantized_rerank_multiplier,
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
        if embedding.iter().any(|v| !v.is_finite()) {
            anyhow::bail!("embedding contains non-finite values");
        }

        if self.current_count >= self.max_elements {
            anyhow::bail!(
                "HNSW index full: {} elements (max {})",
                self.current_count,
                self.max_elements
            );
        }

        if matches!(
            self.distance,
            DistanceMetric::Cosine | DistanceMetric::InnerProduct
        ) && !self.disable_normalization_check
        {
            let norm_sq = crate::simd::sum_squares_f32(embedding);
            if !(NORMALIZATION_NORM_SQ_MIN..=NORMALIZATION_NORM_SQ_MAX).contains(&norm_sq) {
                anyhow::bail!(
                    "{:?} requires L2-normalized vectors; norm_sq={}",
                    self.distance,
                    norm_sq
                );
            }
        }

        // Backends own their internal copy/layout policy. The index API accepts slices
        // to avoid imposing an extra allocation at call sites.
        let origin_id = usize::try_from(doc_id)
            .map_err(|_| anyhow::anyhow!("doc_id {} exceeds usize capacity", doc_id))?;
        self.backend.insert(embedding, origin_id);

        self.current_count += 1;
        Ok(())
    }

    /// Notify backend that a sequential insert burst has completed.
    ///
    /// Some backends keep a read-optimized search snapshot and need a one-shot
    /// refresh after sequential insert-only phases.
    pub fn complete_sequential_inserts(&mut self) {
        self.backend.on_sequential_batch_complete();
    }

    /// Batch insert of pre-validated, pre-normalized vectors.
    ///
    /// Backends are free to choose their own insertion strategy (single-threaded
    /// or parallel), but they must preserve deterministic behavior and API
    /// semantics.
    ///
    /// Callers MUST ensure:
    /// - All embeddings have correct dimension
    /// - All embeddings are L2-normalized when using Cosine/InnerProduct distance
    /// - `origin_ids` are valid `usize`-representable u64 values
    /// - `self.current_count + data.len() <= self.max_elements`
    ///
    /// For small batches (<100 vectors), falls through to sequential insert to
    /// avoid scheduler overhead.
    pub fn parallel_insert_batch(&mut self, data: &[(&[f32], usize)]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let new_count = self.current_count + data.len();
        if new_count > self.max_elements {
            anyhow::bail!(
                "HNSW batch insert would exceed capacity: {} + {} > {}",
                self.current_count,
                data.len(),
                self.max_elements
            );
        }

        // Validate dimensions upfront — cheaper than failing mid-insert
        let needs_norm_check = matches!(
            self.distance,
            DistanceMetric::Cosine | DistanceMetric::InnerProduct
        ) && !self.disable_normalization_check;

        for (i, (embedding, _)) in data.iter().enumerate() {
            if embedding.len() != self.dimension {
                anyhow::bail!(
                    "Batch insert: embedding[{}] dimension mismatch: expected {}, got {}",
                    i,
                    self.dimension,
                    embedding.len()
                );
            }
            if embedding.iter().any(|v| !v.is_finite()) {
                anyhow::bail!("Batch insert: embedding[{}] contains non-finite values", i);
            }
            if needs_norm_check {
                let norm_sq = crate::simd::sum_squares_f32(embedding);
                if !(NORMALIZATION_NORM_SQ_MIN..=NORMALIZATION_NORM_SQ_MAX).contains(&norm_sq) {
                    anyhow::bail!(
                        "Batch insert: embedding[{}] {:?} requires L2-normalized vectors; norm_sq={}",
                        i,
                        self.distance,
                        norm_sq
                    );
                }
            }
        }

        // For small batches, sequential insert avoids Rayon thread-pool overhead
        const PARALLEL_THRESHOLD: usize = 100;
        if data.len() < PARALLEL_THRESHOLD {
            for &(embedding, origin_id) in data {
                self.backend.insert(embedding, origin_id);
            }
            // Ensure backends can refresh any read-optimized search snapshot once
            // after a burst of sequential inserts.
            self.backend.on_sequential_batch_complete();
        } else {
            self.backend.parallel_insert_slice(data);
        }

        self.current_count = new_count;
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
        if query.iter().any(|v| !v.is_finite()) {
            anyhow::bail!("query contains non-finite values");
        }

        if k == 0 {
            anyhow::bail!("k must be greater than 0");
        }
        if k > 10_000 {
            anyhow::bail!("k must be <= 10,000 (requested: {})", k);
        }

        if matches!(
            self.distance,
            DistanceMetric::Cosine | DistanceMetric::InnerProduct
        ) && !self.disable_normalization_check
        {
            let norm_sq = crate::simd::sum_squares_f32(query);
            if !(NORMALIZATION_NORM_SQ_MIN..=NORMALIZATION_NORM_SQ_MAX).contains(&norm_sq) {
                anyhow::bail!(
                    "{:?} requires L2-normalized vectors; norm_sq={}",
                    self.distance,
                    norm_sq
                );
            }
        }

        if self.current_count == 0 {
            return Ok(Vec::new());
        }

        // ef_search controls search quality (higher = better recall, slower)
        let ef_search = if let Some(override_val) = ef_search_override {
            // Clamp safely: bounds must not be inverted.
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
        let results = self.backend.search(query, k, ef_search);

        // For small indexes, HNSW may return fewer results
        // Accepted tradeoff: no external embedding storage overhead
        Ok(results)
    }

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

    pub fn is_full(&self) -> bool {
        self.current_count >= self.max_elements
    }

    pub fn distance_metric(&self) -> DistanceMetric {
        self.distance
    }

    pub fn m(&self) -> usize {
        self.m
    }

    pub fn ef_construction(&self) -> usize {
        self.ef_construction
    }

    pub fn normalization_check_disabled(&self) -> bool {
        self.disable_normalization_check
    }

    pub fn backend_name(&self) -> &'static str {
        self.backend_name
    }

    pub fn ann_search_mode(&self) -> AnnSearchMode {
        self.ann_search_mode
    }

    pub fn quantized_rerank_multiplier(&self) -> usize {
        self.quantized_rerank_multiplier
    }

    /// Estimate backend memory usage for the active ANN engine layout.
    pub fn estimate_memory_bytes(&self) -> usize {
        if self.current_count == 0 {
            return 0;
        }
        // Backends expose exact-ish internal layout accounting; avoid layering
        // legacy rough formulas on top (which double-counts vectors/graph state).
        self.backend.estimated_memory_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize(mut v: Vec<f32>) -> Vec<f32> {
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            v.iter_mut().for_each(|x| *x /= norm);
        }
        v
    }

    #[test]
    fn test_hnsw_basic_operations() {
        let mut index = HnswVectorIndex::new(128, 1000).unwrap();

        // Insert vector
        let embedding = normalize(vec![0.1; 128]);
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
    fn test_hnsw_rejects_non_finite_input_vectors_and_queries() {
        let mut index = HnswVectorIndex::new(4, 10).unwrap();

        let bad_insert = [0.1, f32::NAN, 0.2, 0.3];
        assert!(
            index.add_vector(1, &bad_insert).is_err(),
            "insert must reject non-finite embeddings"
        );

        index
            .add_vector(2, &normalize(vec![1.0, 0.0, 0.0, 0.0]))
            .unwrap();

        let bad_query = [0.1, f32::INFINITY, 0.2, 0.3];
        assert!(
            index.knn_search(&bad_query, 1).is_err(),
            "search must reject non-finite query vectors"
        );
    }

    #[test]
    fn test_hnsw_multiple_vectors() {
        let mut index = HnswVectorIndex::new(4, 100).unwrap();

        // Add 3 vectors
        index
            .add_vector(1, &normalize(vec![1.0, 0.0, 0.0, 0.0]))
            .unwrap();
        index
            .add_vector(2, &normalize(vec![0.0, 1.0, 0.0, 0.0]))
            .unwrap();
        index
            .add_vector(3, &normalize(vec![0.0, 0.0, 1.0, 0.0]))
            .unwrap();

        // Query closest to vector 1
        let query = normalize(vec![0.9, 0.1, 0.0, 0.0]);
        let results = index.knn_search(&query, 2).unwrap();

        assert!(!results.is_empty());
        assert!(results.len() <= 2);
        assert_eq!(results[0].doc_id, 1); // Closest
    }

    #[test]
    fn test_hnsw_empty_index() {
        let index = HnswVectorIndex::new(128, 1000).unwrap();
        let query = normalize(vec![0.1; 128]);
        let results = index.knn_search(&query, 10).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_default_backend_is_single_graph() {
        let index = HnswVectorIndex::new(128, 1000).unwrap();
        assert_eq!(index.backend_name(), "kyro_single_graph");
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
