//! Semantic Adapter for Hybrid Semantic Cache
//!
//! Adds semantic awareness to document-cache admission:
//! - Cosine similarity over recently admitted document embeddings
//! - Configurable bounded embedding history (default: 100K embeddings)
//! - Hybrid decision score compared against the strategy's effective threshold
//!
//! Memory note:
//! - 100K x 384-dim `f32` embeddings require ~146.5 MiB of raw float storage
//!   before `Vec`, `IndexMap`, and allocator overhead.

use indexmap::IndexMap;
use parking_lot::RwLock;
use std::sync::Arc;

/// Semantic adapter configuration
#[derive(Debug, Clone)]
pub struct SemanticConfig {
    /// Threshold for high confidence (definitely hot)
    /// If frequency score > this, skip semantic check
    pub high_confidence_threshold: f32,

    /// Threshold for low confidence (definitely cold)
    /// If frequency score < this, skip semantic check
    pub low_confidence_threshold: f32,

    /// Minimum cosine similarity to consider embeddings similar
    pub semantic_similarity_threshold: f32,

    /// Maximum embeddings to cache
    pub max_cached_embeddings: usize,

    /// Number of recent embeddings to scan for similarity
    /// Scanning full cache is expensive, so limit to recent N
    pub similarity_scan_limit: usize,
}

impl Default for SemanticConfig {
    fn default() -> Self {
        Self {
            // Lower high-confidence threshold (0.60) to engage semantic layer for mid-tier docs
            high_confidence_threshold: 0.60,
            low_confidence_threshold: 0.25,
            semantic_similarity_threshold: 0.80,
            max_cached_embeddings: 100_000,
            similarity_scan_limit: 2_000,
        }
    }
}

/// Semantic adapter statistics
#[derive(Debug, Default, Clone)]
pub struct SemanticStats {
    /// Number of fast path decisions (high/low confidence)
    pub fast_path_decisions: u64,

    /// Number of slow path decisions (semantic check required)
    pub slow_path_decisions: u64,

    /// Number of semantic cache hits (similar embedding found)
    pub semantic_hits: u64,

    /// Number of semantic cache misses (no similar embedding)
    pub semantic_misses: u64,

    /// Current number of cached embeddings
    pub cached_embeddings: usize,
}

/// Embedding cache state consolidated under a single lock to prevent TOCTOU
/// races between dimension validation and cache insertion.
struct EmbeddingCacheState {
    /// doc_id -> embedding, bounded at max capacity with FIFO eviction.
    /// IndexMap guarantees insertion-order iteration.
    entries: IndexMap<u64, Vec<f32>>,
    /// Expected embedding dimension, set on first insert and validated thereafter.
    expected_dim: Option<usize>,
}

/// Lightweight semantic layer over the frequency-based predictor (forms Hybrid Semantic Cache)
///
/// Strategy:
/// 1. Check frequency-based prediction (4-8ns)
/// 2. If frequency is far above the current effective threshold: cache immediately
/// 3. If semantic history is empty: bootstrap on frequency only
/// 4. Otherwise, check similarity against recently admitted document embeddings
/// 5. Compare the hybrid score against the strategy's effective threshold
pub struct SemanticAdapter {
    /// Embedding cache with dimension tracking under a single lock.
    cache_state: Arc<RwLock<EmbeddingCacheState>>,

    /// Configuration
    config: SemanticConfig,

    /// Statistics
    stats: Arc<RwLock<SemanticStats>>,
}

impl SemanticAdapter {
    /// Create new semantic adapter with default config
    pub fn new() -> Self {
        Self::with_config(SemanticConfig::default())
    }

    /// Create new semantic adapter with custom config
    pub fn with_config(config: SemanticConfig) -> Self {
        Self {
            cache_state: Arc::new(RwLock::new(EmbeddingCacheState {
                entries: IndexMap::with_capacity(config.max_cached_embeddings),
                expected_dim: None,
            })),
            config,
            stats: Arc::new(RwLock::new(SemanticStats::default())),
        }
    }

    /// Hybrid cache admission decision
    ///
    /// Combines frequency-based prediction with semantic similarity.
    /// During cold-start (empty semantic cache), falls back to frequency-only.
    ///
    /// # Parameters
    /// - `freq_score`: Frequency-based hotness score from learned predictor (0.0-1.0)
    /// - `embedding`: Candidate document embedding under admission
    /// - `effective_threshold`: Strategy-layer threshold after adaptive admission bias
    ///
    /// # Returns
    /// `true` if vector should be cached, `false` otherwise
    pub fn should_cache(
        &self,
        freq_score: f32,
        embedding: &[f32],
        effective_threshold: f32,
    ) -> bool {
        let effective_threshold = effective_threshold.clamp(0.05, 1.0);
        let fast_accept_threshold = self
            .config
            .high_confidence_threshold
            .max((effective_threshold + 0.25).min(1.0));

        if freq_score >= fast_accept_threshold {
            self.stats.write().fast_path_decisions += 1;
            return true;
        }

        let cache_size = self.cache_size();

        // Bootstrap: seed semantic cache with top docs during warmup
        if cache_size == 0 {
            self.stats.write().fast_path_decisions += 1;
            return freq_score > effective_threshold.max(0.20);
        }

        self.stats.write().slow_path_decisions += 1;
        let semantic_score = self.compute_semantic_score(embedding);

        // Definite reject: both frequency and semantic signal are weak.
        if semantic_score < 0.38 && freq_score <= (effective_threshold * 0.75) {
            return false;
        }

        // Hybrid score uses the same semantic normalization as before, but the
        // controller-owned effective threshold determines how selective HSC is.
        let semantic_floor = 0.35;
        let normalized_semantic = if semantic_score <= semantic_floor {
            0.0
        } else {
            ((semantic_score - semantic_floor) / (1.0 - semantic_floor)).clamp(0.0, 1.0)
        };

        let semantic_bonus = if semantic_score >= 0.96 {
            0.06
        } else if semantic_score >= self.config.semantic_similarity_threshold {
            0.02
        } else {
            0.0
        };

        let hybrid_score =
            (freq_score * 0.55 + normalized_semantic * 0.45 + semantic_bonus).clamp(0.0, 1.0);
        hybrid_score >= effective_threshold
    }

    /// Compute semantic similarity score
    ///
    /// Scans recent embeddings and returns max cosine similarity.
    /// Returns 0.0 if cache is empty.
    ///
    /// # Performance
    /// - Empty cache: <1μs
    /// - Scan 1000 embeddings (384-dim): 1-5ms
    fn compute_semantic_score(&self, candidate_embedding: &[f32]) -> f32 {
        let state = self.cache_state.read();

        if state.entries.is_empty() {
            self.stats.write().semantic_misses += 1;
            return 0.0;
        }

        let scan_start = state
            .entries
            .len()
            .saturating_sub(self.config.similarity_scan_limit);
        let mut max_similarity = 0.0f32;

        for (_, cached_embedding) in state.entries.iter().skip(scan_start) {
            if candidate_embedding.len() != cached_embedding.len() {
                debug_assert!(
                    false,
                    "Embedding dimension mismatch: candidate={}, cached={}",
                    candidate_embedding.len(),
                    cached_embedding.len()
                );
                tracing::warn!(
                    candidate_dim = candidate_embedding.len(),
                    cached_dim = cached_embedding.len(),
                    "Skipping embedding with dimension mismatch"
                );
                continue;
            }
            let similarity = cosine_similarity(candidate_embedding, cached_embedding);

            // Handle NaN/Inf from zero vectors
            if !similarity.is_finite() {
                continue;
            }

            if similarity > max_similarity {
                max_similarity = similarity;
            }

            // Early exit if high similarity found
            if similarity >= self.config.semantic_similarity_threshold {
                self.stats.write().semantic_hits += 1;
                return similarity;
            }
        }

        if max_similarity >= self.config.semantic_similarity_threshold {
            self.stats.write().semantic_hits += 1;
        } else {
            self.stats.write().semantic_misses += 1;
        }

        max_similarity
    }

    /// Cache embedding for future similarity checks
    ///
    /// Implements bounded FIFO eviction:
    /// - If cache < max capacity: insert
    /// - If cache >= max capacity: evict oldest, insert new
    ///
    /// # Parameters
    /// - `doc_id`: Document ID
    /// - `embedding`: Embedding vector (will be cloned)
    pub fn cache_embedding(&self, doc_id: u64, embedding: Vec<f32>) -> anyhow::Result<()> {
        if embedding.is_empty() {
            anyhow::bail!("embedding cannot be empty");
        }

        let embedding_dim = embedding.len();
        let mut state = self.cache_state.write();

        // Validate dimension under the same lock that guards insertion.
        match state.expected_dim {
            None => {
                state.expected_dim = Some(embedding_dim);
            }
            Some(expected) if expected != embedding_dim => {
                anyhow::bail!(
                    "embedding dimension mismatch: expected {} found {}",
                    expected,
                    embedding_dim
                );
            }
            Some(_) => {}
        }

        // Bounded eviction: remove oldest if at capacity.
        if state.entries.len() >= self.config.max_cached_embeddings {
            state.entries.shift_remove_index(0);
        }

        state.entries.insert(doc_id, embedding);

        let cached_count = state.entries.len();
        drop(state);

        let mut stats = self.stats.write();
        stats.cached_embeddings = cached_count;

        Ok(())
    }

    /// Get current statistics
    pub fn stats(&self) -> SemanticStats {
        self.stats.read().clone()
    }

    /// Get configuration
    pub fn config(&self) -> &SemanticConfig {
        &self.config
    }

    /// Clear embedding cache (for testing or memory pressure)
    pub fn clear_cache(&self) {
        {
            let mut state = self.cache_state.write();
            state.entries.clear();
            state.expected_dim = None;
        }

        let mut stats = self.stats.write();
        stats.cached_embeddings = 0;
    }

    /// Get embedding cache size
    pub fn cache_size(&self) -> usize {
        self.cache_state.read().entries.len()
    }
}

impl Default for SemanticAdapter {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute cosine similarity between two vectors
///
/// Formula: cos(θ) = (a · b) / (||a|| * ||b||)
///
/// Returns value in [0.0, 1.0] for normalized vectors.
/// Returns NaN if either vector is zero-length (handled by caller).
///
/// # Performance
/// - 384-dim vectors: ~10-20ns (SIMD-accelerated with AVX2/AVX-512)
/// - Fallback to scalar on non-AVX2 systems: ~100-200ns
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    crate::simd::cosine_similarity_f32(a, b).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity_identical_vectors() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];

        let similarity = cosine_similarity(&a, &b);
        assert!((similarity - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal_vectors() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];

        let similarity = cosine_similarity(&a, &b);
        assert!((similarity - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_opposite_vectors() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![-1.0, -2.0, -3.0];

        let similarity = cosine_similarity(&a, &b);
        // Opposite vectors have negative cosine, clamped to 0.0
        assert!((similarity - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_zero_vector() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![0.0, 0.0, 0.0];

        let similarity = cosine_similarity(&a, &b);
        assert_eq!(similarity, 0.0);
    }

    #[test]
    #[should_panic(expected = "input slices must have the same length")]
    fn test_cosine_similarity_mismatched_dimensions() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![1.0, 2.0];

        let _ = cosine_similarity(&a, &b);
    }

    #[test]
    fn test_semantic_adapter_fast_path_high_confidence() {
        let adapter = SemanticAdapter::new();

        // High frequency score (>= 0.60) should skip semantic check
        let should_cache = adapter.should_cache(0.85, &vec![0.5; 384], 0.18);
        assert!(should_cache);

        // Verify fast path used
        let stats = adapter.stats();
        assert_eq!(stats.fast_path_decisions, 1);
        assert_eq!(stats.slow_path_decisions, 0);
    }

    #[test]
    fn test_semantic_adapter_fast_path_low_confidence() {
        let adapter = SemanticAdapter::new();

        // Cold-start bootstrap still rejects low scores when history is empty.
        let should_cache = adapter.should_cache(0.2, &vec![0.5; 384], 0.18);
        assert!(!should_cache);

        // Verify fast path used
        let stats = adapter.stats();
        assert_eq!(stats.fast_path_decisions, 1);
        assert_eq!(stats.slow_path_decisions, 0);
    }

    #[test]
    fn test_semantic_adapter_slow_path_empty_cache() {
        let adapter = SemanticAdapter::new();

        // Uncertain frequency score (0.25-0.75) with empty cache
        let should_cache = adapter.should_cache(0.5, &vec![0.5; 384], 0.18);

        assert!(should_cache);

        // Verify fast path used (cold-start bypass)
        let stats = adapter.stats();
        assert_eq!(stats.fast_path_decisions, 1);
        assert_eq!(stats.slow_path_decisions, 0);
    }

    #[test]
    fn test_semantic_adapter_slow_path_with_similar_embedding() {
        let adapter = SemanticAdapter::new();

        // Cache 100 embeddings to get past cold-start threshold
        for i in 0..100 {
            adapter
                .cache_embedding(i, vec![i as f32 / 100.0; 384])
                .unwrap();
        }

        // Cache a similar embedding
        let cached_embedding = vec![1.0; 384];
        adapter
            .cache_embedding(100, cached_embedding.clone())
            .unwrap();

        // Candidate embedding with very high similarity to a cached admission history entry
        let candidate_embedding = vec![0.99; 384];
        let should_cache = adapter.should_cache(0.5, &candidate_embedding, 0.18);

        // Cosine similarity should be very high (~1.0), which trips the semantic
        // fast-admit override in the hybrid admission logic.
        assert!(should_cache);

        // Verify slow path and semantic hit
        let stats = adapter.stats();
        assert_eq!(stats.slow_path_decisions, 1);
        assert_eq!(stats.semantic_hits, 1);
    }

    #[test]
    fn test_semantic_adapter_cache_embedding() {
        let adapter = SemanticAdapter::new();

        // Cache some embeddings
        for i in 0..10 {
            adapter.cache_embedding(i, vec![i as f32; 384]).unwrap();
        }

        assert_eq!(adapter.cache_size(), 10);

        // Verify stats updated
        let stats = adapter.stats();
        assert_eq!(stats.cached_embeddings, 10);
    }

    #[test]
    fn test_semantic_adapter_bounded_eviction() {
        let config = SemanticConfig {
            max_cached_embeddings: 5,
            ..Default::default()
        };
        let adapter = SemanticAdapter::with_config(config);

        // Cache 10 embeddings (exceeds capacity)
        for i in 0..10 {
            adapter.cache_embedding(i, vec![i as f32; 384]).unwrap();
        }

        // Should only keep 5 (most recent)
        assert_eq!(adapter.cache_size(), 5);
    }

    #[test]
    fn test_semantic_adapter_clear_cache() {
        let adapter = SemanticAdapter::new();

        adapter.cache_embedding(1, vec![1.0; 384]).unwrap();
        adapter.cache_embedding(2, vec![2.0; 384]).unwrap();

        assert_eq!(adapter.cache_size(), 2);

        adapter.clear_cache();
        assert_eq!(adapter.cache_size(), 0);
    }

    #[test]
    fn test_semantic_adapter_concurrent_access() {
        use std::thread;

        let adapter = Arc::new(SemanticAdapter::new());
        let mut handles = vec![];

        // Spawn 10 threads accessing adapter concurrently
        for i in 0..10 {
            let adapter_clone = Arc::clone(&adapter);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let doc_id = (i * 100 + j) as u64;
                    let embedding = vec![doc_id as f32; 384];

                    // Make decision
                    adapter_clone.should_cache(0.5, &embedding, 0.18);

                    // Cache embedding
                    let _ = adapter_clone.cache_embedding(doc_id, embedding);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have cached embeddings (limited by max capacity)
        assert!(adapter.cache_size() > 0);
    }

    #[test]
    fn test_semantic_config_defaults() {
        let config = SemanticConfig::default();

        assert_eq!(config.high_confidence_threshold, 0.60);
        assert_eq!(config.low_confidence_threshold, 0.25);
        assert_eq!(config.semantic_similarity_threshold, 0.80);
        assert_eq!(config.max_cached_embeddings, 100_000);
        assert_eq!(config.similarity_scan_limit, 2000);
    }

    #[test]
    fn test_effective_threshold_controls_semantic_selectivity() {
        let adapter = SemanticAdapter::new();
        adapter.cache_embedding(1, vec![1.0; 384]).unwrap();

        let candidate_embedding = vec![0.97; 384];
        assert!(
            adapter.should_cache(0.05, &candidate_embedding, 0.20),
            "low threshold should admit strong semantic neighbor"
        );
        assert!(
            !adapter.should_cache(0.05, &candidate_embedding, 0.55),
            "high threshold should reject the same candidate"
        );
    }
}
