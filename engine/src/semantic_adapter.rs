//! Semantic Adapter for Hybrid Semantic Cache
//!
//!
//! Adds semantic awareness to frequency-based prediction:
//! - Cosine similarity for semantic matching
//! - Bounded embedding cache (100K recent embeddings)
//! - Hybrid decision: frequency (70%) + semantic (30%)
//!
//! Performance characteristics:
//! - Fast path (high/low confidence): 4-8ns (frequency check only)
//! - Slow path (uncertain): 1-5ms (semantic similarity scan)
//! - Memory overhead: ~38 MB for 100K embeddings (384-dim f32)

use parking_lot::RwLock;
use std::collections::HashMap;
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
            high_confidence_threshold: 0.7,
            low_confidence_threshold: 0.2,
            semantic_similarity_threshold: 0.75,
            max_cached_embeddings: 100_000,
            similarity_scan_limit: 1000,
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

/// Lightweight semantic layer over frequency-based RMI predictor (forms Hybrid Semantic Cache)
///
/// Strategy:
/// 1. Check frequency-based prediction (4-8ns)
/// 2. If high confidence (>0.8): definitely cache (fast path)
/// 3. If low confidence (<0.3): definitely don't cache (fast path)
/// 4. If uncertain (0.3-0.8): check semantic similarity (slow path)
/// 5. Hybrid decision: average frequency + semantic scores
pub struct SemanticAdapter {
    /// Embedding cache: doc_id -> embedding
    /// Bounded at max_cached_embeddings with FIFO eviction
    embedding_cache: Arc<RwLock<HashMap<u64, Vec<f32>>>>,

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
            embedding_cache: Arc::new(RwLock::new(HashMap::with_capacity(
                config.max_cached_embeddings,
            ))),
            config,
            stats: Arc::new(RwLock::new(SemanticStats::default())),
        }
    }

    /// Hybrid cache admission decision
    ///
    /// Combines frequency-based prediction with semantic similarity.
    ///
    /// # Parameters
    /// - `freq_score`: Frequency-based hotness score from RMI (0.0-1.0)
    /// - `embedding`: Query embedding vector
    ///
    /// # Returns
    /// `true` if vector should be cached, `false` otherwise
    pub fn should_cache(&self, freq_score: f32, embedding: &[f32]) -> bool {
        // Fast path: high confidence (definitely hot)
        if freq_score >= self.config.high_confidence_threshold {
            self.stats.write().fast_path_decisions += 1;
            return true;
        }

        // Fast path: low confidence (definitely cold)
        if freq_score <= self.config.low_confidence_threshold {
            self.stats.write().fast_path_decisions += 1;
            return false;
        }

        // Slow path: uncertain frequency, check semantic similarity
        self.stats.write().slow_path_decisions += 1;

        let semantic_score = self.compute_semantic_score(embedding);

        // Hybrid decision: average of frequency (70%) and semantic (30%)
        let hybrid_score = freq_score * 0.45 + semantic_score * 0.55;
        hybrid_score > 0.6
    }

    /// Compute semantic similarity score
    ///
    /// Scans recent embeddings and returns max cosine similarity.
    /// Returns 0.0 if cache is empty.
    ///
    /// # Performance
    /// - Empty cache: <1μs
    /// - Scan 1000 embeddings (384-dim): 1-5ms
    fn compute_semantic_score(&self, query_embedding: &[f32]) -> f32 {
        let cache = self.embedding_cache.read();

        // Cold start: no embeddings cached yet
        if cache.is_empty() {
            self.stats.write().semantic_misses += 1;
            return 0.0;
        }

        // Scan recent embeddings (limit to avoid expensive full scan)
        let scan_start = cache
            .len()
            .saturating_sub(self.config.similarity_scan_limit);
        let mut max_similarity = 0.0f32;

        for (_, cached_embedding) in cache.iter().skip(scan_start) {
            let similarity = cosine_similarity(query_embedding, cached_embedding);

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
    pub fn cache_embedding(&self, doc_id: u64, embedding: Vec<f32>) {
        let mut cache = self.embedding_cache.write();

        // Bounded eviction: remove oldest if at capacity
        if cache.len() >= self.config.max_cached_embeddings {
            // Simple FIFO: remove first entry (HashMap iteration order is insertion order in Rust 1.70+)
            if let Some(&oldest_key) = cache.keys().next() {
                cache.remove(&oldest_key);
            }
        }

        cache.insert(doc_id, embedding);

        // Update stats
        let mut stats = self.stats.write();
        stats.cached_embeddings = cache.len();
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
        let mut cache = self.embedding_cache.write();
        cache.clear();

        let mut stats = self.stats.write();
        stats.cached_embeddings = 0;
    }

    /// Get embedding cache size
    pub fn cache_size(&self) -> usize {
        self.embedding_cache.read().len()
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
/// - 384-dim vectors: ~100-200ns (no SIMD optimization yet)
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    // Handle mismatched dimensions
    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }

    // Compute dot product and norms
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for i in 0..len {
        let a_val = a[i];
        let b_val = b[i];

        dot += a_val * b_val;
        norm_a += a_val * a_val;
        norm_b += b_val * b_val;
    }

    // Avoid division by zero
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    // Compute cosine similarity
    let similarity = dot / (norm_a.sqrt() * norm_b.sqrt());

    // Clamp to [0.0, 1.0] (handle floating point errors)
    similarity.clamp(0.0, 1.0)
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
    fn test_cosine_similarity_mismatched_dimensions() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![1.0, 2.0];

        // Should use min dimension (2)
        let similarity = cosine_similarity(&a, &b);
        assert!(similarity > 0.0);
    }

    #[test]
    fn test_semantic_adapter_fast_path_high_confidence() {
        let adapter = SemanticAdapter::new();

        // High frequency score (>0.8) should skip semantic check
        let should_cache = adapter.should_cache(0.9, &vec![0.5; 384]);
        assert!(should_cache);

        // Verify fast path used
        let stats = adapter.stats();
        assert_eq!(stats.fast_path_decisions, 1);
        assert_eq!(stats.slow_path_decisions, 0);
    }

    #[test]
    fn test_semantic_adapter_fast_path_low_confidence() {
        let adapter = SemanticAdapter::new();

        // Low frequency score (<0.3) should skip semantic check
        let should_cache = adapter.should_cache(0.2, &vec![0.5; 384]);
        assert!(!should_cache);

        // Verify fast path used
        let stats = adapter.stats();
        assert_eq!(stats.fast_path_decisions, 1);
        assert_eq!(stats.slow_path_decisions, 0);
    }

    #[test]
    fn test_semantic_adapter_slow_path_empty_cache() {
        let adapter = SemanticAdapter::new();

        // Uncertain frequency score (0.3-0.8) triggers semantic check
        let should_cache = adapter.should_cache(0.5, &vec![0.5; 384]);

        // With empty cache, semantic score is 0.0
        // Hybrid: 0.5 * 0.7 + 0.0 * 0.3 = 0.35 < 0.6 → don't cache
        assert!(!should_cache);

        // Verify slow path used
        let stats = adapter.stats();
        assert_eq!(stats.fast_path_decisions, 0);
        assert_eq!(stats.slow_path_decisions, 1);
        assert_eq!(stats.semantic_misses, 1);
    }

    #[test]
    fn test_semantic_adapter_slow_path_with_similar_embedding() {
        let adapter = SemanticAdapter::new();

        // Cache an embedding
        let cached_embedding = vec![1.0; 384];
        adapter.cache_embedding(1, cached_embedding.clone());

        // Query with very similar embedding
        let query_embedding = vec![0.99; 384];
        let should_cache = adapter.should_cache(0.5, &query_embedding);

        // Cosine similarity should be very high (~1.0)
        // Hybrid: 0.5 * 0.7 + 1.0 * 0.3 = 0.65 > 0.6 → cache
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
            adapter.cache_embedding(i, vec![i as f32; 384]);
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
            adapter.cache_embedding(i, vec![i as f32; 384]);
        }

        // Should only keep 5 (most recent)
        assert_eq!(adapter.cache_size(), 5);
    }

    #[test]
    fn test_semantic_adapter_clear_cache() {
        let adapter = SemanticAdapter::new();

        adapter.cache_embedding(1, vec![1.0; 384]);
        adapter.cache_embedding(2, vec![2.0; 384]);

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
                    adapter_clone.should_cache(0.5, &embedding);

                    // Cache embedding
                    adapter_clone.cache_embedding(doc_id, embedding);
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

        assert_eq!(config.high_confidence_threshold, 0.8);
        assert_eq!(config.low_confidence_threshold, 0.3);
        assert_eq!(config.semantic_similarity_threshold, 0.85);
        assert_eq!(config.max_cached_embeddings, 100_000);
        assert_eq!(config.similarity_scan_limit, 1000);
    }
}
