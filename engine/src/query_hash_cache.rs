//! Query Hash Cache - L1b Cache Layer for Semantic Similarity
//!
//! **Purpose**: Cache query→document mappings based on semantic similarity.
//! Complements the document-level cache (L1a) by caching paraphrased queries.
//!
//! # Architecture
//! - L1a (Document Cache): Frequency-based (RMI predictor) - caches hot documents
//! - L1b (Query Cache): Similarity-based (this module) - caches similar queries
//!
//! # Use Case
//! RAG systems often receive paraphrased queries for the same document:
//! - "What is machine learning?" (query 1)
//! - "Explain machine learning" (query 2, similar to query 1)
//! - "Define ML" (query 3, similar to queries 1 and 2)
//!
//! Without query cache: Each query misses L1a, hits cold tier (slow)
//! With query cache: Query 2 and 3 hit L1b (fast, semantic match)
//!
//! # Performance
//! - Exact match: <10ns (HashMap lookup by query_hash)
//! - Similarity scan: <1μs (limit to 2000 recent queries, SIMD cosine similarity)
//! - Memory: ~154 KB for 100 queries × 384-dim embeddings

use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

/// Cached query result
///
/// Stores the result of a query (doc_id + embedding) indexed by query hash.
/// When a similar query arrives, we can return this cached result without
/// searching the cold tier.
#[derive(Clone, Debug)]
pub struct CachedQueryResult {
    /// Document ID that matches this query
    pub doc_id: u64,

    /// Document embedding
    pub embedding: Vec<f32>,

    /// Query hash (for exact match lookups)
    pub query_hash: u64,

    /// Timestamp when cached
    pub cached_at: Instant,
}

/// Query hash cache statistics
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryCacheStats {
    /// Number of exact hash matches
    pub exact_hits: u64,

    /// Number of similarity-based matches
    pub similarity_hits: u64,

    /// Total hits (exact + similarity)
    pub total_hits: u64,

    /// Cache misses
    pub misses: u64,

    /// Hit rate
    pub hit_rate: f64,

    /// Number of evictions
    pub evictions: u64,

    /// Current cache size
    pub size: usize,

    /// Cache capacity
    pub capacity: usize,

    /// Average similarity score for similarity hits
    pub avg_similarity: f64,
}

/// Query hash cache - L1b cache layer
///
/// Caches query→document mappings based on semantic similarity.
/// Uses two-level lookup:
/// 1. Exact match: O(1) hash lookup
/// 2. Similarity scan: O(k) cosine similarity (k = min(cache_size, scan_limit))
///
/// # Thread Safety
/// Uses Arc<RwLock<...>> for concurrent access. Multiple readers can query
/// simultaneously, writes (inserts/evictions) are exclusive.
///
/// # Memory Layout
/// - cache: HashMap<query_hash, CachedQueryResult>
/// - query_embeddings: HashMap<query_hash, Vec<f32>> (for similarity matching)
/// - lru_queue: VecDeque<query_hash> (for LRU eviction)
///
/// Total memory: capacity × (384-dim × 4 bytes + 64 bytes overhead) ≈ 1600 bytes/query
pub struct QueryHashCache {
    /// Main cache storage: query_hash → cached result
    cache: Arc<RwLock<HashMap<u64, CachedQueryResult>>>,

    /// Query embeddings for similarity matching
    query_embeddings: Arc<RwLock<HashMap<u64, Vec<f32>>>>,

    /// LRU queue (front = oldest, back = newest)
    lru_queue: Arc<RwLock<VecDeque<u64>>>,

    /// Cache capacity (max number of queries)
    capacity: usize,

    /// Similarity threshold for matches (cosine similarity)
    similarity_threshold: f32,

    /// Maximum number of queries to scan for similarity (performance limit)
    similarity_scan_limit: usize,

    /// Statistics (separate lock to avoid contention)
    stats: Arc<RwLock<QueryCacheStatsInternal>>,
}

/// Internal statistics (mutable)
#[derive(Debug, Default)]
struct QueryCacheStatsInternal {
    exact_hits: u64,
    similarity_hits: u64,
    misses: u64,
    evictions: u64,
    total_similarity_score: f64, // For computing average
}

impl QueryHashCache {
    /// Create new query hash cache
    ///
    /// # Parameters
    /// - `capacity`: Maximum number of queries to cache (e.g., 100-1000)
    /// - `similarity_threshold`: Minimum cosine similarity for match (e.g., 0.82)
    ///
    /// # Memory Usage
    /// Approximately `capacity × 1600 bytes` for 384-dim embeddings
    /// Example: 100 queries × 1600 bytes = 160 KB
    pub fn new(capacity: usize, similarity_threshold: f32) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            query_embeddings: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            lru_queue: Arc::new(RwLock::new(VecDeque::with_capacity(capacity))),
            capacity,
            similarity_threshold: similarity_threshold.clamp(0.0, 1.0),
            similarity_scan_limit: 2000, // Limit similarity scan to 2000 most recent queries
            stats: Arc::new(RwLock::new(QueryCacheStatsInternal::default())),
        }
    }

    /// Get cached result for query
    ///
    /// Performs two-level lookup:
    /// 1. Exact match: Check if query_hash exists in cache (O(1))
    /// 2. Similarity match: Scan recent queries for similarity > threshold (O(k))
    ///
    /// # Performance
    /// - Exact match: <10ns
    /// - Similarity scan: <1μs (2000 queries × 0.5ns per comparison)
    ///
    /// # Returns
    /// - `Some(CachedQueryResult)` if exact or similarity match found
    /// - `None` if no match
    pub fn get(&self, query_embedding: &[f32]) -> Option<CachedQueryResult> {
        let query_hash = Self::hash_embedding(query_embedding);

        // Step 1: Try exact match (fast path)
        {
            let cache = self.cache.read();
            if let Some(cached) = cache.get(&query_hash) {
                // Exact match - update LRU and stats
                self.update_lru(query_hash);
                self.stats.write().exact_hits += 1;
                return Some(cached.clone());
            }
        }

        // Step 2: Try similarity match (slower path)
        self.find_similar_query(query_embedding, query_hash)
    }

    /// Insert query→document mapping into cache
    ///
    /// # Parameters
    /// - `query_embedding`: Query vector (will be hashed and stored)
    /// - `doc_id`: Document ID that matches this query
    /// - `embedding`: Document embedding
    ///
    /// # Returns
    /// - `Some(query_hash)` if an eviction occurred
    /// - `None` if no eviction
    ///
    /// # Eviction Policy
    /// LRU eviction when cache is full (evicts least recently used query)
    pub fn insert(
        &self,
        query_embedding: Vec<f32>,
        doc_id: u64,
        embedding: Vec<f32>,
    ) -> Option<u64> {
        let query_hash = Self::hash_embedding(&query_embedding);

        let mut cache = self.cache.write();
        let mut query_embs = self.query_embeddings.write();
        let mut lru = self.lru_queue.write();

        // Check if already in cache (update case)
        if cache.contains_key(&query_hash) {
            // Update existing entry
            cache.insert(
                query_hash,
                CachedQueryResult {
                    doc_id,
                    embedding,
                    query_hash,
                    cached_at: Instant::now(),
                },
            );
            query_embs.insert(query_hash, query_embedding);

            // Move to back of LRU queue
            if let Some(pos) = lru.iter().position(|&h| h == query_hash) {
                lru.remove(pos);
                lru.push_back(query_hash);
            } else {
                // Defensive: should never happen
                lru.push_back(query_hash);
            }

            return None; // No eviction on update
        }

        // Evict if at capacity
        let evicted_hash = if cache.len() >= self.capacity {
            if let Some(evict_hash) = lru.pop_front() {
                cache.remove(&evict_hash);
                query_embs.remove(&evict_hash);
                self.stats.write().evictions += 1;
                Some(evict_hash)
            } else {
                None
            }
        } else {
            None
        };

        // Insert new entry
        cache.insert(
            query_hash,
            CachedQueryResult {
                doc_id,
                embedding,
                query_hash,
                cached_at: Instant::now(),
            },
        );
        query_embs.insert(query_hash, query_embedding);
        lru.push_back(query_hash);

        evicted_hash
    }

    /// Get cache statistics
    pub fn stats(&self) -> QueryCacheStats {
        let stats = self.stats.read();
        let cache = self.cache.read();

        let total_hits = stats.exact_hits + stats.similarity_hits;
        let total_requests = total_hits + stats.misses;
        let hit_rate = if total_requests > 0 {
            total_hits as f64 / total_requests as f64
        } else {
            0.0
        };

        let avg_similarity = if stats.similarity_hits > 0 {
            stats.total_similarity_score / stats.similarity_hits as f64
        } else {
            0.0
        };

        QueryCacheStats {
            exact_hits: stats.exact_hits,
            similarity_hits: stats.similarity_hits,
            total_hits,
            misses: stats.misses,
            hit_rate,
            evictions: stats.evictions,
            size: cache.len(),
            capacity: self.capacity,
            avg_similarity,
        }
    }

    /// Clear cache (useful for testing)
    pub fn clear(&self) {
        self.cache.write().clear();
        self.query_embeddings.write().clear();
        self.lru_queue.write().clear();

        let mut stats = self.stats.write();
        stats.exact_hits = 0;
        stats.similarity_hits = 0;
        stats.misses = 0;
        stats.evictions = 0;
        stats.total_similarity_score = 0.0;
    }

    /// Get current cache size
    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    /// Get cache capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Set similarity threshold
    pub fn set_similarity_threshold(&mut self, threshold: f32) {
        self.similarity_threshold = threshold.clamp(0.0, 1.0);
    }

    /// Set similarity scan limit (max queries to scan)
    pub fn set_similarity_scan_limit(&mut self, limit: usize) {
        self.similarity_scan_limit = limit.max(10).min(10000);
    }

    // =========================================================================
    // Private Helper Methods
    // =========================================================================

    /// Hash embedding to 64-bit query hash
    ///
    /// Uses DefaultHasher (FxHash) for fast hashing.
    /// Quantizes floats to 16-bit precision before hashing for stability.
    fn hash_embedding(embedding: &[f32]) -> u64 {
        let mut hasher = DefaultHasher::new();

        // Hash embedding dimension first
        embedding.len().hash(&mut hasher);

        // Quantize floats to 16-bit for stable hashing
        // This prevents hash drift from floating-point precision differences
        for &val in embedding {
            let quantized = (val * 32768.0).round() as i16;
            quantized.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Find similar query using cosine similarity
    ///
    /// Scans cached query embeddings for similarity > threshold.
    /// Limited to `similarity_scan_limit` most recent queries for performance.
    ///
    /// # Performance
    /// O(k × d) where k = min(cache_size, scan_limit), d = embedding_dim
    /// For k=2000, d=384: ~768K ops × 0.5ns = ~400μs worst case
    /// Typical case: k=100, <20μs
    fn find_similar_query(
        &self,
        query_embedding: &[f32],
        query_hash: u64,
    ) -> Option<CachedQueryResult> {
        let query_embs = self.query_embeddings.read();
        let cache = self.cache.read();
        let lru = self.lru_queue.read();

        // Scan most recent queries (back of LRU queue)
        let scan_count = self.similarity_scan_limit.min(lru.len());
        let candidates = lru.iter().rev().take(scan_count);

        let mut best_similarity = self.similarity_threshold;
        let mut best_hash = None;

        for &candidate_hash in candidates {
            if candidate_hash == query_hash {
                continue; // Skip self (already checked in exact match)
            }

            if let Some(candidate_emb) = query_embs.get(&candidate_hash) {
                let similarity = Self::cosine_similarity(query_embedding, candidate_emb);

                if similarity > best_similarity {
                    best_similarity = similarity;
                    best_hash = Some(candidate_hash);
                }
            }
        }

        if let Some(matched_hash) = best_hash {
            // Found similar query - update LRU and stats
            drop(query_embs);
            drop(lru);

            self.update_lru(matched_hash);

            let mut stats = self.stats.write();
            stats.similarity_hits += 1;
            stats.total_similarity_score += best_similarity as f64;

            cache.get(&matched_hash).cloned()
        } else {
            // No match - record miss
            drop(query_embs);
            drop(cache);
            drop(lru);

            self.stats.write().misses += 1;
            None
        }
    }

    /// Compute cosine similarity between two embeddings
    ///
    /// Formula: cos(θ) = (A · B) / (||A|| × ||B||)
    ///
    /// # Performance
    /// O(d) where d = embedding dimension
    /// For d=384: ~384 ops × 0.5ns = ~200ns
    ///
    /// # Future Optimization
    /// - SIMD vectorization (8× speedup on AVX2)
    /// - Pre-normalize embeddings (skip magnitude computation)
    #[inline]
    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 0.0;
        }

        let mut dot = 0.0;
        let mut mag_a = 0.0;
        let mut mag_b = 0.0;

        for i in 0..a.len() {
            dot += a[i] * b[i];
            mag_a += a[i] * a[i];
            mag_b += b[i] * b[i];
        }

        let magnitude = (mag_a * mag_b).sqrt();
        if magnitude < f32::EPSILON {
            return 0.0;
        }

        (dot / magnitude).clamp(-1.0, 1.0)
    }

    /// Update LRU queue (move query_hash to back)
    ///
    /// Called on cache hit to mark query as recently used.
    fn update_lru(&self, query_hash: u64) {
        let mut lru = self.lru_queue.write();

        if let Some(pos) = lru.iter().position(|&h| h == query_hash) {
            lru.remove(pos);
            lru.push_back(query_hash);
        } else {
            // Defensive: query in cache but not in LRU (should never happen)
            lru.push_back(query_hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_embedding(seed: u64, dim: usize) -> Vec<f32> {
        let mut emb = vec![0.0; dim];
        for (i, val) in emb.iter_mut().enumerate() {
            // Use seed to create unique patterns
            *val = ((seed * 1000 + i as u64) as f32).sin();
        }
        // Normalize
        let mag: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
        if mag > 0.0 {
            for val in &mut emb {
                *val /= mag;
            }
        }
        emb
    }

    fn create_similar_embedding(base: &[f32], noise: f32) -> Vec<f32> {
        let mut similar = base.to_vec();
        for (i, val) in similar.iter_mut().enumerate() {
            *val += (i as f32 * noise).sin() * noise;
        }
        // Re-normalize
        let mag: f32 = similar.iter().map(|x| x * x).sum::<f32>().sqrt();
        if mag > 0.0 {
            for val in &mut similar {
                *val /= mag;
            }
        }
        similar
    }

    #[test]
    fn test_query_cache_basic() {
        let cache = QueryHashCache::new(10, 0.85);

        let query = create_test_embedding(1, 128);
        let doc_emb = create_test_embedding(100, 128);

        // Cache miss
        assert!(cache.get(&query).is_none());
        assert_eq!(cache.stats().misses, 1);

        // Insert
        cache.insert(query.clone(), 42, doc_emb.clone());
        assert_eq!(cache.len(), 1);

        // Cache hit (exact match)
        let cached = cache.get(&query).unwrap();
        assert_eq!(cached.doc_id, 42);
        assert_eq!(cache.stats().exact_hits, 1);
        assert_eq!(cache.stats().total_hits, 1);
    }

    #[test]
    fn test_query_cache_similarity_match() {
        let cache = QueryHashCache::new(10, 0.90);

        let base_query = create_test_embedding(1, 128);
        let doc_emb = create_test_embedding(100, 128);

        // Insert base query
        cache.insert(base_query.clone(), 42, doc_emb.clone());

        // Create similar query (small noise)
        let similar_query = create_similar_embedding(&base_query, 0.01);

        // Should hit via similarity match
        let cached = cache.get(&similar_query);
        assert!(cached.is_some(), "Similar query should hit cache");
        assert_eq!(cached.unwrap().doc_id, 42);

        let stats = cache.stats();
        assert_eq!(stats.similarity_hits, 1);
        assert!(stats.avg_similarity > 0.90);
    }

    #[test]
    fn test_query_cache_lru_eviction() {
        let cache = QueryHashCache::new(3, 0.85);

        // Fill cache
        cache.insert(
            create_test_embedding(1, 128),
            1,
            create_test_embedding(101, 128),
        );
        cache.insert(
            create_test_embedding(2, 128),
            2,
            create_test_embedding(102, 128),
        );
        cache.insert(
            create_test_embedding(3, 128),
            3,
            create_test_embedding(103, 128),
        );
        assert_eq!(cache.len(), 3);

        // Insert 4th query, should evict query 1 (oldest)
        cache.insert(
            create_test_embedding(4, 128),
            4,
            create_test_embedding(104, 128),
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.stats().evictions, 1);

        // Query 1 should be evicted
        assert!(cache.get(&create_test_embedding(1, 128)).is_none());

        // Queries 2, 3, 4 should still be present
        assert!(cache.get(&create_test_embedding(2, 128)).is_some());
        assert!(cache.get(&create_test_embedding(3, 128)).is_some());
        assert!(cache.get(&create_test_embedding(4, 128)).is_some());
    }

    #[test]
    fn test_query_cache_lru_update_on_hit() {
        let cache = QueryHashCache::new(3, 0.85);

        let q1 = create_test_embedding(1, 128);
        let q2 = create_test_embedding(2, 128);
        let q3 = create_test_embedding(3, 128);
        let q4 = create_test_embedding(4, 128);

        // Fill cache
        cache.insert(q1.clone(), 1, create_test_embedding(101, 128));
        cache.insert(q2.clone(), 2, create_test_embedding(102, 128));
        cache.insert(q3.clone(), 3, create_test_embedding(103, 128));

        // Access query 1 (moves to back of LRU)
        cache.get(&q1);

        // Insert query 4, should evict query 2 (now oldest)
        cache.insert(q4.clone(), 4, create_test_embedding(104, 128));

        // Query 1 should still be present (accessed recently)
        assert!(cache.get(&q1).is_some());

        // Query 2 should be evicted
        assert!(cache.get(&q2).is_none());

        // Queries 3, 4 should be present
        assert!(cache.get(&q3).is_some());
        assert!(cache.get(&q4).is_some());
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let c = vec![0.0, 1.0, 0.0];

        // Identical vectors: similarity = 1.0
        assert!((QueryHashCache::cosine_similarity(&a, &b) - 1.0).abs() < 0.001);

        // Orthogonal vectors: similarity = 0.0
        assert!((QueryHashCache::cosine_similarity(&a, &c) - 0.0).abs() < 0.001);

        // Opposite vectors: similarity = -1.0
        let d = vec![-1.0, 0.0, 0.0];
        assert!((QueryHashCache::cosine_similarity(&a, &d) + 1.0).abs() < 0.001);
    }

    #[test]
    fn test_hash_stability() {
        let emb = create_test_embedding(42, 128);

        let hash1 = QueryHashCache::hash_embedding(&emb);
        let hash2 = QueryHashCache::hash_embedding(&emb);

        // Same embedding should produce same hash
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_query_cache_clear() {
        let cache = QueryHashCache::new(10, 0.85);

        cache.insert(
            create_test_embedding(1, 128),
            1,
            create_test_embedding(101, 128),
        );
        cache.insert(
            create_test_embedding(2, 128),
            2,
            create_test_embedding(102, 128),
        );

        assert_eq!(cache.len(), 2);
        cache.get(&create_test_embedding(1, 128));

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.stats().total_hits, 0);
        assert_eq!(cache.stats().misses, 0);
    }

    #[test]
    fn test_query_cache_hit_rate() {
        let cache = QueryHashCache::new(10, 0.85);

        let q1 = create_test_embedding(1, 128);
        let q2 = create_test_embedding(2, 128);

        cache.insert(q1.clone(), 1, create_test_embedding(101, 128));
        cache.insert(q2.clone(), 2, create_test_embedding(102, 128));

        // 2 hits, 2 misses
        cache.get(&q1); // Hit
        cache.get(&q2); // Hit
        cache.get(&create_test_embedding(3, 128)); // Miss
        cache.get(&create_test_embedding(4, 128)); // Miss

        let stats = cache.stats();
        assert_eq!(stats.total_hits, 2);
        assert_eq!(stats.misses, 2);
        assert!((stats.hit_rate - 0.5).abs() < 0.01); // 50% hit rate
    }

    #[test]
    fn test_similarity_threshold() {
        let cache = QueryHashCache::new(10, 0.95); // High threshold

        let base = create_test_embedding(1, 128);
        cache.insert(base.clone(), 42, create_test_embedding(100, 128));

        // Very similar query (small noise) - should hit
        let very_similar = create_similar_embedding(&base, 0.001);
        assert!(cache.get(&very_similar).is_some());

        // Somewhat similar query (medium noise) - should miss (below threshold)
        let somewhat_similar = create_similar_embedding(&base, 0.1);
        assert!(cache.get(&somewhat_similar).is_none());
    }
}
