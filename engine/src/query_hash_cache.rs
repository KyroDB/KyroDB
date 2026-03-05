//! Query Hash Cache - L1b Cache Layer for Semantic Similarity
//!
//! **Purpose**: Cache query→top‑k search results based on semantic similarity.
//! Complements the document-level cache (L1a) by caching paraphrased queries.
//!
//! # Architecture
//! - L1a (Document Cache): Frequency-based (Learned predictor) - caches hot documents
//! - L1b (Query Cache): Similarity-based (this module) - caches similar queries
//!
//! # Use Case
//! RAG systems often receive paraphrased queries for the same document:
//! - "What is machine learning?" (query 1)
//! - "Explain machine learning" (query 2, similar to query 1)
//! - "Define ML" (query 3, similar to queries 1 and 2)
//!
//! Without query cache: Each query hits hot/cold tier search
//! With query cache: Query 2 and 3 hit L1b (fast, semantic match)
//!
//! # Performance
//! - Exact match: O(1) hash lookup by query hash
//! - Similarity match: O(min(cache_size, scan_limit) * dim)
//! - Memory: O(capacity * dim)

use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::config::DistanceMetric;
use crate::hnsw_index::SearchResult;
use crate::lru_index::LruIndex;

const INSERT_INVALIDATION_PREFIX_DIMS: usize = 32;

/// Cached query result
///
/// Stores the top‑k search results for a query, indexed by query hash.
/// When a similar query arrives, we can return cached results without
/// re-running vector search.
#[derive(Clone, Debug)]
pub struct CachedQueryResult {
    /// Top-k search results for this query
    pub results: Vec<SearchResult>,

    /// The `k` value used when computing `results` (before truncation).
    ///
    /// This allows the cache to safely answer requests for smaller `k` values by slicing, while
    /// forcing a miss for larger `k` values so callers can recompute accurate results.
    pub requested_k: usize,

    /// Query hash (for exact match lookups)
    pub query_hash: u64,

    /// Logical query-cache scope for isolation between contexts.
    pub scope: u64,

    /// Timestamp when cached
    pub cached_at: Instant,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct QueryCacheKey {
    scope: u64,
    query_hash: u64,
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
/// Caches query→top‑k search results based on semantic similarity.
/// Uses two-level lookup:
/// 1. Exact match: O(1) hash lookup
/// 2. Similarity scan: O(k) cosine similarity (k = min(cache_size, scan_limit))
///
/// # Thread Safety
/// Uses Arc<RwLock<...>> for concurrent access. Multiple readers can query
/// simultaneously, writes (inserts/evictions) are exclusive.
///
/// # Memory Layout
/// - state.cache: query key -> cached top-k result
/// - state.query_embeddings: query key -> embedding for similarity scan
/// - state.lru: O(1) LRU index over query keys
///
/// Total memory: capacity × (384-dim × 4 bytes + 64 bytes overhead) ≈ 1600 bytes/query
pub struct QueryHashCache {
    /// Main state: query cache entry + embedding + LRU order
    state: Arc<RwLock<QueryCacheState>>,

    /// Cache capacity (max number of queries)
    capacity: usize,

    /// Similarity threshold for matches (cosine similarity)
    similarity_threshold: f32,

    /// Maximum number of queries to scan for similarity (performance limit)
    similarity_scan_limit: usize,

    /// Statistics (separate lock to avoid contention)
    stats: Arc<RwLock<QueryCacheStatsInternal>>,

    /// Monotonic generation bumped whenever the underlying search space mutates.
    invalidation_generation: AtomicU64,
}

#[derive(Debug, Default)]
struct QueryCacheState {
    cache: HashMap<QueryCacheKey, CachedQueryResult>,
    query_embeddings: HashMap<QueryCacheKey, Vec<f32>>,
    query_embedding_stats: HashMap<QueryCacheKey, QueryEmbeddingStats>,
    doc_to_query_keys: HashMap<u64, Vec<QueryCacheKey>>,
    lru: LruIndex<QueryCacheKey>,
}

#[derive(Clone, Copy, Debug, Default)]
struct QueryEmbeddingStats {
    norm: f32,
    tail_norm: f32,
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

enum CacheInsertDisposition {
    Inserted(Option<u64>),
    SkippedGeneration,
}

impl QueryHashCache {
    /// Create new query hash cache
    ///
    /// # Parameters
    /// - `capacity`: Maximum number of queries to cache (e.g., 100-1000)
    /// - `similarity_threshold`: Minimum cosine similarity for match (e.g., 0.52)
    ///
    /// # Memory Usage
    /// Approximately `capacity × 1600 bytes` for 384-dim embeddings
    /// Example: 100 queries × 1600 bytes = 160 KB
    pub fn new(capacity: usize, similarity_threshold: f32) -> Self {
        Self {
            state: Arc::new(RwLock::new(QueryCacheState {
                cache: HashMap::with_capacity(capacity),
                query_embeddings: HashMap::with_capacity(capacity),
                query_embedding_stats: HashMap::with_capacity(capacity),
                doc_to_query_keys: HashMap::with_capacity(capacity.saturating_mul(4)),
                lru: LruIndex::with_capacity(capacity),
            })),
            capacity,
            similarity_threshold: similarity_threshold.clamp(0.0, 1.0),
            similarity_scan_limit: 2000, // Limit similarity scan to 2000 most recent queries
            stats: Arc::new(RwLock::new(QueryCacheStatsInternal::default())),
            invalidation_generation: AtomicU64::new(0),
        }
    }

    /// Get cached result for query
    ///
    /// Performs two-level lookup:
    /// 1. Exact match: Check if query_hash exists in cache (O(1))
    /// 2. Similarity match: Scan recent queries for similarity > threshold (O(k))
    ///
    /// # Returns
    /// - `Some(Vec<SearchResult>)` if exact or similarity match found
    /// - `None` if no match
    pub fn get(&self, query_embedding: &[f32], k: usize) -> Option<Vec<SearchResult>> {
        self.get_scoped(0, query_embedding, k)
    }

    /// Scoped variant of `get` for tenant/namespace/filter-isolated query caches.
    pub fn get_scoped(
        &self,
        scope: u64,
        query_embedding: &[f32],
        k: usize,
    ) -> Option<Vec<SearchResult>> {
        let query_hash = Self::hash_embedding(query_embedding);
        let query_key = QueryCacheKey { scope, query_hash };

        // Exact lookup on shared read lock to avoid serializing misses.
        enum ExactLookup {
            Hit(Vec<SearchResult>),
            InsufficientK,
            Miss,
        }

        let exact_lookup = {
            let state = self.state.read();
            match state.cache.get(&query_key) {
                Some(cached) if cached.requested_k >= k => {
                    let take = k.min(cached.results.len());
                    ExactLookup::Hit(cached.results[..take].to_vec())
                }
                Some(_) => ExactLookup::InsufficientK,
                None => ExactLookup::Miss,
            }
        };

        match exact_lookup {
            ExactLookup::Hit(results) => {
                // Best-effort recency refresh: avoid blocking read path when writers are active.
                if let Some(mut state) = self.state.try_write() {
                    let _ = state.lru.touch(query_key);
                }
                self.stats.write().exact_hits += 1;
                return Some(results);
            }
            ExactLookup::InsufficientK => {
                self.stats.write().misses += 1;
                return None;
            }
            ExactLookup::Miss => {}
        }

        // Step 2: Try similarity match (slower path)
        self.find_similar_query(scope, query_embedding, query_key, k)
    }

    /// Insert query→search results into cache
    ///
    /// # Parameters
    /// - `query_embedding`: Query vector (will be hashed and stored)
    /// - `results`: Top-k search results for this query
    ///
    /// # Returns
    /// - `Some(query_hash)` if an eviction occurred
    /// - `None` if no eviction
    ///
    /// # Eviction Policy
    /// LRU eviction when cache is full (evicts least recently used query)
    pub fn insert(&self, query_embedding: Vec<f32>, results: Vec<SearchResult>) -> Option<u64> {
        let requested_k = results.len();
        self.insert_with_k_scoped(0, query_embedding, results, requested_k)
    }

    pub fn insert_with_k(
        &self,
        query_embedding: Vec<f32>,
        results: Vec<SearchResult>,
        requested_k: usize,
    ) -> Option<u64> {
        self.insert_with_k_scoped(0, query_embedding, results, requested_k)
    }

    pub fn insert_with_k_scoped(
        &self,
        scope: u64,
        query_embedding: Vec<f32>,
        results: Vec<SearchResult>,
        requested_k: usize,
    ) -> Option<u64> {
        match self.insert_with_k_scoped_internal(scope, query_embedding, results, requested_k, None)
        {
            CacheInsertDisposition::Inserted(evicted_hash) => evicted_hash,
            CacheInsertDisposition::SkippedGeneration => None,
        }
    }

    pub fn insert_with_k_scoped_if_generation(
        &self,
        scope: u64,
        query_embedding: Vec<f32>,
        results: Vec<SearchResult>,
        requested_k: usize,
        expected_generation: u64,
    ) -> bool {
        matches!(
            self.insert_with_k_scoped_internal(
                scope,
                query_embedding,
                results,
                requested_k,
                Some(expected_generation),
            ),
            CacheInsertDisposition::Inserted(_)
        )
    }

    pub fn invalidation_generation(&self) -> u64 {
        self.invalidation_generation.load(Ordering::Acquire)
    }

    fn insert_with_k_scoped_internal(
        &self,
        scope: u64,
        query_embedding: Vec<f32>,
        results: Vec<SearchResult>,
        requested_k: usize,
        expected_generation: Option<u64>,
    ) -> CacheInsertDisposition {
        let query_hash = Self::hash_embedding(&query_embedding);
        let query_key = QueryCacheKey { scope, query_hash };
        let embedding_stats =
            Self::embedding_stats(&query_embedding, INSERT_INVALIDATION_PREFIX_DIMS);

        let requested_k = requested_k.max(results.len());

        if let Some(expected_generation) = expected_generation {
            if self.invalidation_generation.load(Ordering::Acquire) != expected_generation {
                return CacheInsertDisposition::SkippedGeneration;
            }
        }

        let mut state = self.state.write();

        if let Some(expected_generation) = expected_generation {
            if self.invalidation_generation.load(Ordering::Acquire) != expected_generation {
                return CacheInsertDisposition::SkippedGeneration;
            }
        }

        // Check if already in cache (update case)
        if let Some(existing) = state.cache.get(&query_key) {
            let existing_k = existing.requested_k;
            if requested_k >= existing_k {
                let new_doc_ids = Self::unique_result_doc_ids(&results);
                // Update existing entry (monotonic: keep the larger-k entry).
                if let Some(old_entry) = state.cache.insert(
                    query_key,
                    CachedQueryResult {
                        results,
                        requested_k,
                        query_hash,
                        scope,
                        cached_at: Instant::now(),
                    },
                ) {
                    Self::unindex_entry_docs(&mut state, query_key, &old_entry.results);
                }
                Self::index_entry_doc_ids(&mut state, query_key, &new_doc_ids);
                state.query_embeddings.insert(query_key, query_embedding);
                state
                    .query_embedding_stats
                    .insert(query_key, embedding_stats);
            }
            // Move to MRU position.
            let _ = state.lru.touch(query_key);

            return CacheInsertDisposition::Inserted(None);
        }

        // Evict if at capacity
        let evicted_hash = if state.cache.len() >= self.capacity {
            if let Some(evict_key) = state.lru.pop_lru() {
                if let Some(old_entry) = state.cache.remove(&evict_key) {
                    Self::unindex_entry_docs(&mut state, evict_key, &old_entry.results);
                }
                let _ = state.query_embeddings.remove(&evict_key);
                let _ = state.query_embedding_stats.remove(&evict_key);
                self.stats.write().evictions += 1;
                Some(evict_key.query_hash)
            } else {
                None
            }
        } else {
            None
        };

        // Insert new entry
        let inserted_doc_ids = Self::unique_result_doc_ids(&results);
        state.cache.insert(
            query_key,
            CachedQueryResult {
                results,
                requested_k,
                query_hash,
                scope,
                cached_at: Instant::now(),
            },
        );
        state.query_embeddings.insert(query_key, query_embedding);
        state
            .query_embedding_stats
            .insert(query_key, embedding_stats);
        Self::index_entry_doc_ids(&mut state, query_key, &inserted_doc_ids);
        state.lru.insert_new(query_key);

        CacheInsertDisposition::Inserted(evicted_hash)
    }

    /// Get cache statistics
    pub fn stats(&self) -> QueryCacheStats {
        let state = self.state.read();
        let stats = self.stats.read();

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
            size: state.cache.len(),
            capacity: self.capacity,
            avg_similarity,
        }
    }

    /// Clear cache (useful for testing)
    pub fn clear(&self) {
        let _ = self.invalidation_generation.fetch_add(1, Ordering::AcqRel);
        let mut state = self.state.write();
        state.cache.clear();
        state.query_embeddings.clear();
        state.query_embedding_stats.clear();
        state.doc_to_query_keys.clear();
        state.lru.clear();

        let mut stats = self.stats.write();
        stats.exact_hits = 0;
        stats.similarity_hits = 0;
        stats.misses = 0;
        stats.evictions = 0;
        stats.total_similarity_score = 0.0;
    }

    /// Get current cache size
    pub fn len(&self) -> usize {
        self.state.read().cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.state.read().cache.is_empty()
    }

    /// Get cache capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Remove cached queries that resolve to the specified document ID.
    ///
    /// Uses a reverse index (`doc_id -> query keys`) for near O(affected_entries)
    /// invalidation instead of scanning the full cache.
    /// Returns the number of invalidated entries to aid profiling.
    pub fn invalidate_doc(&self, doc_id: u64) -> usize {
        let _ = self.invalidation_generation.fetch_add(1, Ordering::AcqRel);
        let mut state = self.state.write();
        let mut to_remove = match state.doc_to_query_keys.remove(&doc_id) {
            Some(keys) => keys,
            None => return 0,
        };
        to_remove.sort_unstable_by(|a, b| {
            a.scope
                .cmp(&b.scope)
                .then_with(|| a.query_hash.cmp(&b.query_hash))
        });
        to_remove.dedup();

        let mut removed = 0usize;
        for key in to_remove {
            if Self::remove_entry(&mut state, key) {
                removed += 1;
            }
        }

        removed
    }

    /// Invalidate only cache entries whose top-k boundary can be changed by a new/updated vector.
    ///
    /// This avoids global cache flushes on every write while preserving read-your-writes
    /// semantics for affected query results.
    pub fn invalidate_for_insert(&self, embedding: &[f32], distance: DistanceMetric) -> usize {
        let _ = self.invalidation_generation.fetch_add(1, Ordering::AcqRel);
        let prefix_dims = INSERT_INVALIDATION_PREFIX_DIMS.min(embedding.len());
        let insert_stats = Self::embedding_stats(embedding, prefix_dims);

        let mut state = self.state.write();
        let mut removed_keys = Vec::new();
        for (key, cached) in state.cache.iter() {
            let Some(query_embedding) = state.query_embeddings.get(key) else {
                removed_keys.push(*key);
                continue;
            };

            if cached.results.len() < cached.requested_k {
                removed_keys.push(*key);
                continue;
            }

            if query_embedding.len() != embedding.len() {
                removed_keys.push(*key);
                continue;
            }

            let worst_cached_distance = cached
                .results
                .iter()
                .map(|result| result.distance)
                .fold(f32::NEG_INFINITY, f32::max);
            if !worst_cached_distance.is_finite() {
                removed_keys.push(*key);
                continue;
            }

            let query_stats = state
                .query_embedding_stats
                .get(key)
                .copied()
                .unwrap_or_else(|| Self::embedding_stats(query_embedding, prefix_dims));
            if !Self::insert_can_affect_cached_boundary(
                query_embedding,
                query_stats,
                embedding,
                insert_stats,
                prefix_dims,
                worst_cached_distance,
                distance,
            ) {
                continue;
            }

            let candidate_distance = Self::distance(query_embedding, embedding, distance);
            if !candidate_distance.is_finite() {
                removed_keys.push(*key);
                continue;
            }

            if candidate_distance <= worst_cached_distance {
                removed_keys.push(*key);
            }
        }

        if removed_keys.is_empty() {
            return 0;
        }

        let mut removed = 0usize;
        for key in removed_keys {
            if Self::remove_entry(&mut state, key) {
                removed += 1;
            }
        }
        removed
    }

    /// Set similarity threshold
    pub fn set_similarity_threshold(&mut self, threshold: f32) {
        self.similarity_threshold = threshold.clamp(0.0, 1.0);
    }

    /// Set similarity scan limit (max queries to scan)
    pub fn set_similarity_scan_limit(&mut self, limit: usize) {
        self.similarity_scan_limit = limit.clamp(10, 10000);
    }

    // =========================================================================
    // Private Helper Methods
    // =========================================================================

    #[inline]
    fn embedding_stats(embedding: &[f32], prefix_dims: usize) -> QueryEmbeddingStats {
        let prefix_dims = prefix_dims.min(embedding.len());
        let mut norm_sq = 0.0f32;
        let mut tail_sq = 0.0f32;
        for (idx, &value) in embedding.iter().enumerate() {
            let sq = value * value;
            norm_sq += sq;
            if idx >= prefix_dims {
                tail_sq += sq;
            }
        }
        QueryEmbeddingStats {
            norm: norm_sq.sqrt(),
            tail_norm: tail_sq.sqrt(),
        }
    }

    #[inline]
    fn dot_prefix(a: &[f32], b: &[f32], prefix_dims: usize) -> f32 {
        let limit = prefix_dims.min(a.len()).min(b.len());
        let mut dot = 0.0f32;
        for i in 0..limit {
            dot += a[i] * b[i];
        }
        dot
    }

    #[inline]
    fn l2_prefix_sq(a: &[f32], b: &[f32], prefix_dims: usize) -> f32 {
        let limit = prefix_dims.min(a.len()).min(b.len());
        let mut sq = 0.0f32;
        for i in 0..limit {
            let diff = a[i] - b[i];
            sq += diff * diff;
        }
        sq
    }

    #[inline]
    fn dot_upper_bound_from_prefix(
        query_embedding: &[f32],
        query_stats: QueryEmbeddingStats,
        insert_embedding: &[f32],
        insert_stats: QueryEmbeddingStats,
        prefix_dims: usize,
    ) -> Option<f32> {
        if query_embedding.len() != insert_embedding.len() {
            return None;
        }
        let prefix_dot = Self::dot_prefix(query_embedding, insert_embedding, prefix_dims);
        Some(prefix_dot + query_stats.tail_norm * insert_stats.tail_norm)
    }

    #[inline]
    fn cosine_upper_bound_from_prefix(
        query_embedding: &[f32],
        query_stats: QueryEmbeddingStats,
        insert_embedding: &[f32],
        insert_stats: QueryEmbeddingStats,
        prefix_dims: usize,
    ) -> Option<f32> {
        let denom = query_stats.norm * insert_stats.norm;
        if !denom.is_finite() || denom <= 0.0 {
            return None;
        }
        let upper_dot = Self::dot_upper_bound_from_prefix(
            query_embedding,
            query_stats,
            insert_embedding,
            insert_stats,
            prefix_dims,
        )?;
        Some((upper_dot / denom).clamp(-1.0, 1.0))
    }

    #[inline]
    fn insert_can_affect_cached_boundary(
        query_embedding: &[f32],
        query_stats: QueryEmbeddingStats,
        insert_embedding: &[f32],
        insert_stats: QueryEmbeddingStats,
        prefix_dims: usize,
        worst_cached_distance: f32,
        metric: DistanceMetric,
    ) -> bool {
        if query_embedding.len() != insert_embedding.len() {
            return true;
        }
        if !worst_cached_distance.is_finite() {
            return true;
        }

        match metric {
            DistanceMetric::Euclidean => {
                let radius_sq = worst_cached_distance.max(0.0).powi(2);
                Self::l2_prefix_sq(query_embedding, insert_embedding, prefix_dims) <= radius_sq
            }
            DistanceMetric::Cosine => {
                let similarity_threshold = 1.0 - worst_cached_distance;
                if !similarity_threshold.is_finite() {
                    return true;
                }
                Self::cosine_upper_bound_from_prefix(
                    query_embedding,
                    query_stats,
                    insert_embedding,
                    insert_stats,
                    prefix_dims,
                )
                .map(|upper_bound| upper_bound >= similarity_threshold)
                .unwrap_or(true)
            }
            DistanceMetric::InnerProduct => {
                let dot_threshold = 1.0 - worst_cached_distance;
                if !dot_threshold.is_finite() {
                    return true;
                }
                Self::dot_upper_bound_from_prefix(
                    query_embedding,
                    query_stats,
                    insert_embedding,
                    insert_stats,
                    prefix_dims,
                )
                .map(|upper_bound| upper_bound >= dot_threshold)
                .unwrap_or(true)
            }
        }
    }

    fn unique_result_doc_ids(results: &[SearchResult]) -> Vec<u64> {
        let mut doc_ids: Vec<u64> = results.iter().map(|r| r.doc_id).collect();
        if doc_ids.len() <= 1 {
            return doc_ids;
        }
        doc_ids.sort_unstable();
        doc_ids.dedup();
        doc_ids
    }

    fn index_entry_doc_ids(state: &mut QueryCacheState, key: QueryCacheKey, doc_ids: &[u64]) {
        for &doc_id in doc_ids {
            let keys = state.doc_to_query_keys.entry(doc_id).or_default();
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
    }

    fn unindex_entry_docs(
        state: &mut QueryCacheState,
        key: QueryCacheKey,
        results: &[SearchResult],
    ) {
        for doc_id in Self::unique_result_doc_ids(results) {
            if let Some(keys) = state.doc_to_query_keys.get_mut(&doc_id) {
                keys.retain(|entry_key| *entry_key != key);
                if keys.is_empty() {
                    state.doc_to_query_keys.remove(&doc_id);
                }
            }
        }
    }

    fn remove_entry(state: &mut QueryCacheState, key: QueryCacheKey) -> bool {
        let removed_entry = state.cache.remove(&key);
        let _ = state.query_embeddings.remove(&key);
        let _ = state.query_embedding_stats.remove(&key);
        let _ = state.lru.remove(key);
        if let Some(entry) = removed_entry {
            Self::unindex_entry_docs(state, key, &entry.results);
            true
        } else {
            false
        }
    }

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
    fn find_similar_query(
        &self,
        scope: u64,
        query_embedding: &[f32],
        query_key: QueryCacheKey,
        k: usize,
    ) -> Option<Vec<SearchResult>> {
        let (best_key, best_similarity) = {
            let state = self.state.read();

            // Scan most recent queries (MRU -> LRU).
            let scan_count = self.similarity_scan_limit.min(state.lru.len());
            let mut best_similarity = self.similarity_threshold;
            let mut best_key = None;

            state.lru.for_each_recent(scan_count, |candidate_key| {
                if candidate_key.scope != scope || candidate_key == query_key {
                    return true;
                }

                let Some(candidate) = state.cache.get(&candidate_key) else {
                    return true;
                };
                if candidate.requested_k < k {
                    return true;
                }

                let Some(candidate_emb) = state.query_embeddings.get(&candidate_key) else {
                    return true;
                };

                let similarity = Self::cosine_similarity(query_embedding, candidate_emb);
                if similarity > best_similarity {
                    best_similarity = similarity;
                    best_key = Some(candidate_key);
                }
                true
            });

            (best_key, best_similarity)
        };

        if let Some(matched_key) = best_key {
            // Refresh recency; if entry was evicted in the meantime, count as miss.
            let cached_results = {
                let mut state = self.state.write();
                let _ = state.lru.touch(matched_key);
                state.cache.get(&matched_key).and_then(|entry| {
                    if entry.requested_k < k {
                        return None;
                    }
                    let take = k.min(entry.results.len());
                    Some(entry.results[..take].to_vec())
                })
            };

            if let Some(results) = cached_results {
                let mut stats = self.stats.write();
                stats.similarity_hits += 1;
                stats.total_similarity_score += best_similarity as f64;
                Some(results)
            } else {
                self.stats.write().misses += 1;
                None
            }
        } else {
            // No match - record miss
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

        crate::simd::cosine_similarity_f32(a, b)
    }

    #[inline]
    fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
        match metric {
            DistanceMetric::Euclidean => crate::simd::l2_distance_f32(a, b),
            DistanceMetric::Cosine => 1.0 - crate::simd::cosine_similarity_f32(a, b),
            DistanceMetric::InnerProduct => 1.0 - crate::simd::dot_f32(a, b),
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

    fn make_results(doc_id: u64) -> Vec<SearchResult> {
        vec![SearchResult {
            doc_id,
            distance: 0.0,
        }]
    }

    fn make_results_multi(doc_ids: &[u64]) -> Vec<SearchResult> {
        doc_ids
            .iter()
            .enumerate()
            .map(|(idx, &doc_id)| SearchResult {
                doc_id,
                distance: idx as f32 * 0.01,
            })
            .collect()
    }

    #[test]
    fn test_query_cache_basic() {
        let cache = QueryHashCache::new(10, 0.85);

        let query = create_test_embedding(1, 128);
        // Cache miss
        assert!(cache.get(&query, 1).is_none());
        assert_eq!(cache.stats().misses, 1);

        // Insert
        cache.insert(query.clone(), make_results(42));
        assert_eq!(cache.len(), 1);

        // Cache hit (exact match)
        let cached = cache.get(&query, 1).unwrap();
        assert_eq!(cached[0].doc_id, 42);
        assert_eq!(cache.stats().exact_hits, 1);
        assert_eq!(cache.stats().total_hits, 1);
    }

    #[test]
    fn test_query_cache_similarity_match() {
        let cache = QueryHashCache::new(10, 0.90);

        let base_query = create_test_embedding(1, 128);
        // Insert base query
        cache.insert(base_query.clone(), make_results(42));

        // Create similar query (small noise)
        let similar_query = create_similar_embedding(&base_query, 0.01);

        // Should hit via similarity match
        let cached = cache.get(&similar_query, 1);
        assert!(cached.is_some(), "Similar query should hit cache");
        assert_eq!(cached.unwrap()[0].doc_id, 42);

        let stats = cache.stats();
        assert_eq!(stats.similarity_hits, 1);
        assert!(stats.avg_similarity > 0.90);
    }

    #[test]
    fn test_query_cache_scope_isolation() {
        let cache = QueryHashCache::new(10, 0.90);

        let base_query = create_test_embedding(99, 128);
        cache.insert_with_k_scoped(7, base_query.clone(), make_results(4242), 1);

        // Same query, different scope: must miss (no cross-tenant/context leakage).
        assert!(cache.get_scoped(8, &base_query, 1).is_none());

        // Similar query, different scope: must also miss (similarity matching is scoped).
        let similar_query = create_similar_embedding(&base_query, 0.01);
        assert!(cache.get_scoped(8, &similar_query, 1).is_none());

        // Original scope should still hit.
        let hit = cache
            .get_scoped(7, &base_query, 1)
            .expect("scoped hit expected");
        assert_eq!(hit[0].doc_id, 4242);
    }

    #[test]
    fn test_query_cache_lru_eviction() {
        let cache = QueryHashCache::new(3, 0.85);

        // Fill cache
        cache.insert(create_test_embedding(1, 128), make_results(1));
        cache.insert(create_test_embedding(2, 128), make_results(2));
        cache.insert(create_test_embedding(3, 128), make_results(3));
        assert_eq!(cache.len(), 3);

        // Insert 4th query, should evict query 1 (oldest)
        cache.insert(create_test_embedding(4, 128), make_results(4));
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.stats().evictions, 1);

        // Query 1 should be evicted
        assert!(cache.get(&create_test_embedding(1, 128), 1).is_none());

        // Queries 2, 3, 4 should still be present
        assert!(cache.get(&create_test_embedding(2, 128), 1).is_some());
        assert!(cache.get(&create_test_embedding(3, 128), 1).is_some());
        assert!(cache.get(&create_test_embedding(4, 128), 1).is_some());
    }

    #[test]
    fn test_query_cache_lru_update_on_hit() {
        let cache = QueryHashCache::new(3, 0.85);

        let q1 = create_test_embedding(1, 128);
        let q2 = create_test_embedding(2, 128);
        let q3 = create_test_embedding(3, 128);
        let q4 = create_test_embedding(4, 128);

        // Fill cache
        cache.insert(q1.clone(), make_results(1));
        cache.insert(q2.clone(), make_results(2));
        cache.insert(q3.clone(), make_results(3));

        // Access query 1 (moves to back of LRU)
        cache.get(&q1, 1);

        // Insert query 4, should evict query 2 (now oldest)
        cache.insert(q4.clone(), make_results(4));

        // Query 1 should still be present (accessed recently)
        assert!(cache.get(&q1, 1).is_some());

        // Query 2 should be evicted
        assert!(cache.get(&q2, 1).is_none());

        // Queries 3, 4 should be present
        assert!(cache.get(&q3, 1).is_some());
        assert!(cache.get(&q4, 1).is_some());
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

        cache.insert(create_test_embedding(1, 128), make_results(1));
        cache.insert(create_test_embedding(2, 128), make_results(2));

        assert_eq!(cache.len(), 2);
        cache.get(&create_test_embedding(1, 128), 1);

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.stats().total_hits, 0);
        assert_eq!(cache.stats().misses, 0);
    }

    #[test]
    fn test_invalidate_for_insert_removes_affected_queries() {
        let cache = QueryHashCache::new(10, 0.85);
        let query = create_test_embedding(11, 64);
        let cached_results = vec![SearchResult {
            doc_id: 42,
            distance: 0.25,
        }];
        cache.insert_with_k(query.clone(), cached_results, 1);

        let removed = cache.invalidate_for_insert(&query, DistanceMetric::Cosine);
        assert_eq!(removed, 1);
        assert!(
            cache.get(&query, 1).is_none(),
            "cache entry must be invalidated when inserted vector can enter top-k"
        );
    }

    #[test]
    fn test_invalidate_for_insert_keeps_unaffected_queries() {
        let cache = QueryHashCache::new(10, 0.85);
        let query = vec![1.0, 0.0];
        let orthogonal = vec![0.0, 1.0];
        let cached_results = vec![SearchResult {
            doc_id: 7,
            distance: 0.0,
        }];
        cache.insert_with_k(query.clone(), cached_results, 1);

        let removed = cache.invalidate_for_insert(&orthogonal, DistanceMetric::Cosine);
        assert_eq!(removed, 0);
        assert!(
            cache.get(&query, 1).is_some(),
            "cache entry should be retained when inserted vector cannot affect top-k"
        );
    }

    #[test]
    fn test_invalidate_for_insert_euclidean_prefilter_preserves_exactness() {
        let cache = QueryHashCache::new(10, 0.85);
        let mut query = vec![0.0; 64];
        query[0] = 1.0;
        cache.insert_with_k(
            query.clone(),
            vec![SearchResult {
                doc_id: 7,
                distance: 0.25,
            }],
            1,
        );

        let far = vec![10.0; 64];
        let removed_far = cache.invalidate_for_insert(&far, DistanceMetric::Euclidean);
        assert_eq!(
            removed_far, 0,
            "far insert must not invalidate cached top-k"
        );
        assert!(cache.get(&query, 1).is_some());

        let removed_near = cache.invalidate_for_insert(&query, DistanceMetric::Euclidean);
        assert_eq!(
            removed_near, 1,
            "identical insert must invalidate cached top-k"
        );
        assert!(cache.get(&query, 1).is_none());
    }

    #[test]
    fn test_invalidate_for_insert_inner_product_prefilter_preserves_exactness() {
        let cache = QueryHashCache::new(10, 0.85);
        let mut query = vec![0.0; 64];
        query[0] = 1.0;
        cache.insert_with_k(
            query.clone(),
            vec![SearchResult {
                doc_id: 8,
                distance: 0.4,
            }],
            1,
        );

        let mut opposite = vec![0.0; 64];
        opposite[0] = -1.0;
        let removed_opposite = cache.invalidate_for_insert(&opposite, DistanceMetric::InnerProduct);
        assert_eq!(removed_opposite, 0);
        assert!(cache.get(&query, 1).is_some());

        let mut similar = vec![0.0; 64];
        similar[0] = 0.9;
        similar[1] = 0.1;
        let removed_similar = cache.invalidate_for_insert(&similar, DistanceMetric::InnerProduct);
        assert_eq!(removed_similar, 1);
        assert!(cache.get(&query, 1).is_none());
    }

    #[test]
    fn test_generation_guard_blocks_stale_backfill_after_invalidation() {
        let cache = QueryHashCache::new(10, 0.85);
        let query = create_test_embedding(222, 64);
        let generation = cache.invalidation_generation();

        let removed = cache.invalidate_for_insert(&query, DistanceMetric::Cosine);
        assert_eq!(removed, 0);

        let inserted = cache.insert_with_k_scoped_if_generation(
            0,
            query.clone(),
            make_results(9),
            1,
            generation,
        );
        assert!(
            !inserted,
            "cache fill computed before an invalidating write must be dropped"
        );
        assert!(cache.get(&query, 1).is_none());
    }

    #[test]
    fn test_invalidate_doc_removes_only_affected_queries() {
        let cache = QueryHashCache::new(16, 0.85);
        let q1 = create_test_embedding(101, 64);
        let q2 = create_test_embedding(102, 64);
        let q3 = create_test_embedding(103, 64);

        cache.insert_with_k(q1.clone(), make_results_multi(&[1, 2]), 2);
        cache.insert_with_k(q2.clone(), make_results_multi(&[1, 3]), 2);
        cache.insert_with_k(q3.clone(), make_results_multi(&[4, 5]), 2);

        let removed_doc1 = cache.invalidate_doc(1);
        assert_eq!(removed_doc1, 2, "two cached queries reference doc_id=1");
        assert!(cache.get(&q1, 2).is_none(), "q1 must be invalidated");
        assert!(cache.get(&q2, 2).is_none(), "q2 must be invalidated");
        assert!(cache.get(&q3, 2).is_some(), "q3 should remain valid");

        // Ensure reverse index cleanup is complete; doc_id=2 belonged only to q1.
        let removed_doc2 = cache.invalidate_doc(2);
        assert_eq!(
            removed_doc2, 0,
            "stale reverse-index entries must be cleaned on invalidation"
        );
    }

    #[test]
    fn test_update_reindexes_doc_invalidation_targets() {
        let cache = QueryHashCache::new(8, 0.85);
        let q = create_test_embedding(111, 64);

        cache.insert_with_k(q.clone(), make_results_multi(&[10]), 1);
        cache.insert_with_k(q.clone(), make_results_multi(&[20, 30]), 2);

        assert_eq!(
            cache.invalidate_doc(10),
            0,
            "old doc_id must not remain indexed after entry update"
        );
        assert_eq!(
            cache.invalidate_doc(20),
            1,
            "new doc_ids must drive invalidation after update"
        );
    }

    #[test]
    fn test_query_cache_hit_rate() {
        let cache = QueryHashCache::new(10, 0.85);

        let q1 = create_test_embedding(1, 128);
        let q2 = create_test_embedding(2, 128);

        cache.insert(q1.clone(), make_results(1));
        cache.insert(q2.clone(), make_results(2));

        // 2 hits, 2 misses
        cache.get(&q1, 1); // Hit
        cache.get(&q2, 1); // Hit
        cache.get(&create_test_embedding(3, 128), 1); // Miss
        cache.get(&create_test_embedding(4, 128), 1); // Miss

        let stats = cache.stats();
        assert_eq!(stats.total_hits, 2);
        assert_eq!(stats.misses, 2);
        assert!((stats.hit_rate - 0.5).abs() < 0.01); // 50% hit rate
    }

    #[test]
    fn test_similarity_threshold() {
        let cache = QueryHashCache::new(10, 0.95); // High threshold

        let base = create_test_embedding(1, 128);
        cache.insert(base.clone(), make_results(42));

        // Very similar query (small noise) - should hit
        let very_similar = create_similar_embedding(&base, 0.001);
        assert!(cache.get(&very_similar, 1).is_some());

        // Somewhat similar query (medium noise) - should miss (below threshold)
        let somewhat_similar = create_similar_embedding(&base, 0.1);
        assert!(cache.get(&somewhat_similar, 1).is_none());
    }
}
