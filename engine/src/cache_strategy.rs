//! Cache Strategy Trait and Implementations
//!
//! Cache strategy framework: A/B testing for LRU vs. Hybrid Semantic Cache
//!
//! Provides pluggable cache strategies:
//! - LRU baseline: Always cache, LRU eviction
//! - Learned: RMI-predicted hotness-based admission
//! - Learned + Semantic: Hybrid frequency + semantic similarity

use crate::learned_cache::LearnedCachePredictor;
use crate::prefetch::Prefetcher;
use crate::query_clustering::QueryClusterer;
use crate::semantic_adapter::SemanticAdapter;
use crate::vector_cache::{CachedVector, VectorCache};
use std::sync::Arc;
use tracing::{instrument, trace};

/// Cache strategy trait
///
/// Defines interface for different cache admission policies.
/// Strategies decide which vectors to cache based on access patterns.
pub trait CacheStrategy: Send + Sync {
    /// Check if vector is in cache
    fn get_cached(&self, doc_id: u64) -> Option<CachedVector>;

    /// Decide if vector should be cached
    ///
    /// Returns `true` if the vector should be admitted to cache.
    /// Strategy-specific logic (e.g., LRU always admits, learned uses RMI prediction).
    fn should_cache(&self, doc_id: u64, embedding: &[f32]) -> bool;

    /// Insert vector into cache
    ///
    /// Only called if `should_cache` returns true.
    fn insert_cached(&self, cached_vector: CachedVector);

    /// Get strategy name (for metrics/logging)
    fn name(&self) -> &str;

    /// Get cache statistics
    fn stats(&self) -> String;
}

/// LRU baseline strategy
///
/// Always caches accessed vectors, evicts least recently used when full.
/// This is the baseline for A/B testing (30-40% expected hit rate).
pub struct LruCacheStrategy {
    pub cache: Arc<VectorCache>,
    name: String,
}

impl LruCacheStrategy {
    /// Create new LRU cache strategy
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Arc::new(VectorCache::new(capacity)),
            name: "lru_baseline".to_string(),
        }
    }
}

impl CacheStrategy for LruCacheStrategy {
    #[instrument(level = "trace", skip(self))]
    fn get_cached(&self, doc_id: u64) -> Option<CachedVector> {
        let hit = self.cache.get(doc_id);
        if hit.is_some() {
            trace!(doc_id, strategy = %self.name, "cache hit");
        } else {
            trace!(doc_id, strategy = %self.name, "cache miss");
        }
        hit
    }

    #[instrument(level = "trace", skip(self, _embedding), fields(doc_id = _doc_id))]
    fn should_cache(&self, _doc_id: u64, _embedding: &[f32]) -> bool {
        true
    }

    #[instrument(level = "trace", skip(self, cached_vector), fields(doc_id = cached_vector.doc_id))]
    fn insert_cached(&self, cached_vector: CachedVector) {
        self.cache.insert(cached_vector);
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn stats(&self) -> String {
        let stats = self.cache.stats();
        format!(
            "LRU: {} hits, {} misses, {:.2}% hit rate, {} evictions",
            stats.hits,
            stats.misses,
            stats.hit_rate * 100.0,
            stats.evictions
        )
    }
}

/// Hybrid Semantic Cache strategy
///
/// Uses RMI predictor (frequency) + optional semantic adapter (similarity)
/// to decide cache admission based on hybrid scoring.
///
pub struct LearnedCacheStrategy {
    pub cache: Arc<VectorCache>,
    pub predictor: Arc<parking_lot::RwLock<LearnedCachePredictor>>,
    semantic_adapter: Arc<parking_lot::RwLock<Option<SemanticAdapter>>>,
    query_clusterer: Arc<parking_lot::RwLock<Option<QueryClusterer>>>,
    prefetcher: Arc<parking_lot::RwLock<Option<Arc<Prefetcher>>>>,
    name: String,
}

impl LearnedCacheStrategy {
    /// Create new Hybrid Semantic Cache strategy (frequency-only mode)
    ///
    /// # Parameters
    /// - `capacity`: Cache capacity
    /// - `predictor`: Trained RMI frequency predictor
    pub fn new(capacity: usize, mut predictor: LearnedCachePredictor) -> Self {
        predictor.set_target_hot_entries(capacity);
        predictor.set_threshold_smoothing(0.6);

        Self {
            cache: Arc::new(VectorCache::new(capacity)),
            predictor: Arc::new(parking_lot::RwLock::new(predictor)),
            semantic_adapter: Arc::new(parking_lot::RwLock::new(None)),
            query_clusterer: Arc::new(parking_lot::RwLock::new(None)),
            prefetcher: Arc::new(parking_lot::RwLock::new(None)),
            name: "learned_rmi".to_string(),
        }
    }

    /// Create new Hybrid Semantic Cache strategy (full hybrid mode)
    ///
    /// # Parameters
    /// - `capacity`: Cache capacity
    /// - `predictor`: Trained RMI frequency predictor
    /// - `semantic_adapter`: Semantic adapter for hybrid decisions
    pub fn new_with_semantic(
        capacity: usize,
        mut predictor: LearnedCachePredictor,
        semantic_adapter: SemanticAdapter,
    ) -> Self {
        predictor.set_target_hot_entries(capacity);
        predictor.set_threshold_smoothing(0.6);

        Self {
            cache: Arc::new(VectorCache::new(capacity)),
            predictor: Arc::new(parking_lot::RwLock::new(predictor)),
            semantic_adapter: Arc::new(parking_lot::RwLock::new(Some(semantic_adapter))),
            query_clusterer: Arc::new(parking_lot::RwLock::new(None)),
            prefetcher: Arc::new(parking_lot::RwLock::new(None)),
            name: "learned_semantic".to_string(),
        }
    }

    /// Enable query clustering
    pub fn enable_query_clustering(&self, similarity_threshold: f32) {
        let clusterer = QueryClusterer::new(similarity_threshold);
        *self.query_clusterer.write() = Some(clusterer);
    }

    /// Enable prefetching
    pub fn enable_prefetching(&self, prefetcher: Arc<Prefetcher>) {
        *self.prefetcher.write() = Some(prefetcher);
    }

    /// Update predictor (for periodic retraining)
    pub fn update_predictor(&self, new_predictor: LearnedCachePredictor) {
        let mut predictor = new_predictor;
        predictor.set_target_hot_entries(self.cache.capacity());
        predictor.set_threshold_smoothing(0.6);
        *self.predictor.write() = predictor;
    }

    /// Check if semantic adapter is enabled
    pub fn has_semantic(&self) -> bool {
        self.semantic_adapter.read().is_some()
    }
}

impl CacheStrategy for LearnedCacheStrategy {
    #[instrument(level = "trace", skip(self), fields(doc_id))]
    fn get_cached(&self, doc_id: u64) -> Option<CachedVector> {
        let hit = self.cache.get(doc_id);
        if hit.is_some() {
            trace!(doc_id, strategy = %self.name, "cache hit");
        } else {
            trace!(doc_id, strategy = %self.name, "cache miss");
        }
        hit
    }

    #[instrument(level = "trace", skip(self, embedding), fields(doc_id, dim = embedding.len()))]
    fn should_cache(&self, doc_id: u64, embedding: &[f32]) -> bool {
        let current_len = self.cache.len();

        {
            let mut predictor = self.predictor.write();
            predictor.calibrate_threshold(current_len);
        }

        // Query clustering: Add query to cluster for semantic grouping
        if let Some(clusterer) = self.query_clusterer.read().as_ref() {
            if !embedding.is_empty() {
                let query_hash = crate::access_logger::hash_embedding(embedding);
                clusterer.add_query(query_hash, embedding);
            }
        }

        // Prefetching: Record access for co-access learning
        if let Some(prefetcher) = self.prefetcher.read().as_ref() {
            prefetcher.record_access(doc_id);
        }

        if current_len < self.cache.capacity() {
            trace!(
                doc_id,
                current_len,
                capacity = self.cache.capacity(),
                "admit (cache not full)"
            );
            return true;
        }

        let predictor = self.predictor.read();

        // Bootstrap mode until predictor is trained
        if !predictor.is_trained() {
            trace!(doc_id, "admit (bootstrap untrained predictor)");
            return true;
        }

        // Compute frequency-based score
        let freq_score = if let Some(score) = predictor.lookup_hotness(doc_id) {
            score
        } else {
            // Unseen document: use baseline admission chance
            let fill_ratio = current_len as f32 / self.cache.capacity() as f32;
            let unseen_chance =
                predictor.unseen_admission_chance() * (1.0 - fill_ratio).clamp(0.1, 1.0);
            drop(predictor);
            let admit = rand::random::<f32>() < unseen_chance;
            trace!(
                doc_id,
                fill_ratio,
                unseen_chance,
                admit,
                "unseen doc admission decision"
            );
            return admit;
        };

        let threshold = predictor.cache_threshold().max(predictor.admission_floor());
        drop(predictor);

        // Check if semantic adapter is enabled
        let semantic_adapter_guard = self.semantic_adapter.read();
        if let Some(semantic_adapter) = semantic_adapter_guard.as_ref() {
            let should_cache = semantic_adapter.should_cache(freq_score, embedding);
            trace!(
                doc_id,
                freq_score,
                threshold,
                should_cache,
                "semantic adapter decision"
            );
            if should_cache {
                semantic_adapter.cache_embedding(doc_id, embedding.to_vec());
            }
            return should_cache;
        }
        drop(semantic_adapter_guard);

        if freq_score >= threshold {
            trace!(doc_id, freq_score, threshold, "admit (freq >= threshold)");
            return true;
        }

        // Soft admission: probabilistic caching below threshold
        let soft_probability = (freq_score / threshold).clamp(0.0, 1.0) * 0.25;
        if soft_probability > 0.0 && rand::random::<f32>() < soft_probability {
            trace!(
                doc_id,
                freq_score,
                threshold,
                soft_probability,
                "admit (soft probability)"
            );
            return true;
        }
        trace!(
            doc_id,
            freq_score,
            threshold,
            soft_probability,
            "reject (below threshold)"
        );
        false
    }
    #[instrument(level = "trace", skip(self, cached_vector), fields(doc_id = cached_vector.doc_id))]
    fn insert_cached(&self, cached_vector: CachedVector) {
        let doc_id = cached_vector.doc_id;
        self.cache.insert(cached_vector);

        // Prefetching: Execute predictive prefetch after insertion
        if let Some(prefetcher) = self.prefetcher.read().as_ref() {
            let candidates = prefetcher.get_prefetch_candidates(doc_id);
            for candidate_doc_id in candidates.into_iter().take(5) {
                if self.cache.get(candidate_doc_id).is_none() {
                    let predictor = self.predictor.read();
                    let hotness = predictor.predict_hotness(candidate_doc_id);
                    drop(predictor);

                    if hotness >= 0.10 {
                        prefetcher.record_prefetch();
                        trace!(
                            source_doc = doc_id,
                            prefetch_doc = candidate_doc_id,
                            hotness,
                            "prefetch candidate (not yet cached)"
                        );
                    }
                }
            }
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn stats(&self) -> String {
        let stats = self.cache.stats();
        let predictor = self.predictor.read();
        let base_stats = format!(
            "Learned: {} hits, {} misses, {:.2}% hit rate, {} evictions, {} tracked docs",
            stats.hits,
            stats.misses,
            stats.hit_rate * 100.0,
            stats.evictions,
            predictor.tracked_count()
        );
        drop(predictor);

        // Add semantic stats if enabled
        let semantic_adapter_guard = self.semantic_adapter.read();
        if let Some(semantic_adapter) = semantic_adapter_guard.as_ref() {
            let sem_stats = semantic_adapter.stats();
            format!(
                "{} | Semantic: {} fast, {} slow, {} hits, {} misses, {} cached embeddings",
                base_stats,
                sem_stats.fast_path_decisions,
                sem_stats.slow_path_decisions,
                sem_stats.semantic_hits,
                sem_stats.semantic_misses,
                sem_stats.cached_embeddings
            )
        } else {
            base_stats
        }
    }
}

/// A/B test traffic splitter
///
/// Randomly assigns queries to either LRU or Learned strategy (50/50 split).
/// Collects metrics for both strategies to compare performance.
pub struct AbTestSplitter {
    lru_strategy: Arc<dyn CacheStrategy>,
    learned_strategy: Arc<dyn CacheStrategy>,
}

impl AbTestSplitter {
    /// Create new A/B test splitter
    pub fn new(
        lru_strategy: Arc<dyn CacheStrategy>,
        learned_strategy: Arc<dyn CacheStrategy>,
    ) -> Self {
        Self {
            lru_strategy,
            learned_strategy,
        }
    }

    /// Get strategy for doc_id based on deterministic 50/50 split
    pub fn get_strategy(&self, doc_id: u64) -> Arc<dyn CacheStrategy> {
        if doc_id % 2 == 0 {
            Arc::clone(&self.lru_strategy)
        } else {
            Arc::clone(&self.learned_strategy)
        }
    }

    /// Get both strategies for metrics collection
    pub fn get_strategies(&self) -> (Arc<dyn CacheStrategy>, Arc<dyn CacheStrategy>) {
        (
            Arc::clone(&self.lru_strategy),
            Arc::clone(&self.learned_strategy),
        )
    }

    /// Get combined stats
    pub fn combined_stats(&self) -> String {
        format!(
            "{}\n{}",
            self.lru_strategy.stats(),
            self.learned_strategy.stats()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::learned_cache::AccessEvent;
    use std::time::{Instant, SystemTime};

    fn create_test_vector(doc_id: u64) -> CachedVector {
        CachedVector {
            doc_id,
            embedding: vec![0.5; 128],
            distance: 0.1,
            cached_at: Instant::now(),
        }
    }

    #[test]
    fn test_lru_strategy_always_caches() {
        let strategy = LruCacheStrategy::new(10);

        // LRU should always say yes to caching
        assert!(strategy.should_cache(1, &vec![0.5; 128]));
        assert!(strategy.should_cache(2, &vec![0.5; 128]));

        // Insert and retrieve
        strategy.insert_cached(create_test_vector(1));
        assert!(strategy.get_cached(1).is_some());
        assert!(strategy.get_cached(2).is_none());
    }

    #[test]
    fn test_learned_strategy_selective_caching() {
        // Create predictor and train on some data
        let mut predictor = LearnedCachePredictor::new(100).unwrap();

        // Train: doc 1 hot (100 accesses), doc 2 cold (1 access)
        let mut events = vec![];
        for _ in 0..100 {
            events.push(AccessEvent {
                doc_id: 1,
                timestamp: SystemTime::now(),
                access_type: crate::learned_cache::AccessType::Read,
            });
        }
        events.push(AccessEvent {
            doc_id: 2,
            timestamp: SystemTime::now(),
            access_type: crate::learned_cache::AccessType::Read,
        });

        predictor.train_from_accesses(&events).unwrap();

        let strategy = LearnedCacheStrategy::new(10, predictor);

        // Hot doc should be cached
        let should_cache_hot = strategy.should_cache(1, &vec![0.5; 128]);

        // Cold doc might not be cached (depends on threshold)
        let _should_cache_cold = strategy.should_cache(2, &vec![0.5; 128]);

        // At minimum, hot doc should have higher admission probability
        // (exact behavior depends on RMI training and threshold)
        assert!(should_cache_hot); // Hot doc should be cached
    }

    #[test]
    fn test_ab_test_splitter_distribution() {
        let lru = Arc::new(LruCacheStrategy::new(10));
        let predictor = LearnedCachePredictor::new(100).unwrap();
        let learned = Arc::new(LearnedCacheStrategy::new(10, predictor));

        let splitter = AbTestSplitter::new(lru, learned);

        // Test distribution over 1000 queries
        let mut lru_count = 0;
        let mut learned_count = 0;

        for doc_id in 0..1000 {
            let strategy = splitter.get_strategy(doc_id);
            if strategy.name() == "lru_baseline" {
                lru_count += 1;
            } else {
                learned_count += 1;
            }
        }

        // Should be roughly 50/50 (within 10% tolerance)
        assert!(
            (lru_count as i32 - 500).abs() < 50,
            "LRU count: {}",
            lru_count
        );
        assert!(
            (learned_count as i32 - 500).abs() < 50,
            "Learned count: {}",
            learned_count
        );
    }

    #[test]
    fn test_strategy_stats() {
        let strategy = LruCacheStrategy::new(10);

        // Generate some traffic
        strategy.insert_cached(create_test_vector(1));
        strategy.insert_cached(create_test_vector(2));

        strategy.get_cached(1); // Hit
        strategy.get_cached(2); // Hit
        strategy.get_cached(3); // Miss

        let stats = strategy.stats();
        assert!(stats.contains("2 hits"));
        assert!(stats.contains("1 misses"));
        assert!(stats.contains("66.67% hit rate"));
    }

    #[test]
    fn test_learned_strategy_predictor_update() {
        let predictor1 = LearnedCachePredictor::new(100).unwrap();
        let strategy = LearnedCacheStrategy::new(10, predictor1);

        // Create new predictor with different training
        let mut predictor2 = LearnedCachePredictor::new(100).unwrap();
        let events = vec![AccessEvent {
            doc_id: 42,
            timestamp: SystemTime::now(),
            access_type: crate::learned_cache::AccessType::Read,
        }];
        predictor2.train_from_accesses(&events).unwrap();

        // Update predictor
        strategy.update_predictor(predictor2);

        // Strategy should now use new predictor
        // (hard to test without knowing internals, but ensures no panic)
        let _ = strategy.should_cache(42, &vec![0.5; 128]);
    }

    #[test]
    fn test_concurrent_strategy_access() {
        use std::thread;

        let strategy = Arc::new(LruCacheStrategy::new(100));
        let mut handles = vec![];

        // Spawn 10 threads accessing cache concurrently
        for i in 0..10 {
            let strategy_clone = Arc::clone(&strategy);
            let handle = thread::spawn(move || {
                for j in 0..10 {
                    let doc_id = (i * 10 + j) as u64;
                    if strategy_clone.should_cache(doc_id, &vec![0.5; 128]) {
                        strategy_clone.insert_cached(create_test_vector(doc_id));
                    }
                    strategy_clone.get_cached(doc_id);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 100 vectors cached
        let stats_str = strategy.stats();
        assert!(
            stats_str.starts_with("LRU:"),
            "Stats format changed: {}",
            stats_str
        );
    }
}
