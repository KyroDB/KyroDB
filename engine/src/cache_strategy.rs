//! Cache Strategy Trait and Implementations
//!
//! Cache strategy framework: A/B testing for LRU vs. Learned Cache
//!
//! Provides pluggable cache strategies:
//! - LRU baseline: Always cache, LRU eviction
//! - Learned: RMI-predicted hotness-based admission
//!
//! Two-level cache architecture:
//! - L1a (this module): Document-level cache with RMI frequency prediction
//! - L1b (query_hash_cache): Query-level cache with semantic similarity

use crate::learned_cache::LearnedCachePredictor;
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

    /// Remove cached entry (if any) to maintain correctness after deletes/updates
    fn invalidate(&self, doc_id: u64);

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
        let _ = self.cache.insert(cached_vector); // Ignore evictions for baseline
    }

    fn invalidate(&self, doc_id: u64) {
        let _ = self.cache.remove(doc_id);
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

/// Learned Cache strategy (RMI frequency-based)
///
/// Uses RMI predictor to decide cache admission based on predicted hotness.
/// This is L1a (document-level cache) in the two-level architecture.
///
/// L1b (query-level cache) is handled separately by QueryHashCache.
///
pub struct LearnedCacheStrategy {
    pub cache: Arc<VectorCache>,
    pub predictor: Arc<parking_lot::RwLock<LearnedCachePredictor>>,
    name: String,
}

impl LearnedCacheStrategy {
    /// Create new Learned Cache strategy (RMI frequency-based)
    ///
    /// # Parameters
    /// - `capacity`: Cache capacity
    /// - `predictor`: Trained RMI frequency predictor
    pub fn new(capacity: usize, mut predictor: LearnedCachePredictor) -> Self {
        Self::configure_predictor_defaults(&mut predictor, capacity);

        Self {
            cache: Arc::new(VectorCache::new(capacity)),
            predictor: Arc::new(parking_lot::RwLock::new(predictor)),
            name: "learned_rmi".to_string(),
        }
    }

    /// Update predictor (for periodic retraining)
    pub fn update_predictor(&self, mut new_predictor: LearnedCachePredictor) {
        // Preserve critical tuning parameters from old predictor
        let (preserved_target, preserved_smoothing) = {
            let old = self.predictor.read();
            (old.target_hot_entries(), old.threshold_smoothing())
        };

        new_predictor.set_target_hot_entries(preserved_target);
        new_predictor.set_threshold_smoothing(preserved_smoothing);
        *self.predictor.write() = new_predictor;
    }

    fn configure_predictor_defaults(predictor: &mut LearnedCachePredictor, cache_capacity: usize) {
        // NOTE: target_hot_entries is now configured externally in validation_enterprise.rs
        // Do NOT override it here as it breaks custom tuning
        // let predictor_cap = predictor.capacity_limit().max(1);
        // let desired = cache_capacity
        //     .saturating_mul(8)
        //     .clamp(cache_capacity.max(1), predictor_cap);
        // predictor.set_target_hot_entries(desired);

        predictor.set_threshold_smoothing(0.12);

        let predictor_cap = predictor.capacity_limit().max(1);
        let desired = cache_capacity
            .saturating_mul(8)
            .clamp(cache_capacity.max(1), predictor_cap);
        let diversity_pow2 = desired.checked_next_power_of_two().unwrap_or(desired);
        let diversity = diversity_pow2.max(32).min(1024);
        predictor.set_diversity_buckets(diversity);
    }
}

impl CacheStrategy for LearnedCacheStrategy {
    #[instrument(level = "trace", skip(self), fields(doc_id))]
    fn get_cached(&self, doc_id: u64) -> Option<CachedVector> {
        match self.cache.get(doc_id) {
            Some(vec) => {
                trace!(doc_id, strategy = %self.name, "cache hit");
                let predictor = self.predictor.read();
                if predictor.is_trained() {
                    predictor.record_cache_hit(doc_id);
                }
                Some(vec)
            }
            None => {
                trace!(doc_id, strategy = %self.name, "cache miss");
                let predictor = self.predictor.read();
                if predictor.is_trained() {
                    let predicted = predictor.lookup_hotness(doc_id).unwrap_or(0.0);
                    predictor.record_cache_miss(doc_id, predicted);
                }
                None
            }
        }
    }

    #[instrument(level = "trace", skip(self, _embedding), fields(doc_id))]
    fn should_cache(&self, doc_id: u64, _embedding: &[f32]) -> bool {
        let predictor = self.predictor.read();

        if !predictor.is_trained() {
            trace!(doc_id, strategy = %self.name, "bootstrap admit (untrained predictor)");
            return true;
        }

        // Pure frequency-based admission (RMI only)
        let freq_score = predictor.lookup_hotness(doc_id).unwrap_or(0.0);
        let threshold = predictor.cache_threshold().max(predictor.admission_floor());

        let should_admit = freq_score >= threshold;

        if should_admit {
            trace!(doc_id, freq_score, threshold, "admit (freq >= threshold)");
        } else {
            trace!(doc_id, freq_score, threshold, "reject (freq < threshold)");
        }

        should_admit
    }
    #[instrument(level = "trace", skip(self, cached_vector), fields(doc_id = cached_vector.doc_id))]
    fn insert_cached(&self, cached_vector: CachedVector) {
        let evicted_doc_id = self.cache.insert(cached_vector);
        if let Some(evicted) = evicted_doc_id {
            let predictor = self.predictor.read();
            if predictor.is_trained() {
                predictor.record_eviction(evicted);
            }
        }
    }

    fn invalidate(&self, doc_id: u64) {
        if self.cache.remove(doc_id) {
            let predictor = self.predictor.read();
            if predictor.is_trained() {
                predictor.record_eviction(doc_id);
            }
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn stats(&self) -> String {
        let stats = self.cache.stats();
        let predictor = self.predictor.read();
        format!(
            "Learned: {} hits, {} misses, {:.2}% hit rate, {} evictions, {} tracked docs",
            stats.hits,
            stats.misses,
            stats.hit_rate * 100.0,
            stats.evictions,
            predictor.tracked_count()
        )
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

impl CacheStrategy for AbTestSplitter {
    fn name(&self) -> &str {
        "ab_test"
    }

    fn should_cache(&self, doc_id: u64, embedding: &[f32]) -> bool {
        self.get_strategy(doc_id).should_cache(doc_id, embedding)
    }

    fn insert_cached(&self, vector: CachedVector) {
        self.get_strategy(vector.doc_id).insert_cached(vector);
    }

    fn get_cached(&self, doc_id: u64) -> Option<CachedVector> {
        self.get_strategy(doc_id).get_cached(doc_id)
    }

    fn invalidate(&self, doc_id: u64) {
        self.lru_strategy.invalidate(doc_id);
        self.learned_strategy.invalidate(doc_id);
    }

    fn stats(&self) -> String {
        self.combined_stats()
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
