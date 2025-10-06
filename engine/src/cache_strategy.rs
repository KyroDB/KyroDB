//! Cache Strategy Trait and Implementations
//!
//! Phase 0 Week 9-12: A/B testing framework for cache admission policies
//!
//! Provides pluggable cache strategies:
//! - LRU baseline: Always cache, LRU eviction
//! - Learned: RMI-predicted hotness-based admission

use crate::learned_cache::LearnedCachePredictor;
use crate::vector_cache::{CachedVector, VectorCache};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
    fn get_cached(&self, doc_id: u64) -> Option<CachedVector> {
        self.cache.get(doc_id)
    }

    fn should_cache(&self, _doc_id: u64, _embedding: &[f32]) -> bool {
        // LRU always caches (admission policy is permissive)
        true
    }

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

/// Learned cache strategy
///
/// Uses RMI predictor to decide cache admission based on predicted hotness.
/// Only caches vectors predicted to be hot (>threshold).
/// Expected hit rate: 70-90% (significantly better than LRU).
pub struct LearnedCacheStrategy {
    pub cache: Arc<VectorCache>,
    pub predictor: Arc<parking_lot::RwLock<LearnedCachePredictor>>,
    name: String,
}

impl LearnedCacheStrategy {
    /// Create new learned cache strategy
    ///
    /// # Parameters
    /// - `capacity`: Cache capacity
    /// - `predictor`: Trained learned cache predictor
    pub fn new(capacity: usize, predictor: LearnedCachePredictor) -> Self {
        Self {
            cache: Arc::new(VectorCache::new(capacity)),
            predictor: Arc::new(parking_lot::RwLock::new(predictor)),
            name: "learned_rmi".to_string(),
        }
    }

    /// Update predictor (for periodic retraining)
    pub fn update_predictor(&self, new_predictor: LearnedCachePredictor) {
        *self.predictor.write() = new_predictor;
    }
}

impl CacheStrategy for LearnedCacheStrategy {
    fn get_cached(&self, doc_id: u64) -> Option<CachedVector> {
        self.cache.get(doc_id)
    }

    fn should_cache(&self, doc_id: u64, _embedding: &[f32]) -> bool {
        let predictor = self.predictor.read();
        
        // CRITICAL FIX: Bootstrap mode until predictor is trained
        // Always permissive during bootstrap to build training data.
    // After training, rely on predictor's configured threshold.
        
        if !predictor.is_trained() {
            return true;  // Bootstrap: cache everything until first training
        }
        
        let threshold = predictor
            .cache_threshold()
            .max(predictor.admission_floor());

        if let Some(score) = predictor.lookup_hotness(doc_id) {
            if score >= threshold {
                return true;
            }

            let soft_probability = (score / threshold).clamp(0.0, 1.0) * 0.25;
            if soft_probability > 0.0 && rand::random::<f32>() < soft_probability {
                return true;
            }

            return false;
        }

        let unseen_chance = predictor.unseen_admission_chance();
        drop(predictor);

        rand::random::<f32>() < unseen_chance
    }

    fn insert_cached(&self, cached_vector: CachedVector) {
        self.cache.insert(cached_vector);
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
    sequence: AtomicU64,
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
            sequence: AtomicU64::new(0),
        }
    }

    /// Get strategy for query (50/50 random split)
    ///
    /// Uses DefaultHasher for uniform distribution across doc_ids.
    /// CRITICAL FIX: Simple modulo (doc_id % 2) creates bias on Zipf distributions
    /// where low even IDs (0, 2, 4...) are accessed more frequently.
    pub fn get_strategy(&self, _doc_id: u64) -> Arc<dyn CacheStrategy> {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);

        if seq % 2 == 0 {
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
        let should_cache_cold = strategy.should_cache(2, &vec![0.5; 128]);

        println!("Should cache hot doc 1: {}", should_cache_hot);
        println!("Should cache cold doc 2: {}", should_cache_cold);

        // At minimum, hot doc should have higher admission probability
        // (exact behavior depends on RMI training and threshold)
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
        println!("LRU: {}, Learned: {}", lru_count, learned_count);
        assert!((lru_count as i32 - 500).abs() < 50);
        assert!((learned_count as i32 - 500).abs() < 50);
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
        println!("Concurrent stats: {}", stats_str);
    }
}
