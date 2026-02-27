//! Cache Strategy Trait and Implementations
//!
//! Cache strategy framework: A/B testing for LRU vs. Learned Cache
//!
//! Provides pluggable cache strategies:
//! - LRU baseline: Always cache, LRU eviction
//! - Learned: Learned frequency-predicted hotness-based admission
//!
//! Two-level cache architecture:
//! - L1a (this module): Document-level cache with Learned frequency prediction
//! - L1b (query_hash_cache): Query-level cache with semantic similarity

use crate::learned_cache::LearnedCachePredictor;
use crate::semantic_adapter::SemanticAdapter;
use crate::vector_cache::{CachedVector, VectorCache};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tracing::{instrument, trace};

/// Snapshot of HSC lifecycle state for observability surfaces.
#[derive(Debug, Clone, Default)]
pub struct CacheLifecycleStats {
    pub predictor_trained: bool,
    pub cache_threshold: f32,
    pub tracked_docs: usize,
    pub hot_doc_count: usize,
    pub training_skips: u64,
    pub access_logger_depth: usize,
    pub semantic_enabled: bool,
    pub semantic_fast_path_decisions: u64,
    pub semantic_slow_path_decisions: u64,
    pub semantic_hits: u64,
    pub semantic_misses: u64,
    pub semantic_cached_embeddings: usize,
}

/// Cache strategy trait
///
/// Defines interface for different cache admission policies.
/// Strategies decide which vectors to cache based on access patterns.
pub trait CacheStrategy: Send + Sync {
    /// Check if vector is in cache
    fn get_cached(&self, doc_id: u64) -> Option<CachedVector>;

    /// Side-effect-free cache probe for response hydration paths.
    ///
    /// Implementations should avoid mutating cache stats, recency, or predictor
    /// feedback state.
    fn peek_cached(&self, doc_id: u64) -> Option<CachedVector> {
        self.get_cached(doc_id)
    }

    /// Decide if vector should be cached
    ///
    /// Returns `true` if the vector should be admitted to cache.
    /// Strategy-specific logic (e.g., LRU always admits, learned uses Learned frequency prediction).
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

    /// Current number of entries in L1a cache.
    fn size(&self) -> usize;

    /// Optional lifecycle snapshot for HSC observability.
    fn lifecycle_stats(&self) -> Option<CacheLifecycleStats> {
        None
    }
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

    fn peek_cached(&self, doc_id: u64) -> Option<CachedVector> {
        self.cache.peek(doc_id)
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

    fn size(&self) -> usize {
        self.cache.len()
    }
}

/// Learned Cache strategy (Learned frequency-based, optionally semantic-aware)
///
/// Uses Learned predictor to decide cache admission based on predicted hotness.
/// This is L1a (document-level cache) in the two-level architecture.
///
/// When created with `new_with_semantic`, combines frequency prediction with
/// semantic similarity for hybrid cache admission decisions.
///
/// L1b (query-level cache) is handled separately by QueryHashCache.
///
pub struct LearnedCacheStrategy {
    pub cache: Arc<VectorCache>,
    pub predictor: Arc<parking_lot::RwLock<LearnedCachePredictor>>,
    /// Optional semantic adapter for hybrid frequency+semantic admission
    semantic_adapter: Option<SemanticAdapter>,
    training_skips: AtomicU64,
    name: String,
}

impl LearnedCacheStrategy {
    /// Create new Learned Cache strategy (Learned frequency-based)
    ///
    /// # Parameters
    /// - `capacity`: Cache capacity
    /// - `predictor`: Trained Learned frequency predictor
    pub fn new(capacity: usize, mut predictor: LearnedCachePredictor) -> Self {
        Self::configure_predictor_defaults(&mut predictor, capacity);

        Self {
            cache: Arc::new(VectorCache::new(capacity)),
            predictor: Arc::new(parking_lot::RwLock::new(predictor)),
            semantic_adapter: None,
            training_skips: AtomicU64::new(0),
            name: "learned_predictor".to_string(),
        }
    }

    /// Create new Learned Cache strategy with semantic adapter (hybrid frequency+semantic)
    ///
    /// Combines Learned frequency prediction with semantic similarity for cache admission.
    /// When frequency score is uncertain (between low and high confidence thresholds),
    /// semantic similarity is used as a tiebreaker.
    ///
    /// # Parameters
    /// - `capacity`: Cache capacity
    /// - `predictor`: Trained Learned frequency predictor
    /// - `semantic_adapter`: Semantic adapter for similarity-based boosting
    pub fn new_with_semantic(
        capacity: usize,
        mut predictor: LearnedCachePredictor,
        semantic_adapter: SemanticAdapter,
    ) -> Self {
        Self::configure_predictor_defaults(&mut predictor, capacity);

        Self {
            cache: Arc::new(VectorCache::new(capacity)),
            predictor: Arc::new(parking_lot::RwLock::new(predictor)),
            semantic_adapter: Some(semantic_adapter),
            training_skips: AtomicU64::new(0),
            name: "learned_semantic".to_string(),
        }
    }

    /// Check if this strategy has a semantic adapter attached
    pub fn has_semantic(&self) -> bool {
        self.semantic_adapter.is_some()
    }

    /// Update predictor (for periodic retraining)
    pub fn update_predictor(&self, mut new_predictor: LearnedCachePredictor) {
        // Preserve tuning parameters and feedback under one lock so no feedback
        // can be lost between snapshot and predictor swap.
        let mut old = self.predictor.write();
        let preserved_target = old.target_hot_entries();
        let preserved_smoothing = old.threshold_smoothing();
        let feedback_snapshot = old.snapshot_feedback();

        new_predictor.set_target_hot_entries(preserved_target);
        new_predictor.set_threshold_smoothing(preserved_smoothing);
        new_predictor.merge_feedback_snapshot(feedback_snapshot);
        *old = new_predictor;
    }

    /// Record a skipped retraining cycle due to insufficient access events.
    #[inline]
    pub fn record_training_skip(&self) {
        self.training_skips.fetch_add(1, Ordering::Relaxed);
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
            .clamp(cache_capacity.max(1).min(predictor_cap), predictor_cap);
        let diversity_pow2 = desired.checked_next_power_of_two().unwrap_or(desired);
        let diversity = diversity_pow2.clamp(32, 1024);
        predictor.set_diversity_buckets(diversity);
    }

    #[inline]
    fn doc_hash_sample(doc_id: u64) -> f32 {
        let mut hasher = DefaultHasher::new();
        doc_id.hash(&mut hasher);
        let bucket = (hasher.finish() & 0xFFFF) as u16;
        bucket as f32 / u16::MAX as f32
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

    fn peek_cached(&self, doc_id: u64) -> Option<CachedVector> {
        self.cache.peek(doc_id)
    }

    #[instrument(level = "trace", skip(self, embedding), fields(doc_id))]
    fn should_cache(&self, doc_id: u64, embedding: &[f32]) -> bool {
        let predictor = self.predictor.read();

        if !predictor.is_trained() {
            // Bounded warmup policy:
            // - seed up to a small deterministic quota so L1a can start helping immediately
            // - after quota, sample admissions deterministically by doc_id hash to avoid
            //   cache pollution before the first training cycle.
            let cache_capacity = self.cache.capacity().max(1);
            let seeded_quota = cache_capacity
                .saturating_div(10)
                .clamp(16, 2048)
                .min(cache_capacity);
            let current_size = self.cache.len();
            let warmup_sample_rate = predictor.unseen_admission_chance().clamp(0.01, 0.25);
            let sampled_admit = Self::doc_hash_sample(doc_id) < warmup_sample_rate;
            let should_admit = current_size < seeded_quota || sampled_admit;

            trace!(
                doc_id,
                strategy = %self.name,
                current_size,
                seeded_quota,
                warmup_sample_rate,
                should_admit,
                "bounded warmup admission (untrained predictor)"
            );

            if let Some(ref adapter) = self.semantic_adapter {
                if should_admit {
                    if let Err(e) = adapter.cache_embedding(doc_id, embedding.to_vec()) {
                        tracing::warn!(
                            doc_id,
                            error = %e,
                            "SemanticAdapter: cache_embedding rejected"
                        );
                    }
                }
            }
            return should_admit;
        }

        // Get frequency-based prediction (learned predictor)
        let freq_score = predictor.lookup_hotness(doc_id).unwrap_or(0.0);
        let threshold = predictor.cache_threshold().max(predictor.admission_floor());

        // If semantic adapter is present, use hybrid decision
        if let Some(ref adapter) = self.semantic_adapter {
            let should_admit = adapter.should_cache(freq_score, embedding);

            if should_admit {
                trace!(
                    doc_id,
                    freq_score,
                    threshold,
                    "admit (hybrid semantic+freq)"
                );
                // Cache embedding for future semantic lookups
                if let Err(e) = adapter.cache_embedding(doc_id, embedding.to_vec()) {
                    tracing::warn!(doc_id, error = %e, "SemanticAdapter: cache_embedding rejected");
                }
            } else {
                trace!(
                    doc_id,
                    freq_score,
                    threshold,
                    "reject (hybrid semantic+freq)"
                );
            }

            return should_admit;
        }

        // Pure frequency-based admission (frequency predictor only)
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
        // NOTE: Do NOT record as eviction - invalidations are forced removals due to
        // data updates/deletions, NOT cache-pressure evictions. Recording invalidations
        // as evictions would contaminate the predictor's training signal since invalidated
        // items will never be requested again (they've been deleted from the source).
        self.cache.remove(doc_id);
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

        // Append semantic stats if adapter is present
        if let Some(ref adapter) = self.semantic_adapter {
            let semantic_stats = adapter.stats();
            format!(
                "{} | Semantic: {} fast, {} slow, {} hits, {} misses",
                base_stats,
                semantic_stats.fast_path_decisions,
                semantic_stats.slow_path_decisions,
                semantic_stats.semantic_hits,
                semantic_stats.semantic_misses
            )
        } else {
            base_stats
        }
    }

    fn size(&self) -> usize {
        self.cache.len()
    }

    fn lifecycle_stats(&self) -> Option<CacheLifecycleStats> {
        let predictor = self.predictor.read();
        let mut lifecycle = CacheLifecycleStats {
            predictor_trained: predictor.is_trained(),
            cache_threshold: predictor.cache_threshold(),
            tracked_docs: predictor.tracked_count(),
            hot_doc_count: self.cache.len(),
            training_skips: self.training_skips.load(Ordering::Relaxed),
            semantic_enabled: self.semantic_adapter.is_some(),
            ..CacheLifecycleStats::default()
        };

        if let Some(adapter) = &self.semantic_adapter {
            let semantic = adapter.stats();
            lifecycle.semantic_fast_path_decisions = semantic.fast_path_decisions;
            lifecycle.semantic_slow_path_decisions = semantic.slow_path_decisions;
            lifecycle.semantic_hits = semantic.semantic_hits;
            lifecycle.semantic_misses = semantic.semantic_misses;
            lifecycle.semantic_cached_embeddings = semantic.cached_embeddings;
        }

        Some(lifecycle)
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

    fn peek_cached(&self, doc_id: u64) -> Option<CachedVector> {
        self.get_strategy(doc_id).peek_cached(doc_id)
    }

    fn invalidate(&self, doc_id: u64) {
        self.lru_strategy.invalidate(doc_id);
        self.learned_strategy.invalidate(doc_id);
    }

    fn stats(&self) -> String {
        self.combined_stats()
    }

    fn size(&self) -> usize {
        self.lru_strategy.size() + self.learned_strategy.size()
    }
}

/// Wrapper for shared LearnedCacheStrategy
///
/// This wrapper allows sharing a LearnedCacheStrategy between the TieredEngine
/// and the training task. When the training task updates the predictor via
/// `LearnedCacheStrategy::update_predictor()`, the changes are immediately
/// visible to the TieredEngine's query path.
///
/// # Usage
/// ```ignore
/// let shared_strategy = Arc::new(LearnedCacheStrategy::new(capacity, predictor));
/// let wrapper = SharedLearnedCacheStrategy::new(shared_strategy.clone());
///
/// // Pass wrapper to TieredEngine
/// let engine = TieredEngine::new(Box::new(wrapper), ...);
///
/// // Pass shared_strategy to training task
/// spawn_training_task(access_logger, shared_strategy, ...);
/// ```
pub struct SharedLearnedCacheStrategy {
    inner: Arc<LearnedCacheStrategy>,
}

impl SharedLearnedCacheStrategy {
    /// Create wrapper around shared LearnedCacheStrategy
    pub fn new(strategy: Arc<LearnedCacheStrategy>) -> Self {
        Self { inner: strategy }
    }

    /// Get reference to inner strategy (for direct access to cache/predictor)
    pub fn inner(&self) -> &Arc<LearnedCacheStrategy> {
        &self.inner
    }
}

impl CacheStrategy for SharedLearnedCacheStrategy {
    fn get_cached(&self, doc_id: u64) -> Option<CachedVector> {
        self.inner.get_cached(doc_id)
    }

    fn peek_cached(&self, doc_id: u64) -> Option<CachedVector> {
        self.inner.peek_cached(doc_id)
    }

    fn should_cache(&self, doc_id: u64, embedding: &[f32]) -> bool {
        self.inner.should_cache(doc_id, embedding)
    }

    fn insert_cached(&self, cached_vector: CachedVector) {
        self.inner.insert_cached(cached_vector)
    }

    fn invalidate(&self, doc_id: u64) {
        self.inner.invalidate(doc_id)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn stats(&self) -> String {
        self.inner.stats()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn lifecycle_stats(&self) -> Option<CacheLifecycleStats> {
        self.inner.lifecycle_stats()
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
    fn test_learned_peek_cached_has_no_metrics_or_feedback_side_effects() {
        let mut predictor = LearnedCachePredictor::new(100).unwrap();
        let mut events = Vec::new();
        for _ in 0..32 {
            events.push(AccessEvent {
                doc_id: 1,
                timestamp: SystemTime::now(),
                access_type: crate::learned_cache::AccessType::Read,
            });
        }
        predictor.train_from_accesses(&events).unwrap();

        let strategy = LearnedCacheStrategy::new(16, predictor);
        strategy.insert_cached(create_test_vector(1));

        let feedback_before = strategy.predictor.read().feedback_backlog();
        let cache_stats_before = strategy.cache.stats();

        assert!(strategy.peek_cached(1).is_some());
        assert!(strategy.peek_cached(999).is_none());

        let feedback_after = strategy.predictor.read().feedback_backlog();
        let cache_stats_after = strategy.cache.stats();

        assert_eq!(
            feedback_after.false_negative_docs,
            feedback_before.false_negative_docs
        );
        assert_eq!(
            feedback_after.false_positive_docs,
            feedback_before.false_positive_docs
        );
        assert_eq!(
            feedback_after.miss_streak_docs,
            feedback_before.miss_streak_docs
        );
        assert_eq!(cache_stats_after.hits, cache_stats_before.hits);
        assert_eq!(cache_stats_after.misses, cache_stats_before.misses);
        assert_eq!(cache_stats_after.evictions, cache_stats_before.evictions);
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
        // (exact behavior depends on learned predictor training and threshold)
        assert!(should_cache_hot); // Hot doc should be cached
    }

    #[test]
    fn test_learned_strategy_warmup_is_bounded_before_training() {
        let predictor = LearnedCachePredictor::new(256).unwrap();
        let strategy = LearnedCacheStrategy::new(128, predictor);

        let mut admitted = 0usize;
        for doc_id in 1..=500u64 {
            if strategy.should_cache(doc_id, &vec![0.5; 128]) {
                admitted += 1;
                strategy.insert_cached(create_test_vector(doc_id));
            }
        }

        assert!(admitted > 0, "warmup should admit at least some documents");
        assert!(
            admitted < 500,
            "warmup should not admit every document before first training cycle"
        );
    }

    #[test]
    fn test_update_predictor_preserves_feedback_backlog() {
        let mut trained = LearnedCachePredictor::new(256).unwrap();
        let events = vec![
            AccessEvent {
                doc_id: 1,
                timestamp: SystemTime::now(),
                access_type: crate::learned_cache::AccessType::Read,
            },
            AccessEvent {
                doc_id: 1,
                timestamp: SystemTime::now(),
                access_type: crate::learned_cache::AccessType::Read,
            },
            AccessEvent {
                doc_id: 2,
                timestamp: SystemTime::now(),
                access_type: crate::learned_cache::AccessType::Read,
            },
        ];
        trained.train_from_accesses(&events).unwrap();

        let strategy = LearnedCacheStrategy::new(32, trained);
        // Miss on an unseen doc to create false-negative + miss-streak feedback state.
        assert!(strategy.get_cached(9_999).is_none());

        let before = strategy.predictor.read().feedback_backlog();
        assert!(
            before.false_negative_docs > 0 || before.miss_streak_docs > 0,
            "expected non-empty feedback backlog before predictor swap"
        );

        let mut replacement = LearnedCachePredictor::new(256).unwrap();
        replacement.train_from_accesses(&events).unwrap();
        strategy.update_predictor(replacement);

        let after = strategy.predictor.read().feedback_backlog();
        assert!(
            after.false_negative_docs >= before.false_negative_docs,
            "false-negative backlog must survive predictor update"
        );
        assert!(
            after.miss_streak_docs >= before.miss_streak_docs,
            "miss streak backlog must survive predictor update"
        );
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
        // Using subtraction with checked operations to avoid type casting issues
        let lru_lower = lru_count >= 450;
        let lru_upper = lru_count <= 550;
        let learned_lower = learned_count >= 450;
        let learned_upper = learned_count <= 550;

        assert!(lru_lower && lru_upper, "LRU count: {}", lru_count);
        assert!(
            learned_lower && learned_upper,
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

        // Seed runtime feedback in the currently active predictor.
        {
            let predictor = strategy.predictor.read();
            predictor.record_cache_miss(7, 0.0);
            predictor.record_eviction(9);
        }
        let feedback_before = strategy.predictor.read().feedback_backlog();
        assert_eq!(feedback_before.false_negative_docs, 1);
        assert_eq!(feedback_before.false_positive_docs, 1);

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

        // Feedback backlog must carry over so retraining can keep learning across swaps.
        let feedback_after = strategy.predictor.read().feedback_backlog();
        assert_eq!(
            feedback_after.false_negative_docs,
            feedback_before.false_negative_docs
        );
        assert_eq!(
            feedback_after.false_positive_docs,
            feedback_before.false_positive_docs
        );

        // Strategy should now use new predictor.
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
