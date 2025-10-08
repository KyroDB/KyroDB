//! Learned Cache Predictor using HashMap for cache hotness prediction
//!
//! Phase 0 Week 3-4: Direct doc_id → hotness_score mapping
//!
//! This module predicts cache hotness: doc_id → P(hot | recent_accesses)
//! HashMap learns access patterns and predicts which documents should be cached.
//!
//! Architecture:
//! - HashMap stores doc_id → hotness_score (O(1) lookup)
//! - HNSW performs k-NN search (separate concern)
//! - Access logger feeds training data
//!
//! Target: 60-80% cache hit rate vs 15-25% LRU baseline (1M corpus)

use anyhow::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Access event for training learned cache predictor
///
/// Phase 0.5.1: Added embedding field for semantic similarity computation
#[derive(Debug, Clone)]
pub struct AccessEvent {
    pub doc_id: u64,
    pub timestamp: SystemTime,
    pub access_type: AccessType,
    /// Query embedding (384-dim for MS MARCO, or configured embedding_dim)
    /// Used for semantic similarity-based cache admission in hybrid predictor
    pub embedding: Vec<f32>,
}

impl Default for AccessEvent {
    fn default() -> Self {
        Self {
            doc_id: 0,
            timestamp: SystemTime::UNIX_EPOCH,
            access_type: AccessType::default(),
            embedding: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessType {
    #[default]
    Read,
    Write,
}

/// Learned cache predictor using RMI to predict document hotness
///
/// Predicts P(hot | recent_accesses) for each document ID.
/// Documents with high predicted hotness are kept in cache.
///
/// # Architecture
/// - Layer 1: Root model (coarse-grained segment selection)
/// - Layer 2: Segment models (fine-grained hotness prediction per doc_id)
/// - Training: Updates from access pattern logger every 10 minutes
///
/// # Example
/// ```no_run
/// use kyrodb_engine::learned_cache::{LearnedCachePredictor, AccessEvent, AccessType};
/// use std::time::SystemTime;
///
/// let mut predictor = LearnedCachePredictor::new(10_000).unwrap();
///
/// // Simulate access pattern
/// let accesses = vec![
///     AccessEvent {
///         doc_id: 42,
///         timestamp: SystemTime::now(),
///         access_type: AccessType::Read,
///     },
/// ];
///
/// predictor.train_from_accesses(&accesses).unwrap();
///
/// // Predict if doc should be cached
/// if predictor.should_cache(42) {
///     println!("Doc 42 is hot, cache it!");
/// }
/// ```
pub struct LearnedCachePredictor {
    /// Direct mapping: doc_id → hotness_score (O(1) lookup)
    /// Thread-safe with RwLock for concurrent reads during queries
    hotness_map: Arc<RwLock<HashMap<u64, f32>>>,

    /// Hotness threshold for cache admission (tunable)
    /// Default: 0.2 (aligned with sqrt-scaled hotness scores)
    cache_threshold: f32,

    /// Minimum admission floor regardless of configured threshold
    /// Prevents overly conservative tuning from starving the cache.
    admission_floor: f32,

    /// Probability of admitting unseen documents to avoid cache starvation
    unseen_admission_chance: f32,

    /// Target number of documents classified as hot after training
    /// Defaults to cache capacity; tuned by cache strategy at runtime.
    target_hot_entries: usize,

    /// Exponential moving average factor for threshold calibration (0 = no smoothing)
    threshold_smoothing: f32,

    /// Training window (how far back to consider accesses)
    training_window: Duration,

    /// Recency decay half-life (exponential decay for old accesses)
    recency_halflife: Duration,

    /// Total capacity (max doc_ids to track)
    capacity: usize,

    /// Last training timestamp
    last_trained: SystemTime,

    /// Training interval (how often to retrain)
    training_interval: Duration,
}

impl LearnedCachePredictor {
    /// Create new learned cache predictor
    ///
    /// # Parameters
    /// - `capacity`: Maximum number of doc_ids to track (e.g., 10K-1M)
    ///
    /// # Defaults
    /// - Cache threshold: 0.2 (calibrated with sqrt scaling)
    /// - Training window: 24 hours
    /// - Recency half-life: 1 hour
    /// - Training interval: 10 minutes
    pub fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            hotness_map: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            cache_threshold: 0.2, // Calibrated for sqrt-scaled hotness scores
            admission_floor: 0.05,
            unseen_admission_chance: 0.10,
            target_hot_entries: capacity,
            threshold_smoothing: 0.6,
            training_window: Duration::from_secs(24 * 3600), // 24 hours
            recency_halflife: Duration::from_secs(3600),     // 1 hour
            capacity,
            last_trained: UNIX_EPOCH,
            training_interval: Duration::from_secs(600), // 10 minutes
        })
    }

    /// Create predictor with custom parameters (for testing/tuning)
    pub fn with_config(
        capacity: usize,
        cache_threshold: f32,
        training_window: Duration,
        recency_halflife: Duration,
        training_interval: Duration,
    ) -> Result<Self> {
        let admission_floor = 0.05;
        Ok(Self {
            hotness_map: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            cache_threshold: cache_threshold.clamp(admission_floor, 1.0),
            admission_floor,
            unseen_admission_chance: 0.10,
            target_hot_entries: capacity,
            threshold_smoothing: 0.6,
            training_window,
            recency_halflife,
            capacity,
            last_trained: UNIX_EPOCH,
            training_interval,
        })
    }

    /// Predict hotness score for document ID
    ///
    /// Returns value in [0.0, 1.0] where:
    /// - 0.0 = cold (never seen or very rare)
    /// - 1.0 = hot (frequently accessed recently)
    ///
    /// # Performance
    /// - O(1) HashMap lookup
    /// - Thread-safe with RwLock (concurrent reads)
    pub fn predict_hotness(&self, doc_id: u64) -> f32 {
        self.lookup_hotness(doc_id).unwrap_or(0.0)
    }

    /// Lookup raw hotness score without default fallback
    pub fn lookup_hotness(&self, doc_id: u64) -> Option<f32> {
        let map = self.hotness_map.read();
        map.get(&doc_id).copied()
    }

    /// Decide if document should be cached based on predicted hotness
    ///
    /// Returns `true` if predicted hotness > cache_threshold
    ///
    /// PERMISSIVE by default (threshold=0.3): caches anything reasonably hot
    pub fn should_cache(&self, doc_id: u64) -> bool {
        self.predict_hotness(doc_id) > self.cache_threshold
    }

    /// Check if predictor has been trained
    ///
    /// Returns false if hotness_map is empty (never trained)
    pub fn is_trained(&self) -> bool {
        let map = self.hotness_map.read();
        !map.is_empty()
    }

    /// Train predictor from access events
    ///
    /// # Algorithm
    /// 1. Compute hotness scores from access events:
    ///    - Hotness = frequency × recency_weight
    ///    - Recency weight = exp(-age / half_life)
    /// 2. Normalize scores to [0, 1]
    /// 3. Update HashMap atomically (direct doc_id → hotness mapping)
    ///
    /// # Performance
    /// - O(n log n) for sorting + O(k) for HashMap update
    /// - Target: <5ms for 100K accesses
    /// - Memory: 24 bytes per tracked document
    pub fn train_from_accesses(&mut self, accesses: &[AccessEvent]) -> Result<()> {
        if accesses.is_empty() {
            return Ok(());
        }

        // Compute hotness scores (frequency + recency weighted)
        let hotness_map = self.compute_hotness_scores(accesses);

        // Sort by hotness (descending) to keep hottest documents
        let mut hotness_vec: Vec<(u64, f32)> = hotness_map.into_iter().collect();
        hotness_vec.sort_by(|(_, score_a), (_, score_b)| {
            score_b
                .partial_cmp(score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Truncate to capacity (keep only top K hottest documents)
        if hotness_vec.len() > self.capacity {
            hotness_vec.truncate(self.capacity);
        }

        self.recalibrate_threshold(&hotness_vec);

        // Update HashMap atomically
        {
            let mut map = self.hotness_map.write();
            map.clear();
            map.extend(hotness_vec);
        }

        self.last_trained = SystemTime::now();
        Ok(())
    }

    /// Check if predictor needs retraining
    pub fn needs_training(&self) -> bool {
        SystemTime::now()
            .duration_since(self.last_trained)
            .unwrap_or(Duration::ZERO)
            >= self.training_interval
    }

    /// Get cache admission threshold
    pub fn cache_threshold(&self) -> f32 {
        self.cache_threshold
    }

    /// Get minimum admission floor
    pub fn admission_floor(&self) -> f32 {
        self.admission_floor
    }

    /// Probability of admitting unseen documents during learned mode
    pub fn unseen_admission_chance(&self) -> f32 {
        self.unseen_admission_chance
    }

    /// Update cache admission threshold (for tuning)
    pub fn set_cache_threshold(&mut self, threshold: f32) {
        self.cache_threshold = threshold.clamp(self.admission_floor, 1.0);
    }

    /// Configure desired number of documents classified as hot
    pub fn set_target_hot_entries(&mut self, target: usize) {
        self.target_hot_entries = target.max(1);
    }

    /// Configure threshold smoothing factor (0 = no smoothing, 1 = frozen)
    pub fn set_threshold_smoothing(&mut self, smoothing: f32) {
        self.threshold_smoothing = smoothing.clamp(0.0, 0.95);
    }

    /// Get number of tracked documents
    pub fn tracked_count(&self) -> usize {
        let map = self.hotness_map.read();
        map.len()
    }

    /// Get statistics for monitoring
    pub fn stats(&self) -> CachePredictorStats {
        let map = self.hotness_map.read();

        let hot_count = map
            .values()
            .filter(|score| **score > self.cache_threshold)
            .count();

        let avg_hotness = if map.is_empty() {
            0.0
        } else {
            map.values().sum::<f32>() / map.len() as f32
        };

        CachePredictorStats {
            tracked_docs: map.len(),
            hot_docs: hot_count,
            cache_threshold: self.cache_threshold,
            avg_hotness,
            last_trained: self.last_trained,
        }
    }

    /// Compute hotness scores from access events
    ///
    /// Hotness formula:
    /// - hotness(doc_id) = Σ recency_weight(access)
    /// - recency_weight = exp(-age_seconds / half_life_seconds)
    /// - age_seconds = now - access.timestamp
    ///
    /// Intuition: Recent frequent accesses → high hotness
    fn compute_hotness_scores(&self, accesses: &[AccessEvent]) -> HashMap<u64, f32> {
        let mut hotness = HashMap::new();
        let now = SystemTime::now();

        // Compute cutoff timestamp (training window)
        let cutoff = now.checked_sub(self.training_window).unwrap_or(UNIX_EPOCH);

        for access in accesses {
            // Skip accesses outside training window
            if access.timestamp < cutoff {
                continue;
            }

            // Compute recency weight with exponential decay
            let age = now
                .duration_since(access.timestamp)
                .unwrap_or(Duration::ZERO);

            let age_seconds = age.as_secs_f32();
            let halflife_seconds = self.recency_halflife.as_secs_f32();

            // Exponential decay: weight = exp(-age / half_life)
            let recency_weight = (-age_seconds / halflife_seconds).exp();

            // Accumulate weighted accesses
            *hotness.entry(access.doc_id).or_insert(0.0) += recency_weight;
        }

        // Normalize to [0, 1]
        if !hotness.is_empty() {
            let max_hotness = hotness.values().cloned().fold(f32::NEG_INFINITY, f32::max);

            if max_hotness > 0.0 {
                for score in hotness.values_mut() {
                    let normalized = (*score / max_hotness).clamp(0.0, 1.0);
                    *score = normalized.sqrt();
                }
            }
        }

        hotness
    }

    fn recalibrate_threshold(&mut self, sorted_hotness: &[(u64, f32)]) {
        if sorted_hotness.is_empty() {
            self.cache_threshold = self.cache_threshold.max(self.admission_floor);
            return;
        }

        let desired = self.target_hot_entries.min(sorted_hotness.len());
        if desired == 0 {
            self.cache_threshold = self.admission_floor;
            return;
        }

        let index = desired.saturating_sub(1);
        let mut target_threshold = sorted_hotness[index].1;
        target_threshold = target_threshold.clamp(self.admission_floor, 1.0);

        if self.threshold_smoothing <= f32::EPSILON {
            self.cache_threshold = target_threshold;
            return;
        }

        let smoothed = self.cache_threshold * self.threshold_smoothing
            + target_threshold * (1.0 - self.threshold_smoothing);
        self.cache_threshold = smoothed.clamp(self.admission_floor, 1.0);
    }
}

/// Statistics for learned cache predictor monitoring
#[derive(Debug, Clone)]
pub struct CachePredictorStats {
    pub tracked_docs: usize,
    pub hot_docs: usize,
    pub cache_threshold: f32,
    pub avg_hotness: f32,
    pub last_trained: SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_access(doc_id: u64, seconds_ago: u64) -> AccessEvent {
        AccessEvent {
            doc_id,
            timestamp: SystemTime::now() - Duration::from_secs(seconds_ago),
            access_type: AccessType::Read,
        }
    }

    #[test]
    fn test_learned_cache_basic() {
        let predictor = LearnedCachePredictor::new(1000).unwrap();
        assert_eq!(predictor.tracked_count(), 0);
        assert!((predictor.cache_threshold() - 0.2).abs() < f32::EPSILON);
    }

    #[test]
    fn test_predict_hotness_empty() {
        let predictor = LearnedCachePredictor::new(1000).unwrap();
        // Untrained predictor should return 0.0 for all doc_ids
        assert_eq!(predictor.predict_hotness(42), 0.0);
        assert!(!predictor.should_cache(42));
    }

    #[test]
    fn test_train_single_hot_document() {
        let mut predictor = LearnedCachePredictor::new(1000).unwrap();

        // Doc 42 accessed 10 times recently
        let accesses: Vec<AccessEvent> = (0..10)
            .map(|i| create_access(42, i * 60)) // Every minute for 10 minutes
            .collect();

        predictor.train_from_accesses(&accesses).unwrap();

        // Doc 42 should be predicted as hot
        assert!(predictor.predict_hotness(42) > 0.9); // Very hot
        assert!(predictor.should_cache(42));

        // Other docs should be cold
        assert_eq!(predictor.predict_hotness(100), 0.0);
        assert!(!predictor.should_cache(100));
    }

    #[test]
    fn test_hotness_decay_with_time() {
        let mut predictor = LearnedCachePredictor::new(1000).unwrap();

        // Doc 42: accessed recently (hot)
        // Doc 99: accessed long ago (cold)
        let accesses = vec![
            create_access(42, 60),    // 1 minute ago
            create_access(42, 120),   // 2 minutes ago
            create_access(99, 7200),  // 2 hours ago (beyond half-life)
            create_access(99, 14400), // 4 hours ago
        ];

        predictor.train_from_accesses(&accesses).unwrap();

        // Doc 42 should be hotter than doc 99 (recency matters)
        let hotness_42 = predictor.predict_hotness(42);
        let hotness_99 = predictor.predict_hotness(99);

        assert!(hotness_42 > hotness_99);
        assert!(hotness_42 > 0.8); // Recent accesses = high hotness
        assert!(hotness_99 < 0.3); // Old accesses = low hotness
    }

    #[test]
    fn test_hotness_normalization() {
        let mut predictor = LearnedCachePredictor::new(1000).unwrap();

        // Multiple documents with varying access counts
        let accesses = vec![
            // Doc 1: 10 accesses (hottest)
            create_access(1, 60),
            create_access(1, 120),
            create_access(1, 180),
            create_access(1, 240),
            create_access(1, 300),
            create_access(1, 360),
            create_access(1, 420),
            create_access(1, 480),
            create_access(1, 540),
            create_access(1, 600),
            // Doc 2: 5 accesses (medium)
            create_access(2, 60),
            create_access(2, 120),
            create_access(2, 180),
            create_access(2, 240),
            create_access(2, 300),
            // Doc 3: 1 access (coldest)
            create_access(3, 60),
        ];

        predictor.train_from_accesses(&accesses).unwrap();

        let h1 = predictor.predict_hotness(1);
        let h2 = predictor.predict_hotness(2);
        let h3 = predictor.predict_hotness(3);

        // Hotness should be normalized to [0, 1]
        assert!(h1 >= 0.0 && h1 <= 1.0);
        assert!(h2 >= 0.0 && h2 <= 1.0);
        assert!(h3 >= 0.0 && h3 <= 1.0);

        // Hottest doc should be close to 1.0
        assert!(h1 > 0.95);

        // Ordering should be: h1 > h2 > h3
        assert!(h1 > h2);
        assert!(h2 > h3);
    }

    #[test]
    fn test_cache_threshold_tuning() {
        let mut predictor = LearnedCachePredictor::new(1000).unwrap();

        let accesses = vec![
            create_access(1, 60), // Hot doc
            create_access(2, 60), // Medium doc
        ];

        predictor.train_from_accesses(&accesses).unwrap();

        // With higher threshold, only hottest docs cached
        predictor.set_cache_threshold(0.7);
        let hot_count_high = predictor.stats().hot_docs;

        // With threshold 0.3, more docs cached
        predictor.set_cache_threshold(0.3);
        let hot_count_low = predictor.stats().hot_docs;

        // Lower threshold → more docs considered hot
        assert!(hot_count_low >= hot_count_high);
    }

    #[test]
    fn test_threshold_auto_calibration_matches_target() {
        let mut predictor = LearnedCachePredictor::new(128).unwrap();
        predictor.set_target_hot_entries(5);
        predictor.set_threshold_smoothing(0.0);

        let mut accesses = Vec::new();
        for doc in 0..20usize {
            let doc_id = doc as u64;
            for _ in 0..(20 - doc) {
                accesses.push(create_access(doc_id, 30));
            }
        }

        predictor.train_from_accesses(&accesses).unwrap();

        let threshold = predictor.cache_threshold();
        let mut hot_docs = 0;
        for doc in 0..20u64 {
            if predictor.predict_hotness(doc) >= threshold {
                hot_docs += 1;
            }
        }

        assert_eq!(hot_docs, 5);
    }

    #[test]
    fn test_training_window() {
        let mut predictor = LearnedCachePredictor::with_config(
            1000,
            0.7,
            Duration::from_secs(3600), // 1 hour training window
            Duration::from_secs(1800), // 30 min half-life
            Duration::from_secs(600),
        )
        .unwrap();

        // Access within window (counted)
        let access_recent = create_access(1, 1800); // 30 min ago

        // Access outside window (ignored)
        let access_old = create_access(2, 7200); // 2 hours ago

        predictor
            .train_from_accesses(&[access_recent, access_old])
            .unwrap();

        // Doc 1 should be tracked, doc 2 should not
        assert!(predictor.predict_hotness(1) > 0.0);
        assert_eq!(predictor.predict_hotness(2), 0.0);
    }

    #[test]
    fn test_capacity_limit() {
        let capacity = 100;
        let mut predictor = LearnedCachePredictor::new(capacity).unwrap();

        // Generate accesses for 200 docs (exceeds capacity)
        let accesses: Vec<AccessEvent> = (0..200).map(|doc_id| create_access(doc_id, 60)).collect();

        predictor.train_from_accesses(&accesses).unwrap();

        // Should only track first `capacity` docs (sorted by doc_id)
        assert_eq!(predictor.tracked_count(), capacity);
    }

    #[test]
    fn test_needs_training() {
        let mut predictor = LearnedCachePredictor::with_config(
            1000,
            0.7,
            Duration::from_secs(3600),
            Duration::from_secs(1800),
            Duration::from_secs(1), // 1 second training interval
        )
        .unwrap();

        // Initially needs training (last_trained = UNIX_EPOCH)
        assert!(predictor.needs_training());

        // After training, shouldn't need retraining immediately
        predictor
            .train_from_accesses(&[create_access(1, 60)])
            .unwrap();

        // Wait for training interval
        std::thread::sleep(Duration::from_secs(2));

        // Now should need retraining
        assert!(predictor.needs_training());
    }

    #[test]
    fn test_stats() {
        let mut predictor = LearnedCachePredictor::new(1000).unwrap();

        let accesses = vec![
            create_access(1, 60),
            create_access(1, 120),
            create_access(2, 60),
        ];

        predictor.train_from_accesses(&accesses).unwrap();

        let stats = predictor.stats();
        assert_eq!(stats.tracked_docs, 2);
        assert!(stats.hot_docs > 0);
        assert!(stats.cache_threshold >= predictor.admission_floor());
        assert!(stats.cache_threshold <= 1.0);
        assert!(stats.avg_hotness > 0.0 && stats.avg_hotness <= 1.0);
    }
}
