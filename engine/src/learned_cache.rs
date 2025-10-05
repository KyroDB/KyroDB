//! Learned Cache Predictor using RMI for cache hotness prediction
//!
//! Phase 0 Week 3-4: Adapt RMI for learned cache (NOT k-NN search)
//!
//! This module predicts cache hotness: doc_id → P(hot | recent_accesses)
//! RMI learns access patterns and predicts which documents should be cached.
//!
//! Architecture:
//! - RMI predicts cache hotness (not vector positions)
//! - HNSW performs k-NN search (separate concern)
//! - Access logger feeds training data to RMI
//!
//! Target: 70-90% cache hit rate vs 30-40% LRU baseline

use crate::rmi_core::RmiIndex;
use anyhow::Result;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Access event for training learned cache predictor
#[derive(Debug, Clone, Copy)]
pub struct AccessEvent {
    pub doc_id: u64,
    pub timestamp: SystemTime,
    pub access_type: AccessType,
}

impl Default for AccessEvent {
    fn default() -> Self {
        Self {
            doc_id: 0,
            timestamp: SystemTime::UNIX_EPOCH,
            access_type: AccessType::default(),
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
    /// RMI index for hotness prediction (doc_id → position in hotness_scores)
    /// Option because index is empty until first training
    hotness_index: Option<RmiIndex>,

    /// Current hotness scores (training data: doc_id → hotness_score)
    hotness_scores: Vec<(u64, f32)>,

    /// Hotness threshold for cache admission (tunable)
    cache_threshold: f32,

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
    /// - `capacity`: Maximum number of doc_ids to track (e.g., 10M)
    ///
    /// # Defaults
    /// - Cache threshold: 0.7 (70th percentile hotness)
    /// - Training window: 24 hours
    /// - Recency half-life: 1 hour
    /// - Training interval: 10 minutes
    pub fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            hotness_index: None, // Empty until first training
            hotness_scores: Vec::with_capacity(capacity),
            cache_threshold: 0.7,
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
        Ok(Self {
            hotness_index: None, // Empty until first training
            hotness_scores: Vec::with_capacity(capacity),
            cache_threshold,
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
    /// - 0.0 = cold (rarely accessed)
    /// - 1.0 = hot (frequently accessed recently)
    ///
    /// # Performance
    /// - O(1) RMI prediction + O(log k) bounded search
    /// - Target: <100ns P99 latency
    pub fn predict_hotness(&self, doc_id: u64) -> f32 {
        if self.hotness_scores.is_empty() {
            return 0.0;
        }

        // Use RMI to find position of doc_id in hotness_scores
        let index = match &self.hotness_index {
            Some(idx) => idx,
            None => return 0.0, // Not trained yet
        };

        match index.get(doc_id) {
            Some(position) => {
                // Found doc_id in training data, look up hotness score
                let pos = position as usize;
                if pos < self.hotness_scores.len() {
                    self.hotness_scores[pos].1
                } else {
                    0.0
                }
            }
            None => {
                // Doc_id not in training data (never accessed or evicted)
                0.0
            }
        }
    }

    /// Decide if document should be cached based on predicted hotness
    ///
    /// Returns `true` if predicted hotness > cache_threshold
    pub fn should_cache(&self, doc_id: u64) -> bool {
        self.predict_hotness(doc_id) > self.cache_threshold
    }

    /// Train predictor from access events
    ///
    /// # Algorithm
    /// 1. Compute hotness scores from access events:
    ///    - Hotness = frequency × recency_weight
    ///    - Recency weight = exp(-age / half_life)
    /// 2. Normalize scores to [0, 1]
    /// 3. Build RMI index for fast lookup
    ///
    /// # Performance
    /// - O(n log n) for sorting + O(n) for RMI training
    /// - Target: <100ms for 10M doc_ids
    pub fn train_from_accesses(&mut self, accesses: &[AccessEvent]) -> Result<()> {
        if accesses.is_empty() {
            return Ok(());
        }

        // Compute hotness scores
        let hotness_map = self.compute_hotness_scores(accesses);

        // Convert to sorted vec for RMI training
        let mut hotness_vec: Vec<(u64, f32)> = hotness_map.into_iter().collect();
        hotness_vec.sort_by_key(|(doc_id, _)| *doc_id);

        // Truncate to capacity if needed
        if hotness_vec.len() > self.capacity {
            hotness_vec.truncate(self.capacity);
        }

        // Prepare training data for RMI (doc_id → position mapping)
        let training_data: Vec<(u64, u64)> = hotness_vec
            .iter()
            .enumerate()
            .map(|(pos, (doc_id, _))| (*doc_id, pos as u64))
            .collect();

        // Build RMI index for fast lookup
        // Target segment size: 100 keys per segment (good balance for <100ns lookups)
        let target_segment_size = 100;
        self.hotness_index = Some(RmiIndex::build(training_data, target_segment_size));

        // Store hotness scores for lookup
        self.hotness_scores = hotness_vec;
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

    /// Update cache admission threshold (for tuning)
    pub fn set_cache_threshold(&mut self, threshold: f32) {
        self.cache_threshold = threshold.clamp(0.0, 1.0);
    }

    /// Get number of tracked documents
    pub fn tracked_count(&self) -> usize {
        self.hotness_scores.len()
    }

    /// Get statistics for monitoring
    pub fn stats(&self) -> CachePredictorStats {
        let hot_count = self
            .hotness_scores
            .iter()
            .filter(|(_, score)| *score > self.cache_threshold)
            .count();

        let avg_hotness = if self.hotness_scores.is_empty() {
            0.0
        } else {
            self.hotness_scores
                .iter()
                .map(|(_, score)| score)
                .sum::<f32>()
                / self.hotness_scores.len() as f32
        };

        CachePredictorStats {
            tracked_docs: self.hotness_scores.len(),
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
                    *score /= max_hotness;
                }
            }
        }

        hotness
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
        assert_eq!(predictor.cache_threshold(), 0.7);
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

        // With threshold 0.7, only hottest docs cached
        predictor.set_cache_threshold(0.7);
        let hot_count_high = predictor.stats().hot_docs;

        // With threshold 0.3, more docs cached
        predictor.set_cache_threshold(0.3);
        let hot_count_low = predictor.stats().hot_docs;

        // Lower threshold → more docs considered hot
        assert!(hot_count_low >= hot_count_high);
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
        assert_eq!(stats.cache_threshold, 0.7);
        assert!(stats.avg_hotness > 0.0 && stats.avg_hotness <= 1.0);
    }
}
