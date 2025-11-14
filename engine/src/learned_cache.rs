//! Hybrid Semantic Cache Predictor (Frequency Component)
//!
//! **Purpose**: RMI-based frequency prediction for document-level hotness.
//! Combined with semantic similarity adapter for hybrid cache admission decisions.
//!
//! This module predicts cache hotness: doc_id → P(hot | recent_accesses)
//! RMI learns access patterns and predicts which documents should be cached.
//!
//! Architecture:
//! - RMI stores doc_id → hotness_score (O(log n) lookup)
//! - Semantic adapter computes embedding similarity (optional hybrid layer)
//! - Access logger feeds training data
//!
//! Target: 70-90% cache hit rate vs 30-40% LRU baseline

use anyhow::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_DIVERSITY_BUCKETS: usize = 256;
const MAX_DIVERSITY_BUCKETS: usize = 4096;
const MISS_DEMOTION_THRESHOLD: u32 = 6;
const MISS_PENALTY_RATE: f32 = 0.35;

/// Access event for training Hybrid Semantic Cache predictor
///
#[derive(Debug, Clone)]
pub struct AccessEvent {
    pub doc_id: u64,
    pub timestamp: SystemTime,
    pub access_type: AccessType,
    // CRITICAL: Removed embedding field to fix 107MB memory leak
    // Embeddings are not used in RMI training (only doc_id matters)
    // This reduces AccessEvent from ~1568 bytes to ~32 bytes (48× smaller!)
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

/// Hybrid Semantic Cache predictor using RMI to predict document hotness (frequency component)
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

    /// Target cache utilization for auto-tuning (0.0-1.0)
    target_utilization: f32,

    /// Threshold adjustment rate for calibration (0.0-1.0)
    threshold_adjustment_rate: f32,

    /// Auto-tune threshold based on cache utilization
    auto_tune_enabled: bool,

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

    /// Last calibration timestamp (for rate-limited auto-tuning)
    last_calibration: Arc<parking_lot::RwLock<SystemTime>>,

    /// FEEDBACK LOOP: Track false positives (predicted hot but evicted without re-access)
    /// doc_id → number of times evicted from cache
    false_positives: Arc<RwLock<HashMap<u64, u32>>>,

    /// FEEDBACK LOOP: Track false negatives (predicted cold but was accessed)
    /// doc_id → number of cache misses
    false_negatives: Arc<RwLock<HashMap<u64, u32>>>,

    /// Diversity guardrails: number of hash buckets for hotset selection
    diversity_bucket_count: usize,

    /// Miss counters for consecutive cache misses (used for demotion)
    miss_counters: Arc<RwLock<HashMap<u64, MissStats>>>,

    /// Threshold (in consecutive misses) before applying demotion penalty
    miss_demotion_threshold: u32,

    /// Penalty rate applied per miss streak when demoting hot candidates
    miss_penalty_rate: f32,
}

#[derive(Debug, Default, Clone, Copy)]
struct MissStats {
    consecutive: u32,
    total: u32,
}

impl LearnedCachePredictor {
    /// Create new Hybrid Semantic Cache predictor
    ///
    /// # Parameters
    /// - `capacity`: Maximum number of doc_ids to track (e.g., 10K-1M)
    ///
    /// # Defaults
    /// - Cache threshold: adaptive based on capacity
    /// - Training window: 1 hour (faster adaptation)
    /// - Recency half-life: 30 minutes (faster decay)
    /// - Training interval: 10 minutes
    /// - Auto-tune: ENABLED (target 80% utilization for learning headroom)
    pub fn new(capacity: usize) -> Result<Self> {
        // FIXED: Use RMI as WIDE NET (recall-focused), semantic adapter filters false positives
        // Threshold balances recall (don't reject hot docs) vs precision (don't admit cold docs)
        // Target: Predict 5-6x cache capacity as "potentially hot" for semantic filtering
        let initial_threshold = if capacity < 100 {
            0.22 // Tiny caches: admit ~300 docs (6x capacity of 50)
        } else if capacity <= 1000 {
            0.15 // Small caches: widen candidate pool for semantic layer
        } else {
            0.18 // Large caches: maintain moderate recall bias
        };

        Ok(Self {
            hotness_map: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            cache_threshold: initial_threshold,
            admission_floor: 0.10,
            unseen_admission_chance: 0.25,
            target_hot_entries: capacity,
            threshold_smoothing: 0.3,
            target_utilization: 0.80,
            threshold_adjustment_rate: 0.10,
            auto_tune_enabled: false,
            training_window: Duration::from_secs(3600),
            recency_halflife: Duration::from_secs(1800),
            capacity,
            last_trained: UNIX_EPOCH,
            training_interval: Duration::from_secs(600),
            last_calibration: Arc::new(parking_lot::RwLock::new(UNIX_EPOCH)),
            false_positives: Arc::new(RwLock::new(HashMap::new())),
            false_negatives: Arc::new(RwLock::new(HashMap::new())),
            diversity_bucket_count: DEFAULT_DIVERSITY_BUCKETS,
            miss_counters: Arc::new(RwLock::new(HashMap::new())),
            miss_demotion_threshold: MISS_DEMOTION_THRESHOLD,
            miss_penalty_rate: MISS_PENALTY_RATE,
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
        let admission_floor = 0.10;
        Ok(Self {
            hotness_map: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            cache_threshold: cache_threshold.clamp(admission_floor, 1.0),
            admission_floor,
            unseen_admission_chance: 0.20,
            target_hot_entries: capacity,
            threshold_smoothing: 0.6,
            target_utilization: 0.85,
            threshold_adjustment_rate: 0.05,
            auto_tune_enabled: false,
            training_window,
            recency_halflife,
            capacity,
            last_trained: UNIX_EPOCH,
            training_interval,
            last_calibration: Arc::new(parking_lot::RwLock::new(UNIX_EPOCH)),
            false_positives: Arc::new(RwLock::new(HashMap::new())),
            false_negatives: Arc::new(RwLock::new(HashMap::new())),
            diversity_bucket_count: DEFAULT_DIVERSITY_BUCKETS,
            miss_counters: Arc::new(RwLock::new(HashMap::new())),
            miss_demotion_threshold: MISS_DEMOTION_THRESHOLD,
            miss_penalty_rate: MISS_PENALTY_RATE,
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
        let mut hotness_map = self.compute_hotness_scores(accesses);

        // FEEDBACK LOOP: Apply corrections based on false positives/negatives
        // Penalize docs that were cached but evicted (false positives)
        // Boost docs that were accessed but not cached (false negatives)
        self.apply_feedback_corrections(&mut hotness_map);

        // Sort by hotness (descending) to keep hottest documents
        let mut hotness_vec: Vec<(u64, f32)> = hotness_map.into_iter().collect();
        hotness_vec.sort_by(|(_, score_a), (_, score_b)| {
            score_b
                .partial_cmp(score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Truncate to target_hot_entries with diversity guardrails
        let limit = self.target_hot_entries.min(hotness_vec.len());
        let selected = self.select_with_diversity(&hotness_vec, limit);

        self.recalibrate_threshold(&selected);

        // Update HashMap atomically
        {
            let mut map = self.hotness_map.write();
            map.clear();
            map.extend(selected);
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

    /// Auto-calibrate threshold based on current cache utilization
    ///
    /// Adjusts admission threshold to maintain target cache utilization (default: 85%).
    /// Called periodically by cache strategy to dynamically tune threshold.
    ///
    /// # Parameters
    /// - `current_cache_size`: Current number of entries in cache
    ///
    /// # Algorithm
    /// - If utilization < 75%: Lower threshold by 5% (admit more)
    /// - If utilization > 95%: Raise threshold by 5% (admit less)
    /// - Clamped to [0.05, 0.95] bounds
    pub fn calibrate_threshold(&mut self, current_cache_size: usize) {
        if !self.auto_tune_enabled {
            return;
        }

        // RATE LIMITING: Only calibrate once every 60 seconds to prevent per-query instability
        const CALIBRATION_INTERVAL_SECS: u64 = 60;
        let now = SystemTime::now();
        let mut last_cal = self.last_calibration.write();
        if let Ok(elapsed) = now.duration_since(*last_cal) {
            if elapsed.as_secs() < CALIBRATION_INTERVAL_SECS {
                return; // Too soon, skip calibration
            }
        }
        *last_cal = now;
        drop(last_cal);

        let capacity = self.capacity;
        if capacity == 0 {
            return;
        }

        let current_utilization = current_cache_size as f32 / capacity as f32;
        let target = self.target_utilization;
        let rate = self.threshold_adjustment_rate;

        let lower_bound = target * 0.9;
        let upper_bound = target * 1.1;

        if current_utilization < lower_bound {
            let new_threshold = self.cache_threshold * (1.0 - rate);
            self.cache_threshold = new_threshold.max(self.admission_floor);
        } else if current_utilization > upper_bound {
            let new_threshold = self.cache_threshold * (1.0 + rate);
            self.cache_threshold = new_threshold.min(0.95);
        }
    }

    /// Enable or disable auto-tuning
    pub fn set_auto_tune(&mut self, enabled: bool) {
        self.auto_tune_enabled = enabled;
    }

    /// Set target cache utilization (0.0-1.0)
    pub fn set_target_utilization(&mut self, target: f32) {
        self.target_utilization = target.clamp(0.1, 1.0);
    }

    /// Set threshold adjustment rate (0.0-1.0)
    pub fn set_adjustment_rate(&mut self, rate: f32) {
        self.threshold_adjustment_rate = rate.clamp(0.01, 0.5);
    }

    /// Maximum number of documents this predictor can track
    pub fn capacity_limit(&self) -> usize {
        self.capacity
    }

    /// Configure diversity bucket count for hotset selection
    pub fn set_diversity_buckets(&mut self, buckets: usize) {
        let bounded = buckets.clamp(1, MAX_DIVERSITY_BUCKETS);
        self.diversity_bucket_count = bounded;
    }

    /// Set miss demotion threshold (consecutive misses before penalty applies)
    pub fn set_miss_demotion_threshold(&mut self, threshold: u32) {
        self.miss_demotion_threshold = threshold.max(1);
    }

    /// Set miss penalty rate (per-miss multiplier)
    pub fn set_miss_penalty_rate(&mut self, rate: f32) {
        self.miss_penalty_rate = rate.clamp(0.05, 1.0);
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
        let halflife_seconds = self.recency_halflife.as_secs_f32().max(1.0);

        for access in accesses {
            if access.timestamp < cutoff {
                continue;
            }

            let age = now
                .duration_since(access.timestamp)
                .unwrap_or(Duration::ZERO);
            let age_seconds = age.as_secs_f32();

            // Exponential decay with much faster drop-off (configured via halflife)
            let recency_weight = (-age_seconds / halflife_seconds).exp();
            *hotness.entry(access.doc_id).or_insert(0.0) += recency_weight;
        }

        for score in hotness.values_mut() {
            // Logarithmic compression: diminishing returns for repeated hits
            let log_weight = score.ln_1p();
            // Bounded mapping to [0,1): emphasize top docs without global normalization
            let bounded = log_weight / (1.0 + log_weight);
            *score = bounded.clamp(0.0, 1.0);
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

    fn select_with_diversity(
        &self,
        sorted_hotness: &[(u64, f32)],
        limit: usize,
    ) -> Vec<(u64, f32)> {
        if limit == 0 || sorted_hotness.is_empty() {
            return Vec::new();
        }

        if self.diversity_bucket_count <= 1 {
            return sorted_hotness.iter().take(limit).cloned().collect();
        }

        let bucket_count = self.diversity_bucket_count;
        let mut bucket_usage = vec![0usize; bucket_count];
        let max_per_bucket = ((limit + bucket_count - 1) / bucket_count).max(1);

        let mut selected = Vec::with_capacity(limit);
        let mut overflow = Vec::new();

        for &(doc_id, score) in sorted_hotness.iter() {
            let bucket = self.bucket_for_doc(doc_id);
            if bucket_usage[bucket] < max_per_bucket {
                bucket_usage[bucket] += 1;
                selected.push((doc_id, score));
                if selected.len() == limit {
                    return selected;
                }
            } else {
                overflow.push((doc_id, score));
            }
        }

        for candidate in overflow.into_iter() {
            if selected.len() == limit {
                break;
            }
            selected.push(candidate);
        }

        selected
    }

    fn bucket_for_doc(&self, doc_id: u64) -> usize {
        if self.diversity_bucket_count <= 1 {
            return 0;
        }
        let hash = doc_id.wrapping_mul(0x9E3779B97F4A7C15);
        (hash as usize) % self.diversity_bucket_count
    }

    /// FEEDBACK LOOP: Called when cache evicts a document that wasn't re-accessed
    /// This indicates RMI predicted the doc as hot, but it turned out to be a false positive
    pub fn record_eviction(&self, doc_id: u64) {
        *self.false_positives.write().entry(doc_id).or_insert(0) += 1;
    }

    /// FEEDBACK LOOP: Called when cache misses a document (predicted cold but accessed)
    /// This indicates RMI missed a hot document (false negative)
    pub fn record_cache_miss(&self, doc_id: u64, predicted_hotness: f32) {
        if predicted_hotness < self.cache_threshold {
            *self.false_negatives.write().entry(doc_id).or_insert(0) += 1;
        }

        let mut misses = self.miss_counters.write();
        let entry = misses.entry(doc_id).or_insert_with(MissStats::default);
        entry.consecutive = entry.consecutive.saturating_add(1);
        entry.total = entry.total.saturating_add(1);
    }

    /// FEEDBACK LOOP: Called when cache hits so we can reset miss streaks
    pub fn record_cache_hit(&self, doc_id: u64) {
        let mut misses = self.miss_counters.write();
        misses.remove(&doc_id);
    }

    /// FEEDBACK LOOP: During training, penalize false positives and boost false negatives
    /// This corrects RMI predictions based on actual cache behavior
    /// - False positives (evicted without re-access): up to 30% multiplicative penalty
    /// - False negatives (accessed but not cached): +0.02 per miss (capped)
    pub fn apply_feedback_corrections(&self, hotness_scores: &mut HashMap<u64, f32>) {
        let fps = self.false_positives.read();
        let fns = self.false_negatives.read();

        // Penalize false positives (was cached but not re-accessed)
        for (doc_id, evictions) in fps.iter() {
            if let Some(score) = hotness_scores.get_mut(doc_id) {
                let penalty = (0.10 * *evictions as f32).min(0.30);
                let adjusted = *score * (1.0 - penalty);
                *score = adjusted.max(self.admission_floor);
            }
        }

        // Boost false negatives (was accessed but not cached)
        for (doc_id, misses) in fns.iter() {
            if let Some(score) = hotness_scores.get_mut(doc_id) {
                let boost = (0.02 * *misses as f32).min(0.20);
                *score = (*score + boost).min(1.0);
            }
        }

        // Clear feedback after applying (fresh start for next cycle)
        drop(fps);
        drop(fns);
        self.false_positives.write().clear();
        self.false_negatives.write().clear();

        self.apply_miss_penalties(hotness_scores);
    }

    fn apply_miss_penalties(&self, hotness_scores: &mut HashMap<u64, f32>) {
        let mut misses = self.miss_counters.write();
        if misses.is_empty() {
            return;
        }

        let threshold = self.miss_demotion_threshold;
        for (doc_id, stats) in misses.iter() {
            if stats.consecutive >= threshold {
                if let Some(score) = hotness_scores.get_mut(doc_id) {
                    let streak = stats.consecutive.min(64) as f32;
                    let penalty = (1.0 / (1.0 + self.miss_penalty_rate * streak)).clamp(0.0, 1.0);
                    *score = (*score * penalty).max(self.admission_floor);
                }
            }
        }

        // Retain only streaks that are still active (no cache hit yet)
        misses.retain(|_, stats| stats.consecutive > 0);
    }
}

/// Statistics for Hybrid Semantic Cache predictor monitoring
#[derive(Debug, Clone)]
pub struct CachePredictorStats {
    pub tracked_docs: usize,
    pub hot_docs: usize,
    pub cache_threshold: f32,
    pub avg_hotness: f32,
    pub last_trained: SystemTime,
}

/// Extended statistics including percentile buckets and threshold coverage
#[derive(Debug, Clone)]
pub struct CachePredictorStatsExt {
    pub base: CachePredictorStats,
    pub above_threshold: usize,
    pub p50: f32,
    pub p90: f32,
    pub p99: f32,
    pub mean_hot_hits: f32,
    pub mean_cold_misses: f32,
}

impl LearnedCachePredictor {
    /// Compute extended stats (expensive; avoid calling on hot path)
    pub fn stats_extended(
        &self,
        hit_doc_ids: &[u64],
        miss_doc_ids: &[u64],
    ) -> CachePredictorStatsExt {
        let map = self.hotness_map.read();
        let mut scores: Vec<f32> = map.values().copied().collect();
        scores.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let pct = |p: f32| -> f32 {
            if scores.is_empty() {
                return 0.0;
            }
            let idx = ((p * (scores.len() as f32 - 1.0)).round() as usize).min(scores.len() - 1);
            scores[idx]
        };
        let threshold = self.cache_threshold;
        let above_threshold = map.values().filter(|s| **s >= threshold).count();
        let hit_sum: f32 = hit_doc_ids
            .iter()
            .filter_map(|id| map.get(id))
            .copied()
            .sum();
        let miss_sum: f32 = miss_doc_ids
            .iter()
            .filter_map(|id| map.get(id))
            .copied()
            .sum();
        let mean_hot_hits = if hit_doc_ids.is_empty() {
            0.0
        } else {
            hit_sum / hit_doc_ids.len() as f32
        };
        let mean_cold_misses = if miss_doc_ids.is_empty() {
            0.0
        } else {
            miss_sum / miss_doc_ids.len() as f32
        };
        CachePredictorStatsExt {
            base: self.stats(),
            above_threshold,
            p50: pct(0.50),
            p90: pct(0.90),
            p99: pct(0.99),
            mean_hot_hits,
            mean_cold_misses,
        }
    }
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
        assert!((predictor.cache_threshold() - 0.15).abs() < f32::EPSILON);
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
        assert!(predictor.predict_hotness(42) > 0.6); // Very hot
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
        assert!(hotness_42 > 0.5); // Recent accesses = high hotness
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
    fn test_miss_streak_penalizes_hot_doc() {
        let mut predictor = LearnedCachePredictor::new(256).unwrap();
        predictor.set_target_hot_entries(10);

        let accesses: Vec<AccessEvent> = (0..12).map(|i| create_access(1, i * 30)).collect();
        predictor.train_from_accesses(&accesses).unwrap();
        let baseline = predictor.predict_hotness(1);
        assert!(baseline > 0.5);

        for _ in 0..8 {
            predictor.record_cache_miss(1, 0.0);
        }

        predictor.train_from_accesses(&accesses).unwrap();
        let penalized = predictor.predict_hotness(1);
        assert!(penalized < baseline);
    }

    #[test]
    fn test_diversity_buckets_expand_selection() {
        let mut predictor = LearnedCachePredictor::new(64).unwrap();
        predictor.set_target_hot_entries(4);
        predictor.set_diversity_buckets(2);
        predictor.set_threshold_smoothing(0.0);

        let mut accesses = Vec::new();
        // Bucket 0 docs (even ids) dominate frequency
        for _ in 0..20 {
            accesses.push(create_access(0, 30));
            accesses.push(create_access(2, 30));
        }
        // Bucket 1 doc (odd id) minimal accesses
        for _ in 0..2 {
            accesses.push(create_access(1, 30));
        }

        predictor.train_from_accesses(&accesses).unwrap();

        assert!(predictor.lookup_hotness(1).unwrap_or(0.0) > 0.0);
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
