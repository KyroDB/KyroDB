//! Strategy-layer adaptive admission controller for HSC.
//!
//! The learned predictor estimates `doc_id -> hotness_score`, but it does not
//! know the live state of the L1a cache. This module closes that loop by
//! sampling real cache occupancy, eviction pressure, and predictor feedback
//! backlog, then emitting a bounded admission bias that the cache strategy
//! applies on top of the predictor's trained threshold.

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

const UTILIZATION_DEADBAND: f32 = 0.03;
const FEEDBACK_SAMPLE_FLOOR: usize = 4;
const REQUEST_SAMPLE_FLOOR: u64 = 32;
const INSERT_SAMPLE_FLOOR: u64 = 8;
const MISS_STREAK_WEIGHT: f32 = 0.5;
const MAX_STEP_FRACTION: f32 = 0.35;
const ADJUSTMENT_EPSILON: f32 = 0.005;
const MISS_PRESSURE_FLOOR: f32 = 0.35;

/// Runtime config for the strategy-layer adaptive admission controller.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdaptiveAdmissionConfig {
    /// Enable adaptive admission tuning.
    pub enabled: bool,

    /// Desired steady-state L1a occupancy.
    pub target_utilization: f32,

    /// Minimum time between controller adjustments.
    pub control_interval_secs: u64,

    /// Maximum absolute threshold bias added on top of the predictor threshold.
    pub max_bias: f32,
}

impl Default for AdaptiveAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            target_utilization: 0.90,
            control_interval_secs: 15,
            max_bias: 0.18,
        }
    }
}

impl AdaptiveAdmissionConfig {
    pub fn control_interval(&self) -> Duration {
        Duration::from_secs(self.control_interval_secs)
    }
}

/// Signals sampled from the live HSC strategy.
#[derive(Debug, Clone, Copy, Default)]
pub struct AdaptiveAdmissionSignals {
    pub cache_size: usize,
    pub cache_capacity: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    pub cache_insertions: u64,
    pub false_positive_docs: usize,
    pub false_negative_docs: usize,
    pub miss_streak_docs: usize,
}

/// Exported snapshot for observability and tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct AdaptiveAdmissionSnapshot {
    pub enabled: bool,
    pub target_utilization: f32,
    pub current_utilization: f32,
    pub admission_bias: f32,
    pub effective_threshold: f32,
    pub adjustments: u64,
}

/// Low-frequency admission controller for the L1a cache.
pub struct AdaptiveAdmissionController {
    config: AdaptiveAdmissionConfig,
    admission_bias: f32,
    adjustments: u64,
    last_update: Option<Instant>,
    last_hits: u64,
    last_misses: u64,
    last_evictions: u64,
    last_insertions: u64,
}

impl AdaptiveAdmissionController {
    pub fn new(config: AdaptiveAdmissionConfig) -> Self {
        Self {
            config,
            admission_bias: 0.0,
            adjustments: 0,
            last_update: None,
            last_hits: 0,
            last_misses: 0,
            last_evictions: 0,
            last_insertions: 0,
        }
    }

    pub fn set_config(&mut self, config: AdaptiveAdmissionConfig) {
        self.config = config;
        if !self.config.enabled {
            self.admission_bias = 0.0;
        }
        self.admission_bias = self
            .admission_bias
            .clamp(-self.config.max_bias, self.config.max_bias);
    }

    pub fn config(&self) -> &AdaptiveAdmissionConfig {
        &self.config
    }

    pub fn needs_refresh(&self) -> bool {
        if !self.config.enabled {
            return false;
        }

        match self.last_update {
            None => true,
            Some(last) => last.elapsed() >= self.config.control_interval(),
        }
    }

    pub fn snapshot(
        &self,
        predictor_threshold: f32,
        admission_floor: f32,
        signals: AdaptiveAdmissionSignals,
    ) -> AdaptiveAdmissionSnapshot {
        AdaptiveAdmissionSnapshot {
            enabled: self.config.enabled,
            target_utilization: self.config.target_utilization,
            current_utilization: utilization(signals.cache_size, signals.cache_capacity),
            admission_bias: if self.config.enabled {
                self.admission_bias
            } else {
                0.0
            },
            effective_threshold: effective_threshold(
                predictor_threshold,
                admission_floor,
                if self.config.enabled {
                    self.admission_bias
                } else {
                    0.0
                },
            ),
            adjustments: self.adjustments,
        }
    }

    pub fn observe(
        &mut self,
        predictor_threshold: f32,
        admission_floor: f32,
        signals: AdaptiveAdmissionSignals,
    ) -> AdaptiveAdmissionSnapshot {
        if !self.config.enabled {
            self.admission_bias = 0.0;
            self.seed_counters(signals);
            self.last_update = Some(Instant::now());
            return self.snapshot(predictor_threshold, admission_floor, signals);
        }

        let now = Instant::now();
        let has_history = self.last_update.is_some();

        let delta_hits = signals.cache_hits.saturating_sub(self.last_hits);
        let delta_misses = signals.cache_misses.saturating_sub(self.last_misses);
        let delta_requests = delta_hits.saturating_add(delta_misses);
        let delta_evictions = signals.cache_evictions.saturating_sub(self.last_evictions);
        let delta_insertions = signals
            .cache_insertions
            .saturating_sub(self.last_insertions);

        let current_utilization = utilization(signals.cache_size, signals.cache_capacity);
        let utilization_signal =
            compute_utilization_signal(current_utilization, self.config.target_utilization);
        let feedback_signal = compute_feedback_signal(signals);
        let eviction_pressure = if has_history && delta_insertions >= INSERT_SAMPLE_FLOOR {
            (delta_evictions as f32 / delta_insertions as f32).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let miss_pressure = if has_history && delta_requests >= REQUEST_SAMPLE_FLOOR {
            (delta_misses as f32 / delta_requests as f32).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let mut raw_signal =
            utilization_signal * 0.60 + feedback_signal * 0.25 + eviction_pressure * 0.15;

        if current_utilization < self.config.target_utilization - UTILIZATION_DEADBAND
            && miss_pressure > MISS_PRESSURE_FLOOR
        {
            let normalized_miss_pressure = ((miss_pressure - MISS_PRESSURE_FLOOR)
                / (1.0 - MISS_PRESSURE_FLOOR))
                .clamp(0.0, 1.0);
            raw_signal -= normalized_miss_pressure * 0.10;
        }

        let desired_bias = raw_signal.clamp(-1.0, 1.0) * self.config.max_bias;
        let max_step = (self.config.max_bias * MAX_STEP_FRACTION).clamp(
            ADJUSTMENT_EPSILON,
            self.config.max_bias.max(ADJUSTMENT_EPSILON),
        );
        let delta = (desired_bias - self.admission_bias).clamp(-max_step, max_step);
        let new_bias =
            (self.admission_bias + delta).clamp(-self.config.max_bias, self.config.max_bias);

        if (new_bias - self.admission_bias).abs() >= ADJUSTMENT_EPSILON {
            self.adjustments = self.adjustments.saturating_add(1);
        }
        self.admission_bias = new_bias;

        self.seed_counters(signals);
        self.last_update = Some(now);

        self.snapshot(predictor_threshold, admission_floor, signals)
    }

    fn seed_counters(&mut self, signals: AdaptiveAdmissionSignals) {
        self.last_hits = signals.cache_hits;
        self.last_misses = signals.cache_misses;
        self.last_evictions = signals.cache_evictions;
        self.last_insertions = signals.cache_insertions;
    }
}

fn utilization(cache_size: usize, cache_capacity: usize) -> f32 {
    if cache_capacity == 0 {
        return 0.0;
    }

    (cache_size as f32 / cache_capacity as f32).clamp(0.0, 1.0)
}

fn effective_threshold(base_threshold: f32, admission_floor: f32, bias: f32) -> f32 {
    (base_threshold + bias).clamp(admission_floor, 1.0)
}

fn compute_utilization_signal(current: f32, target: f32) -> f32 {
    let error = current - target;
    if error.abs() <= UTILIZATION_DEADBAND {
        return 0.0;
    }

    if error > 0.0 {
        (error / (1.0 - target).max(0.05)).clamp(0.0, 1.0)
    } else {
        (error / target.max(0.05)).clamp(-1.0, 0.0)
    }
}

fn compute_feedback_signal(signals: AdaptiveAdmissionSignals) -> f32 {
    let cold_pressure =
        signals.false_negative_docs as f32 + signals.miss_streak_docs as f32 * MISS_STREAK_WEIGHT;
    let hot_pressure = signals.false_positive_docs as f32;
    let total = cold_pressure + hot_pressure;

    if total < FEEDBACK_SAMPLE_FLOOR as f32 {
        return 0.0;
    }

    ((hot_pressure - cold_pressure) / total).clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signals(cache_size: usize, capacity: usize) -> AdaptiveAdmissionSignals {
        AdaptiveAdmissionSignals {
            cache_size,
            cache_capacity: capacity,
            ..AdaptiveAdmissionSignals::default()
        }
    }

    #[test]
    fn controller_becomes_more_selective_when_cache_is_overfull_and_eviction_heavy() {
        let mut controller = AdaptiveAdmissionController::new(AdaptiveAdmissionConfig {
            control_interval_secs: 0,
            max_bias: 0.20,
            ..AdaptiveAdmissionConfig::default()
        });

        let mut sample = signals(96, 100);
        controller.observe(0.18, 0.10, sample);

        sample.cache_hits = 64;
        sample.cache_misses = 16;
        sample.cache_insertions = 24;
        sample.cache_evictions = 20;
        sample.false_positive_docs = 12;
        let snapshot = controller.observe(0.18, 0.10, sample);

        assert!(snapshot.admission_bias > 0.0);
        assert!(snapshot.effective_threshold > 0.18);
    }

    #[test]
    fn controller_relaxes_threshold_when_underfilled_and_false_negatives_dominate() {
        let mut controller = AdaptiveAdmissionController::new(AdaptiveAdmissionConfig {
            control_interval_secs: 0,
            max_bias: 0.20,
            ..AdaptiveAdmissionConfig::default()
        });

        let mut sample = signals(20, 100);
        controller.observe(0.22, 0.10, sample);

        sample.cache_hits = 8;
        sample.cache_misses = 64;
        sample.false_negative_docs = 14;
        sample.miss_streak_docs = 10;
        let snapshot = controller.observe(0.22, 0.10, sample);

        assert!(snapshot.admission_bias < 0.0);
        assert!(snapshot.effective_threshold < 0.22);
    }

    #[test]
    fn disabled_controller_reports_zero_bias() {
        let mut controller = AdaptiveAdmissionController::new(AdaptiveAdmissionConfig {
            enabled: false,
            control_interval_secs: 0,
            ..AdaptiveAdmissionConfig::default()
        });

        let mut sample = signals(100, 100);
        sample.cache_insertions = 32;
        sample.cache_evictions = 32;
        sample.false_positive_docs = 32;
        let snapshot = controller.observe(0.18, 0.10, sample);

        assert!(!snapshot.enabled);
        assert_eq!(snapshot.admission_bias, 0.0);
        assert!((snapshot.effective_threshold - 0.18).abs() < f32::EPSILON);
    }
}
