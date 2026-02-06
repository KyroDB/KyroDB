//!
//! This file contains ONLY the essential learned predictor algorithm components needed for
//! Hybrid Semantic Cache prediction.
//!

use std::sync::Arc;

/// Local linear model for position prediction
///
/// Core learned predictor component: predicts position in sorted array using linear regression.
/// Reusable for Hybrid Semantic Cache: predicts doc_id → hotness_score lookup position.
#[derive(Debug, Clone)]
pub struct LocalLinearModel {
    slope: f64,
    intercept: f64,
    key_min: u64,
    key_max: u64,
    error_bound: u32,
    data_len: usize,
}

impl LocalLinearModel {
    /// Build linear model from sorted data
    pub fn new(data: &[(u64, u64)]) -> Self {
        if data.is_empty() {
            return Self {
                slope: 0.0,
                intercept: 0.0,
                key_min: 0,
                key_max: 0,
                error_bound: 0,
                data_len: 0,
            };
        }

        if data.len() == 1 {
            return Self {
                slope: 0.0,
                intercept: 0.0,
                key_min: data[0].0,
                key_max: data[0].0,
                error_bound: 0,
                data_len: 1,
            };
        }

        let data_len = data.len();
        let key_min = data[0].0;
        let key_max = data[data_len - 1].0;

        // Linear regression: position = slope * key + intercept
        // Degenerate case: all keys identical — flat model predicting the midpoint.
        let (slope, intercept) = if key_max == key_min {
            (0.0, (data_len - 1) as f64 / 2.0)
        } else {
            let s = (data_len - 1) as f64 / (key_max - key_min) as f64;
            (s, -(key_min as f64) * s)
        };

        // Calculate maximum prediction error
        let mut max_error = 0u32;
        for (i, &(key, _)) in data.iter().enumerate() {
            let predicted = (slope * key as f64 + intercept).max(0.0) as usize;
            let predicted_clamped = predicted.min(data_len - 1);
            let error = (predicted_clamped as i64 - i as i64).unsigned_abs() as u32;
            max_error = max_error.max(error);
        }

        // Use the true maximum error so bounded_search never misses a key.
        // A large error_bound degrades to O(log n) binary search, but remains correct.
        let error_bound = max_error.max(1);

        Self {
            slope,
            intercept,
            key_min,
            key_max,
            error_bound,
            data_len,
        }
    }

    /// Predict position for key
    #[inline(always)]
    pub fn predict(&self, key: u64) -> usize {
        if self.data_len == 0 {
            return 0;
        }
        let predicted = (self.slope * key as f64 + self.intercept).max(0.0) as usize;
        predicted.min(self.data_len - 1)
    }

    /// Get error bound for bounded search
    #[inline]
    pub fn error_bound(&self) -> u32 {
        self.error_bound
    }

    /// Check if key is in model's range
    #[inline]
    pub fn contains_key(&self, key: u64) -> bool {
        key >= self.key_min && key <= self.key_max
    }
}

/// Segment of sorted data with local learned model
///
/// Core learned predictor component: each segment learns a local linear model for its key range.
/// Reusable for Hybrid Semantic Cache: segment stores (doc_id, hotness_score) pairs.
pub struct LearnedPredictorSegment {
    local_model: LocalLinearModel,
    data: Arc<Vec<(u64, u64)>>,
}

impl LearnedPredictorSegment {
    /// Create segment from sorted data
    pub fn new(data: Vec<(u64, u64)>) -> Self {
        let local_model = LocalLinearModel::new(&data);
        Self {
            local_model,
            data: Arc::new(data),
        }
    }

    /// Bounded search: O(log ε) where ε = error_bound
    pub fn bounded_search(&self, key: u64) -> Option<u64> {
        if self.data.is_empty() {
            return None;
        }

        let predicted_pos = self.local_model.predict(key);
        let epsilon = self.local_model.error_bound() as usize;

        let data = self.data.as_slice();
        let data_len = data.len();

        // Calculate bounded search window
        let start = predicted_pos.saturating_sub(epsilon);
        let end = (predicted_pos + epsilon + 1).min(data_len);

        if start >= end {
            return None;
        }

        // Binary search in bounded window
        match data[start..end].binary_search_by_key(&key, |(k, _)| *k) {
            Ok(idx) => Some(data[start + idx].1),
            Err(_) => None,
        }
    }

    /// Get data reference (for external iteration if needed)
    pub fn data(&self) -> &[(u64, u64)] {
        &self.data
    }

    /// Get local model (for diagnostics)
    pub fn model(&self) -> &LocalLinearModel {
        &self.local_model
    }
}

/// Segment router: maps key → segment index
///
/// Core learned predictor component: top-level model that routes keys to segments.
/// Reusable for Hybrid Semantic Cache: routes doc_id to appropriate cache segment.
pub struct SegmentRouter {
    /// (key_min, key_max, original_segment_index) for each non-empty segment.
    key_ranges: Vec<(u64, u64, usize)>,
    segment_count: usize,
}

impl SegmentRouter {
    /// Build router from segment key ranges
    pub fn new(segments: &[LearnedPredictorSegment]) -> Self {
        let key_ranges = segments
            .iter()
            .enumerate()
            .filter_map(|(idx, seg)| {
                let data = seg.data();
                if data.is_empty() {
                    None
                } else {
                    Some((data[0].0, data[data.len() - 1].0, idx))
                }
            })
            .collect();

        Self {
            key_ranges,
            segment_count: segments.len(),
        }
    }

    /// Find segment index for key (returns index into the original segments array)
    pub fn find_segment(&self, key: u64) -> Option<usize> {
        self.key_ranges
            .binary_search_by(|(min, max, _)| {
                if key < *min {
                    std::cmp::Ordering::Greater
                } else if key > *max {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
            .map(|idx| self.key_ranges[idx].2)
    }

    pub fn segment_count(&self) -> usize {
        self.segment_count
    }
}

/// Multi-segment learned predictor index
///
/// Complete learned predictor with routing + segments.
/// Learned index for cache predictor: Learned Frequency Predictor (learned predictor).
pub struct LearnedPredictorIndex {
    segments: Vec<LearnedPredictorSegment>,
    router: SegmentRouter,
}

impl LearnedPredictorIndex {
    /// Build learned predictor from sorted data
    pub fn build(data: Vec<(u64, u64)>, target_segment_size: usize) -> Self {
        if data.is_empty() {
            return Self {
                segments: Vec::new(),
                router: SegmentRouter {
                    key_ranges: Vec::new(),
                    segment_count: 0,
                },
            };
        }

        // Partition data into segments
        let segments: Vec<LearnedPredictorSegment> = data
            .chunks(target_segment_size)
            .map(|chunk| LearnedPredictorSegment::new(chunk.to_vec()))
            .collect();

        let router = SegmentRouter::new(&segments);

        Self { segments, router }
    }

    /// Lookup value by key
    pub fn get(&self, key: u64) -> Option<u64> {
        let segment_idx = self.router.find_segment(key)?;
        self.segments.get(segment_idx)?.bounded_search(key)
    }

    /// Get segment count
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_linear_model() {
        let data = vec![(10, 100), (20, 200), (30, 300), (40, 400)];
        let model = LocalLinearModel::new(&data);

        assert_eq!(model.predict(10), 0);
        assert_eq!(model.predict(40), 3);
        assert!(model.error_bound() >= 1);
    }

    #[test]
    fn test_local_linear_model_identical_keys() {
        // Degenerate case: all keys equal must not divide by zero
        let data = vec![(5, 100), (5, 200), (5, 300)];
        let model = LocalLinearModel::new(&data);

        let predicted = model.predict(5);
        assert!(
            predicted < data.len(),
            "predicted {} out of range",
            predicted
        );
        assert!(model.error_bound() >= 1);
    }

    #[test]
    fn test_learned_predictor_segment() {
        let data = vec![(1, 10), (2, 20), (3, 30), (4, 40)];
        let segment = LearnedPredictorSegment::new(data);

        assert_eq!(segment.bounded_search(2), Some(20));
        assert_eq!(segment.bounded_search(99), None);
    }

    #[test]
    fn test_learned_predictor_index() {
        let data: Vec<(u64, u64)> = (0..1000).map(|i| (i, i * 10)).collect();
        let index = LearnedPredictorIndex::build(data, 100);

        assert_eq!(index.get(42), Some(420));
        assert_eq!(index.get(999), Some(9990));
        assert_eq!(index.get(1000), None);
    }
}
