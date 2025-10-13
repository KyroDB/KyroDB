//!
//! This file contains ONLY the essential RMI algorithm components needed for
//! Hybrid Semantic Cache prediction.
//!

use std::sync::Arc;

/// Maximum search window for bounded search guarantee
const MAX_SEARCH_WINDOW: usize = 16;

/// Local linear model for position prediction
///
/// Core RMI component: predicts position in sorted array using linear regression.
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
        let slope = (data_len - 1) as f64 / (key_max - key_min) as f64;
        let intercept = -(key_min as f64) * slope;

        // Calculate maximum prediction error
        let mut max_error = 0u32;
        for (i, &(key, _)) in data.iter().enumerate() {
            let predicted = (slope * key as f64 + intercept).max(0.0) as usize;
            let predicted_clamped = predicted.min(data_len - 1);
            let error = (predicted_clamped as i64 - i as i64).unsigned_abs() as u32;
            max_error = max_error.max(error);
        }

        let error_bound = max_error.max(1).min(MAX_SEARCH_WINDOW as u32 / 2);

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
/// Core RMI component: each segment learns a local linear model for its key range.
/// Reusable for Hybrid Semantic Cache: segment stores (doc_id, hotness_score) pairs.
pub struct RmiSegment {
    local_model: LocalLinearModel,
    data: Arc<Vec<(u64, u64)>>,
}

impl RmiSegment {
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
/// Core RMI component: top-level model that routes keys to segments.
/// Reusable for Hybrid Semantic Cache: routes doc_id to appropriate cache segment.
pub struct SegmentRouter {
    key_ranges: Vec<(u64, u64)>,
    segment_count: usize,
}

impl SegmentRouter {
    /// Build router from segment key ranges
    pub fn new(segments: &[RmiSegment]) -> Self {
        let key_ranges = segments
            .iter()
            .filter_map(|seg| {
                let data = seg.data();
                if data.is_empty() {
                    None
                } else {
                    Some((data[0].0, data[data.len() - 1].0))
                }
            })
            .collect();

        Self {
            key_ranges,
            segment_count: segments.len(),
        }
    }

    /// Find segment index for key
    pub fn find_segment(&self, key: u64) -> Option<usize> {
        // Binary search over key ranges
        self.key_ranges
            .binary_search_by(|(min, max)| {
                if key < *min {
                    std::cmp::Ordering::Greater
                } else if key > *max {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
    }

    pub fn segment_count(&self) -> usize {
        self.segment_count
    }
}

/// Multi-segment RMI index
///
/// Complete RMI with routing + segments.
/// Learned index for cache predictor: Recursive Model Index (RMI).
pub struct RmiIndex {
    segments: Vec<RmiSegment>,
    router: SegmentRouter,
}

impl RmiIndex {
    /// Build RMI from sorted data
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
        let segments: Vec<RmiSegment> = data
            .chunks(target_segment_size)
            .map(|chunk| RmiSegment::new(chunk.to_vec()))
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
        assert!(model.error_bound() <= 8);
    }

    #[test]
    fn test_rmi_segment() {
        let data = vec![(1, 10), (2, 20), (3, 30), (4, 40)];
        let segment = RmiSegment::new(data);

        assert_eq!(segment.bounded_search(2), Some(20));
        assert_eq!(segment.bounded_search(99), None);
    }

    #[test]
    fn test_rmi_index() {
        let data: Vec<(u64, u64)> = (0..1000).map(|i| (i, i * 10)).collect();
        let index = RmiIndex::build(data, 100);

        assert_eq!(index.get(42), Some(420));
        assert_eq!(index.get(999), Some(9990));
        assert_eq!(index.get(1000), None);
    }
}
