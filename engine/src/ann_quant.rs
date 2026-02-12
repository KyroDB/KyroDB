//! Quantized ANN distance helpers for single-graph backend.
//!
//! Supports SQ8 payloads with fp32 rerank in higher layers.

use crate::config::{AnnSearchMode, DistanceMetric};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuantizationKind {
    Sq8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuantizationAppendError {
    DimensionMismatch { expected: usize, found: usize },
    NonFiniteInput,
}

impl fmt::Display for QuantizationAppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuantizationAppendError::DimensionMismatch { expected, found } => {
                write!(
                    f,
                    "quantized append dimension mismatch: expected {} found {}",
                    expected, found
                )
            }
            QuantizationAppendError::NonFiniteInput => {
                write!(f, "quantized append rejected non-finite input")
            }
        }
    }
}

#[inline]
pub(crate) fn quantization_kind_for_mode(mode: AnnSearchMode) -> Option<QuantizationKind> {
    match mode {
        AnnSearchMode::Fp32Strict => None,
        AnnSearchMode::Sq8Rerank => Some(QuantizationKind::Sq8),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QuantizedStorage {
    dimension: usize,
    scales: Vec<f32>,
    norm_sq: Vec<f32>,
    inv_norm: Vec<f32>,
    sq8_values: Vec<i8>,
}

impl QuantizedStorage {
    pub(crate) fn new(_kind: QuantizationKind, dimension: usize) -> Self {
        Self {
            dimension,
            scales: Vec::new(),
            norm_sq: Vec::new(),
            inv_norm: Vec::new(),
            sq8_values: Vec::new(),
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.scales.is_empty()
    }

    pub(crate) fn reserve_for_additional(&mut self, additional: usize) {
        if additional == 0 {
            return;
        }
        self.scales.reserve(additional);
        self.norm_sq.reserve(additional);
        self.inv_norm.reserve(additional);
        self.sq8_values
            .reserve(additional.saturating_mul(self.dimension));
    }

    pub(crate) fn append(&mut self, vector: &[f32]) -> Result<(), QuantizationAppendError> {
        if vector.len() != self.dimension {
            return Err(QuantizationAppendError::DimensionMismatch {
                expected: self.dimension,
                found: vector.len(),
            });
        }
        if vector.iter().any(|v| !v.is_finite()) {
            return Err(QuantizationAppendError::NonFiniteInput);
        }

        self.append_sq8(vector);
        Ok(())
    }

    pub(crate) fn estimated_memory_bytes(&self) -> usize {
        self.scales.len() * std::mem::size_of::<f32>()
            + self.norm_sq.len() * std::mem::size_of::<f32>()
            + self.inv_norm.len() * std::mem::size_of::<f32>()
            + self.sq8_values.len() * std::mem::size_of::<i8>()
    }

    #[inline]
    fn max_abs(vector: &[f32]) -> f32 {
        vector.iter().fold(0.0f32, |acc, &v| v.abs().max(acc))
    }

    fn append_sq8(&mut self, vector: &[f32]) {
        let max_abs = Self::max_abs(vector);
        let scale = if max_abs > f32::EPSILON {
            max_abs / 127.0
        } else {
            1.0
        };
        let inv = scale.recip();
        let mut sum_q_sq = 0.0f32;
        for &value in vector {
            let q = (value * inv).round().clamp(-127.0, 127.0) as i8;
            self.sq8_values.push(q);
            let qf = q as f32;
            sum_q_sq += qf * qf;
        }
        let norm_sq = scale * scale * sum_q_sq;
        let norm = norm_sq.sqrt();
        let inv_norm = if norm > 0.0 && norm.is_finite() {
            norm.recip()
        } else {
            0.0
        };
        self.scales.push(scale);
        self.norm_sq.push(norm_sq);
        self.inv_norm.push(inv_norm);
    }

    #[inline]
    fn dot_sq8(&self, query: &[f32], dense_id: u32) -> Option<f32> {
        if query.len() != self.dimension {
            return None;
        }
        let id = dense_id as usize;
        let start = id.checked_mul(self.dimension)?;
        let end = start.checked_add(self.dimension)?;
        let values = self.sq8_values.get(start..end)?;
        let mut sum = 0.0f32;
        for (qv, &query_v) in values.iter().zip(query.iter()) {
            sum += (*qv as f32) * query_v;
        }
        self.scales.get(id).map(|scale| scale * sum)
    }

    pub(crate) fn distance_approx(
        &self,
        metric: DistanceMetric,
        query: &[f32],
        query_norm_sq: f32,
        query_inv_norm: f32,
        dense_id: u32,
    ) -> Option<f32> {
        let dot = self.dot_sq8(query, dense_id)?;
        match metric {
            DistanceMetric::Euclidean => {
                let vec_norm_sq = *self.norm_sq.get(dense_id as usize)?;
                Some((vec_norm_sq + query_norm_sq - 2.0 * dot).max(0.0))
            }
            DistanceMetric::Cosine | DistanceMetric::InnerProduct => {
                let vec_inv_norm = *self.inv_norm.get(dense_id as usize)?;
                let vec_norm_sq = *self.norm_sq.get(dense_id as usize)?;
                let denom_sq = vec_norm_sq * query_norm_sq;
                if denom_sq > (f32::EPSILON * f32::EPSILON)
                    && vec_inv_norm > 0.0
                    && query_inv_norm > 0.0
                {
                    Some((1.0 - (dot * vec_inv_norm * query_inv_norm)).max(0.0))
                } else {
                    Some(0.0)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn l2(a: &[f32], b: &[f32]) -> f32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| {
                let d = x - y;
                d * d
            })
            .sum::<f32>()
    }

    #[test]
    fn sq8_distance_tracks_fp32_l2_ordering() {
        let vectors = vec![
            vec![0.1, -0.2, 0.3, 0.4],
            vec![0.5, 0.4, 0.3, 0.2],
            vec![-0.9, 0.2, -0.1, 0.7],
        ];
        let mut store = QuantizedStorage::new(QuantizationKind::Sq8, 4);
        for vector in &vectors {
            store.append(vector).expect("shape must match");
        }

        let query = vec![0.05, -0.15, 0.25, 0.35];
        let query_norm_sq = query.iter().map(|v| v * v).sum::<f32>();
        let query_inv_norm = query_norm_sq.sqrt().recip();
        let mut exact = Vec::new();
        let mut approx = Vec::new();
        for (idx, vector) in vectors.iter().enumerate() {
            exact.push((idx, l2(&query, vector)));
            approx.push((
                idx,
                store
                    .distance_approx(
                        DistanceMetric::Euclidean,
                        &query,
                        query_norm_sq,
                        query_inv_norm,
                        idx as u32,
                    )
                    .unwrap_or(f32::MAX),
            ));
        }
        exact.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        approx.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        assert_eq!(exact[0].0, approx[0].0);
    }

    #[test]
    fn append_rejects_dimension_mismatch() {
        let mut store = QuantizedStorage::new(QuantizationKind::Sq8, 4);
        let err = store
            .append(&[0.1, 0.2, 0.3])
            .expect_err("dimension mismatch must be rejected");
        assert!(matches!(
            err,
            QuantizationAppendError::DimensionMismatch {
                expected: 4,
                found: 3
            }
        ));
    }

    #[test]
    fn append_rejects_non_finite_input() {
        let mut store = QuantizedStorage::new(QuantizationKind::Sq8, 2);
        let err = store
            .append(&[f32::NAN, 0.1])
            .expect_err("NaN payload must be rejected");
        assert_eq!(err, QuantizationAppendError::NonFiniteInput);
    }

    #[test]
    fn cosine_distance_uses_norms_for_non_unit_vectors() {
        let mut store = QuantizedStorage::new(QuantizationKind::Sq8, 3);
        let vector = [2.0, 0.0, 0.0];
        store.append(&vector).expect("shape must match");

        let query = [1.0, 0.0, 0.0];
        let query_norm_sq = query.iter().map(|v| v * v).sum::<f32>();
        let query_inv_norm = query_norm_sq.sqrt().recip();
        let d = store
            .distance_approx(
                DistanceMetric::Cosine,
                &query,
                query_norm_sq,
                query_inv_norm,
                0,
            )
            .expect("distance must exist");
        assert!(d.is_finite());
        assert!(d <= 1e-4, "expected near-zero cosine distance, got {d}");
    }

    #[test]
    fn cosine_distance_preserves_ranking_for_small_nonzero_vector_norms() {
        // vec norm is below sqrt(EPSILON), query norm is larger.
        // Previous denominator-based logic still computes cosine because
        // sqrt(vec_norm_sq * query_norm_sq) > EPSILON.
        let mut store = QuantizedStorage::new(QuantizationKind::Sq8, 2);
        store.append(&[1e-4, 0.0]).expect("shape must match");
        store.append(&[-1e-4, 0.0]).expect("shape must match");

        let query = [2e-3, 0.0];
        let query_norm_sq = query.iter().map(|v| v * v).sum::<f32>();
        let query_norm = query_norm_sq.sqrt();
        let query_inv_norm = if query_norm > 0.0 && query_norm.is_finite() {
            query_norm.recip()
        } else {
            0.0
        };

        let aligned = store
            .distance_approx(
                DistanceMetric::Cosine,
                &query,
                query_norm_sq,
                query_inv_norm,
                0,
            )
            .expect("aligned distance should exist");
        let opposite = store
            .distance_approx(
                DistanceMetric::Cosine,
                &query,
                query_norm_sq,
                query_inv_norm,
                1,
            )
            .expect("opposite distance should exist");

        assert!(aligned.is_finite() && opposite.is_finite());
        assert!(
            aligned + 0.5 < opposite,
            "cosine ranking collapsed for small nonzero norms: aligned={aligned} opposite={opposite}"
        );
    }
}
