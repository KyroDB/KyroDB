//! Quantized ANN distance helpers for single-graph backend.
//!
//! Supports SQ8 and SQ4 payloads with fp32 rerank in higher layers.

use crate::config::{AnnSearchMode, DistanceMetric};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuantizationKind {
    Sq8,
    Sq4,
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
        AnnSearchMode::Sq4Rerank => Some(QuantizationKind::Sq4),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QuantizedStorage {
    kind: QuantizationKind,
    dimension: usize,
    scales: Vec<f32>,
    norm_sq: Vec<f32>,
    sq8_values: Vec<i8>,
    sq4_values: Vec<u8>,
}

impl QuantizedStorage {
    pub(crate) fn new(kind: QuantizationKind, dimension: usize) -> Self {
        Self {
            kind,
            dimension,
            scales: Vec::new(),
            norm_sq: Vec::new(),
            sq8_values: Vec::new(),
            sq4_values: Vec::new(),
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
        match self.kind {
            QuantizationKind::Sq8 => {
                self.sq8_values
                    .reserve(additional.saturating_mul(self.dimension));
            }
            QuantizationKind::Sq4 => {
                self.sq4_values
                    .reserve(additional.saturating_mul(self.dimension.div_ceil(2)));
            }
        }
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

        match self.kind {
            QuantizationKind::Sq8 => self.append_sq8(vector),
            QuantizationKind::Sq4 => self.append_sq4(vector),
        }
        Ok(())
    }

    pub(crate) fn estimated_memory_bytes(&self) -> usize {
        self.scales.len() * std::mem::size_of::<f32>()
            + self.norm_sq.len() * std::mem::size_of::<f32>()
            + self.sq8_values.len() * std::mem::size_of::<i8>()
            + self.sq4_values.len() * std::mem::size_of::<u8>()
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
        self.scales.push(scale);
        self.norm_sq.push(scale * scale * sum_q_sq);
    }

    fn append_sq4(&mut self, vector: &[f32]) {
        let max_abs = Self::max_abs(vector);
        let scale = if max_abs > f32::EPSILON {
            max_abs / 7.0
        } else {
            1.0
        };
        let inv = scale.recip();
        let mut sum_q_sq = 0.0f32;

        let mut idx = 0usize;
        while idx < vector.len() {
            let q0 = (vector[idx] * inv).round().clamp(-8.0, 7.0) as i8;
            sum_q_sq += (q0 as f32) * (q0 as f32);
            let low = Self::encode_i4(q0);
            idx += 1;

            let mut packed = low;
            if idx < vector.len() {
                let q1 = (vector[idx] * inv).round().clamp(-8.0, 7.0) as i8;
                sum_q_sq += (q1 as f32) * (q1 as f32);
                packed |= Self::encode_i4(q1) << 4;
                idx += 1;
            }
            self.sq4_values.push(packed);
        }

        self.scales.push(scale);
        self.norm_sq.push(scale * scale * sum_q_sq);
    }

    #[inline]
    fn encode_i4(v: i8) -> u8 {
        (v as i16 & 0x0F) as u8
    }

    #[inline]
    fn decode_i4(v: u8) -> i8 {
        if v & 0x08 != 0 {
            (v as i8) - 16
        } else {
            v as i8
        }
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

    #[inline]
    fn dot_sq4(&self, query: &[f32], dense_id: u32) -> Option<f32> {
        if query.len() != self.dimension {
            return None;
        }
        let id = dense_id as usize;
        let packed_width = self.dimension.div_ceil(2);
        let start = id.checked_mul(packed_width)?;
        let end = start.checked_add(packed_width)?;
        let packed = self.sq4_values.get(start..end)?;
        let mut sum = 0.0f32;

        for (pair_idx, &byte) in packed.iter().enumerate() {
            let base = pair_idx * 2;
            let low = Self::decode_i4(byte & 0x0F) as f32;
            sum += low * query[base];
            let next = base + 1;
            if next < self.dimension {
                let high = Self::decode_i4((byte >> 4) & 0x0F) as f32;
                sum += high * query[next];
            }
        }

        self.scales.get(id).map(|scale| scale * sum)
    }

    #[inline]
    fn dot_approx(&self, query: &[f32], dense_id: u32) -> Option<f32> {
        match self.kind {
            QuantizationKind::Sq8 => self.dot_sq8(query, dense_id),
            QuantizationKind::Sq4 => self.dot_sq4(query, dense_id),
        }
    }

    pub(crate) fn distance_approx(
        &self,
        metric: DistanceMetric,
        query: &[f32],
        query_norm_sq: f32,
        dense_id: u32,
    ) -> Option<f32> {
        let dot = self.dot_approx(query, dense_id)?;
        match metric {
            DistanceMetric::Euclidean => {
                let vec_norm_sq = *self.norm_sq.get(dense_id as usize)?;
                Some((vec_norm_sq + query_norm_sq - 2.0 * dot).max(0.0))
            }
            DistanceMetric::Cosine | DistanceMetric::InnerProduct => {
                let vec_norm_sq = *self.norm_sq.get(dense_id as usize)?;
                let denom = (vec_norm_sq * query_norm_sq).sqrt();
                if denom > f32::EPSILON {
                    Some((1.0 - (dot / denom)).max(0.0))
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
        let mut exact = Vec::new();
        let mut approx = Vec::new();
        for (idx, vector) in vectors.iter().enumerate() {
            exact.push((idx, l2(&query, vector)));
            approx.push((
                idx,
                store
                    .distance_approx(DistanceMetric::Euclidean, &query, query_norm_sq, idx as u32)
                    .unwrap_or(f32::MAX),
            ));
        }
        exact.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        approx.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        assert_eq!(exact[0].0, approx[0].0);
    }

    #[test]
    fn sq4_distance_returns_finite_values() {
        let vectors = vec![
            vec![0.3, -0.1, 0.7, -0.4, 0.2],
            vec![0.4, 0.5, -0.2, 0.1, -0.9],
        ];
        let mut store = QuantizedStorage::new(QuantizationKind::Sq4, 5);
        for vector in &vectors {
            store.append(vector).expect("shape must match");
        }
        let query = vec![0.1, -0.2, 0.5, -0.3, 0.4];
        let query_norm_sq = query.iter().map(|v| v * v).sum::<f32>();

        for idx in 0..vectors.len() {
            let d = store
                .distance_approx(DistanceMetric::Euclidean, &query, query_norm_sq, idx as u32)
                .unwrap_or(f32::MAX);
            assert!(d.is_finite());
            assert!(d >= 0.0);
        }
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
        let mut store = QuantizedStorage::new(QuantizationKind::Sq4, 2);
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
        let d = store
            .distance_approx(DistanceMetric::Cosine, &query, query_norm_sq, 0)
            .expect("distance must exist");
        assert!(d.is_finite());
        assert!(d <= 1e-4, "expected near-zero cosine distance, got {d}");
    }
}
