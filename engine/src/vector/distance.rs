//! SIMD-optimized distance computation for vector similarity search
//! 
//! This module provides high-performance distance functions using SIMD instructions
//! when available, with fallback to scalar implementations.

use crate::schema::DistanceMetric;
#[cfg(feature = "simd-optimized")]
use wide::f32x8;

/// Trait for distance computation between vectors
pub trait DistanceFunction: Send + Sync {
    /// Compute distance between two vectors
    fn distance(&self, a: &[f32], b: &[f32]) -> f32;
    
    /// Distance metric type
    fn metric(&self) -> DistanceMetric;
    
    /// Whether this implementation uses SIMD
    fn is_simd(&self) -> bool {
        false
    }
}

/// Factory for creating optimized distance functions
pub struct SIMDDistance;

impl SIMDDistance {
    /// Create the most optimized distance function for the given metric
    pub fn new(metric: DistanceMetric) -> Box<dyn DistanceFunction> {
        #[cfg(all(feature = "simd-optimized", any(target_arch = "x86", target_arch = "x86_64")))]
        {
            if is_x86_feature_detected!("avx2") {
                return Self::create_avx2(metric);
            }
        }
        
        // Fallback to scalar implementation
        Self::create_scalar(metric)
    }
    
    #[cfg(feature = "simd-optimized")]
    fn create_avx2(metric: DistanceMetric) -> Box<dyn DistanceFunction> {
        match metric {
            DistanceMetric::Euclidean => Box::new(EuclideanSIMD),
            DistanceMetric::Cosine => Box::new(CosineSIMD),
            DistanceMetric::DotProduct => Box::new(DotProductSIMD),
            DistanceMetric::Manhattan => Box::new(ManhattanSIMD),
        }
    }
    
    fn create_scalar(metric: DistanceMetric) -> Box<dyn DistanceFunction> {
        match metric {
            DistanceMetric::Euclidean => Box::new(EuclideanScalar),
            DistanceMetric::Cosine => Box::new(CosineScalar),
            DistanceMetric::DotProduct => Box::new(DotProductScalar),
            DistanceMetric::Manhattan => Box::new(ManhattanScalar),
        }
    }
}

// =============================================================================
// SIMD Implementations (AVX2)
// =============================================================================

#[cfg(feature = "simd-optimized")]
pub struct EuclideanSIMD;

#[cfg(feature = "simd-optimized")]
impl DistanceFunction for EuclideanSIMD {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        
        let mut sum = f32x8::ZERO;
        let chunks = a.len() / 8;
        
        // Process 8 elements at a time with AVX2
        for i in 0..chunks {
            let base = i * 8;
            let va = f32x8::new([
                a[base], a[base + 1], a[base + 2], a[base + 3],
                a[base + 4], a[base + 5], a[base + 6], a[base + 7],
            ]);
            let vb = f32x8::new([
                b[base], b[base + 1], b[base + 2], b[base + 3],
                b[base + 4], b[base + 5], b[base + 6], b[base + 7],
            ]);
            
            let diff = va - vb;
            sum += diff * diff;
        }
        
        // Handle remaining elements
        let mut scalar_sum = sum.reduce_add();
        for i in (chunks * 8)..a.len() {
            let diff = a[i] - b[i];
            scalar_sum += diff * diff;
        }
        
        scalar_sum.sqrt()
    }
    
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::Euclidean
    }
    
    fn is_simd(&self) -> bool {
        true
    }
}

#[cfg(feature = "simd-optimized")]
pub struct CosineSIMD;

#[cfg(feature = "simd-optimized")]
impl DistanceFunction for CosineSIMD {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        
        let mut dot_product = f32x8::ZERO;
        let mut norm_a = f32x8::ZERO;
        let mut norm_b = f32x8::ZERO;
        
        let chunks = a.len() / 8;
        
        // Process 8 elements at a time
        for i in 0..chunks {
            let base = i * 8;
            let va = f32x8::new([
                a[base], a[base + 1], a[base + 2], a[base + 3],
                a[base + 4], a[base + 5], a[base + 6], a[base + 7],
            ]);
            let vb = f32x8::new([
                b[base], b[base + 1], b[base + 2], b[base + 3],
                b[base + 4], b[base + 5], b[base + 6], b[base + 7],
            ]);
            
            dot_product += va * vb;
            norm_a += va * va;
            norm_b += vb * vb;
        }
        
        // Handle remaining elements
        let mut dot_sum = dot_product.reduce_add();
        let mut norm_a_sum = norm_a.reduce_add();
        let mut norm_b_sum = norm_b.reduce_add();
        
        for i in (chunks * 8)..a.len() {
            dot_sum += a[i] * b[i];
            norm_a_sum += a[i] * a[i];
            norm_b_sum += b[i] * b[i];
        }
        
        let norm_product = (norm_a_sum * norm_b_sum).sqrt();
        if norm_product == 0.0 {
            return 1.0; // Maximum distance for zero vectors
        }
        
        // Cosine distance = 1 - cosine_similarity
        1.0 - (dot_sum / norm_product)
    }
    
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::Cosine
    }
    
    fn is_simd(&self) -> bool {
        true
    }
}

#[cfg(feature = "simd-optimized")]
pub struct DotProductSIMD;

#[cfg(feature = "simd-optimized")]
impl DistanceFunction for DotProductSIMD {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        
        let mut dot_product = f32x8::ZERO;
        let chunks = a.len() / 8;
        
        for i in 0..chunks {
            let base = i * 8;
            let va = f32x8::new([
                a[base], a[base + 1], a[base + 2], a[base + 3],
                a[base + 4], a[base + 5], a[base + 6], a[base + 7],
            ]);
            let vb = f32x8::new([
                b[base], b[base + 1], b[base + 2], b[base + 3],
                b[base + 4], b[base + 5], b[base + 6], b[base + 7],
            ]);
            
            dot_product += va * vb;
        }
        
        let mut result = dot_product.reduce_add();
        for i in (chunks * 8)..a.len() {
            result += a[i] * b[i];
        }
        
        // Return negative dot product as distance (higher dot product = lower distance)
        -result
    }
    
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::DotProduct
    }
    
    fn is_simd(&self) -> bool {
        true
    }
}

#[cfg(feature = "simd-optimized")]
pub struct ManhattanSIMD;

#[cfg(feature = "simd-optimized")]
impl DistanceFunction for ManhattanSIMD {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        
        let mut sum = f32x8::ZERO;
        let chunks = a.len() / 8;
        
        for i in 0..chunks {
            let base = i * 8;
            let va = f32x8::new([
                a[base], a[base + 1], a[base + 2], a[base + 3],
                a[base + 4], a[base + 5], a[base + 6], a[base + 7],
            ]);
            let vb = f32x8::new([
                b[base], b[base + 1], b[base + 2], b[base + 3],
                b[base + 4], b[base + 5], b[base + 6], b[base + 7],
            ]);
            
            let diff = va - vb;
            sum += diff.abs();
        }
        
        let mut result = sum.reduce_add();
        for i in (chunks * 8)..a.len() {
            result += (a[i] - b[i]).abs();
        }
        
        result
    }
    
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::Manhattan
    }
    
    fn is_simd(&self) -> bool {
        true
    }
}

// =============================================================================
// Scalar Implementations (Fallback)
// =============================================================================

pub struct EuclideanScalar;

impl DistanceFunction for EuclideanScalar {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    }
    
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::Euclidean
    }
}

pub struct CosineScalar;

impl DistanceFunction for CosineScalar {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        
        let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        
        let norm_product = norm_a * norm_b;
        if norm_product == 0.0 {
            return 1.0; // Maximum distance for zero vectors
        }
        
        1.0 - (dot_product / norm_product)
    }
    
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::Cosine
    }
}

pub struct DotProductScalar;

impl DistanceFunction for DotProductScalar {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        
        let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        -dot_product // Negative because higher dot product = lower distance
    }
    
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::DotProduct
    }
}

pub struct ManhattanScalar;

impl DistanceFunction for ManhattanScalar {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).abs())
            .sum()
    }
    
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::Manhattan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_euclidean_distance() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![2.0, 3.0, 4.0, 5.0];
        
        let distance_fn = SIMDDistance::new(DistanceMetric::Euclidean);
        let distance = distance_fn.distance(&a, &b);
        
        // Expected: sqrt(4 * 1^2) = 2.0
        assert_relative_eq!(distance, 2.0, epsilon = 1e-6);
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        
        let distance_fn = SIMDDistance::new(DistanceMetric::Cosine);
        let distance = distance_fn.distance(&a, &b);
        
        // Expected: 1.0 (orthogonal vectors)
        assert_relative_eq!(distance, 1.0, epsilon = 1e-6);
    }

    #[test]
    fn test_dot_product_distance() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        
        let distance_fn = SIMDDistance::new(DistanceMetric::DotProduct);
        let distance = distance_fn.distance(&a, &b);
        
        // Expected: -(1*4 + 2*5 + 3*6) = -32.0
        assert_relative_eq!(distance, -32.0, epsilon = 1e-6);
    }

    #[test]
    fn test_manhattan_distance() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        
        let distance_fn = SIMDDistance::new(DistanceMetric::Manhattan);
        let distance = distance_fn.distance(&a, &b);
        
        // Expected: |1-4| + |2-5| + |3-6| = 3 + 3 + 3 = 9.0
        assert_relative_eq!(distance, 9.0, epsilon = 1e-6);
    }
}
