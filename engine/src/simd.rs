//! SIMD-accelerated vector math with safe runtime dispatch.
//!
//! Design goals:
//! - Always safe to run: never executes unsupported instructions (no SIGILL).
//! - One binary works across a wide range of CPUs.
//! - Uses the best available ISA at runtime: AVX-512F(+FMA) → AVX2(+FMA) → SSE2 → scalar.
//! - Builds on non-x86 platforms: uses NEON on aarch64 when available; otherwise falls back to scalar.

/// Returns the dot product $\sum_i a_i b_i$.
///
/// # Panics
/// Panics if `a.len() != b.len()`.
#[inline]
pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "input slices must have the same length: got {} and {}",
        a.len(),
        b.len()
    );
    let len = a.len();
    if len == 0 {
        return 0.0;
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        // NEON (AdvSIMD) is ubiquitous on aarch64, but keep a defensive check.
        if std::arch::is_aarch64_feature_detected!("neon") {
            return dot_f32_neon(a, b, len);
        }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe {
        if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma") {
            return dot_f32_avx512(a, b, len);
        }
        // AVX2 does not strictly imply FMA; guard to avoid executing unsupported instructions.
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            return dot_f32_avx2(a, b, len);
        }
        // SSE2 is part of the x86_64 baseline, but keep a defensive check.
        if std::is_x86_feature_detected!("sse2") {
            return dot_f32_sse2(a, b, len);
        }
    }

    dot_f32_scalar(a, b, len)
}

/// Returns the L2 norm $\|v\|_2$.
#[inline]
pub fn l2_norm_f32(v: &[f32]) -> f32 {
    sum_squares_f32(v).sqrt()
}

/// Returns $\sum_i v_i^2$.
#[inline]
pub fn sum_squares_f32(v: &[f32]) -> f32 {
    if v.is_empty() {
        return 0.0;
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return sum_squares_f32_neon(v);
        }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe {
        if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma") {
            return sum_squares_f32_avx512(v);
        }
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            return sum_squares_f32_avx2(v);
        }
        if std::is_x86_feature_detected!("sse2") {
            return sum_squares_f32_sse2(v);
        }
    }

    sum_squares_f32_scalar(v)
}

/// Returns the Euclidean (L2) distance $\|a-b\|_2$.
///
/// # Panics
/// Panics if `a.len() != b.len()`.
#[inline]
pub fn l2_distance_f32(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "input slices must have the same length: got {} and {}",
        a.len(),
        b.len()
    );
    let len = a.len();
    if len == 0 {
        return 0.0;
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return l2_distance_f32_neon(a, b, len);
        }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe {
        if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma") {
            return l2_distance_f32_avx512(a, b, len);
        }
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            return l2_distance_f32_avx2(a, b, len);
        }
        if std::is_x86_feature_detected!("sse2") {
            return l2_distance_f32_sse2(a, b, len);
        }
    }

    l2_distance_f32_scalar(a, b, len)
}

/// Computes cosine similarity: $(a\cdot b)/(\|a\|\|b\|)$.
/// Returns 0.0 if either vector is empty or degenerate.
///
/// # Panics
/// Panics if `a.len() != b.len()`.
#[inline]
pub fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "input slices must have the same length: got {} and {}",
        a.len(),
        b.len()
    );
    let len = a.len();
    if len == 0 {
        return 0.0;
    }

    // Fast path: compute dot and both norms in one pass where possible.
    let (dot, norm_a_sq, norm_b_sq) = dot_and_norms_f32(a, b, len);
    if !dot.is_finite() {
        return 0.0;
    }
    if !norm_a_sq.is_finite() || !norm_b_sq.is_finite() {
        return 0.0;
    }
    if norm_a_sq <= 0.0 || norm_b_sq <= 0.0 {
        return 0.0;
    }
    let denom = (norm_a_sq.sqrt()) * (norm_b_sq.sqrt());
    if !denom.is_finite() || denom <= f32::EPSILON {
        return 0.0;
    }
    let result = dot / denom;
    if !result.is_finite() {
        return 0.0;
    }
    result.clamp(-1.0, 1.0)
}

#[inline]
fn dot_f32_scalar(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut dot = 0.0;
    for i in 0..len {
        dot += a[i] * b[i];
    }
    dot
}

#[inline]
fn sum_squares_f32_scalar(v: &[f32]) -> f32 {
    let mut sum = 0.0;
    for &x in v {
        sum += x * x;
    }
    sum
}

#[inline]
fn l2_distance_f32_scalar(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut sum = 0.0;
    for i in 0..len {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

#[inline]
fn dot_and_norms_f32(a: &[f32], b: &[f32], len: usize) -> (f32, f32, f32) {
    #[cfg(target_arch = "aarch64")]
    unsafe {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return dot_and_norms_f32_neon(a, b, len);
        }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe {
        if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma") {
            return dot_and_norms_f32_avx512(a, b, len);
        }
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            return dot_and_norms_f32_avx2(a, b, len);
        }
        if std::is_x86_feature_detected!("sse2") {
            return dot_and_norms_f32_sse2(a, b, len);
        }
    }

    let mut dot = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;
    for i in 0..len {
        let va = a[i];
        let vb = b[i];
        dot += va * vb;
        norm_a += va * va;
        norm_b += vb * vb;
    }
    (dot, norm_a, norm_b)
}

// ===== x86_64 SIMD implementations =====

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
unsafe fn dot_f32_avx2(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();
    let chunks = len / 8;
    let chunks4 = chunks / 4;
    for i in 0..chunks4 {
        let off = i * 32;
        let va = _mm256_loadu_ps(a.as_ptr().add(off));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off));
        acc0 = _mm256_fmadd_ps(va, vb, acc0);

        let va = _mm256_loadu_ps(a.as_ptr().add(off + 8));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off + 8));
        acc1 = _mm256_fmadd_ps(va, vb, acc1);

        let va = _mm256_loadu_ps(a.as_ptr().add(off + 16));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off + 16));
        acc2 = _mm256_fmadd_ps(va, vb, acc2);

        let va = _mm256_loadu_ps(a.as_ptr().add(off + 24));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off + 24));
        acc3 = _mm256_fmadd_ps(va, vb, acc3);
    }
    for i in (chunks4 * 4)..chunks {
        let off = i * 8;
        let va = _mm256_loadu_ps(a.as_ptr().add(off));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off));
        acc0 = _mm256_fmadd_ps(va, vb, acc0);
    }
    let acc = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    let mut tmp = [0.0f32; 8];
    _mm256_storeu_ps(tmp.as_mut_ptr(), acc);
    let mut sum: f32 = tmp.iter().sum();
    for i in (chunks * 8)..len {
        sum += a[i] * b[i];
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[target_feature(enable = "fma")]
unsafe fn dot_f32_avx512(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut acc0 = _mm512_setzero_ps();
    let mut acc1 = _mm512_setzero_ps();
    let mut acc2 = _mm512_setzero_ps();
    let mut acc3 = _mm512_setzero_ps();
    let chunks = len / 16;
    let chunks4 = chunks / 4;
    for i in 0..chunks4 {
        let off = i * 64;
        let va = _mm512_loadu_ps(a.as_ptr().add(off) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off) as *const _);
        acc0 = _mm512_fmadd_ps(va, vb, acc0);

        let va = _mm512_loadu_ps(a.as_ptr().add(off + 16) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off + 16) as *const _);
        acc1 = _mm512_fmadd_ps(va, vb, acc1);

        let va = _mm512_loadu_ps(a.as_ptr().add(off + 32) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off + 32) as *const _);
        acc2 = _mm512_fmadd_ps(va, vb, acc2);

        let va = _mm512_loadu_ps(a.as_ptr().add(off + 48) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off + 48) as *const _);
        acc3 = _mm512_fmadd_ps(va, vb, acc3);
    }
    for i in (chunks4 * 4)..chunks {
        let off = i * 16;
        let va = _mm512_loadu_ps(a.as_ptr().add(off) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off) as *const _);
        acc0 = _mm512_fmadd_ps(va, vb, acc0);
    }
    let acc = _mm512_add_ps(_mm512_add_ps(acc0, acc1), _mm512_add_ps(acc2, acc3));
    let mut tmp = [0.0f32; 16];
    _mm512_storeu_ps(tmp.as_mut_ptr() as *mut _, acc);
    let mut sum: f32 = tmp.iter().sum();
    for i in (chunks * 16)..len {
        sum += a[i] * b[i];
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn dot_f32_sse2(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut acc = _mm_setzero_ps();
    let chunks = len / 4;
    for i in 0..chunks {
        let off = i * 4;
        let va = _mm_loadu_ps(a.as_ptr().add(off));
        let vb = _mm_loadu_ps(b.as_ptr().add(off));
        acc = _mm_add_ps(acc, _mm_mul_ps(va, vb));
    }
    let mut tmp = [0.0f32; 4];
    _mm_storeu_ps(tmp.as_mut_ptr(), acc);
    let mut sum: f32 = tmp.iter().sum();
    for i in (chunks * 4)..len {
        sum += a[i] * b[i];
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
unsafe fn sum_squares_f32_avx2(v: &[f32]) -> f32 {
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();
    let chunks = v.len() / 8;
    let chunks4 = chunks / 4;
    for i in 0..chunks4 {
        let off = i * 32;
        let x = _mm256_loadu_ps(v.as_ptr().add(off));
        acc0 = _mm256_fmadd_ps(x, x, acc0);

        let x = _mm256_loadu_ps(v.as_ptr().add(off + 8));
        acc1 = _mm256_fmadd_ps(x, x, acc1);

        let x = _mm256_loadu_ps(v.as_ptr().add(off + 16));
        acc2 = _mm256_fmadd_ps(x, x, acc2);

        let x = _mm256_loadu_ps(v.as_ptr().add(off + 24));
        acc3 = _mm256_fmadd_ps(x, x, acc3);
    }
    for i in (chunks4 * 4)..chunks {
        let off = i * 8;
        let x = _mm256_loadu_ps(v.as_ptr().add(off));
        acc0 = _mm256_fmadd_ps(x, x, acc0);
    }
    let acc = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    let mut tmp = [0.0f32; 8];
    _mm256_storeu_ps(tmp.as_mut_ptr(), acc);
    let mut sum: f32 = tmp.iter().sum();
    for i in (chunks * 8)..v.len() {
        let x = v[i];
        sum += x * x;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[target_feature(enable = "fma")]
unsafe fn sum_squares_f32_avx512(v: &[f32]) -> f32 {
    let mut acc0 = _mm512_setzero_ps();
    let mut acc1 = _mm512_setzero_ps();
    let mut acc2 = _mm512_setzero_ps();
    let mut acc3 = _mm512_setzero_ps();
    let chunks = v.len() / 16;
    let chunks4 = chunks / 4;
    for i in 0..chunks4 {
        let off = i * 64;
        let x = _mm512_loadu_ps(v.as_ptr().add(off) as *const _);
        acc0 = _mm512_fmadd_ps(x, x, acc0);

        let x = _mm512_loadu_ps(v.as_ptr().add(off + 16) as *const _);
        acc1 = _mm512_fmadd_ps(x, x, acc1);

        let x = _mm512_loadu_ps(v.as_ptr().add(off + 32) as *const _);
        acc2 = _mm512_fmadd_ps(x, x, acc2);

        let x = _mm512_loadu_ps(v.as_ptr().add(off + 48) as *const _);
        acc3 = _mm512_fmadd_ps(x, x, acc3);
    }
    for i in (chunks4 * 4)..chunks {
        let off = i * 16;
        let x = _mm512_loadu_ps(v.as_ptr().add(off) as *const _);
        acc0 = _mm512_fmadd_ps(x, x, acc0);
    }
    let acc = _mm512_add_ps(_mm512_add_ps(acc0, acc1), _mm512_add_ps(acc2, acc3));
    let mut tmp = [0.0f32; 16];
    _mm512_storeu_ps(tmp.as_mut_ptr() as *mut _, acc);
    let mut sum: f32 = tmp.iter().sum();
    for i in (chunks * 16)..v.len() {
        let x = v[i];
        sum += x * x;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn sum_squares_f32_sse2(v: &[f32]) -> f32 {
    let mut acc = _mm_setzero_ps();
    let chunks = v.len() / 4;
    for i in 0..chunks {
        let off = i * 4;
        let x = _mm_loadu_ps(v.as_ptr().add(off));
        acc = _mm_add_ps(acc, _mm_mul_ps(x, x));
    }
    let mut tmp = [0.0f32; 4];
    _mm_storeu_ps(tmp.as_mut_ptr(), acc);
    let mut sum: f32 = tmp.iter().sum();
    for i in (chunks * 4)..v.len() {
        let x = v[i];
        sum += x * x;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
unsafe fn l2_distance_f32_avx2(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();
    let chunks = len / 8;
    let chunks4 = chunks / 4;
    for i in 0..chunks4 {
        let off = i * 32;
        let va = _mm256_loadu_ps(a.as_ptr().add(off));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off));
        let d = _mm256_sub_ps(va, vb);
        acc0 = _mm256_fmadd_ps(d, d, acc0);

        let va = _mm256_loadu_ps(a.as_ptr().add(off + 8));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off + 8));
        let d = _mm256_sub_ps(va, vb);
        acc1 = _mm256_fmadd_ps(d, d, acc1);

        let va = _mm256_loadu_ps(a.as_ptr().add(off + 16));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off + 16));
        let d = _mm256_sub_ps(va, vb);
        acc2 = _mm256_fmadd_ps(d, d, acc2);

        let va = _mm256_loadu_ps(a.as_ptr().add(off + 24));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off + 24));
        let d = _mm256_sub_ps(va, vb);
        acc3 = _mm256_fmadd_ps(d, d, acc3);
    }
    for i in (chunks4 * 4)..chunks {
        let off = i * 8;
        let va = _mm256_loadu_ps(a.as_ptr().add(off));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off));
        let d = _mm256_sub_ps(va, vb);
        acc0 = _mm256_fmadd_ps(d, d, acc0);
    }
    let acc = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    let mut tmp = [0.0f32; 8];
    _mm256_storeu_ps(tmp.as_mut_ptr(), acc);
    let mut sum: f32 = tmp.iter().sum();
    for i in (chunks * 8)..len {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[target_feature(enable = "fma")]
unsafe fn l2_distance_f32_avx512(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut acc0 = _mm512_setzero_ps();
    let mut acc1 = _mm512_setzero_ps();
    let mut acc2 = _mm512_setzero_ps();
    let mut acc3 = _mm512_setzero_ps();
    let chunks = len / 16;
    let chunks4 = chunks / 4;
    for i in 0..chunks4 {
        let off = i * 64;
        let va = _mm512_loadu_ps(a.as_ptr().add(off) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off) as *const _);
        let d = _mm512_sub_ps(va, vb);
        acc0 = _mm512_fmadd_ps(d, d, acc0);

        let va = _mm512_loadu_ps(a.as_ptr().add(off + 16) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off + 16) as *const _);
        let d = _mm512_sub_ps(va, vb);
        acc1 = _mm512_fmadd_ps(d, d, acc1);

        let va = _mm512_loadu_ps(a.as_ptr().add(off + 32) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off + 32) as *const _);
        let d = _mm512_sub_ps(va, vb);
        acc2 = _mm512_fmadd_ps(d, d, acc2);

        let va = _mm512_loadu_ps(a.as_ptr().add(off + 48) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off + 48) as *const _);
        let d = _mm512_sub_ps(va, vb);
        acc3 = _mm512_fmadd_ps(d, d, acc3);
    }
    for i in (chunks4 * 4)..chunks {
        let off = i * 16;
        let va = _mm512_loadu_ps(a.as_ptr().add(off) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off) as *const _);
        let d = _mm512_sub_ps(va, vb);
        acc0 = _mm512_fmadd_ps(d, d, acc0);
    }
    let acc = _mm512_add_ps(_mm512_add_ps(acc0, acc1), _mm512_add_ps(acc2, acc3));
    let mut tmp = [0.0f32; 16];
    _mm512_storeu_ps(tmp.as_mut_ptr() as *mut _, acc);
    let mut sum: f32 = tmp.iter().sum();
    for i in (chunks * 16)..len {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn l2_distance_f32_sse2(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut acc = _mm_setzero_ps();
    let chunks = len / 4;
    for i in 0..chunks {
        let off = i * 4;
        let va = _mm_loadu_ps(a.as_ptr().add(off));
        let vb = _mm_loadu_ps(b.as_ptr().add(off));
        let d = _mm_sub_ps(va, vb);
        acc = _mm_add_ps(acc, _mm_mul_ps(d, d));
    }
    let mut tmp = [0.0f32; 4];
    _mm_storeu_ps(tmp.as_mut_ptr(), acc);
    let mut sum: f32 = tmp.iter().sum();
    for i in (chunks * 4)..len {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
unsafe fn dot_and_norms_f32_avx2(a: &[f32], b: &[f32], len: usize) -> (f32, f32, f32) {
    let mut dot = _mm256_setzero_ps();
    let mut na = _mm256_setzero_ps();
    let mut nb = _mm256_setzero_ps();
    let chunks = len / 8;
    for i in 0..chunks {
        let off = i * 8;
        let va = _mm256_loadu_ps(a.as_ptr().add(off));
        let vb = _mm256_loadu_ps(b.as_ptr().add(off));
        dot = _mm256_fmadd_ps(va, vb, dot);
        na = _mm256_fmadd_ps(va, va, na);
        nb = _mm256_fmadd_ps(vb, vb, nb);
    }
    let mut dot_tmp = [0.0f32; 8];
    let mut na_tmp = [0.0f32; 8];
    let mut nb_tmp = [0.0f32; 8];
    _mm256_storeu_ps(dot_tmp.as_mut_ptr(), dot);
    _mm256_storeu_ps(na_tmp.as_mut_ptr(), na);
    _mm256_storeu_ps(nb_tmp.as_mut_ptr(), nb);
    let mut dot_sum: f32 = dot_tmp.iter().sum();
    let mut na_sum: f32 = na_tmp.iter().sum();
    let mut nb_sum: f32 = nb_tmp.iter().sum();
    for i in (chunks * 8)..len {
        let va = a[i];
        let vb = b[i];
        dot_sum += va * vb;
        na_sum += va * va;
        nb_sum += vb * vb;
    }
    (dot_sum, na_sum, nb_sum)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[target_feature(enable = "fma")]
unsafe fn dot_and_norms_f32_avx512(a: &[f32], b: &[f32], len: usize) -> (f32, f32, f32) {
    let mut dot = _mm512_setzero_ps();
    let mut na = _mm512_setzero_ps();
    let mut nb = _mm512_setzero_ps();
    let chunks = len / 16;
    for i in 0..chunks {
        let off = i * 16;
        let va = _mm512_loadu_ps(a.as_ptr().add(off) as *const _);
        let vb = _mm512_loadu_ps(b.as_ptr().add(off) as *const _);
        dot = _mm512_fmadd_ps(va, vb, dot);
        na = _mm512_fmadd_ps(va, va, na);
        nb = _mm512_fmadd_ps(vb, vb, nb);
    }
    let mut dot_tmp = [0.0f32; 16];
    let mut na_tmp = [0.0f32; 16];
    let mut nb_tmp = [0.0f32; 16];
    _mm512_storeu_ps(dot_tmp.as_mut_ptr() as *mut _, dot);
    _mm512_storeu_ps(na_tmp.as_mut_ptr() as *mut _, na);
    _mm512_storeu_ps(nb_tmp.as_mut_ptr() as *mut _, nb);
    let mut dot_sum: f32 = dot_tmp.iter().sum();
    let mut na_sum: f32 = na_tmp.iter().sum();
    let mut nb_sum: f32 = nb_tmp.iter().sum();
    for i in (chunks * 16)..len {
        let va = a[i];
        let vb = b[i];
        dot_sum += va * vb;
        na_sum += va * va;
        nb_sum += vb * vb;
    }
    (dot_sum, na_sum, nb_sum)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn dot_and_norms_f32_sse2(a: &[f32], b: &[f32], len: usize) -> (f32, f32, f32) {
    let mut dot = _mm_setzero_ps();
    let mut na = _mm_setzero_ps();
    let mut nb = _mm_setzero_ps();
    let chunks = len / 4;
    for i in 0..chunks {
        let off = i * 4;
        let va = _mm_loadu_ps(a.as_ptr().add(off));
        let vb = _mm_loadu_ps(b.as_ptr().add(off));
        dot = _mm_add_ps(dot, _mm_mul_ps(va, vb));
        na = _mm_add_ps(na, _mm_mul_ps(va, va));
        nb = _mm_add_ps(nb, _mm_mul_ps(vb, vb));
    }
    let mut dot_tmp = [0.0f32; 4];
    let mut na_tmp = [0.0f32; 4];
    let mut nb_tmp = [0.0f32; 4];
    _mm_storeu_ps(dot_tmp.as_mut_ptr(), dot);
    _mm_storeu_ps(na_tmp.as_mut_ptr(), na);
    _mm_storeu_ps(nb_tmp.as_mut_ptr(), nb);
    let mut dot_sum: f32 = dot_tmp.iter().sum();
    let mut na_sum: f32 = na_tmp.iter().sum();
    let mut nb_sum: f32 = nb_tmp.iter().sum();
    for i in (chunks * 4)..len {
        let va = a[i];
        let vb = b[i];
        dot_sum += va * vb;
        na_sum += va * va;
        nb_sum += vb * vb;
    }
    (dot_sum, na_sum, nb_sum)
}

// ===== aarch64 NEON (AdvSIMD) implementations =====

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn hadd_f32x4(v: float32x4_t) -> f32 {
    // Sum lanes without relying on vaddvq_f32 (keeps compatibility conservative).
    let sum2: float32x2_t = vadd_f32(vget_low_f32(v), vget_high_f32(v));
    let sum1: float32x2_t = vpadd_f32(sum2, sum2);
    vget_lane_f32(sum1, 0)
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_f32_neon(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut acc: float32x4_t = vdupq_n_f32(0.0);
    let chunks = len / 4;
    for i in 0..chunks {
        let off = i * 4;
        let va = vld1q_f32(a.as_ptr().add(off));
        let vb = vld1q_f32(b.as_ptr().add(off));
        acc = vmlaq_f32(acc, va, vb);
    }
    let mut sum = hadd_f32x4(acc);
    for i in (chunks * 4)..len {
        sum += a[i] * b[i];
    }
    sum
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn sum_squares_f32_neon(v: &[f32]) -> f32 {
    let mut acc: float32x4_t = vdupq_n_f32(0.0);
    let chunks = v.len() / 4;
    for i in 0..chunks {
        let off = i * 4;
        let x = vld1q_f32(v.as_ptr().add(off));
        acc = vmlaq_f32(acc, x, x);
    }
    let mut sum = hadd_f32x4(acc);
    for &x in v.iter().skip(chunks * 4) {
        sum += x * x;
    }
    sum
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn l2_distance_f32_neon(a: &[f32], b: &[f32], len: usize) -> f32 {
    let mut acc: float32x4_t = vdupq_n_f32(0.0);
    let chunks = len / 4;
    for i in 0..chunks {
        let off = i * 4;
        let va = vld1q_f32(a.as_ptr().add(off));
        let vb = vld1q_f32(b.as_ptr().add(off));
        let d = vsubq_f32(va, vb);
        acc = vmlaq_f32(acc, d, d);
    }
    let mut sum = hadd_f32x4(acc);
    for i in (chunks * 4)..len {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_and_norms_f32_neon(a: &[f32], b: &[f32], len: usize) -> (f32, f32, f32) {
    let mut dot: float32x4_t = vdupq_n_f32(0.0);
    let mut na: float32x4_t = vdupq_n_f32(0.0);
    let mut nb: float32x4_t = vdupq_n_f32(0.0);
    let chunks = len / 4;
    for i in 0..chunks {
        let off = i * 4;
        let va = vld1q_f32(a.as_ptr().add(off));
        let vb = vld1q_f32(b.as_ptr().add(off));
        dot = vmlaq_f32(dot, va, vb);
        na = vmlaq_f32(na, va, va);
        nb = vmlaq_f32(nb, vb, vb);
    }
    let mut dot_sum = hadd_f32x4(dot);
    let mut na_sum = hadd_f32x4(na);
    let mut nb_sum = hadd_f32x4(nb);
    for i in (chunks * 4)..len {
        let va = a[i];
        let vb = b[i];
        dot_sum += va * vb;
        na_sum += va * va;
        nb_sum += vb * vb;
    }
    (dot_sum, na_sum, nb_sum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dot_matches_scalar() {
        let a = (0..513).map(|i| (i as f32).sin()).collect::<Vec<_>>();
        let b = (0..513).map(|i| (i as f32).cos()).collect::<Vec<_>>();
        let expected = dot_f32_scalar(&a, &b, a.len());
        let got = dot_f32(&a, &b);
        assert!((expected - got).abs() < 1e-2);
    }

    #[test]
    fn l2_distance_basic() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![1.0, 2.0, 3.0, 5.0];
        let d = l2_distance_f32(&a, &b);
        assert!((d - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_basic() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity_f32(&a, &b) - 1.0).abs() < 1e-6);

        let c = vec![0.0, 1.0, 0.0];
        assert!((cosine_similarity_f32(&a, &c) - 0.0).abs() < 1e-6);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_kernels_match_scalar() {
        if !std::arch::is_aarch64_feature_detected!("neon") {
            return;
        }

        let a = (0..1025).map(|i| (i as f32).sin()).collect::<Vec<_>>();
        let b = (0..1025).map(|i| (i as f32).cos()).collect::<Vec<_>>();

        let expected_dot = dot_f32_scalar(&a, &b, a.len());
        let expected_ss = sum_squares_f32_scalar(&a);
        let expected_l2 = l2_distance_f32_scalar(&a, &b, a.len());
        let expected_trip = {
            let mut dot = 0.0f32;
            let mut na = 0.0f32;
            let mut nb = 0.0f32;
            for i in 0..a.len() {
                let va = a[i];
                let vb = b[i];
                dot += va * vb;
                na += va * va;
                nb += vb * vb;
            }
            (dot, na, nb)
        };

        unsafe {
            let got_dot = dot_f32_neon(&a, &b, a.len());
            let got_ss = sum_squares_f32_neon(&a);
            let got_l2 = l2_distance_f32_neon(&a, &b, a.len());
            let got_trip = dot_and_norms_f32_neon(&a, &b, a.len());

            assert!((expected_dot - got_dot).abs() < 1e-2);
            assert!((expected_ss - got_ss).abs() < 1e-2);
            assert!((expected_l2 - got_l2).abs() < 1e-2);
            assert!((expected_trip.0 - got_trip.0).abs() < 1e-2);
            assert!((expected_trip.1 - got_trip.1).abs() < 1e-2);
            assert!((expected_trip.2 - got_trip.2).abs() < 1e-2);
        }
    }
}
