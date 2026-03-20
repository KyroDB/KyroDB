/// 128-bit integrity digest for normalized vector payloads.
///
/// This is a deterministic MurmurHash3 x64_128-style digest over the exact IEEE-754
/// bit pattern of each `f32` lane. It is used to detect accidental drift/corruption
/// between canonical cold-tier storage, the hot-tier mirror, and L1a.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct VectorIntegrityDigest {
    pub hi: u64,
    pub lo: u64,
}

impl VectorIntegrityDigest {
    pub const ZERO: Self = Self { hi: 0, lo: 0 };

    pub const fn new(hi: u64, lo: u64) -> Self {
        Self { hi, lo }
    }
}

/// Canonical identity of a stored vector payload.
///
/// `version` advances on in-place replacement of a live canonical record for a
/// `doc_id`. Delete + reinsert starts a fresh coherence epoch for that ID.
/// `digest` tracks the exact normalized embedding bytes for corruption detection
/// across cold-tier canonical storage, the hot-tier mirror, and L1a.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VectorCoherenceToken {
    pub version: u64,
    pub digest: VectorIntegrityDigest,
}

impl VectorCoherenceToken {
    pub const fn new(version: u64, digest: VectorIntegrityDigest) -> Self {
        Self { version, digest }
    }

    pub fn for_embedding(version: u64, embedding: &[f32]) -> Self {
        Self::new(version, digest_embedding(embedding))
    }
}

/// Stable in-memory integrity digest for an embedding payload.
///
/// This is not persisted on disk; canonical storage recomputes it on recovery.
/// The goal is runtime coherence verification, not adversarial tamper-proofing.
pub fn digest_embedding(embedding: &[f32]) -> VectorIntegrityDigest {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    fn fmix64(mut k: u64) -> u64 {
        k ^= k >> 33;
        k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
        k ^= k >> 33;
        k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        k ^= k >> 33;
        k
    }

    #[inline]
    fn mix_k1(mut k1: u64) -> u64 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1.wrapping_mul(C2)
    }

    #[inline]
    fn mix_k2(mut k2: u64) -> u64 {
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2.wrapping_mul(C1)
    }

    let mut h1 = 0u64;
    let mut h2 = 0u64;

    let mut chunks = embedding.chunks_exact(4);
    for chunk in &mut chunks {
        let k1 = u64::from(chunk[0].to_bits()) | (u64::from(chunk[1].to_bits()) << 32);
        let k2 = u64::from(chunk[2].to_bits()) | (u64::from(chunk[3].to_bits()) << 32);

        h1 ^= mix_k1(k1);
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        h2 ^= mix_k2(k2);
        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let tail = chunks.remainder();
    if !tail.is_empty() {
        let k1 = match tail.len() {
            1 => u64::from(tail[0].to_bits()),
            2 => u64::from(tail[0].to_bits()) | (u64::from(tail[1].to_bits()) << 32),
            _ => u64::from(tail[0].to_bits()) | (u64::from(tail[1].to_bits()) << 32),
        };
        h1 ^= mix_k1(k1);

        if tail.len() == 3 {
            h2 ^= mix_k2(u64::from(tail[2].to_bits()));
        }
    }

    let len_bytes = (embedding.len() as u64).wrapping_mul(std::mem::size_of::<f32>() as u64);
    h1 ^= len_bytes;
    h2 ^= len_bytes;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    VectorIntegrityDigest::new(h1, h2)
}

#[inline]
pub fn embedding_matches_token(embedding: &[f32], token: VectorCoherenceToken) -> bool {
    digest_embedding(embedding) == token.digest
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_changes_with_embedding_payload() {
        assert_ne!(digest_embedding(&[1.0, 0.0]), digest_embedding(&[0.0, 1.0]));
    }

    #[test]
    fn digest_is_stable_for_same_payload() {
        let digest = digest_embedding(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(digest, digest_embedding(&[1.0, 2.0, 3.0, 4.0, 5.0]));
    }

    #[test]
    fn token_validation_detects_payload_changes() {
        let token = VectorCoherenceToken::for_embedding(7, &[1.0, 2.0, 3.0]);
        assert!(embedding_matches_token(&[1.0, 2.0, 3.0], token));
        assert!(!embedding_matches_token(&[1.0, 2.0, 4.0], token));
    }
}
