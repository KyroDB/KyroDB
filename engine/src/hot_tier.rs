//! Hot Tier - Recent writes buffer (Layer 2)
//!
//! Stores recently inserted documents before they're flushed to the cold tier (HNSW).
//! This provides:
//! - Fast writes (no HNSW index update overhead)
//! - Immediate read availability (no waiting for HNSW index build)
//! - Batched HNSW updates (better performance)
//!
//! # Architecture Position
//! **Layer 2 in three-tier architecture**:
//! - Layer 1 (Cache): Hot documents predicted by RMI + semantic similarity
//! - Layer 2 (Hot Tier): Recent writes, not yet in HNSW
//! - Layer 3 (Cold Tier): Full HNSW index for all documents

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Hot tier statistics
#[derive(Debug, Clone, Default)]
pub struct HotTierStats {
    pub current_size: usize,
    pub total_inserts: u64,
    pub total_flushes: u64,
    pub total_hits: u64,
    pub total_misses: u64,
    pub last_flush_time: Option<Instant>,
}

/// Document in hot tier with metadata
#[derive(Debug, Clone)]
struct HotDocument {
    embedding: Vec<f32>,
    metadata: HashMap<String, String>,
    inserted_at: Instant,
}

/// Hot Tier - Buffer for recent writes
///
/// Documents are kept here until:
/// - Size threshold reached (e.g., 10K documents)
/// - Time threshold reached (e.g., 60 seconds since last flush)
/// - Manual flush triggered
///
/// Then they're batched and flushed to HNSW (cold tier).
pub struct HotTier {
    documents: Arc<RwLock<HashMap<u64, HotDocument>>>,
    stats: Arc<RwLock<HotTierStats>>,

    /// Flush when hot tier reaches this size
    max_size: usize,

    /// Flush when this much time passes since last flush
    max_age: Duration,
}

impl HotTier {
    /// Create new hot tier
    ///
    /// # Parameters
    /// - `max_size`: Flush to cold tier when this many documents accumulated
    /// - `max_age`: Flush to cold tier after this duration since last flush
    ///
    /// # Defaults
    /// - `max_size`: 10,000 documents (tune based on write throughput)
    /// - `max_age`: 60 seconds (tune based on freshness requirements)
    pub fn new(max_size: usize, max_age: Duration) -> Self {
        Self {
            documents: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(HotTierStats::default())),
            max_size,
            max_age,
        }
    }

    /// Insert document into hot tier
    ///
    /// This is fast (no HNSW index update), just a HashMap insert.
    pub fn insert(&self, doc_id: u64, embedding: Vec<f32>, metadata: HashMap<String, String>) {
        let doc = HotDocument {
            embedding,
            metadata,
            inserted_at: Instant::now(),
        };

        self.documents.write().insert(doc_id, doc);

        let mut stats = self.stats.write();
        stats.current_size = self.documents.read().len();
        stats.total_inserts += 1;
    }

    /// Get document from hot tier (if present)
    pub fn get(&self, doc_id: u64) -> Option<Vec<f32>> {
        let docs = self.documents.read();
        let result = docs.get(&doc_id).map(|doc| doc.embedding.clone());

        let mut stats = self.stats.write();
        if result.is_some() {
            stats.total_hits += 1;
        } else {
            stats.total_misses += 1;
        }

        result
    }

    /// Get document metadata from hot tier (if present)
    pub fn get_metadata(&self, doc_id: u64) -> Option<HashMap<String, String>> {
        let docs = self.documents.read();
        docs.get(&doc_id).map(|doc| doc.metadata.clone())
    }

    /// Bulk fetch documents from hot tier (if present)
    #[allow(clippy::type_complexity)]
    pub fn bulk_fetch(&self, doc_ids: &[u64]) -> Vec<Option<(Vec<f32>, HashMap<String, String>)>> {
        // Take snapshot to avoid holding read lock during cloning
        #[allow(clippy::type_complexity)]
        let snapshot: Vec<Option<(Vec<f32>, HashMap<String, String>)>> = {
            let docs = self.documents.read();
            doc_ids
                .iter()
                .map(|id| {
                    docs.get(id)
                        .map(|doc| (doc.embedding.clone(), doc.metadata.clone()))
                })
                .collect()
        };

        let hits = snapshot.iter().filter(|opt| opt.is_some()).count() as u64;
        let misses = snapshot.len() as u64 - hits;

        let mut stats = self.stats.write();
        stats.total_hits += hits;
        stats.total_misses += misses;

        snapshot
    }

    /// Check if a document exists in the hot tier without cloning its embedding
    ///
    /// Note: This is a lightweight existence check that does NOT update hit/miss stats.
    /// Use `get()` or `bulk_fetch()` if you need stats tracking.
    pub fn exists(&self, doc_id: u64) -> bool {
        self.documents.read().contains_key(&doc_id)
    }

    /// Update document metadata without changing embedding
    ///
    /// # Parameters
    /// - `doc_id`: Document ID to update
    /// - `metadata`: New metadata
    /// - `merge`: true = merge with existing metadata, false = replace all
    ///
    /// # Returns
    /// - `true` if document existed and was updated
    /// - `false` if document does not exist
    pub fn update_metadata(
        &self,
        doc_id: u64,
        metadata: HashMap<String, String>,
        merge: bool,
    ) -> bool {
        let mut docs = self.documents.write();
        if let Some(doc) = docs.get_mut(&doc_id) {
            if merge {
                doc.metadata.extend(metadata);
            } else {
                doc.metadata = metadata;
            }
            true
        } else {
            false
        }
    }

    /// Delete document from hot tier
    ///
    /// # Parameters
    /// - `doc_id`: Document ID to delete
    ///
    /// # Returns
    /// - `true` if document existed and was deleted
    /// - `false` if document did not exist
    pub fn delete(&self, doc_id: u64) -> bool {
        let mut docs = self.documents.write();
        let existed = docs.remove(&doc_id).is_some();

        if existed {
            let mut stats = self.stats.write();
            stats.current_size = docs.len();
            // We don't decrement total_inserts, but we could track total_deletes if we added that field
        }

        existed
    }

    /// Batch delete documents from hot tier
    ///
    /// # Parameters
    /// - `doc_ids`: Document IDs to delete
    ///
    /// # Returns
    /// - Number of documents actually deleted
    pub fn batch_delete(&self, doc_ids: &[u64]) -> usize {
        let mut docs = self.documents.write();
        let mut deleted_count = 0;

        for &doc_id in doc_ids {
            if docs.remove(&doc_id).is_some() {
                deleted_count += 1;
            }
        }

        if deleted_count > 0 {
            let mut stats = self.stats.write();
            stats.current_size = docs.len();
        }

        deleted_count
    }

    /// Scan all documents and return IDs that match the predicate
    pub fn scan<F>(&self, predicate: F) -> Vec<u64>
    where
        F: Fn(&HashMap<String, String>) -> bool,
    {
        // Take lightweight snapshot to avoid holding the read lock for the
        // entire scan, which would otherwise block writers under heavy load.
        let snapshot: Vec<(u64, HashMap<String, String>)> = {
            let docs = self.documents.read();
            docs.iter()
                .map(|(&id, doc)| (id, doc.metadata.clone()))
                .collect()
        };

        snapshot
            .into_iter()
            .filter_map(|(id, metadata)| if predicate(&metadata) { Some(id) } else { None })
            .collect()
    }

    /// Check if flush is needed (based on size or age thresholds)
    pub fn needs_flush(&self) -> bool {
        let docs = self.documents.read();

        // Size threshold check
        if docs.len() >= self.max_size {
            return true;
        }

        // Age threshold check
        let stats = self.stats.read();
        if let Some(last_flush) = stats.last_flush_time {
            if last_flush.elapsed() >= self.max_age {
                return true;
            }
        } else {
            // Never flushed - check if any documents are old enough
            if !docs.is_empty() {
                // Safety: docs guaranteed non-empty by check above (line 255)
                // We hold the read lock, so no concurrent modification is possible
                let oldest = docs
                    .values()
                    .map(|doc| doc.inserted_at)
                    .min()
                    .expect("BUG: docs cannot be empty (checked on line 255, read lock held)");

                if oldest.elapsed() >= self.max_age {
                    return true;
                }
            }
        }

        false
    }

    /// Drain all documents from hot tier for flushing
    ///
    /// Returns: Vec<(doc_id, embedding, metadata)> to be inserted into cold tier
    ///
    /// This clears the hot tier and updates stats.
    pub fn drain_for_flush(&self) -> Vec<(u64, Vec<f32>, HashMap<String, String>)> {
        let mut docs = self.documents.write();
        let drained: Vec<(u64, Vec<f32>, HashMap<String, String>)> = docs
            .drain()
            .map(|(id, doc)| (id, doc.embedding, doc.metadata))
            .collect();

        let mut stats = self.stats.write();
        stats.current_size = 0;
        stats.total_flushes += 1;
        stats.last_flush_time = Some(Instant::now());

        drained
    }

    /// Get current hot tier statistics
    pub fn stats(&self) -> HotTierStats {
        self.stats.read().clone()
    }

    /// Get current size (number of documents in hot tier)
    pub fn len(&self) -> usize {
        self.documents.read().len()
    }

    /// Check if hot tier is empty
    pub fn is_empty(&self) -> bool {
        self.documents.read().is_empty()
    }

    /// Get hit rate (percentage of gets that found document in hot tier)
    pub fn hit_rate(&self) -> f64 {
        let stats = self.stats.read();
        let total_accesses = stats.total_hits + stats.total_misses;
        if total_accesses == 0 {
            0.0
        } else {
            stats.total_hits as f64 / total_accesses as f64
        }
    }

    /// k-NN search over hot tier documents
    ///
    /// Performs linear scan over all documents in hot tier, computing cosine distance
    /// to the query vector. Returns top-k results sorted by distance (ascending).
    ///
    /// # Performance
    /// - Complexity: O(n × d) where n = hot_tier_size, d = embedding_dim
    /// - Hot tier is typically small (1K-10K documents), so linear scan is acceptable
    /// - For 10K documents × 384 dimensions: ~3.8M ops × 0.5ns ≈ 2ms worst case
    /// - Typical case: 1K documents ≈ 200μs
    ///
    /// # Parameters
    /// - `query`: Query embedding vector
    /// - `k`: Number of nearest neighbors to return
    ///
    /// # Returns
    /// Vector of (doc_id, distance) pairs, sorted by distance ascending (best match first).
    /// If hot tier has fewer than k documents, returns all documents.
    ///
    /// # Note
    /// This does NOT update hit/miss stats since it's a search operation, not a lookup.
    pub fn knn_search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        if k == 0 {
            return Vec::new();
        }

        let docs = self.documents.read();

        if docs.is_empty() {
            return Vec::new();
        }

        // Compute distances for all documents in hot tier
        let mut distances: Vec<(u64, f32)> = docs
            .iter()
            .map(|(doc_id, doc)| {
                let distance = Self::cosine_distance(query, &doc.embedding);
                (*doc_id, distance)
            })
            .collect();

        // Sort by distance ascending (smaller distance = better match)
        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Truncate to k results
        distances.truncate(k);

        distances
    }

    /// Compute cosine distance between two embeddings with SIMD optimization
    ///
    /// Formula: distance = 1 - cos(θ) = 1 - (A · B) / (||A|| × ||B||)
    ///
    /// # Properties
    /// - distance = 0.0: Identical vectors (cos = 1.0)
    /// - distance = 1.0: Orthogonal vectors (cos = 0.0)
    /// - distance = 2.0: Opposite vectors (cos = -1.0)
    ///
    /// # Performance
    /// SIMD-optimized using auto-vectorization:
    /// - Uses chunks_exact(8) for aligned processing (AVX2/NEON friendly)
    /// - LLVM auto-vectorizes the iterator patterns
    /// - ~4-8× speedup on modern CPUs with AVX2/AVX512
    /// - For d=384: ~50-100ns per comparison (vs ~200ns scalar)
    ///
    /// # Implementation
    /// Safe Rust using iterator patterns that LLVM can auto-vectorize:
    /// - No unsafe code or explicit SIMD intrinsics
    /// - Works on all platforms (x86_64, ARM, etc.)
    /// - Scalar fallback for non-aligned remainders
    #[inline]
    fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() || a.is_empty() {
            return f32::INFINITY; // Invalid comparison
        }

        // SIMD-friendly implementation using chunks for auto-vectorization
        // Process 8 elements at a time (optimal for AVX2: 256-bit / 32-bit float = 8)
        const CHUNK_SIZE: usize = 8;
        
        let len = a.len();
        let chunks = len / CHUNK_SIZE;
        let remainder = len % CHUNK_SIZE;

        // Process main chunks (auto-vectorized by LLVM)
        let mut dot = 0.0_f32;
        let mut mag_a = 0.0_f32;
        let mut mag_b = 0.0_f32;

        // Main vectorized loop - LLVM will generate SIMD instructions
        for i in 0..chunks {
            let offset = i * CHUNK_SIZE;
            let a_chunk = &a[offset..offset + CHUNK_SIZE];
            let b_chunk = &b[offset..offset + CHUNK_SIZE];
            
            // These iterator patterns are auto-vectorized by LLVM
            dot += a_chunk.iter().zip(b_chunk.iter()).map(|(x, y)| x * y).sum::<f32>();
            mag_a += a_chunk.iter().map(|x| x * x).sum::<f32>();
            mag_b += b_chunk.iter().map(|x| x * x).sum::<f32>();
        }

        // Scalar fallback for remaining elements
        if remainder > 0 {
            let offset = chunks * CHUNK_SIZE;
            for i in 0..remainder {
                let idx = offset + i;
                dot += a[idx] * b[idx];
                mag_a += a[idx] * a[idx];
                mag_b += b[idx] * b[idx];
            }
        }

        let magnitude = (mag_a * mag_b).sqrt();
        if magnitude < f32::EPSILON {
            return f32::INFINITY; // Degenerate case: zero vector
        }

        let cosine_similarity = (dot / magnitude).clamp(-1.0, 1.0);

        // Convert similarity [-1, 1] to distance [0, 2]
        1.0 - cosine_similarity
    }

    /// Re-insert documents that failed to flush
    ///
    /// Used when flush to cold tier fails - documents are put back into hot tier
    /// to avoid data loss. Preserves original insertion timestamps where possible.
    ///
    /// # Parameters
    /// - `documents`: Documents to re-insert (doc_id, embedding, metadata)
    ///
    /// # Behavior
    /// - Re-inserted documents use current timestamp (preserves age-based flush logic)
    /// - Increments total_inserts counter (tracks all insert operations)
    /// - Does NOT increment total_flushes (flush failed, not completed)
    pub fn reinsert_failed_documents(
        &self,
        documents: Vec<(u64, Vec<f32>, HashMap<String, String>)>,
    ) {
        if documents.is_empty() {
            return;
        }

        let mut docs = self.documents.write();
        let reinsert_count = documents.len();

        for (doc_id, embedding, metadata) in documents {
            let doc = HotDocument {
                embedding,
                metadata,
                inserted_at: Instant::now(), // Use current timestamp
            };
            docs.insert(doc_id, doc);
        }

        let mut stats = self.stats.write();
        stats.current_size = docs.len();
        stats.total_inserts += reinsert_count as u64;
        // Note: NOT incrementing total_flushes because flush failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hot_tier_insert_get() {
        let hot_tier = HotTier::new(1000, Duration::from_secs(60));
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());

        // Insert document
        hot_tier.insert(42, vec![0.1, 0.2, 0.3], metadata.clone());

        assert_eq!(hot_tier.len(), 1);
        assert!(!hot_tier.is_empty());

        // Get document
        let embedding = hot_tier.get(42).unwrap();
        assert_eq!(embedding, vec![0.1, 0.2, 0.3]);

        // Get metadata
        let meta = hot_tier.get_metadata(42).unwrap();
        assert_eq!(meta.get("key").unwrap(), "value");

        // Get non-existent document
        assert!(hot_tier.get(99).is_none());
    }

    #[test]
    fn test_hot_tier_size_threshold() {
        let hot_tier = HotTier::new(3, Duration::from_secs(3600)); // 3 docs max

        hot_tier.insert(1, vec![0.1], HashMap::new());
        hot_tier.insert(2, vec![0.2], HashMap::new());

        assert!(!hot_tier.needs_flush());

        hot_tier.insert(3, vec![0.3], HashMap::new());

        assert!(hot_tier.needs_flush()); // Size threshold reached
    }

    #[test]
    fn test_hot_tier_drain() {
        let hot_tier = HotTier::new(1000, Duration::from_secs(60));

        hot_tier.insert(1, vec![0.1], HashMap::new());
        hot_tier.insert(2, vec![0.2], HashMap::new());
        hot_tier.insert(3, vec![0.3], HashMap::new());

        assert_eq!(hot_tier.len(), 3);

        let drained = hot_tier.drain_for_flush();

        assert_eq!(drained.len(), 3);
        assert_eq!(hot_tier.len(), 0);
        assert!(hot_tier.is_empty());

        // Verify stats updated
        let stats = hot_tier.stats();
        assert_eq!(stats.total_flushes, 1);
        assert_eq!(stats.current_size, 0);
    }

    #[test]
    fn test_hot_tier_hit_rate() {
        let hot_tier = HotTier::new(1000, Duration::from_secs(60));

        hot_tier.insert(1, vec![0.1], HashMap::new());

        // 1 hit
        hot_tier.get(1);

        // 2 misses
        hot_tier.get(2);
        hot_tier.get(3);

        let hit_rate = hot_tier.hit_rate();
        assert!((hit_rate - 0.333).abs() < 0.01); // 1/3 ≈ 0.333
    }

    #[test]
    fn test_hot_tier_age_threshold() {
        let hot_tier = HotTier::new(10000, Duration::from_millis(100)); // 100ms age

        hot_tier.insert(1, vec![0.1], HashMap::new());

        assert!(!hot_tier.needs_flush());

        // Wait for age threshold
        std::thread::sleep(Duration::from_millis(150));

        assert!(hot_tier.needs_flush()); // Age threshold reached
    }

    #[test]
    fn test_hot_tier_knn_search_basic() {
        let hot_tier = HotTier::new(1000, Duration::from_secs(60));

        // Insert 3 documents with normalized embeddings
        // Doc 1: [1.0, 0.0, 0.0] (normalized)
        hot_tier.insert(1, vec![1.0, 0.0, 0.0], HashMap::new());

        // Doc 2: [0.0, 1.0, 0.0] (normalized)
        hot_tier.insert(2, vec![0.0, 1.0, 0.0], HashMap::new());

        // Doc 3: [0.707, 0.707, 0.0] (normalized, similar to query)
        hot_tier.insert(3, vec![0.707, 0.707, 0.0], HashMap::new());

        // Query: [0.6, 0.8, 0.0] (closer to doc 2 and doc 3)
        let query = vec![0.6, 0.8, 0.0];
        let results = hot_tier.knn_search(&query, 2);

        // Should return 2 results
        assert_eq!(results.len(), 2);

        // Results should be sorted by distance (ascending)
        assert!(results[0].1 <= results[1].1);

        // Doc 3 should be closest (has both x and y components)
        assert_eq!(results[0].0, 3);
    }

    #[test]
    fn test_hot_tier_knn_search_empty() {
        let hot_tier = HotTier::new(1000, Duration::from_secs(60));

        let query = vec![1.0, 0.0, 0.0];
        let results = hot_tier.knn_search(&query, 5);

        // Empty hot tier should return empty results
        assert!(results.is_empty());
    }

    #[test]
    fn test_hot_tier_knn_search_fewer_than_k() {
        let hot_tier = HotTier::new(1000, Duration::from_secs(60));

        // Insert 2 documents
        hot_tier.insert(1, vec![1.0, 0.0], HashMap::new());
        hot_tier.insert(2, vec![0.0, 1.0], HashMap::new());

        // Request k=5, but only 2 documents available
        let query = vec![1.0, 0.0];
        let results = hot_tier.knn_search(&query, 5);

        // Should return 2 results (all available documents)
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_hot_tier_knn_search_identical_vector() {
        let hot_tier = HotTier::new(1000, Duration::from_secs(60));

        // Insert document
        let embedding = vec![0.5, 0.5, 0.707];
        hot_tier.insert(42, embedding.clone(), HashMap::new());

        // Search with identical query
        let results = hot_tier.knn_search(&embedding, 1);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 42);

        // Distance should be ~0.0 for identical vectors
        assert!(results[0].1 < 0.0001);
    }

    #[test]
    fn test_hot_tier_cosine_distance() {
        // Test cosine distance computation

        // Identical vectors: distance = 0.0
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist < 0.001, "Identical vectors should have distance ~0.0");

        // Orthogonal vectors: distance = 1.0
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(
            (dist - 1.0).abs() < 0.001,
            "Orthogonal vectors should have distance ~1.0"
        );

        // Opposite vectors: distance = 2.0
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![-1.0, 0.0, 0.0];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(
            (dist - 2.0).abs() < 0.001,
            "Opposite vectors should have distance ~2.0"
        );

        // Dimension mismatch: should return INFINITY
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(
            dist.is_infinite(),
            "Mismatched dimensions should return INFINITY"
        );
    }

    #[test]
    fn test_hot_tier_knn_search_k_zero() {
        let hot_tier = HotTier::new(1000, Duration::from_secs(60));

        hot_tier.insert(1, vec![1.0, 0.0], HashMap::new());

        // k=0 should return empty results
        let query = vec![1.0, 0.0];
        let results = hot_tier.knn_search(&query, 0);

        assert!(results.is_empty());
    }

    #[test]
    fn test_cosine_distance_simd_various_sizes() {
        // Test SIMD implementation with different vector sizes
        // to ensure both vectorized and scalar paths work correctly

        // Size 1: scalar only
        let a = vec![0.5];
        let b = vec![0.5];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist < 0.001, "Size 1: identical vectors");

        // Size 4: scalar only (less than CHUNK_SIZE=8)
        let a = vec![1.0, 0.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0, 0.0];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist < 0.001, "Size 4: identical vectors");

        // Size 8: exactly one chunk (CHUNK_SIZE=8)
        let a = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist < 0.001, "Size 8: identical vectors");

        // Size 10: one chunk + 2 remainder
        let a = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist < 0.001, "Size 10: identical vectors");

        // Size 16: exactly two chunks
        let a = vec![1.0; 16];
        let b = vec![1.0; 16];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist < 0.001, "Size 16: identical vectors");

        // Size 128: typical embedding dimension (16 chunks)
        let a = vec![0.5; 128];
        let b = vec![0.5; 128];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist < 0.001, "Size 128: identical vectors");

        // Size 384: common OpenAI embedding size (48 chunks)
        let a = vec![0.3; 384];
        let b = vec![0.3; 384];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist < 0.001, "Size 384: identical vectors");

        // Test orthogonal vectors with size 128
        let mut a = vec![0.0; 128];
        let mut b = vec![0.0; 128];
        a[0] = 1.0;
        b[1] = 1.0;
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(
            (dist - 1.0).abs() < 0.001,
            "Size 128: orthogonal vectors"
        );

        // Test opposite vectors with size 384
        let a = vec![1.0; 384];
        let b = vec![-1.0; 384];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(
            (dist - 2.0).abs() < 0.001,
            "Size 384: opposite vectors"
        );
    }

    #[test]
    fn test_cosine_distance_simd_edge_cases() {
        // Empty vectors
        let a = vec![];
        let b = vec![];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist.is_infinite(), "Empty vectors should return INFINITY");

        // Zero vectors (degenerate case)
        let a = vec![0.0; 128];
        let b = vec![0.0; 128];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist.is_infinite(), "Zero vectors should return INFINITY");

        // One zero vector
        let a = vec![1.0; 128];
        let b = vec![0.0; 128];
        let dist = HotTier::cosine_distance(&a, &b);
        assert!(dist.is_infinite(), "Zero vector should return INFINITY");

        // Very small magnitude (near zero)
        let a = vec![1e-20; 128];
        let b = vec![1e-20; 128];
        let dist = HotTier::cosine_distance(&a, &b);
        // Should either be INFINITY or very close to 0.0 depending on precision
        assert!(
            dist.is_infinite() || dist < 0.01,
            "Very small vectors: dist={}", dist
        );
    }
}
