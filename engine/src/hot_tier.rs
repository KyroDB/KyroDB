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
    pub fn insert(&self, doc_id: u64, embedding: Vec<f32>) {
        let doc = HotDocument {
            embedding,
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
                let oldest = docs.values().map(|doc| doc.inserted_at).min().unwrap();

                if oldest.elapsed() >= self.max_age {
                    return true;
                }
            }
        }

        false
    }

    /// Drain all documents from hot tier for flushing
    ///
    /// Returns: Vec<(doc_id, embedding)> to be inserted into cold tier
    ///
    /// This clears the hot tier and updates stats.
    pub fn drain_for_flush(&self) -> Vec<(u64, Vec<f32>)> {
        let mut docs = self.documents.write();
        let drained: Vec<(u64, Vec<f32>)> =
            docs.drain().map(|(id, doc)| (id, doc.embedding)).collect();

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

    /// Re-insert documents that failed to flush
    ///
    /// Used when flush to cold tier fails - documents are put back into hot tier
    /// to avoid data loss. Preserves original insertion timestamps where possible.
    ///
    /// # Parameters
    /// - `documents`: Documents to re-insert (doc_id, embedding pairs)
    ///
    /// # Behavior
    /// - Re-inserted documents use current timestamp (preserves age-based flush logic)
    /// - Increments total_inserts counter (tracks all insert operations)
    /// - Does NOT increment total_flushes (flush failed, not completed)
    pub fn reinsert_failed_documents(&self, documents: Vec<(u64, Vec<f32>)>) {
        if documents.is_empty() {
            return;
        }

        let mut docs = self.documents.write();
        let reinsert_count = documents.len();

        for (doc_id, embedding) in documents {
            let doc = HotDocument {
                embedding,
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

        // Insert document
        hot_tier.insert(42, vec![0.1, 0.2, 0.3]);

        assert_eq!(hot_tier.len(), 1);
        assert!(!hot_tier.is_empty());

        // Get document
        let embedding = hot_tier.get(42).unwrap();
        assert_eq!(embedding, vec![0.1, 0.2, 0.3]);

        // Get non-existent document
        assert!(hot_tier.get(99).is_none());
    }

    #[test]
    fn test_hot_tier_size_threshold() {
        let hot_tier = HotTier::new(3, Duration::from_secs(3600)); // 3 docs max

        hot_tier.insert(1, vec![0.1]);
        hot_tier.insert(2, vec![0.2]);

        assert!(!hot_tier.needs_flush());

        hot_tier.insert(3, vec![0.3]);

        assert!(hot_tier.needs_flush()); // Size threshold reached
    }

    #[test]
    fn test_hot_tier_drain() {
        let hot_tier = HotTier::new(1000, Duration::from_secs(60));

        hot_tier.insert(1, vec![0.1]);
        hot_tier.insert(2, vec![0.2]);
        hot_tier.insert(3, vec![0.3]);

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

        hot_tier.insert(1, vec![0.1]);

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

        hot_tier.insert(1, vec![0.1]);

        assert!(!hot_tier.needs_flush());

        // Wait for age threshold
        std::thread::sleep(Duration::from_millis(150));

        assert!(hot_tier.needs_flush()); // Age threshold reached
    }
}
