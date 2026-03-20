//! Vector Cache Storage Layer
//!
//! Vector cache storage: In-memory cache with LRU eviction policy
//!
//! Provides fast in-memory storage for frequently accessed vectors.
//! Supports both LRU baseline and Hybrid Semantic Cache admission policies.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::coherence::VectorCoherenceToken;
use crate::lru_index::LruIndex;

/// Cached vector with metadata.
///
/// `coherence` is the canonical cold-tier token observed when this L1a entry
/// was created. Reads must re-check it before serving the cached embedding.
#[derive(Clone, Debug)]
pub struct CachedVector {
    pub doc_id: u64,
    pub embedding: Vec<f32>,
    pub coherence: VectorCoherenceToken,
    pub distance: f32,
    pub cached_at: Instant,
}

/// Vector cache with LRU eviction
///
/// # Thread Safety
/// Uses a single RwLock protecting an insertion-ordered map.
/// Recency updates are O(1) and atomic with value access.
///
/// # Performance
/// - Get miss: O(1) with shared read lock
/// - Get hit: O(1) recency promotion (upgradable read → write)
/// - Insert/eviction: O(1)
pub struct VectorCache {
    /// Combined cache state: insertion-ordered map (front=oldest, back=newest)
    state: Arc<RwLock<CacheState>>,

    /// Maximum capacity
    capacity: usize,

    /// Cache statistics (separate lock to avoid contention on hot path)
    stats: Arc<RwLock<CacheStats>>,
}

/// Internal cache state protected by single RwLock
struct CacheState {
    cache: HashMap<u64, CachedVector>,
    lru: LruIndex<u64>,
}

/// Cache statistics
struct CacheStats {
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl VectorCache {
    /// Create new vector cache with specified capacity
    ///
    /// # Parameters
    /// - `capacity`: Maximum number of vectors to cache
    ///
    /// # Memory Usage
    /// Approximately `capacity * (embedding_dim * 4 + 32)` bytes
    /// For 10k vectors with 128-dim embeddings: ~5MB
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Arc::new(RwLock::new(CacheState {
                cache: HashMap::with_capacity(capacity),
                lru: LruIndex::with_capacity(capacity),
            })),
            capacity,
            stats: Arc::new(RwLock::new(CacheStats {
                hits: 0,
                misses: 0,
                evictions: 0,
            })),
        }
    }

    /// Get cached vector by doc_id
    ///
    /// Returns `Some(CachedVector)` if in cache, `None` otherwise.
    /// Updates LRU queue on hit (moves to back).
    ///
    /// # Performance
    /// O(1) lookup. Miss path stays on shared read lock; hit path upgrades lock
    /// to move entry to the back of the LRU order.
    pub fn get(&self, doc_id: u64) -> Option<CachedVector> {
        let state = self.state.upgradable_read();
        let Some(cached) = state.cache.get(&doc_id).cloned() else {
            self.stats.write().misses += 1;
            return None;
        };

        let mut state = parking_lot::RwLockUpgradableReadGuard::upgrade(state);
        let _ = state.lru.touch(doc_id);
        self.stats.write().hits += 1;
        Some(cached)
    }

    /// Side-effect-free cache probe.
    ///
    /// Returns a cloned cached vector without mutating hit/miss counters or LRU order.
    pub fn peek(&self, doc_id: u64) -> Option<CachedVector> {
        self.state.read().cache.get(&doc_id).cloned()
    }

    /// Insert vector into cache
    ///
    /// If cache is full, evicts least recently used entry.
    /// Returns the evicted doc_id if an eviction occurred (for feedback tracking).
    ///
    /// # Performance
    /// O(1) update/insert and O(1) LRU eviction.
    pub fn insert(&self, cached_vector: CachedVector) -> Option<u64> {
        let doc_id = cached_vector.doc_id;
        let mut state = self.state.write();

        // Update existing entry and promote to MRU.
        if let std::collections::hash_map::Entry::Occupied(mut entry) = state.cache.entry(doc_id) {
            entry.insert(cached_vector);
            let _ = state.lru.touch(doc_id);
            return None;
        }

        // Evict if at capacity
        let evicted_doc_id = if state.cache.len() >= self.capacity {
            if let Some(evict_id) = state.lru.pop_lru() {
                state.cache.remove(&evict_id);
                self.stats.write().evictions += 1;
                Some(evict_id)
            } else {
                None
            }
        } else {
            None
        };

        // Insert new entry
        state.cache.insert(doc_id, cached_vector);
        state.lru.insert_new(doc_id);

        evicted_doc_id
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStatsSnapshot {
        let state = self.state.read();
        let stats = self.stats.read();

        let total_requests = stats.hits + stats.misses;
        let hit_rate = if total_requests > 0 {
            stats.hits as f64 / total_requests as f64
        } else {
            0.0
        };

        CacheStatsSnapshot {
            hits: stats.hits,
            misses: stats.misses,
            evictions: stats.evictions,
            size: state.cache.len(),
            capacity: self.capacity,
            hit_rate,
        }
    }

    /// Clear cache (useful for testing)
    pub fn clear(&self) {
        let mut state = self.state.write();
        state.cache.clear();
        state.lru.clear();

        let mut stats = self.stats.write();
        stats.hits = 0;
        stats.misses = 0;
        stats.evictions = 0;
    }

    /// Get current size
    pub fn len(&self) -> usize {
        self.state.read().cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.state.read().cache.is_empty()
    }

    /// Remove cached vector by doc_id (if present)
    pub fn remove(&self, doc_id: u64) -> bool {
        let mut state = self.state.write();
        if state.cache.remove(&doc_id).is_some() {
            let _ = state.lru.remove(doc_id);
            true
        } else {
            false
        }
    }

    /// Get maximum capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Cache statistics snapshot
#[derive(Debug, Clone, Copy)]
pub struct CacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub size: usize,
    pub capacity: usize,
    pub hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_vector(doc_id: u64, dim: usize) -> CachedVector {
        let embedding = vec![0.5; dim];
        CachedVector {
            doc_id,
            coherence: VectorCoherenceToken::for_embedding(0, &embedding),
            embedding,
            distance: 0.1,
            cached_at: Instant::now(),
        }
    }

    #[test]
    fn test_cache_basic_operations() {
        let cache = VectorCache::new(10);

        // Cache miss
        assert!(cache.get(1).is_none());
        assert_eq!(cache.stats().misses, 1);

        // Insert and hit
        cache.insert(create_test_vector(1, 128));
        assert!(cache.get(1).is_some());
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_peek_has_no_side_effects() {
        let cache = VectorCache::new(10);
        cache.insert(create_test_vector(1, 128));

        let before = cache.stats();
        assert!(cache.peek(1).is_some());
        assert!(cache.peek(999).is_none());
        let after = cache.stats();

        assert_eq!(after.hits, before.hits);
        assert_eq!(after.misses, before.misses);
        assert_eq!(after.evictions, before.evictions);
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = VectorCache::new(3);

        // Fill cache
        cache.insert(create_test_vector(1, 128));
        cache.insert(create_test_vector(2, 128));
        cache.insert(create_test_vector(3, 128));
        assert_eq!(cache.len(), 3);

        // Insert 4th vector, should evict doc 1 (oldest)
        cache.insert(create_test_vector(4, 128));
        assert_eq!(cache.len(), 3);
        assert!(cache.get(1).is_none()); // Evicted
        assert!(cache.get(2).is_some()); // Still present
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn test_cache_lru_update_on_access() {
        let cache = VectorCache::new(3);

        // Fill cache
        cache.insert(create_test_vector(1, 128));
        cache.insert(create_test_vector(2, 128));
        cache.insert(create_test_vector(3, 128));

        // Access doc 1 (moves to back of LRU queue)
        cache.get(1);

        // Insert doc 4, should evict doc 2 (now oldest)
        cache.insert(create_test_vector(4, 128));
        assert!(cache.get(1).is_some()); // Still present (accessed recently)
        assert!(cache.get(2).is_none()); // Evicted (oldest)
        assert!(cache.get(3).is_some()); // Still present
        assert!(cache.get(4).is_some()); // Newly inserted
    }

    #[test]
    fn test_cache_hit_rate_calculation() {
        let cache = VectorCache::new(10);

        cache.insert(create_test_vector(1, 128));
        cache.insert(create_test_vector(2, 128));

        // 2 hits, 2 misses
        cache.get(1); // Hit
        cache.get(2); // Hit
        cache.get(3); // Miss
        cache.get(4); // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 2);
        assert!((stats.hit_rate - 0.5).abs() < 0.01); // 50% hit rate
    }

    #[test]
    fn test_cache_clear() {
        let cache = VectorCache::new(10);

        cache.insert(create_test_vector(1, 128));
        cache.insert(create_test_vector(2, 128));
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.stats().hits, 0);
        assert_eq!(cache.stats().misses, 0);
    }

    #[test]
    fn test_cache_update_existing() {
        let cache = VectorCache::new(10);

        // Insert doc 1
        cache.insert(create_test_vector(1, 128));
        let old_embedding = cache.get(1).unwrap().embedding.clone();

        // Update doc 1 (different embedding)
        let mut updated = create_test_vector(1, 128);
        updated.embedding = vec![0.9; 128];
        cache.insert(updated);

        // Should have new embedding
        let new_cached = cache.get(1).unwrap();
        assert_ne!(old_embedding, new_cached.embedding);
        assert_eq!(cache.len(), 1); // Still only 1 entry
    }

    #[test]
    fn test_cache_concurrent_access() {
        use std::thread;

        let cache = Arc::new(VectorCache::new(100));
        let mut handles = vec![];

        // Spawn 10 threads inserting and reading concurrently
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                for j in 0..10 {
                    let doc_id = (i * 10 + j) as u64;
                    cache_clone.insert(create_test_vector(doc_id, 128));
                    cache_clone.get(doc_id);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All 100 vectors should be cached
        assert_eq!(cache.len(), 100);
    }

    #[test]
    fn test_cache_memory_efficiency() {
        let cache = VectorCache::new(1000);

        // Insert 1000 vectors
        for i in 0..1000 {
            cache.insert(create_test_vector(i, 128));
        }

        // Should not exceed capacity
        assert_eq!(cache.len(), 1000);

        // Insert 1 more, should evict oldest
        cache.insert(create_test_vector(1000, 128));
        assert_eq!(cache.len(), 1000);
        assert!(cache.get(0).is_none()); // First vector evicted
    }
}
