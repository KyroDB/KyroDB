//! Vector Cache Storage Layer
//!
//! Vector cache storage: In-memory cache with LRU eviction policy
//!
//! Provides fast in-memory storage for frequently accessed vectors.
//! Supports both LRU baseline and Hybrid Semantic Cache admission policies.

use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

/// Cached vector with metadata
#[derive(Clone, Debug)]
pub struct CachedVector {
    pub doc_id: u64,
    pub embedding: Vec<f32>,
    pub distance: f32,
    pub cached_at: Instant,
}

/// Vector cache with LRU eviction
///
/// # Thread Safety
/// Uses single RwLock protecting both HashMap and VecDeque to prevent TOCTOU races.
/// Multiple readers can access simultaneously, writes are exclusive.
///
/// # Concurrency Design
/// Previous design used separate locks for cache and LRU queue, which caused race:
/// - Thread A: reads cache (finds doc_id) → releases lock
/// - Thread B: evicts doc_id from both cache and queue
/// - Thread A: acquires LRU lock → tries to update queue for evicted doc_id → queue drift
///
/// Current design: Single lock protects both structures atomically.
///
/// # Performance
/// - Get: O(1) hashmap lookup + O(n) LRU queue update (where n = capacity)
/// - Insert: O(1) hashmap insert + O(1) LRU queue push
/// - Eviction: O(1) (evicts oldest when full)
///
/// Lock contention: Acceptable for cache sizes <100k entries. For larger caches,
/// consider sharded locks or lock-free skip list.
pub struct VectorCache {
    /// Combined cache state: (HashMap, LRU queue)
    /// Single lock prevents TOCTOU races between cache lookup and LRU update
    state: Arc<RwLock<CacheState>>,

    /// Maximum capacity
    capacity: usize,

    /// Cache statistics (separate lock to avoid contention on hot path)
    stats: Arc<RwLock<CacheStats>>,
}

/// Internal cache state protected by single RwLock
struct CacheState {
    /// Main cache storage (doc_id → cached vector)
    cache: HashMap<u64, CachedVector>,

    /// LRU queue (front = oldest, back = newest)
    lru_queue: VecDeque<u64>,
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
                lru_queue: VecDeque::with_capacity(capacity),
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
    /// O(1) hashmap lookup + O(n) LRU queue update (where n = capacity)
    /// Typically <100ns for small caches (<10k entries)
    ///
    /// # Atomicity
    /// Single lock acquisition ensures no TOCTOU race between cache lookup and LRU update.
    pub fn get(&self, doc_id: u64) -> Option<CachedVector> {
        let mut state = self.state.write();

        if let Some(cached) = state.cache.get(&doc_id).cloned() {
            // Cache hit - update LRU queue atomically
            if let Some(pos) = state.lru_queue.iter().position(|&id| id == doc_id) {
                state.lru_queue.remove(pos);
                state.lru_queue.push_back(doc_id);
            }

            // Update stats (separate lock to reduce contention)
            self.stats.write().hits += 1;

            Some(cached)
        } else {
            // Cache miss
            self.stats.write().misses += 1;
            None
        }
    }

    /// Insert vector into cache
    ///
    /// If cache is full, evicts least recently used entry.
    /// Returns the evicted doc_id if an eviction occurred (for feedback tracking).
    ///
    /// # Performance
    /// O(1) hashmap insert + O(1) LRU queue push
    ///
    /// # Atomicity
    /// Single lock ensures cache and LRU queue remain synchronized.
    pub fn insert(&self, cached_vector: CachedVector) -> Option<u64> {
        let doc_id = cached_vector.doc_id;
        let mut state = self.state.write();

        // Check if already in cache (update case) - use Entry API to avoid double lookup
        if let std::collections::hash_map::Entry::Occupied(mut e) = state.cache.entry(doc_id) {
            e.insert(cached_vector);

            // Update LRU position
            if let Some(pos) = state.lru_queue.iter().position(|&id| id == doc_id) {
                state.lru_queue.remove(pos);
                state.lru_queue.push_back(doc_id);
            } else {
                // Defensive: doc_id in cache but not in queue (should never happen with single lock)
                state.lru_queue.push_back(doc_id);
            }

            return None;  // No eviction on update
        }

        // Evict if at capacity
        let evicted_doc_id = if state.cache.len() >= self.capacity {
            if let Some(evict_id) = state.lru_queue.pop_front() {
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
        state.lru_queue.push_back(doc_id);

        evicted_doc_id
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStatsSnapshot {
        let stats = self.stats.read();
        let state = self.state.read();

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
        state.lru_queue.clear();

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
        CachedVector {
            doc_id,
            embedding: vec![0.5; dim],
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
