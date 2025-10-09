//! Vector Cache Storage Layer
//!
//! Vector cache storage: In-memory cache with LRU eviction policy
//!
//! Provides fast in-memory storage for frequently accessed vectors.
//! Supports both LRU baseline and learned cache admission policies.

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
/// Uses RwLock for concurrent access. Multiple readers can access simultaneously,
/// writes are exclusive.
///
/// # Performance
/// - Get: O(1) hashmap lookup
/// - Insert: O(1) hashmap insert + O(1) LRU update
/// - Eviction: O(1) (evicts oldest when full)
pub struct VectorCache {
    /// Main cache storage (doc_id → cached vector)
    cache: Arc<RwLock<HashMap<u64, CachedVector>>>,

    /// LRU queue (front = oldest, back = newest)
    lru_queue: Arc<RwLock<VecDeque<u64>>>,

    /// Maximum capacity
    capacity: usize,

    /// Cache statistics
    hits: Arc<RwLock<u64>>,
    misses: Arc<RwLock<u64>>,
    evictions: Arc<RwLock<u64>>,
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
            cache: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            lru_queue: Arc::new(RwLock::new(VecDeque::with_capacity(capacity))),
            capacity,
            hits: Arc::new(RwLock::new(0)),
            misses: Arc::new(RwLock::new(0)),
            evictions: Arc::new(RwLock::new(0)),
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
    pub fn get(&self, doc_id: u64) -> Option<CachedVector> {
        let cache = self.cache.read();

        if let Some(cached) = cache.get(&doc_id) {
            // Cache hit
            *self.hits.write() += 1;

            // Update LRU queue (move to back)
            let mut lru = self.lru_queue.write();
            if let Some(pos) = lru.iter().position(|&id| id == doc_id) {
                lru.remove(pos);
                lru.push_back(doc_id);
            }

            Some(cached.clone())
        } else {
            // Cache miss
            *self.misses.write() += 1;
            None
        }
    }

    /// Insert vector into cache
    ///
    /// If cache is full, evicts least recently used entry.
    ///
    /// # Performance
    /// O(1) hashmap insert + O(1) LRU queue push
    pub fn insert(&self, cached_vector: CachedVector) {
        let doc_id = cached_vector.doc_id;
        let mut cache = self.cache.write();
        let mut lru = self.lru_queue.write();

        // Check if already in cache
        if cache.contains_key(&doc_id) {
            // Update existing entry
            cache.insert(doc_id, cached_vector);

            // Update LRU position
            if let Some(pos) = lru.iter().position(|&id| id == doc_id) {
                lru.remove(pos);
                lru.push_back(doc_id);
            } else {
                // CRITICAL FIX: doc_id in cache but not in LRU queue
                // This can happen if state is corrupted - add it now
                lru.push_back(doc_id);
            }

            // CRITICAL FIX: Check capacity even on update
            // Handles case where lru_queue grew unbounded
            while lru.len() > self.capacity {
                if let Some(evict_id) = lru.pop_front() {
                    cache.remove(&evict_id);
                    *self.evictions.write() += 1;
                }
            }

            return;
        }

        // Evict if at capacity BEFORE inserting
        while cache.len() >= self.capacity {
            if let Some(evict_id) = lru.pop_front() {
                cache.remove(&evict_id);
                *self.evictions.write() += 1;
            } else {
                break; // Queue empty but cache full (should not happen)
            }
        }

        // Insert new entry
        cache.insert(doc_id, cached_vector);
        lru.push_back(doc_id);

        // DEFENSIVE: Final capacity check
        debug_assert_eq!(
            cache.len(),
            lru.len(),
            "Cache/LRU size mismatch: cache={}, lru={}",
            cache.len(),
            lru.len()
        );
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let hits = *self.hits.read();
        let misses = *self.misses.read();
        let evictions = *self.evictions.read();
        let size = self.cache.read().len();

        let total_requests = hits + misses;
        let hit_rate = if total_requests > 0 {
            hits as f64 / total_requests as f64
        } else {
            0.0
        };

        CacheStats {
            hits,
            misses,
            evictions,
            size,
            capacity: self.capacity,
            hit_rate,
        }
    }

    /// Clear cache (useful for testing)
    pub fn clear(&self) {
        self.cache.write().clear();
        self.lru_queue.write().clear();
        *self.hits.write() = 0;
        *self.misses.write() = 0;
        *self.evictions.write() = 0;
    }

    /// Get current size
    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    /// Get maximum capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Cache statistics
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
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
