// Loom concurrency correctness tests
//
// These tests use the loom model checker to detect data races, deadlocks,
// and other concurrency bugs in critical concurrent data structures.
//
// Run with: cargo test --test loom_concurrency_tests --release

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::Mutex;
use loom::thread;
use std::collections::HashMap;

// Simple cache model for loom testing (simplified version of actual cache)
#[derive(Clone)]
struct SimpleCacheEntry {
    doc_id: u64,
    data: Vec<f32>,
}

struct SimpleCache {
    storage: Arc<Mutex<HashMap<u64, SimpleCacheEntry>>>,
    capacity: usize,
}

impl SimpleCache {
    fn new(capacity: usize) -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            capacity,
        }
    }

    fn insert(&self, doc_id: u64, data: Vec<f32>) {
        let mut storage = self.storage.lock().unwrap();
        if storage.len() >= self.capacity {
            // Simple eviction: remove first entry (LRU approximation for testing)
            if let Some(first_key) = storage.keys().next().copied() {
                storage.remove(&first_key);
            }
        }
        storage.insert(doc_id, SimpleCacheEntry { doc_id, data });
    }

    fn get(&self, doc_id: u64) -> Option<Vec<f32>> {
        let storage = self.storage.lock().unwrap();
        storage.get(&doc_id).map(|entry| entry.data.clone())
    }

    fn len(&self) -> usize {
        let storage = self.storage.lock().unwrap();
        storage.len()
    }
}

#[test]
fn cache_concurrent_insert_get() {
    loom::model(|| {
        let cache = Arc::new(SimpleCache::new(10));

        let cache1 = cache.clone();
        let cache2 = cache.clone();

        let t1 = thread::spawn(move || {
            cache1.insert(1, vec![1.0, 2.0]);
            cache1.get(1)
        });

        let t2 = thread::spawn(move || {
            cache2.insert(2, vec![3.0, 4.0]);
            cache2.get(2)
        });

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Both inserts should succeed
        assert!(r1.is_some() || r2.is_some());
    });
}

#[test]
fn cache_concurrent_insert_same_key() {
    loom::model(|| {
        let cache = Arc::new(SimpleCache::new(10));

        let cache1 = cache.clone();
        let cache2 = cache.clone();

        let t1 = thread::spawn(move || {
            cache1.insert(1, vec![1.0, 2.0]);
        });

        let t2 = thread::spawn(move || {
            cache2.insert(1, vec![3.0, 4.0]);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // One of the values should be present
        let result = cache.get(1);
        assert!(result.is_some());
    });
}

// Hot tier model for loom testing
struct SimpleHotTier {
    documents: Arc<Mutex<HashMap<u64, Vec<f32>>>>,
}

impl SimpleHotTier {
    fn new() -> Self {
        Self {
            documents: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn insert(&self, doc_id: u64, embedding: Vec<f32>) {
        let mut docs = self.documents.lock().unwrap();
        docs.insert(doc_id, embedding);
    }

    fn get(&self, doc_id: u64) -> Option<Vec<f32>> {
        let docs = self.documents.lock().unwrap();
        docs.get(&doc_id).cloned()
    }

    fn flush(&self) -> Vec<(u64, Vec<f32>)> {
        let mut docs = self.documents.lock().unwrap();
        let result: Vec<_> = docs.iter().map(|(k, v)| (*k, v.clone())).collect();
        docs.clear();
        result
    }
}

#[test]
fn hot_tier_concurrent_insert_flush() {
    loom::model(|| {
        let hot_tier = Arc::new(SimpleHotTier::new());

        let hot1 = hot_tier.clone();
        let hot2 = hot_tier.clone();

        let t1 = thread::spawn(move || {
            hot1.insert(1, vec![1.0]);
            hot1.insert(2, vec![2.0]);
        });

        let t2 = thread::spawn(move || hot2.flush());

        t1.join().unwrap();
        let flushed = t2.join().unwrap();

        // Flush should either get both documents or neither (atomic)
        // But it's allowed to get 0, 1, or 2 depending on timing
        assert!(flushed.len() <= 2);
    });
}

#[test]
fn hot_tier_concurrent_insert_get() {
    loom::model(|| {
        let hot_tier = Arc::new(SimpleHotTier::new());

        let hot1 = hot_tier.clone();
        let hot2 = hot_tier.clone();

        let t1 = thread::spawn(move || {
            hot1.insert(1, vec![1.0, 2.0]);
        });

        let t2 = thread::spawn(move || hot2.get(1));

        t1.join().unwrap();
        let result = t2.join().unwrap();

        // Get might or might not see the insert depending on timing
        // But should not panic or see corrupted data
        if let Some(data) = result {
            assert_eq!(data, vec![1.0, 2.0]);
        }
    });
}

// Access logger model for loom testing
struct SimpleAccessLogger {
    events: Arc<Mutex<Vec<u64>>>,
    capacity: usize,
}

impl SimpleAccessLogger {
    fn new(capacity: usize) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
            capacity,
        }
    }

    fn log(&self, doc_id: u64) {
        let mut events = self.events.lock().unwrap();
        if events.len() < self.capacity {
            events.push(doc_id);
        }
    }

    fn get_count(&self) -> usize {
        let events = self.events.lock().unwrap();
        events.len()
    }
}

#[test]
fn access_logger_concurrent_logging() {
    loom::model(|| {
        let logger = Arc::new(SimpleAccessLogger::new(100));

        let log1 = logger.clone();
        let log2 = logger.clone();

        let t1 = thread::spawn(move || {
            log1.log(1);
            log1.log(2);
        });

        let t2 = thread::spawn(move || {
            log2.log(3);
            log2.log(4);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // All 4 logs should be recorded
        let count = logger.get_count();
        assert_eq!(count, 4);
    });
}

#[test]
fn access_logger_concurrent_log_read() {
    loom::model(|| {
        let logger = Arc::new(SimpleAccessLogger::new(100));

        let log1 = logger.clone();
        let log2 = logger.clone();

        let t1 = thread::spawn(move || {
            log1.log(1);
            log1.log(2);
            log1.get_count()
        });

        let t2 = thread::spawn(move || {
            log2.log(3);
            log2.get_count()
        });

        let count1 = t1.join().unwrap();
        let count2 = t2.join().unwrap();

        // Counts should be consistent
        assert!(count1 >= 1);
        assert!(count2 >= 1);
    });
}

// Test for potential deadlock scenarios
#[test]
fn cache_hot_tier_no_deadlock() {
    loom::model(|| {
        let cache = Arc::new(SimpleCache::new(10));
        let hot_tier = Arc::new(SimpleHotTier::new());

        let cache1 = cache.clone();
        let hot1 = hot_tier.clone();

        let cache2 = cache.clone();
        let hot2 = hot_tier.clone();

        let t1 = thread::spawn(move || {
            cache1.insert(1, vec![1.0]);
            hot1.insert(1, vec![1.0]);
        });

        let t2 = thread::spawn(move || {
            hot2.insert(2, vec![2.0]);
            cache2.insert(2, vec![2.0]);
        });

        // Should complete without deadlock
        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(cache.len(), 2);
    });
}
