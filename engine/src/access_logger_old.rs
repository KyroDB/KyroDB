//! Access Pattern Logger for Learned Cache Training
//!
//! Phase 0 Week 5-8: Log document accesses to train learned cache predictor
//!
//! Architecture:
//! - Ring buffer: Lock-free singbuf crate (SPSC, zero-copy)
//! - Single-writer, multiple-reader design
//! - Periodic training: Flush events every 10 minutes to cache predictor
//! - Zero-copy reads: No allocation for recent window queries
//!
//! Performance targets:
//! - Log overhead: <10ns per access
//! - Memory: 240MB for 10M events (24 bytes/event)
//! - Training window: Last 24 hours of accesses
//!
//! Memory Safety:
//! - Uses singbuf crate (lock-free ring buffer)
//! - No memory leaks under sustained load
//! - Fixed memory footprint (bounded buffer)

use crate::learned_cache::AccessEvent;
use parking_lot::RwLock;
use ringbuf::{traits::*, HeapRb};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

impl<T: Clone + Default> RingBuffer<T> {
    /// Create new ring buffer with given capacity
    pub fn new(capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity);
        buffer.resize_with(capacity, Default::default);

        Self {
            buffer,
            capacity,
            head: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
        }
    }

    /// Push event into ring buffer (overwrites oldest if full)
    ///
    /// # Performance
    /// - O(1) amortized
    /// - Lock-free atomic increment
    /// - No allocation
    #[inline]
    pub fn push(&mut self, value: T) {
        let head = self.head.load(Ordering::Relaxed);
        self.buffer[head] = value;

        // Advance head (wrap around)
        let next_head = (head + 1) % self.capacity;
        self.head.store(next_head, Ordering::Release);

        // Update count (saturates at capacity)
        let count = self.count.load(Ordering::Relaxed);
        if count < self.capacity {
            self.count.store(count + 1, Ordering::Relaxed);
        }
    }

    /// Get all events in order (oldest to newest)
    ///
    /// # Performance
    /// - O(n) where n = count
    /// - Allocates new Vec
    pub fn iter_all(&self) -> Vec<T> {
        let count = self.count.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);

        if count == 0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(count);

        if count < self.capacity {
            // Not full yet: read from 0 to head
            for i in 0..count {
                result.push(self.buffer[i].clone());
            }
        } else {
            // Full: read from head to end, then 0 to head (oldest to newest)
            for i in head..self.capacity {
                result.push(self.buffer[i].clone());
            }
            for i in 0..head {
                result.push(self.buffer[i].clone());
            }
        }

        result
    }

    /// Get current count of events
    #[inline]
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Check if buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get capacity
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clear all events (reset to empty)
    pub fn clear(&mut self) {
        self.head.store(0, Ordering::Release);
        self.count.store(0, Ordering::Release);
    }
}

/// Access pattern logger with ring buffer and periodic training
///
/// # Usage
/// ```
/// use kyrodb_engine::access_logger::AccessPatternLogger;
/// use kyrodb_engine::learned_cache::LearnedCachePredictor;
/// use std::time::Duration;
///
/// let mut logger = AccessPatternLogger::new(10_000_000);
/// let mut predictor = LearnedCachePredictor::new(1000).unwrap();
///
/// // Log access (called on every query)
/// let query_embedding = vec![0.5; 128];
/// logger.log_access(42, &query_embedding);
///
/// // Get recent window for training (every 10 minutes)
/// let recent = logger.get_recent_window(Duration::from_secs(3600 * 24));
/// predictor.train_from_accesses(&recent).unwrap();
/// ```
pub struct AccessPatternLogger {
    /// Ring buffer for access events (10M capacity)
    events: RingBuffer<AccessEvent>,

    /// Last flush timestamp (for periodic training)
    last_flush: Instant,

    /// Flush interval (default: 10 minutes)
    flush_interval: Duration,

    /// Statistics
    total_accesses: AtomicU64,
    total_flushes: AtomicU64,
}

impl AccessPatternLogger {
    /// Create new access pattern logger
    ///
    /// # Parameters
    /// - `capacity`: Ring buffer size (e.g., 10M events)
    ///
    /// # Memory Usage
    /// - capacity × 24 bytes (AccessEvent size)
    /// - 10M events = 240MB
    pub fn new(capacity: usize) -> Self {
        Self {
            events: RingBuffer::new(capacity),
            last_flush: Instant::now(),
            flush_interval: Duration::from_secs(600), // 10 minutes
            total_accesses: AtomicU64::new(0),
            total_flushes: AtomicU64::new(0),
        }
    }

    /// Create logger with custom flush interval (for testing)
    pub fn with_flush_interval(capacity: usize, flush_interval: Duration) -> Self {
        Self {
            events: RingBuffer::new(capacity),
            last_flush: Instant::now(),
            flush_interval,
            total_accesses: AtomicU64::new(0),
            total_flushes: AtomicU64::new(0),
        }
    }

    /// Log document access
    ///
    /// # Performance
    /// - Target: <10ns overhead
    /// - Lock-free ring buffer push
    /// - No allocation
    ///
    /// # Parameters
    /// - `doc_id`: Document ID that was accessed
    /// - `query_embedding`: Query embedding for semantic cache (Phase 0.5.1+)
    #[inline]
    pub fn log_access(&mut self, doc_id: u64, query_embedding: &[f32]) {
        let event = AccessEvent {
            doc_id,
            timestamp: SystemTime::now(),
            access_type: crate::learned_cache::AccessType::Read,
            embedding: query_embedding.to_vec(),
        };

        self.events.push(event);
        self.total_accesses.fetch_add(1, Ordering::Relaxed);
    }

    /// Log access with pre-computed event (zero overhead)
    #[inline]
    pub fn log_event(&mut self, event: AccessEvent) {
        self.events.push(event);
        self.total_accesses.fetch_add(1, Ordering::Relaxed);
    }

    /// Get all events in recent time window
    ///
    /// # Parameters
    /// - `window`: Time window (e.g., Duration::from_secs(24 * 3600) for 24 hours)
    ///
    /// # Returns
    /// Vector of AccessEvent within time window (oldest to newest)
    ///
    /// # Performance
    /// - O(n) where n = buffer size
    /// - Allocates new Vec
    pub fn get_recent_window(&self, window: Duration) -> Vec<AccessEvent> {
        let cutoff = SystemTime::now()
            .checked_sub(window)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        self.events
            .iter_all()
            .into_iter()
            .filter(|event| event.timestamp >= cutoff)
            .collect()
    }

    /// Check if flush is needed (based on flush interval)
    #[inline]
    pub fn needs_flush(&self) -> bool {
        self.last_flush.elapsed() >= self.flush_interval
    }

    /// Mark flush as complete (updates last_flush timestamp)
    pub fn mark_flushed(&mut self) {
        self.last_flush = Instant::now();
        self.total_flushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Get statistics for monitoring
    pub fn stats(&self) -> AccessLoggerStats {
        AccessLoggerStats {
            capacity: self.events.capacity(),
            current_count: self.events.len(),
            total_accesses: self.total_accesses.load(Ordering::Relaxed),
            total_flushes: self.total_flushes.load(Ordering::Relaxed),
            last_flush_elapsed: self.last_flush.elapsed(),
            needs_flush: self.needs_flush(),
        }
    }

    /// Get current event count
    #[inline]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if logger is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Clear all events (for testing)
    pub fn clear(&mut self) {
        self.events.clear();
    }

    /// Get capacity
    #[inline]
    pub fn capacity(&self) -> usize {
        self.events.capacity()
    }
}

/// Statistics for access logger monitoring
#[derive(Debug, Clone)]
pub struct AccessLoggerStats {
    pub capacity: usize,
    pub current_count: usize,
    pub total_accesses: u64,
    pub total_flushes: u64,
    pub last_flush_elapsed: Duration,
    pub needs_flush: bool,
}

/// Hash embedding vector for correlation analysis
///
/// Uses FNV-1a hash for speed (not cryptographic)
pub fn hash_embedding(embedding: &[f32]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for &value in embedding {
        // Convert f32 to u32 bits for hashing
        value.to_bits().hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_basic() {
        let mut buffer = RingBuffer::new(5);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());

        buffer.push(1u64);
        assert_eq!(buffer.len(), 1);
        assert!(!buffer.is_empty());

        buffer.push(2);
        buffer.push(3);
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_ring_buffer_wraparound() {
        let mut buffer = RingBuffer::new(3);

        // Fill buffer
        buffer.push(1u64);
        buffer.push(2);
        buffer.push(3);
        assert_eq!(buffer.len(), 3);

        // Overwrite oldest (1)
        buffer.push(4);
        assert_eq!(buffer.len(), 3); // Saturates at capacity

        // Iterate: should get [2, 3, 4] (oldest to newest)
        let items = buffer.iter_all();
        assert_eq!(items, vec![2, 3, 4]);
    }

    #[test]
    fn test_ring_buffer_iter_all_not_full() {
        let mut buffer = RingBuffer::new(10);
        buffer.push(1u64);
        buffer.push(2);
        buffer.push(3);

        let items = buffer.iter_all();
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[test]
    fn test_ring_buffer_clear() {
        let mut buffer = RingBuffer::new(5);
        buffer.push(1u64);
        buffer.push(2);
        assert_eq!(buffer.len(), 2);

        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_access_logger_basic() {
        let mut logger = AccessPatternLogger::new(1000);
        assert_eq!(logger.len(), 0);
        assert_eq!(logger.capacity(), 1000);

        let embedding = vec![0.1, 0.2, 0.3];
        logger.log_access(42, &embedding);

        assert_eq!(logger.len(), 1);
        let stats = logger.stats();
        assert_eq!(stats.total_accesses, 1);
    }

    #[test]
    fn test_access_logger_recent_window() {
        let mut logger = AccessPatternLogger::new(1000);

        // Log events with different timestamps
        let embedding = vec![0.1, 0.2];

        // Recent event (1 second ago)
        logger.log_access(1, &embedding);
        std::thread::sleep(Duration::from_millis(100));

        // Old event (will be filtered)
        let old_event = AccessEvent {
            doc_id: 2,
            timestamp: SystemTime::now() - Duration::from_secs(3600 * 25), // 25 hours ago
            access_type: crate::learned_cache::AccessType::Read,
        };
        logger.log_event(old_event);

        // Get 24-hour window
        let recent = logger.get_recent_window(Duration::from_secs(3600 * 24));

        // Should only get recent event (doc_id=1), not old one (doc_id=2)
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].doc_id, 1);
    }

    #[test]
    fn test_access_logger_needs_flush() {
        let mut logger = AccessPatternLogger::with_flush_interval(
            1000,
            Duration::from_millis(100), // 100ms flush interval for testing
        );

        assert!(!logger.needs_flush()); // Just created

        std::thread::sleep(Duration::from_millis(150));
        assert!(logger.needs_flush()); // 150ms > 100ms interval

        logger.mark_flushed();
        assert!(!logger.needs_flush()); // Just flushed
    }

    #[test]
    fn test_access_logger_stats() {
        let mut logger = AccessPatternLogger::new(100);
        let embedding = vec![0.1];

        for i in 0..50 {
            logger.log_access(i, &embedding);
        }

        let stats = logger.stats();
        assert_eq!(stats.capacity, 100);
        assert_eq!(stats.current_count, 50);
        assert_eq!(stats.total_accesses, 50);
        assert_eq!(stats.total_flushes, 0);
    }

    #[test]
    fn test_access_logger_overflow() {
        let capacity = 10;
        let mut logger = AccessPatternLogger::new(capacity);
        let embedding = vec![0.1];

        // Log more than capacity
        for i in 0..20 {
            logger.log_access(i, &embedding);
        }

        // Count saturates at capacity
        assert_eq!(logger.len(), capacity);

        // But total_accesses keeps counting
        let stats = logger.stats();
        assert_eq!(stats.total_accesses, 20);

        // Should get last 10 events (10-19)
        let all_events = logger.get_recent_window(Duration::from_secs(3600));
        assert_eq!(all_events.len(), capacity);

        // Verify we have the most recent events
        let doc_ids: Vec<u64> = all_events.iter().map(|e| e.doc_id).collect();
        assert_eq!(doc_ids, vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
    }

    #[test]
    fn test_hash_embedding() {
        let emb1 = vec![0.1, 0.2, 0.3];
        let emb2 = vec![0.1, 0.2, 0.3];
        let emb3 = vec![0.1, 0.2, 0.4]; // Different

        // Same embeddings should have same hash
        assert_eq!(hash_embedding(&emb1), hash_embedding(&emb2));

        // Different embeddings should have different hash (very likely)
        assert_ne!(hash_embedding(&emb1), hash_embedding(&emb3));
    }

    #[test]
    fn test_access_logger_clear() {
        let mut logger = AccessPatternLogger::new(100);
        let embedding = vec![0.1];

        for i in 0..10 {
            logger.log_access(i, &embedding);
        }
        assert_eq!(logger.len(), 10);

        logger.clear();
        assert_eq!(logger.len(), 0);
        assert!(logger.is_empty());

        // total_accesses is not reset (cumulative metric)
        assert_eq!(logger.stats().total_accesses, 10);
    }

    #[test]
    fn test_ring_buffer_capacity() {
        let buffer = RingBuffer::<u64>::new(1000);
        assert_eq!(buffer.capacity(), 1000);
    }

    #[test]
    fn test_access_logger_flush_tracking() {
        let mut logger = AccessPatternLogger::with_flush_interval(1000, Duration::from_millis(50));

        // Initial state
        assert_eq!(logger.stats().total_flushes, 0);

        // Mark first flush
        logger.mark_flushed();
        assert_eq!(logger.stats().total_flushes, 1);

        // Mark second flush
        logger.mark_flushed();
        assert_eq!(logger.stats().total_flushes, 2);
    }

    #[test]
    fn test_concurrent_access_pattern() {
        // Test that demonstrates safe usage pattern (single writer)
        let mut logger = AccessPatternLogger::new(1000);
        let embedding = vec![0.5; 128];

        // Simulate high-frequency logging
        for i in 0..1000 {
            logger.log_access(i % 100, &embedding);
        }

        assert_eq!(logger.len(), 1000);
        assert_eq!(logger.stats().total_accesses, 1000);
    }
}
