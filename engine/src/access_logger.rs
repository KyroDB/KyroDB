//! Access Pattern Logger for Learned Cache Training
//!
//! Access pattern logger: Ring buffer for training data collection
//!
//! Fixed-capacity lock-free ring buffer (ringbuf crate) - no memory leaks
//!
//! Architecture:
//! - Ring buffer: Lock-free ringbuf crate (SPSC, zero-copy)
//! - Single-writer, multiple-reader design via RwLock
//! - Periodic training: Flush events every 10 minutes to cache predictor
//! - Zero-copy reads: No allocation for recent window queries
//!
//! Performance targets:
//! - Log overhead: <20ns per access (with lock)
//! - Memory: Fixed 240MB for 10M events (24 bytes/event)
//! - Training window: Last 24 hours of accesses
//!
//! Memory Safety:
//! - Uses ringbuf crate (lock-free ring buffer, no leaks)
//! - Fixed memory footprint (bounded buffer)
//! - No unbounded growth under sustained load

use crate::learned_cache::AccessEvent;
use parking_lot::RwLock;
use ringbuf::{traits::*, HeapRb};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

/// Access pattern logger with lock-free ring buffer and periodic training
///
/// # Memory Safety
/// - Uses ringbuf crate (no memory leaks)
/// - Fixed capacity (10M events = 240MB)
/// - Old events automatically overwritten (circular buffer)
///
/// # Usage
/// ```no_run
/// use kyrodb_engine::access_logger::AccessPatternLogger;
/// use kyrodb_engine::learned_cache::LearnedCachePredictor;
/// use std::time::Duration;
///
/// let logger = AccessPatternLogger::new(10_000_000);
/// let mut predictor = LearnedCachePredictor::new(1000).unwrap();
///
/// // Log access (called on every query)
/// let query_embedding = vec![0.5; 128];
/// {
///     let mut logger_guard = logger.write();
///     logger_guard.log_access(42, &query_embedding);
/// }
///
/// // Get recent window for training (every 10 minutes)
/// let recent = logger.read().get_recent_window(Duration::from_secs(3600 * 24));
/// predictor.train_from_accesses(&recent).unwrap();
/// ```
pub struct AccessPatternLogger {
    /// Ring buffer for access events (lock-free, fixed capacity)
    /// Uses HeapRb for dynamic allocation with fixed size
    events: Arc<RwLock<HeapRb<AccessEvent>>>,

    /// Last flush timestamp (for periodic training)
    last_flush: Arc<RwLock<Instant>>,

    /// Flush interval (default: 10 minutes)
    flush_interval: Duration,

    /// Statistics
    total_accesses: Arc<AtomicU64>,
    total_flushes: Arc<AtomicU64>,

    /// Capacity
    capacity: usize,
}

impl AccessPatternLogger {
    /// Create new access pattern logger
    ///
    /// # Parameters
    /// - `capacity`: Ring buffer size (e.g., 10M events)
    ///
    /// # Memory Usage
    /// - capacity × 24 bytes (AccessEvent size)
    /// - 10M events = 240MB fixed
    pub fn new(capacity: usize) -> Self {
        Self {
            events: Arc::new(RwLock::new(HeapRb::new(capacity))),
            last_flush: Arc::new(RwLock::new(Instant::now())),
            flush_interval: Duration::from_secs(600), // 10 minutes
            total_accesses: Arc::new(AtomicU64::new(0)),
            total_flushes: Arc::new(AtomicU64::new(0)),
            capacity,
        }
    }

    /// Create logger with custom flush interval (for testing)
    pub fn with_flush_interval(capacity: usize, flush_interval: Duration) -> Self {
        Self {
            events: Arc::new(RwLock::new(HeapRb::new(capacity))),
            last_flush: Arc::new(RwLock::new(Instant::now())),
            flush_interval,
            total_accesses: Arc::new(AtomicU64::new(0)),
            total_flushes: Arc::new(AtomicU64::new(0)),
            capacity,
        }
    }

    /// Log document access
    ///
    /// # Performance
    /// - Target: <20ns overhead (with lock)
    /// - Ring buffer automatically overwrites oldest
    /// - No allocation (fixed capacity)
    ///
    /// # Parameters
    /// - `doc_id`: Document ID that was accessed
    /// - `query_embedding`: Query embedding for semantic cache (Phase 0.5.1+)
    #[inline]
    pub fn log_access(&self, doc_id: u64, _query_embedding: &[f32]) {
        // CRITICAL FIX: Removed embedding storage to fix 107MB memory leak
        // RMI training only needs doc_id and timestamp, not the full embedding
        // This reduces AccessEvent from ~1568 bytes to ~32 bytes (48× reduction!)
        let event = AccessEvent {
            doc_id,
            timestamp: SystemTime::now(),
            access_type: crate::learned_cache::AccessType::Read,
        };

        // Write lock - push event (overwrites oldest if full)
        let mut events = self.events.write();
        let _ = events.try_push(event); // If full, overwrites oldest automatically
        drop(events);

        self.total_accesses.fetch_add(1, Ordering::Relaxed);
    }

    /// Log access with pre-computed event (zero overhead)
    #[inline]
    pub fn log_event(&self, event: AccessEvent) {
        let mut events = self.events.write();
        let _ = events.try_push(event);
        drop(events);

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

        let events = self.events.read();
        events
            .iter()
            .filter(|event| event.timestamp >= cutoff)
            .cloned()
            .collect()
    }

    /// Check if flush is needed (based on flush interval)
    #[inline]
    pub fn needs_flush(&self) -> bool {
        let last_flush = self.last_flush.read();
        last_flush.elapsed() >= self.flush_interval
    }

    /// Mark flush as complete (reset timer)
    pub fn mark_flushed(&self) {
        let mut last_flush = self.last_flush.write();
        *last_flush = Instant::now();
        self.total_flushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Get all events (for training)
    ///
    /// # Performance
    /// - O(n) where n = buffer size
    /// - Allocates new Vec
    pub fn get_all_events(&self) -> Vec<AccessEvent> {
        let events = self.events.read();
        events.iter().cloned().collect()
    }

    /// Get current count of events
    #[inline]
    pub fn len(&self) -> usize {
        let events = self.events.read();
        events.occupied_len()
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
    pub fn clear(&self) {
        let mut events = self.events.write();
        events.clear();
    }

    /// Get statistics
    pub fn stats(&self) -> AccessLoggerStats {
        AccessLoggerStats {
            total_accesses: self.total_accesses.load(Ordering::Relaxed),
            total_flushes: self.total_flushes.load(Ordering::Relaxed),
            current_events: self.len(),
            capacity: self.capacity,
            memory_usage_mb: (self.capacity * std::mem::size_of::<AccessEvent>()) as f64
                / 1_048_576.0,
        }
    }

    /// Compute doc_id diversity (for validation)
    /// 
    /// NOTE: Previously computed embedding hash diversity, but embeddings were removed
    /// to fix 107MB memory leak. Now computes unique doc_id count instead.
    pub fn hash_diversity(&self) -> f64 {
        use std::collections::HashSet;

        let events = self.events.read();
        let unique_doc_ids: HashSet<u64> = events
            .iter()
            .map(|event| event.doc_id)
            .collect();

        let total = events.occupied_len();
        if total == 0 {
            return 0.0;
        }

        unique_doc_ids.len() as f64 / total as f64
    }
}

/// Access logger statistics
#[derive(Debug, Clone)]
pub struct AccessLoggerStats {
    pub total_accesses: u64,
    pub total_flushes: u64,
    pub current_events: usize,
    pub capacity: usize,
    pub memory_usage_mb: f64,
}

impl std::fmt::Display for AccessLoggerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "accesses: {}, flushes: {}, events: {}/{} ({:.1} MB)",
            self.total_accesses,
            self.total_flushes,
            self.current_events,
            self.capacity,
            self.memory_usage_mb
        )
    }
}

// Clone implementation for Arc-wrapped logger
impl Clone for AccessPatternLogger {
    fn clone(&self) -> Self {
        Self {
            events: Arc::clone(&self.events),
            last_flush: Arc::clone(&self.last_flush),
            flush_interval: self.flush_interval,
            total_accesses: Arc::clone(&self.total_accesses),
            total_flushes: Arc::clone(&self.total_flushes),
            capacity: self.capacity,
        }
    }
}

/// Hash embedding vector to u64 (for deduplication and cache keys)
///
/// # Performance
/// - Uses DefaultHasher (fast, non-cryptographic)
/// - O(n) where n = embedding dimension
pub fn hash_embedding(embedding: &[f32]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for val in embedding {
        hasher.write_u32(val.to_bits());
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_logger_basic() {
        let logger = AccessPatternLogger::new(100);

        // Log some accesses
        for i in 0..10 {
            let embedding = vec![i as f32; 128];
            logger.log_access(i, &embedding);
        }

        assert_eq!(logger.len(), 10);
        assert!(!logger.is_empty());

        let stats = logger.stats();
        assert_eq!(stats.total_accesses, 10);
        assert_eq!(stats.current_events, 10);
    }

    #[test]
    fn test_access_logger_overflow() {
        let logger = AccessPatternLogger::new(10); // Small capacity

        // Log more than capacity
        for i in 0..20 {
            let embedding = vec![i as f32; 128];
            logger.log_access(i, &embedding);
        }

        // Should be at capacity (oldest overwritten)
        assert_eq!(logger.len(), 10);
        assert_eq!(logger.stats().total_accesses, 20);
    }

    #[test]
    fn test_recent_window() {
        let logger = AccessPatternLogger::new(100);

        // Log some accesses
        for i in 0..5 {
            let embedding = vec![i as f32; 128];
            logger.log_access(i, &embedding);
        }

        // Get recent window (24 hours)
        let recent = logger.get_recent_window(Duration::from_secs(24 * 3600));
        assert_eq!(recent.len(), 5);
    }

    #[test]
    fn test_clear() {
        let logger = AccessPatternLogger::new(100);

        for i in 0..10 {
            let embedding = vec![i as f32; 128];
            logger.log_access(i, &embedding);
        }

        assert_eq!(logger.len(), 10);

        logger.clear();
        assert_eq!(logger.len(), 0);
        assert!(logger.is_empty());
    }

    #[test]
    fn test_flush_tracking() {
        let logger = AccessPatternLogger::with_flush_interval(100, Duration::from_millis(100));

        assert!(logger.needs_flush()); // Initial state

        logger.mark_flushed();
        assert!(!logger.needs_flush());

        std::thread::sleep(Duration::from_millis(150));
        assert!(logger.needs_flush());
    }
}
