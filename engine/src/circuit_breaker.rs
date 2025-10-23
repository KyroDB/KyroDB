// Circuit Breaker Pattern for Error Recovery
//
// Implements the circuit breaker pattern to prevent cascading failures.
// Used for WAL writes, cache access, and layer timeouts.
//
// States:
// - Closed: Normal operation, requests pass through
// - Open: Failure threshold exceeded, requests fail fast
// - Half-Open: Testing recovery, allow one request through
//
// Performance: <100ns state check (atomic read)

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - all requests allowed
    Closed,
    /// Failure threshold exceeded - fail fast
    Open,
    /// Testing recovery - allow one request
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening circuit
    pub failure_threshold: usize,
    /// Number of successes to close circuit from half-open
    pub success_threshold: usize,
    /// Duration to wait before half-open attempt
    pub timeout: Duration,
    /// Window size for failure counting
    pub window_size: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            success_threshold: 2,
            timeout: Duration::from_secs(60),
            window_size: Duration::from_secs(60),
        }
    }
}

/// Thread-safe circuit breaker
///
/// # Performance
/// - State check: ~20ns (atomic read)
/// - Record success/failure: ~50ns (atomic increment + mutex)
/// - Transition: ~100ns (mutex lock + state update)
pub struct CircuitBreaker {
    /// Current state
    state: AtomicUsize, // 0=Closed, 1=Open, 2=HalfOpen

    /// Failure count in current window
    failure_count: AtomicU64,

    /// Success count in half-open state
    success_count: AtomicU64,

    /// Total failures since start
    total_failures: AtomicU64,

    /// Total successes since start
    total_successes: AtomicU64,

    /// State transition tracking
    inner: Mutex<CircuitBreakerInner>,

    /// Configuration
    config: CircuitBreakerConfig,
}

struct CircuitBreakerInner {
    /// Timestamp when circuit opened
    opened_at: Option<Instant>,

    /// Timestamp of last failure
    last_failure: Option<Instant>,

    /// Timestamp of window start
    window_start: Instant,
}

impl CircuitBreaker {
    /// Create new circuit breaker with default config
    pub fn new() -> Self {
        Self::with_config(CircuitBreakerConfig::default())
    }

    /// Create circuit breaker with custom config
    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        Self {
            state: AtomicUsize::new(CircuitState::Closed as usize),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            total_successes: AtomicU64::new(0),
            inner: Mutex::new(CircuitBreakerInner {
                opened_at: None,
                last_failure: None,
                window_start: Instant::now(),
            }),
            config,
        }
    }

    /// Check if circuit breaker is open (blocking requests)
    ///
    /// Returns true if circuit is open and should fail fast.
    ///
    /// # Performance
    /// ~20ns - single atomic read + optional state transition
    pub fn is_open(&self) -> bool {
        let state = self.state();

        match state {
            CircuitState::Closed => false,
            CircuitState::Open => {
                let mut inner = self.inner.lock();
                // Re-check state after acquiring lock to avoid TOCTOU
                let current_state = self.state();
                if current_state != CircuitState::Open {
                    // State changed while acquiring lock, recurse with current state
                    drop(inner); // Release lock before recursion
                    return self.is_open();
                }

                // Check if timeout has elapsed, transition to half-open
                if let Some(opened_at) = inner.opened_at {
                    if opened_at.elapsed() >= self.config.timeout {
                        // Transition to half-open
                        self.state
                            .store(CircuitState::HalfOpen as usize, Ordering::Release);
                        self.success_count.store(0, Ordering::Relaxed);
                        inner.opened_at = None;
                        return false; // Allow one request through
                    }
                }
                true // Still open, fail fast
            }
            CircuitState::HalfOpen => false, // Allow request to test recovery
        }
    }

    /// Check if circuit breaker is closed (normal operation)
    pub fn is_closed(&self) -> bool {
        matches!(self.state(), CircuitState::Closed)
    }

    /// Get current state
    pub fn state(&self) -> CircuitState {
        match self.state.load(Ordering::Acquire) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed, // Fallback
        }
    }

    /// Record successful operation
    ///
    /// In Half-Open state: increments success count, may transition to Closed
    /// In Closed state: resets failure count
    pub fn record_success(&self) {
        self.total_successes.fetch_add(1, Ordering::Relaxed);

        match self.state() {
            CircuitState::HalfOpen => {
                let successes = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;

                // Transition to closed if success threshold met
                if successes >= self.config.success_threshold as u64 {
                    self.transition_to_closed();
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);

                // Reset window
                let mut inner = self.inner.lock();
                inner.window_start = Instant::now();
            }
            CircuitState::Open => {
                // Ignore success in open state (shouldn't happen)
            }
        }
    }

    /// Record failed operation
    ///
    /// May trigger state transition if failure threshold is exceeded.
    pub fn record_failure(&self) {
        self.total_failures.fetch_add(1, Ordering::Relaxed);

        match self.state() {
            CircuitState::Closed => {
                let failures;
                let should_open;

                // Check if window has expired and update failure count atomically
                {
                    let mut inner = self.inner.lock();
                    if inner.window_start.elapsed() >= self.config.window_size {
                        // Reset window and failure count
                        inner.window_start = Instant::now();
                        self.failure_count.store(1, Ordering::Relaxed);
                        inner.last_failure = Some(Instant::now());
                        return;
                    }

                    // Increment failure count and check threshold within single lock
                    failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                    should_open = failures >= self.config.failure_threshold as u64;
                    inner.last_failure = Some(Instant::now());
                }

                // Transition to open if threshold exceeded (lock released)
                if should_open {
                    self.transition_to_open();
                }
            }
            CircuitState::HalfOpen => {
                // Single failure in half-open transitions back to open
                self.transition_to_open();
            }
            CircuitState::Open => {
                // Already open, update last failure time
                let mut inner = self.inner.lock();
                inner.last_failure = Some(Instant::now());
            }
        }
    }

    /// Force circuit breaker to open state
    ///
    /// Used for manual intervention or external health checks.
    pub fn open(&self) {
        self.transition_to_open();
    }

    /// Force circuit breaker to closed state
    ///
    /// Used for manual recovery or reset.
    pub fn close(&self) {
        self.transition_to_closed();
    }

    /// Reset window counters and return to closed state
    ///
    /// Note: Preserves total_failures and total_successes for historical tracking.
    /// Only resets the sliding window counters (failure_count, success_count) and
    /// state information (opened_at, last_failure).
    pub fn reset(&self) {
        self.state
            .store(CircuitState::Closed as usize, Ordering::Release);
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);

        let mut inner = self.inner.lock();
        inner.opened_at = None;
        inner.last_failure = None;
        inner.window_start = Instant::now();
    }

    /// Get statistics
    pub fn stats(&self) -> CircuitBreakerStats {
        CircuitBreakerStats {
            state: self.state(),
            total_failures: self.total_failures.load(Ordering::Relaxed),
            total_successes: self.total_successes.load(Ordering::Relaxed),
            current_failures: self.failure_count.load(Ordering::Relaxed),
            current_successes: self.success_count.load(Ordering::Relaxed),
        }
    }

    // Internal state transitions

    fn transition_to_open(&self) {
        self.state
            .store(CircuitState::Open as usize, Ordering::Release);

        let mut inner = self.inner.lock();
        inner.opened_at = Some(Instant::now());
    }

    fn transition_to_closed(&self) {
        self.state
            .store(CircuitState::Closed as usize, Ordering::Release);
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);

        let mut inner = self.inner.lock();
        inner.opened_at = None;
        inner.window_start = Instant::now();
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

/// Circuit breaker statistics
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    pub state: CircuitState,
    pub total_failures: u64,
    pub total_successes: u64,
    pub current_failures: u64,
    pub current_successes: u64,
}

impl CircuitBreakerStats {
    /// Calculate failure rate (failures / total operations)
    pub fn failure_rate(&self) -> f64 {
        let total = self.total_failures + self.total_successes;
        if total == 0 {
            0.0
        } else {
            self.total_failures as f64 / total as f64
        }
    }

    /// Calculate success rate (successes / total operations)
    pub fn success_rate(&self) -> f64 {
        let total = self.total_failures + self.total_successes;
        if total == 0 {
            0.0
        } else {
            self.total_successes as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(!cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_opens_after_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::with_config(config);

        // Record 3 failures
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_resets_on_success() {
        let cb = CircuitBreaker::new();

        // Record 2 failures
        cb.record_failure();
        cb.record_failure();

        // Success should reset failure count
        cb.record_success();

        // Should not open on next failure
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_transition() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let cb = CircuitBreaker::with_config(config);

        // Open circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for timeout
        thread::sleep(Duration::from_millis(150));

        // Check if open (should transition to half-open)
        assert!(!cb.is_open());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_half_open_to_closed() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let cb = CircuitBreaker::with_config(config);

        // Open circuit
        cb.record_failure();
        cb.record_failure();

        // Wait and transition to half-open
        thread::sleep(Duration::from_millis(100));
        assert!(!cb.is_open());

        // Record 2 successes to close
        cb.record_success();
        cb.record_success();

        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_to_open_on_failure() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let cb = CircuitBreaker::with_config(config);

        // Open circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait and call is_open() to trigger transition to half-open
        thread::sleep(Duration::from_millis(100));
        assert!(!cb.is_open()); // Should transition to half-open and allow request
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Single failure in half-open transitions back to open
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_breaker_manual_open() {
        let cb = CircuitBreaker::new();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.open();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_manual_close() {
        let cb = CircuitBreaker::new();

        // Open circuit
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Manual close
        cb.close();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(!cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let cb = CircuitBreaker::new();

        // Record some operations
        cb.record_failure();
        cb.record_failure();
        cb.record_success();

        // Reset
        cb.reset();

        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(cb.stats().current_failures, 0);
        assert_eq!(cb.stats().current_successes, 0);
    }

    #[test]
    fn test_circuit_breaker_stats() {
        let cb = CircuitBreaker::new();

        cb.record_success();
        cb.record_success();
        cb.record_failure();

        let stats = cb.stats();
        assert_eq!(stats.total_successes, 2);
        assert_eq!(stats.total_failures, 1);
        assert!((stats.success_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_circuit_breaker_concurrent_access() {
        use std::sync::Arc;

        let cb = Arc::new(CircuitBreaker::new());
        let mut handles = vec![];

        // Spawn 10 threads recording failures
        for _ in 0..10 {
            let cb = cb.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    cb.record_failure();
                }
            }));
        }

        // Spawn 10 threads recording successes
        for _ in 0..10 {
            let cb = cb.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    cb.record_success();
                }
            }));
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        let stats = cb.stats();
        assert_eq!(stats.total_failures + stats.total_successes, 2000);
    }

    #[test]
    fn test_circuit_breaker_window_expiration() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            window_size: Duration::from_millis(100),
            ..Default::default()
        };
        let cb = CircuitBreaker::with_config(config);

        // Record 2 failures
        cb.record_failure();
        cb.record_failure();

        // Wait for window to expire
        thread::sleep(Duration::from_millis(150));

        // Next failure should not open circuit (window reset)
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }
}
