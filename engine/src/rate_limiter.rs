// Per-tenant rate limiting using token bucket algorithm
//
// Design:
// - Token bucket algorithm: Fixed refill rate, burst capacity
// - Per-tenant buckets: HashMap<tenant_id, TokenBucket>
// - Optional global bucket for total QPS cap
// - Smooth refill: Fractional tokens for consistent rate
// - Lock per bucket: Minimize contention (not global lock)
//
// Performance: ~100ns per check (HashMap lookup + bucket update)

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

/// Token bucket rate limiter
///
/// Implements token bucket algorithm with smooth refill:
/// - Tokens refill at constant rate (tokens per second)
/// - Burst capacity = max tokens
/// - Each request consumes one token
/// - Reject if no tokens available
///
/// # Example
/// ```ignore
/// let mut bucket = TokenBucket::new(100); // 100 QPS
/// assert!(bucket.try_consume()); // First request succeeds
/// ```
pub struct TokenBucket {
    /// Maximum tokens (burst capacity)
    capacity: u32,

    /// Current available tokens (float for smooth refill)
    tokens: f64,

    /// Tokens added per second
    refill_rate: f64,

    /// Last time tokens were refilled
    last_refill: Instant,
}

impl TokenBucket {
    /// Create new token bucket with given QPS limit
    ///
    /// Initially full (tokens = capacity).
    pub fn new(max_qps: u32) -> Self {
        Self {
            capacity: max_qps,
            tokens: max_qps as f64,
            refill_rate: max_qps as f64,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token
    ///
    /// Returns true if token was consumed (request allowed).
    /// Returns false if no tokens available (rate limit exceeded).
    ///
    /// Automatically refills tokens based on elapsed time.
    pub fn try_consume(&mut self) -> bool {
        self.refill();

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Refund one consumed token (best-effort).
    ///
    /// Used to maintain fairness when a downstream/global limit rejects after a tenant token
    /// was already consumed.
    pub fn refund_one(&mut self) {
        self.tokens = (self.tokens + 1.0).min(self.capacity as f64);
    }

    /// Refill tokens based on elapsed time
    ///
    /// Tokens = min(capacity, current_tokens + (elapsed_seconds * refill_rate))
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();

        if elapsed > 0.0 {
            let new_tokens = self.tokens + (elapsed * self.refill_rate);
            self.tokens = new_tokens.min(self.capacity as f64);
            self.last_refill = now;
        }
    }

    /// Get current token count (for observability)
    pub fn available_tokens(&mut self) -> f64 {
        self.refill();
        self.tokens
    }

    /// Get bucket capacity (for validation)
    pub(crate) fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Reset bucket to full capacity (for testing)
    #[cfg(test)]
    pub fn reset(&mut self) {
        self.tokens = self.capacity as f64;
        self.last_refill = Instant::now();
    }
}

/// Per-tenant rate limiter
///
/// Manages token buckets for multiple tenants with lazy initialization.
/// Each tenant gets independent rate limit enforcement.
///
/// # Example
/// ```ignore
/// let limiter = RateLimiter::new();
/// let allowed = limiter.check_limit("tenant_123", 1000);
/// if allowed {
///     // Process request
/// } else {
///     // Reject with 429 Too Many Requests
/// }
/// ```
pub struct RateLimiter {
    /// Per-tenant token buckets (lazy initialized)
    buckets: Arc<parking_lot::RwLock<HashMap<String, Arc<Mutex<TokenBucket>>>>>,
    /// Tenants already warned about capacity mismatch (avoid log spam)
    #[allow(dead_code)]
    warned_tenants: Arc<parking_lot::RwLock<HashSet<String>>>,
    /// Optional global token bucket (shared across all tenants)
    global_bucket: Option<Mutex<TokenBucket>>,
}

impl RateLimiter {
    /// Create new empty rate limiter
    pub fn new() -> Self {
        Self::new_with_global(None)
    }

    /// Create rate limiter with optional global QPS cap.
    pub fn new_with_global(max_qps_global: Option<u32>) -> Self {
        Self {
            buckets: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            warned_tenants: Arc::new(parking_lot::RwLock::new(HashSet::new())),
            global_bucket: max_qps_global.map(TokenBucket::new).map(Mutex::new),
        }
    }

    /// Check rate limit for tenant
    ///
    /// Returns true if request is allowed (within rate limit).
    /// Returns false if rate limit exceeded (should reject with 429).
    ///
    /// Token bucket is created on first request for tenant with the provided max_qps.
    /// Subsequent calls should use the same max_qps value.
    ///
    /// # Performance
    /// - O(1) HashMap lookup: ~50ns
    /// - Token bucket update: ~50ns
    /// - Total: ~100ns per request
    ///
    pub fn check_limit(&self, tenant_id: &str, max_qps: u32) -> bool {
        // Fast path: read lock for existing bucket
        {
            let buckets = self.buckets.read();
            if let Some(bucket) = buckets.get(tenant_id) {
                // Validate that max_qps matches existing bucket capacity
                let bucket_capacity = bucket.lock().capacity();
                if bucket_capacity != max_qps {
                    // Defensive: never panic on a misconfiguration; continue using the existing bucket.
                    // This mismatch should be impossible in normal operation.
                    #[cfg(debug_assertions)]
                    debug_assert_eq!(
                        bucket_capacity, max_qps,
                        "Rate limit mismatch for tenant {}: existing capacity {}, requested {}",
                        tenant_id, bucket_capacity, max_qps
                    );
                    #[cfg(not(debug_assertions))]
                    {
                        let mut warned = self.warned_tenants.write();
                        if !warned.contains(tenant_id) {
                            tracing::warn!(
                                tenant_id = %tenant_id,
                                existing_capacity = bucket_capacity,
                                requested_qps = max_qps,
                                "Rate limit mismatch: using existing bucket capacity"
                            );
                            warned.insert(tenant_id.to_string());
                        }
                    }
                }

                let bucket = Arc::clone(bucket);
                drop(buckets); // Release read lock before acquiring mutex

                // Consume tenant token first.
                if !bucket.lock().try_consume() {
                    return false;
                }

                // Consume global token only after tenant passes.
                if let Some(global) = &self.global_bucket {
                    if !global.lock().try_consume() {
                        bucket.lock().refund_one();
                        return false;
                    }
                }

                return true;
            }
        }

        // Slow path: write lock to create new bucket
        let bucket = {
            let mut buckets = self.buckets.write();

            // Double-check: another thread might have created it
            Arc::clone(
                buckets
                    .entry(tenant_id.to_string())
                    .or_insert_with(|| Arc::new(Mutex::new(TokenBucket::new(max_qps)))),
            )
        };

        // Consume tenant token after releasing write lock.
        if !bucket.lock().try_consume() {
            return false;
        }

        // Consume global token only after tenant passes.
        if let Some(global) = &self.global_bucket {
            if !global.lock().try_consume() {
                bucket.lock().refund_one();
                return false;
            }
        }

        true
    }

    /// Get current available tokens for tenant (for observability)
    pub fn available_tokens(&self, tenant_id: &str) -> Option<f64> {
        let buckets = self.buckets.read();
        buckets
            .get(tenant_id)
            .map(|bucket| bucket.lock().available_tokens())
    }

    /// Reset rate limit for tenant (for testing)
    #[cfg(test)]
    pub fn reset_tenant(&self, tenant_id: &str) {
        let buckets = self.buckets.read();
        if let Some(bucket) = buckets.get(tenant_id) {
            bucket.lock().reset();
        }
        self.warned_tenants.write().remove(tenant_id);
    }

    /// Clear all rate limit state (for testing)
    #[cfg(test)]
    pub fn clear(&self) {
        self.buckets.write().clear();
        self.warned_tenants.write().clear();
    }

    /// Get count of tracked tenants
    pub fn tenant_count(&self) -> usize {
        self.buckets.read().len()
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn test_token_bucket_basic() {
        let mut bucket = TokenBucket::new(10);

        // Initially full: 10 requests succeed
        for _ in 0..10 {
            assert!(bucket.try_consume());
        }

        // 11th request fails (bucket empty)
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_token_bucket_refill() {
        let mut bucket = TokenBucket::new(10);

        // Drain bucket
        for _ in 0..10 {
            assert!(bucket.try_consume());
        }
        assert!(!bucket.try_consume());

        // Wait 200ms (should refill ~2 tokens at 10 QPS)
        thread::sleep(Duration::from_millis(200));

        // Should allow 2 more requests
        assert!(bucket.try_consume());
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume()); // 3rd fails
    }

    #[test]
    fn test_token_bucket_smooth_refill() {
        let mut bucket = TokenBucket::new(1000); // 1000 QPS

        // Consume all tokens
        for _ in 0..1000 {
            assert!(bucket.try_consume());
        }

        // Wait 10ms (should refill ~10 tokens at 1000 QPS)
        thread::sleep(Duration::from_millis(10));

        let available = bucket.available_tokens();
        // Allow wider range due to timing variance across systems
        assert!(
            (5.0..=20.0).contains(&available),
            "Expected ~10 tokens, got {}",
            available
        );
    }

    #[test]
    fn test_token_bucket_capped_at_capacity() {
        let mut bucket = TokenBucket::new(10);

        // Wait 2 seconds (would refill 20 tokens at 10 QPS)
        thread::sleep(Duration::from_secs(2));

        // Should be capped at capacity (10)
        let available = bucket.available_tokens();
        assert_eq!(available, 10.0);
    }

    #[test]
    fn test_rate_limiter_per_tenant() {
        let limiter = RateLimiter::new();

        // Tenant A: 10 QPS
        for _ in 0..10 {
            assert!(limiter.check_limit("tenant_a", 10));
        }
        assert!(!limiter.check_limit("tenant_a", 10)); // 11th fails

        // Tenant B: independent limit, should succeed
        for _ in 0..10 {
            assert!(limiter.check_limit("tenant_b", 10));
        }
        assert!(!limiter.check_limit("tenant_b", 10)); // 11th fails

        // Both tenants tracked
        assert_eq!(limiter.tenant_count(), 2);
    }

    #[test]
    fn test_rate_limiter_different_limits() {
        let limiter = RateLimiter::new();

        // Tenant A: 100 QPS
        for _ in 0..100 {
            assert!(limiter.check_limit("tenant_a", 100));
        }
        assert!(!limiter.check_limit("tenant_a", 100));

        // Tenant B: 10 QPS (much lower)
        for _ in 0..10 {
            assert!(limiter.check_limit("tenant_b", 10));
        }
        assert!(!limiter.check_limit("tenant_b", 10));
    }

    #[test]
    fn test_rate_limiter_refill() {
        let limiter = RateLimiter::new();

        // Drain tenant's bucket
        for _ in 0..10 {
            assert!(limiter.check_limit("tenant_a", 10));
        }
        assert!(!limiter.check_limit("tenant_a", 10));

        // Wait for refill
        thread::sleep(Duration::from_millis(200));

        // Should allow ~2 more requests
        assert!(limiter.check_limit("tenant_a", 10));
        assert!(limiter.check_limit("tenant_a", 10));
        assert!(!limiter.check_limit("tenant_a", 10)); // 3rd fails
    }

    #[test]
    fn test_rate_limiter_concurrent_access() {
        use std::sync::Arc;

        let limiter = Arc::new(RateLimiter::new());
        let mut handles = vec![];

        // 10 threads, each trying to consume 10 tokens
        for _ in 0..10 {
            let limiter = Arc::clone(&limiter);
            let handle = thread::spawn(move || {
                let mut count = 0;
                for _ in 0..10 {
                    if limiter.check_limit("tenant_shared", 50) {
                        count += 1;
                    }
                }
                count
            });
            handles.push(handle);
        }

        // Collect results
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        // Should allow ~50 requests total (50 QPS bucket)
        // Some threads will be rejected
        assert!(
            (45..=55).contains(&total),
            "Expected ~50 requests, got {}",
            total
        );
    }

    #[test]
    fn test_available_tokens() {
        let limiter = RateLimiter::new();

        // New tenant: should have full bucket
        limiter.check_limit("tenant_a", 100);
        let available = limiter.available_tokens("tenant_a").unwrap();
        assert!((99.0..=100.0).contains(&available));

        // Consume more tokens
        for _ in 0..50 {
            limiter.check_limit("tenant_a", 100);
        }

        let available = limiter.available_tokens("tenant_a").unwrap();
        assert!(
            (48.0..=50.0).contains(&available),
            "Expected ~49 tokens, got {}",
            available
        );

        // Unknown tenant: returns None
        assert!(limiter.available_tokens("unknown").is_none());
    }

    #[test]
    fn test_reset_tenant() {
        let limiter = RateLimiter::new();

        // Drain bucket
        for _ in 0..10 {
            limiter.check_limit("tenant_a", 10);
        }
        assert!(!limiter.check_limit("tenant_a", 10));

        // Reset
        limiter.reset_tenant("tenant_a");

        // Should work again
        for _ in 0..10 {
            assert!(limiter.check_limit("tenant_a", 10));
        }
    }

    #[test]
    fn test_high_qps_precision() {
        let mut bucket = TokenBucket::new(10000); // 10K QPS

        // Start from a deterministic empty state.
        bucket.tokens = 0.0;
        bucket.last_refill = Instant::now();

        // Measure elapsed time around refill to avoid scheduler-dependent flakiness.
        // Under sanitizers, the observed elapsed time can be much larger than 1ms.
        let refill_start = Instant::now();
        thread::sleep(Duration::from_millis(1));
        let elapsed_before = refill_start.elapsed().as_secs_f64();
        let available = bucket.available_tokens();
        let elapsed_after = refill_start.elapsed().as_secs_f64();
        let expected_min = (elapsed_before * 10_000.0).min(10_000.0);
        let expected_max = (elapsed_after * 10_000.0).min(10_000.0);
        let slack = 5.0;

        assert!(
            available + slack >= expected_min && available <= expected_max + slack,
            "Expected refill within [{:.3}, {:.3}] (+/- {:.1}) tokens, got {:.3} (elapsed_before={:.6}s elapsed_after={:.6}s)",
            expected_min,
            expected_max,
            slack,
            available,
            elapsed_before,
            elapsed_after
        );
    }

    #[test]
    fn test_burst_capacity() {
        let mut bucket = TokenBucket::new(100);

        // Consume half
        for _ in 0..50 {
            assert!(bucket.try_consume());
        }

        // Wait 1 second (refills 100 tokens, but capped at capacity)
        thread::sleep(Duration::from_secs(1));

        // Should have exactly 100 tokens (not 150)
        for _ in 0..100 {
            assert!(bucket.try_consume());
        }
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_global_limit_caps_total_throughput() {
        // Global limit of 5 QPS, tenant limit of 10 QPS.
        // The global bucket should reject after 5 requests regardless of tenant allowance.
        let limiter = RateLimiter::new_with_global(Some(5));

        let mut allowed = 0;
        for _ in 0..10 {
            if limiter.check_limit("tenant_a", 10) {
                allowed += 1;
            }
        }
        assert_eq!(allowed, 5, "global limit must cap total throughput at 5");
    }

    #[test]
    fn test_global_limit_refunds_tenant_token_on_reject() {
        // Global limit of 2. After 2 requests, global bucket is exhausted.
        // Verify the tenant's tokens are refunded so they aren't leaked.
        let limiter = RateLimiter::new_with_global(Some(2));

        assert!(limiter.check_limit("tenant_a", 100));
        assert!(limiter.check_limit("tenant_a", 100));

        // 3rd request: tenant has tokens but global is exhausted → tenant token refunded.
        assert!(!limiter.check_limit("tenant_a", 100));

        let available = limiter.available_tokens("tenant_a").unwrap();
        // 100 capacity - 2 consumed (third was refunded) = ~98
        assert!(
            available >= 97.0,
            "tenant token should have been refunded, got {}",
            available
        );
    }

    #[test]
    fn test_global_limit_shared_across_tenants() {
        // Global limit of 3. Two tenants each with per-tenant limit of 10.
        // Combined requests across both tenants must not exceed 3.
        let limiter = RateLimiter::new_with_global(Some(3));

        assert!(limiter.check_limit("tenant_a", 10));
        assert!(limiter.check_limit("tenant_b", 10));
        assert!(limiter.check_limit("tenant_a", 10));

        // Global is now exhausted
        assert!(!limiter.check_limit("tenant_a", 10));
        assert!(!limiter.check_limit("tenant_b", 10));
    }

    #[test]
    fn test_no_global_limit_unlimited() {
        // Without global limit, only per-tenant limits apply.
        let limiter = RateLimiter::new_with_global(None);

        let mut allowed = 0;
        for _ in 0..50 {
            if limiter.check_limit("tenant_a", 50) {
                allowed += 1;
            }
        }
        assert_eq!(
            allowed, 50,
            "without global limit, all 50 requests should pass"
        );
    }
}
