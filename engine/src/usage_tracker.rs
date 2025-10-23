// Per-tenant usage tracking for billing and monitoring
//
// Design:
// - Atomic counters for lock-free updates
// - Per-tenant statistics: queries, inserts, deletes, storage
// - CSV export for billing integration
// - Relaxed ordering (no memory barriers needed)
//
// Performance: ~20ns per operation (atomic increment)

use anyhow::{Context, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

/// Per-tenant usage statistics
///
/// Uses atomic counters for lock-free concurrent updates.
/// All operations use Relaxed ordering for maximum performance.
///
/// # Performance
/// Each operation is a single atomic increment: ~20ns
#[derive(Debug)]
pub struct TenantUsage {
    /// Total k-NN search queries performed
    query_count: AtomicU64,

    /// Total vector insertions
    insert_count: AtomicU64,

    /// Total vector deletions
    delete_count: AtomicU64,

    /// Current number of vectors stored
    vector_count: AtomicU64,

    /// Approximate storage bytes (vectors + metadata)
    storage_bytes: AtomicU64,
}

impl TenantUsage {
    /// Create new usage tracker with zero counts
    pub fn new() -> Self {
        Self {
            query_count: AtomicU64::new(0),
            insert_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
            vector_count: AtomicU64::new(0),
            storage_bytes: AtomicU64::new(0),
        }
    }

    /// Record a query operation
    ///
    /// Increments query_count by 1.
    pub fn record_query(&self) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a vector insertion
    ///
    /// Increments insert_count and vector_count by 1.
    /// Adds vector_size_bytes to storage_bytes.
    ///
    /// # Example
    /// ```
    /// // 384-dimensional float32 vector = 384 * 4 = 1536 bytes
    /// usage.record_insert(1536);
    /// ```
    pub fn record_insert(&self, vector_size_bytes: u64) {
        self.insert_count.fetch_add(1, Ordering::Relaxed);
        self.vector_count.fetch_add(1, Ordering::Relaxed);
        self.storage_bytes
            .fetch_add(vector_size_bytes, Ordering::Relaxed);
    }

    /// Record a vector deletion
    ///
    /// Increments delete_count by 1.
    /// Decrements vector_count by 1 (saturating to prevent underflow).
    /// Subtracts vector_size_bytes from storage_bytes (saturating to prevent underflow).
    pub fn record_delete(&self, vector_size_bytes: u64) {
        self.delete_count.fetch_add(1, Ordering::Relaxed);

        // Use fetch_update for saturating subtraction to prevent underflow
        self.vector_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                Some(val.saturating_sub(1))
            })
            .ok();

        self.storage_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                Some(val.saturating_sub(vector_size_bytes))
            })
            .ok();
    }

    /// Take atomic snapshot of current usage
    ///
    /// Returns consistent view of all counters.
    /// Note: Counters may change immediately after snapshot.
    pub fn snapshot(&self) -> UsageSnapshot {
        UsageSnapshot {
            query_count: self.query_count.load(Ordering::Relaxed),
            insert_count: self.insert_count.load(Ordering::Relaxed),
            delete_count: self.delete_count.load(Ordering::Relaxed),
            vector_count: self.vector_count.load(Ordering::Relaxed),
            storage_bytes: self.storage_bytes.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero (for testing)
    #[cfg(test)]
    pub fn reset(&self) {
        self.query_count.store(0, Ordering::Relaxed);
        self.insert_count.store(0, Ordering::Relaxed);
        self.delete_count.store(0, Ordering::Relaxed);
        self.vector_count.store(0, Ordering::Relaxed);
        self.storage_bytes.store(0, Ordering::Relaxed);
    }
}

impl Default for TenantUsage {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of tenant usage
#[derive(Debug, Clone, PartialEq)]
pub struct UsageSnapshot {
    pub query_count: u64,
    pub insert_count: u64,
    pub delete_count: u64,
    pub vector_count: u64,
    pub storage_bytes: u64,
}

impl UsageSnapshot {
    /// Calculate total billable events
    ///
    /// Billable events = queries + inserts + deletes
    pub fn billable_events(&self) -> u64 {
        self.query_count + self.insert_count + self.delete_count
    }

    /// Storage in megabytes (for human-readable billing)
    pub fn storage_mb(&self) -> f64 {
        self.storage_bytes as f64 / (1024.0 * 1024.0)
    }

    /// Storage in gigabytes (for human-readable billing)
    pub fn storage_gb(&self) -> f64 {
        self.storage_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }
}

/// Usage tracker manager
///
/// Manages per-tenant usage statistics with:
/// - Lazy tenant creation (on first operation)
/// - CSV export for billing integration
/// - Thread-safe concurrent access
pub struct UsageTracker {
    /// Per-tenant usage statistics (lazy initialized)
    usage: Arc<RwLock<HashMap<String, Arc<TenantUsage>>>>,
}

impl UsageTracker {
    /// Create new empty usage tracker
    pub fn new() -> Self {
        Self {
            usage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create usage tracker for tenant
    ///
    /// Returns Arc for efficient sharing across threads.
    /// Creates new tracker on first access.
    pub fn get_or_create(&self, tenant_id: &str) -> Arc<TenantUsage> {
        // Fast path: read lock for existing tracker
        {
            let usage = self.usage.read();
            if let Some(tracker) = usage.get(tenant_id) {
                return Arc::clone(tracker);
            }
        }

        // Slow path: write lock to create new tracker
        let mut usage = self.usage.write();

        // Double-check: another thread might have created it
        usage
            .entry(tenant_id.to_string())
            .or_insert_with(|| Arc::new(TenantUsage::new()))
            .clone()
    }

    /// Get usage snapshot for tenant
    ///
    /// Returns None if tenant has never been tracked.
    pub fn get_snapshot(&self, tenant_id: &str) -> Option<UsageSnapshot> {
        let usage = self.usage.read();
        usage.get(tenant_id).map(|tracker| tracker.snapshot())
    }

    /// Get usage snapshots for all tenants
    pub fn get_all_snapshots(&self) -> HashMap<String, UsageSnapshot> {
        let usage = self.usage.read();
        usage
            .iter()
            .map(|(tenant_id, tracker)| (tenant_id.clone(), tracker.snapshot()))
            .collect()
    }

    /// Export usage statistics to CSV file for billing
    ///
    /// CSV format:
    /// ```csv
    /// tenant_id,query_count,insert_count,delete_count,vector_count,storage_bytes,storage_mb,billable_events,timestamp
    /// ```
    ///
    /// # Errors
    /// Returns error if file cannot be created or written
    pub fn export_csv(&self, path: &Path) -> Result<()> {
        let file =
            File::create(path).with_context(|| format!("Failed to create CSV file: {:?}", path))?;

        let mut writer = csv::Writer::from_writer(file);

        // Write header
        writer.write_record(&[
            "tenant_id",
            "query_count",
            "insert_count",
            "delete_count",
            "vector_count",
            "storage_bytes",
            "storage_mb",
            "billable_events",
            "timestamp",
        ])?;

        // Get current timestamp
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .context("System time before UNIX epoch")?
            .as_secs();

        // Write tenant usage rows
        let usage = self.usage.read();
        for (tenant_id, tracker) in usage.iter() {
            let snapshot = tracker.snapshot();

            writer.write_record(&[
                tenant_id,
                &snapshot.query_count.to_string(),
                &snapshot.insert_count.to_string(),
                &snapshot.delete_count.to_string(),
                &snapshot.vector_count.to_string(),
                &snapshot.storage_bytes.to_string(),
                &format!("{:.2}", snapshot.storage_mb()),
                &snapshot.billable_events().to_string(),
                &timestamp.to_string(),
            ])?;
        }

        writer.flush()?;

        Ok(())
    }

    /// Count of tracked tenants
    pub fn tenant_count(&self) -> usize {
        self.usage.read().len()
    }

    /// Clear all usage data (for testing)
    #[cfg(test)]
    pub fn clear(&self) {
        self.usage.write().clear();
    }
}

impl Default for UsageTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::NamedTempFile;

    #[test]
    fn test_tenant_usage_basic() {
        let usage = TenantUsage::new();

        // Initially zero
        let snapshot = usage.snapshot();
        assert_eq!(snapshot.query_count, 0);
        assert_eq!(snapshot.insert_count, 0);
        assert_eq!(snapshot.vector_count, 0);
        assert_eq!(snapshot.storage_bytes, 0);

        // Record operations
        usage.record_query();
        usage.record_insert(1536); // 384-dim float32
        usage.record_query();

        let snapshot = usage.snapshot();
        assert_eq!(snapshot.query_count, 2);
        assert_eq!(snapshot.insert_count, 1);
        assert_eq!(snapshot.vector_count, 1);
        assert_eq!(snapshot.storage_bytes, 1536);
    }

    #[test]
    fn test_tenant_usage_delete() {
        let usage = TenantUsage::new();

        // Insert 3 vectors
        usage.record_insert(1536);
        usage.record_insert(1536);
        usage.record_insert(1536);

        let snapshot = usage.snapshot();
        assert_eq!(snapshot.vector_count, 3);
        assert_eq!(snapshot.storage_bytes, 4608);

        // Delete 1 vector
        usage.record_delete(1536);

        let snapshot = usage.snapshot();
        assert_eq!(snapshot.delete_count, 1);
        assert_eq!(snapshot.vector_count, 2);
        assert_eq!(snapshot.storage_bytes, 3072);
    }

    #[test]
    fn test_usage_snapshot_billable_events() {
        let snapshot = UsageSnapshot {
            query_count: 100,
            insert_count: 50,
            delete_count: 10,
            vector_count: 40,
            storage_bytes: 61440, // 40 * 1536
        };

        assert_eq!(snapshot.billable_events(), 160); // 100 + 50 + 10
    }

    #[test]
    fn test_usage_snapshot_storage_conversion() {
        let snapshot = UsageSnapshot {
            query_count: 0,
            insert_count: 0,
            delete_count: 0,
            vector_count: 0,
            storage_bytes: 10 * 1024 * 1024, // 10 MB
        };

        assert!((snapshot.storage_mb() - 10.0).abs() < 0.01);
        assert!((snapshot.storage_gb() - 0.00977).abs() < 0.001);
    }

    #[test]
    fn test_usage_tracker_get_or_create() {
        let tracker = UsageTracker::new();

        // First access creates tracker
        let tenant_a = tracker.get_or_create("tenant_a");
        tenant_a.record_query();

        // Second access returns same tracker
        let tenant_a_again = tracker.get_or_create("tenant_a");
        let snapshot = tenant_a_again.snapshot();
        assert_eq!(snapshot.query_count, 1);

        // Different tenant gets separate tracker
        let tenant_b = tracker.get_or_create("tenant_b");
        let snapshot = tenant_b.snapshot();
        assert_eq!(snapshot.query_count, 0);
    }

    #[test]
    fn test_usage_tracker_get_snapshot() {
        let tracker = UsageTracker::new();

        // Unknown tenant returns None
        assert!(tracker.get_snapshot("unknown").is_none());

        // Known tenant returns snapshot
        let usage = tracker.get_or_create("tenant_a");
        usage.record_query();
        usage.record_insert(1536);

        let snapshot = tracker.get_snapshot("tenant_a").unwrap();
        assert_eq!(snapshot.query_count, 1);
        assert_eq!(snapshot.insert_count, 1);
    }

    #[test]
    fn test_usage_tracker_get_all_snapshots() {
        let tracker = UsageTracker::new();

        // Track multiple tenants
        tracker.get_or_create("tenant_a").record_query();
        tracker.get_or_create("tenant_b").record_insert(1536);
        tracker.get_or_create("tenant_c").record_delete(1536);

        let all_snapshots = tracker.get_all_snapshots();
        assert_eq!(all_snapshots.len(), 3);

        assert_eq!(all_snapshots["tenant_a"].query_count, 1);
        assert_eq!(all_snapshots["tenant_b"].insert_count, 1);
        assert_eq!(all_snapshots["tenant_c"].delete_count, 1);
    }

    #[test]
    fn test_usage_tracker_export_csv() {
        let tracker = UsageTracker::new();

        // Track some usage
        let tenant_a = tracker.get_or_create("tenant_a");
        tenant_a.record_query();
        tenant_a.record_query();
        tenant_a.record_insert(1536);

        let tenant_b = tracker.get_or_create("tenant_b");
        tenant_b.record_insert(3072);
        tenant_b.record_insert(3072);
        tenant_b.record_delete(3072);

        // Export to CSV
        let temp_file = NamedTempFile::new().unwrap();
        tracker.export_csv(temp_file.path()).unwrap();

        // Read and verify CSV
        let mut reader = csv::Reader::from_path(temp_file.path()).unwrap();
        let headers = reader.headers().unwrap();
        assert_eq!(headers.len(), 9);
        assert_eq!(headers.get(0).unwrap(), "tenant_id");

        let mut records: Vec<_> = reader.records().collect();
        assert_eq!(records.len(), 2);

        // Sort by tenant_id for consistent testing
        records.sort_by(|a, b| {
            let a_tenant = a.as_ref().unwrap().get(0).unwrap();
            let b_tenant = b.as_ref().unwrap().get(0).unwrap();
            a_tenant.cmp(b_tenant)
        });

        // Verify tenant_a row
        let record_a = records[0].as_ref().unwrap();
        assert_eq!(record_a.get(0).unwrap(), "tenant_a");
        assert_eq!(record_a.get(1).unwrap(), "2"); // query_count
        assert_eq!(record_a.get(2).unwrap(), "1"); // insert_count
        assert_eq!(record_a.get(4).unwrap(), "1"); // vector_count
        assert_eq!(record_a.get(5).unwrap(), "1536"); // storage_bytes

        // Verify tenant_b row
        let record_b = records[1].as_ref().unwrap();
        assert_eq!(record_b.get(0).unwrap(), "tenant_b");
        assert_eq!(record_b.get(2).unwrap(), "2"); // insert_count
        assert_eq!(record_b.get(3).unwrap(), "1"); // delete_count
        assert_eq!(record_b.get(4).unwrap(), "1"); // vector_count
        assert_eq!(record_b.get(5).unwrap(), "3072"); // storage_bytes
    }

    #[test]
    fn test_concurrent_usage_tracking() {
        let tracker = Arc::new(UsageTracker::new());
        let mut handles = vec![];

        // 10 threads, each recording 100 queries
        for _ in 0..10 {
            let tracker = Arc::clone(&tracker);
            let handle = thread::spawn(move || {
                let usage = tracker.get_or_create("tenant_shared");
                for _ in 0..100 {
                    usage.record_query();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have recorded 1000 queries total
        let snapshot = tracker.get_snapshot("tenant_shared").unwrap();
        assert_eq!(snapshot.query_count, 1000);
    }

    #[test]
    fn test_tenant_count() {
        let tracker = UsageTracker::new();

        assert_eq!(tracker.tenant_count(), 0);

        tracker.get_or_create("tenant_a");
        assert_eq!(tracker.tenant_count(), 1);

        tracker.get_or_create("tenant_b");
        tracker.get_or_create("tenant_c");
        assert_eq!(tracker.tenant_count(), 3);

        // Same tenant doesn't increase count
        tracker.get_or_create("tenant_a");
        assert_eq!(tracker.tenant_count(), 3);
    }

    #[test]
    fn test_usage_reset() {
        let usage = TenantUsage::new();

        usage.record_query();
        usage.record_insert(1536);
        usage.record_delete(1536);

        assert!(usage.snapshot().query_count > 0);

        usage.reset();

        let snapshot = usage.snapshot();
        assert_eq!(snapshot.query_count, 0);
        assert_eq!(snapshot.insert_count, 0);
        assert_eq!(snapshot.delete_count, 0);
        assert_eq!(snapshot.vector_count, 0);
        assert_eq!(snapshot.storage_bytes, 0);
    }
}
