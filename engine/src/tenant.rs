// Tenant isolation and namespace management for multi-tenant vector database
//
// Design:
// - Namespace strategy: Prefix document IDs with tenant_id
// - Format: <tenant_id>:<doc_id> (e.g., "acme_corp:12345")
// - Benefits: Simple, no separate indexes, works with existing HNSW
// - Lock-free doc_id allocation per tenant using atomics
//
// Performance: ~10ns for namespace operations (string formatting)

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Tenant-aware document ID manager
///
/// Provides:
/// - Sequential doc_id allocation per tenant
/// - Namespace prefixing for isolation
/// - Result filtering to enforce boundaries
///
/// # Example
/// ```
/// let manager = TenantManager::new();
/// let doc_id = manager.allocate_doc_id("acme_corp");
/// let namespaced = manager.namespace_doc_id("acme_corp", doc_id);
/// // namespaced = "acme_corp:0"
/// ```
pub struct TenantManager {
    /// Per-tenant doc_id counters (atomic for lock-free allocation)
    next_doc_id: Arc<RwLock<HashMap<String, Arc<AtomicU64>>>>,
}

impl TenantManager {
    /// Create new tenant manager
    pub fn new() -> Self {
        Self {
            next_doc_id: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Validate tenant_id format
    ///
    /// Ensures tenant_id does not contain colon (used as namespace separator).
    ///
    /// # Panics
    /// Panics if tenant_id is empty or contains a colon.
    fn validate_tenant_id(tenant_id: &str) {
        assert!(!tenant_id.is_empty(), "tenant_id cannot be empty");
        assert!(
            !tenant_id.contains(':'),
            "tenant_id cannot contain colon (reserved as namespace separator): {}",
            tenant_id
        );
    }

    /// Allocate new document ID for tenant
    ///
    /// Returns sequential ID starting from 0 for each tenant.
    /// Thread-safe and lock-free (uses atomic counter).
    ///
    /// # Performance
    /// O(1) with atomic fetch_add: ~10ns
    ///
    /// # Panics
    /// Panics if tenant_id is empty or contains a colon.
    pub fn allocate_doc_id(&self, tenant_id: &str) -> u64 {
        Self::validate_tenant_id(tenant_id);
        // Fast path: read lock for existing counter
        {
            let counters = self.next_doc_id.read();
            if let Some(counter) = counters.get(tenant_id) {
                return counter.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Slow path: write lock to create new counter
        let mut counters = self.next_doc_id.write();

        // Double-check: another thread might have created it
        let counter = counters
            .entry(tenant_id.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));

        counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Create tenant-namespaced document ID for storage
    ///
    /// Format: `<tenant_id>:<doc_id>`
    ///
    /// # Example
    /// ```
    /// let namespaced = manager.namespace_doc_id("acme_corp", 12345);
    /// assert_eq!(namespaced, "acme_corp:12345");
    /// ```
    ///
    /// # Panics
    /// Panics if tenant_id is empty or contains a colon.
    pub fn namespace_doc_id(&self, tenant_id: &str, doc_id: u64) -> String {
        Self::validate_tenant_id(tenant_id);
        format!("{}:{}", tenant_id, doc_id)
    }

    /// Parse tenant_id from namespaced document ID
    ///
    /// Returns None if format is invalid (no colon separator).
    ///
    /// # Example
    /// ```
    /// let tenant_id = manager.parse_tenant_id("acme_corp:12345");
    /// assert_eq!(tenant_id, Some("acme_corp".to_string()));
    /// ```
    pub fn parse_tenant_id(&self, namespaced_id: &str) -> Option<String> {
        namespaced_id
            .split_once(':')
            .map(|(tenant_id, _)| tenant_id.to_string())
    }

    /// Parse doc_id from namespaced document ID
    ///
    /// Returns None if format is invalid or doc_id is not a valid u64.
    ///
    /// # Example
    /// ```
    /// let doc_id = manager.parse_doc_id("acme_corp:12345");
    /// assert_eq!(doc_id, Some(12345));
    /// ```
    pub fn parse_doc_id(&self, namespaced_id: &str) -> Option<u64> {
        namespaced_id
            .split_once(':')
            .and_then(|(_, doc_id_str)| doc_id_str.parse::<u64>().ok())
    }

    /// Check if document belongs to tenant
    ///
    /// Returns true if namespaced_id starts with `<tenant_id>:`.
    pub fn belongs_to_tenant(&self, namespaced_id: &str, tenant_id: &str) -> bool {
        let prefix = format!("{}:", tenant_id);
        namespaced_id.starts_with(&prefix)
    }

    /// Get current doc_id counter for tenant (for observability)
    ///
    /// Returns 0 if tenant has not allocated any doc_ids yet.
    pub fn current_doc_id(&self, tenant_id: &str) -> u64 {
        let counters = self.next_doc_id.read();
        counters
            .get(tenant_id)
            .map(|counter| counter.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get count of tracked tenants
    pub fn tenant_count(&self) -> usize {
        self.next_doc_id.read().len()
    }

    /// Reset doc_id counter for tenant (for testing)
    #[cfg(test)]
    pub fn reset_tenant(&self, tenant_id: &str) {
        let counters = self.next_doc_id.read();
        if let Some(counter) = counters.get(tenant_id) {
            counter.store(0, Ordering::Relaxed);
        }
    }

    /// Clear all tenant state (for testing)
    #[cfg(test)]
    pub fn clear(&self) {
        self.next_doc_id.write().clear();
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Search result with document ID and distance
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    /// Namespaced document ID (format: tenant_id:doc_id)
    pub id: String,

    /// Distance/similarity score
    pub distance: f32,
}

impl SearchResult {
    pub fn new(id: String, distance: f32) -> Self {
        Self { id, distance }
    }
}

/// Filter search results to only include tenant's documents
///
/// This is a critical security boundary: ensures Tenant A cannot see Tenant B's data.
///
/// # Example
/// ```
/// let results = vec![
///     SearchResult::new("acme:1".to_string(), 0.1),
///     SearchResult::new("startup:5".to_string(), 0.2),
///     SearchResult::new("acme:3".to_string(), 0.3),
/// ];
/// let filtered = filter_tenant_results(results, "acme");
/// assert_eq!(filtered.len(), 2); // Only acme:1 and acme:3
/// ```
pub fn filter_tenant_results(results: Vec<SearchResult>, tenant_id: &str) -> Vec<SearchResult> {
    let prefix = format!("{}:", tenant_id);
    results
        .into_iter()
        .filter(|r| r.id.starts_with(&prefix))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_allocate_doc_id_sequential() {
        let manager = TenantManager::new();

        // Allocate IDs for tenant A
        assert_eq!(manager.allocate_doc_id("tenant_a"), 0);
        assert_eq!(manager.allocate_doc_id("tenant_a"), 1);
        assert_eq!(manager.allocate_doc_id("tenant_a"), 2);

        // Allocate IDs for tenant B (independent sequence)
        assert_eq!(manager.allocate_doc_id("tenant_b"), 0);
        assert_eq!(manager.allocate_doc_id("tenant_b"), 1);

        // Tenant A continues from 3
        assert_eq!(manager.allocate_doc_id("tenant_a"), 3);
    }

    #[test]
    fn test_namespace_doc_id() {
        let manager = TenantManager::new();

        let namespaced = manager.namespace_doc_id("acme_corp", 12345);
        assert_eq!(namespaced, "acme_corp:12345");

        let namespaced = manager.namespace_doc_id("startup_x", 0);
        assert_eq!(namespaced, "startup_x:0");
    }

    #[test]
    fn test_parse_tenant_id() {
        let manager = TenantManager::new();

        assert_eq!(
            manager.parse_tenant_id("acme_corp:12345"),
            Some("acme_corp".to_string())
        );

        assert_eq!(
            manager.parse_tenant_id("startup_x:0"),
            Some("startup_x".to_string())
        );

        // Invalid format: no colon
        assert_eq!(manager.parse_tenant_id("invalid"), None);

        // Empty doc_id is OK (just returns tenant part)
        assert_eq!(manager.parse_tenant_id("acme:"), Some("acme".to_string()));
    }

    #[test]
    fn test_parse_doc_id() {
        let manager = TenantManager::new();

        assert_eq!(manager.parse_doc_id("acme_corp:12345"), Some(12345));
        assert_eq!(manager.parse_doc_id("startup_x:0"), Some(0));

        // Invalid: no colon
        assert_eq!(manager.parse_doc_id("invalid"), None);

        // Invalid: doc_id not a number
        assert_eq!(manager.parse_doc_id("acme:abc"), None);
    }

    #[test]
    fn test_belongs_to_tenant() {
        let manager = TenantManager::new();

        assert!(manager.belongs_to_tenant("acme:123", "acme"));
        assert!(manager.belongs_to_tenant("startup_x:0", "startup_x"));

        // Different tenant
        assert!(!manager.belongs_to_tenant("acme:123", "startup_x"));

        // Prefix match (not exact)
        assert!(!manager.belongs_to_tenant("acme_corp:123", "acme"));
    }

    #[test]
    fn test_current_doc_id() {
        let manager = TenantManager::new();

        // No allocations yet
        assert_eq!(manager.current_doc_id("tenant_a"), 0);

        // Allocate some IDs
        manager.allocate_doc_id("tenant_a");
        manager.allocate_doc_id("tenant_a");
        manager.allocate_doc_id("tenant_a");

        // Counter is at 3 (next ID to allocate)
        assert_eq!(manager.current_doc_id("tenant_a"), 3);
    }

    #[test]
    fn test_tenant_count() {
        let manager = TenantManager::new();

        assert_eq!(manager.tenant_count(), 0);

        manager.allocate_doc_id("tenant_a");
        assert_eq!(manager.tenant_count(), 1);

        manager.allocate_doc_id("tenant_b");
        assert_eq!(manager.tenant_count(), 2);

        // Same tenant doesn't increase count
        manager.allocate_doc_id("tenant_a");
        assert_eq!(manager.tenant_count(), 2);
    }

    #[test]
    fn test_concurrent_allocation() {
        let manager = Arc::new(TenantManager::new());
        let mut handles = vec![];

        // 10 threads, each allocating 100 doc_ids
        for _ in 0..10 {
            let manager = Arc::clone(&manager);
            let handle = thread::spawn(move || {
                let mut ids = vec![];
                for _ in 0..100 {
                    ids.push(manager.allocate_doc_id("tenant_shared"));
                }
                ids
            });
            handles.push(handle);
        }

        // Collect all allocated IDs
        let mut all_ids = HashSet::new();
        for handle in handles {
            let ids = handle.join().unwrap();
            for id in ids {
                all_ids.insert(id);
            }
        }

        // Should have allocated 1000 unique IDs (0..999)
        assert_eq!(all_ids.len(), 1000);
        assert_eq!(manager.current_doc_id("tenant_shared"), 1000);
    }

    #[test]
    fn test_filter_tenant_results() {
        let results = vec![
            SearchResult::new("acme:1".to_string(), 0.1),
            SearchResult::new("startup:5".to_string(), 0.2),
            SearchResult::new("acme:3".to_string(), 0.3),
            SearchResult::new("startup:2".to_string(), 0.4),
            SearchResult::new("acme:7".to_string(), 0.5),
        ];

        let acme_results = filter_tenant_results(results.clone(), "acme");
        assert_eq!(acme_results.len(), 3);
        assert_eq!(acme_results[0].id, "acme:1");
        assert_eq!(acme_results[1].id, "acme:3");
        assert_eq!(acme_results[2].id, "acme:7");

        let startup_results = filter_tenant_results(results, "startup");
        assert_eq!(startup_results.len(), 2);
        assert_eq!(startup_results[0].id, "startup:5");
        assert_eq!(startup_results[1].id, "startup:2");
    }

    #[test]
    fn test_filter_tenant_results_empty() {
        let results = vec![
            SearchResult::new("acme:1".to_string(), 0.1),
            SearchResult::new("acme:2".to_string(), 0.2),
        ];

        // No results for different tenant
        let filtered = filter_tenant_results(results, "startup");
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_filter_tenant_results_no_prefix_collision() {
        let results = vec![
            SearchResult::new("acme:1".to_string(), 0.1),
            SearchResult::new("acme_corp:2".to_string(), 0.2),
            SearchResult::new("acme_startup:3".to_string(), 0.3),
        ];

        // Should only match exact "acme:" prefix
        let filtered = filter_tenant_results(results, "acme");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "acme:1");
    }

    #[test]
    fn test_full_workflow() {
        let manager = TenantManager::new();

        // Tenant A inserts 3 documents
        let doc_id_a1 = manager.allocate_doc_id("tenant_a");
        let doc_id_a2 = manager.allocate_doc_id("tenant_a");
        let doc_id_a3 = manager.allocate_doc_id("tenant_a");

        let namespaced_a1 = manager.namespace_doc_id("tenant_a", doc_id_a1);
        let namespaced_a2 = manager.namespace_doc_id("tenant_a", doc_id_a2);
        let namespaced_a3 = manager.namespace_doc_id("tenant_a", doc_id_a3);

        // Tenant B inserts 2 documents
        let doc_id_b1 = manager.allocate_doc_id("tenant_b");
        let doc_id_b2 = manager.allocate_doc_id("tenant_b");

        let namespaced_b1 = manager.namespace_doc_id("tenant_b", doc_id_b1);
        let namespaced_b2 = manager.namespace_doc_id("tenant_b", doc_id_b2);

        // Simulate search results (mixed tenants)
        let search_results = vec![
            SearchResult::new(namespaced_a1.clone(), 0.1),
            SearchResult::new(namespaced_b1.clone(), 0.15),
            SearchResult::new(namespaced_a2.clone(), 0.2),
            SearchResult::new(namespaced_b2.clone(), 0.25),
            SearchResult::new(namespaced_a3.clone(), 0.3),
        ];

        // Filter for Tenant A
        let a_results = filter_tenant_results(search_results.clone(), "tenant_a");
        assert_eq!(a_results.len(), 3);
        assert_eq!(a_results[0].id, namespaced_a1);
        assert_eq!(a_results[1].id, namespaced_a2);
        assert_eq!(a_results[2].id, namespaced_a3);

        // Filter for Tenant B
        let b_results = filter_tenant_results(search_results, "tenant_b");
        assert_eq!(b_results.len(), 2);
        assert_eq!(b_results[0].id, namespaced_b1);
        assert_eq!(b_results[1].id, namespaced_b2);
    }

    #[test]
    fn test_reset_tenant() {
        let manager = TenantManager::new();

        manager.allocate_doc_id("tenant_a");
        manager.allocate_doc_id("tenant_a");
        assert_eq!(manager.current_doc_id("tenant_a"), 2);

        manager.reset_tenant("tenant_a");
        assert_eq!(manager.current_doc_id("tenant_a"), 0);

        // Next allocation starts from 0 again
        assert_eq!(manager.allocate_doc_id("tenant_a"), 0);
    }

    #[test]
    #[should_panic(expected = "tenant_id cannot be empty")]
    fn test_empty_tenant_id_panics() {
        let manager = TenantManager::new();
        manager.allocate_doc_id("");
    }

    #[test]
    #[should_panic(expected = "tenant_id cannot contain colon")]
    fn test_tenant_id_with_colon_panics() {
        let manager = TenantManager::new();
        manager.allocate_doc_id("evil:corp");
    }

    #[test]
    #[should_panic(expected = "tenant_id cannot contain colon")]
    fn test_namespace_doc_id_with_colon_panics() {
        let manager = TenantManager::new();
        manager.namespace_doc_id("evil:corp", 123);
    }

    #[test]
    fn test_tenant_id_validation_edge_cases() {
        let manager = TenantManager::new();

        // Valid tenant IDs with underscores
        assert_eq!(manager.allocate_doc_id("acme_corp"), 0);
        assert_eq!(manager.allocate_doc_id("startup_x_2025"), 0);

        // Valid tenant IDs with numbers
        assert_eq!(manager.allocate_doc_id("tenant123"), 0);
        assert_eq!(manager.allocate_doc_id("org_456_test"), 0);
    }
}
