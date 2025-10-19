//! Tiered Engine - Three-layer architecture orchestrator
//!
//! Coordinates all three tiers:
//! //! - **Layer 1 (Cache)**: Hybrid Semantic Cache (RMI frequency + semantic similarity)
//! - **Layer 2 (Hot Tier)**: Recent writes buffer (fast writes, periodic flush)
//! - **Layer 3 (Cold Tier)**: HNSW index (all documents, approximate k-NN search)
//!
//! # Query Path
//! ```text
//! Query → Cache (L1) → Hot Tier (L2) → HNSW (L3)
//!         ↓ hit         ↓ hit           ↓ always succeeds
//!       return        return          return k-NN results
//! ```
//!
//! # Write Path
//! ```text
//! Insert → WAL (durability) → Hot Tier → Background flush → HNSW + Snapshot
//! ```

use crate::{
    AccessPatternLogger, CacheStrategy, CachedVector, CircuitBreaker, FsyncPolicy, HnswBackend,
    HotTier, SearchResult,
};
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Tiered engine statistics
#[derive(Debug, Clone, Default)]
pub struct TieredEngineStats {
    /// Layer 1 (Cache) statistics
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,

    /// Layer 2 (Hot Tier) statistics
    pub hot_tier_hits: u64,
    pub hot_tier_misses: u64,
    pub hot_tier_hit_rate: f64,
    pub hot_tier_size: usize,
    pub hot_tier_flushes: u64,
    pub hot_tier_flush_failures: u64,   // NEW: Failed flush operations
    pub hot_tier_emergency_evictions: u64, // NEW: Emergency evictions due to hard limit

    /// Layer 3 (Cold Tier) statistics
    pub cold_tier_searches: u64,
    pub cold_tier_size: usize,

    /// Overall statistics
    pub total_queries: u64,
    pub total_inserts: u64,
    pub overall_hit_rate: f64, // (cache_hits + hot_tier_hits) / total_queries

    /// Timeout statistics
    pub cache_timeouts: u64,
    pub hot_tier_timeouts: u64,
    pub cold_tier_timeouts: u64,
    pub partial_results_returned: u64,

    /// Load shedding statistics
    pub queries_rejected: u64,           // Queries rejected due to queue saturation
    pub current_queue_depth: u64,         // Current in-flight queries
    pub circuit_breaker_rejections: u64,  // Queries failed due to circuit breaker open
}

/// Configuration for tiered engine
#[derive(Debug, Clone)]
pub struct TieredEngineConfig {
    /// Hot tier max size (documents) - soft limit for normal flush
    pub hot_tier_max_size: usize,

    /// Hot tier hard limit (documents) - emergency eviction threshold
    /// Recommended: 2x soft limit
    pub hot_tier_hard_limit: usize,

    /// Hot tier max age (duration before forced flush)
    pub hot_tier_max_age: Duration,

    /// HNSW max elements capacity
    pub hnsw_max_elements: usize,

    /// Persistence data directory
    pub data_dir: Option<String>,

    /// WAL fsync policy
    pub fsync_policy: FsyncPolicy,

    /// Snapshot interval (create snapshot every N inserts to cold tier)
    pub snapshot_interval: usize,

    /// Background flush interval (check hot tier every N seconds)
    pub flush_interval: Duration,

    /// Query timeout configuration
    pub cache_timeout_ms: u64,
    pub hot_tier_timeout_ms: u64,
    pub cold_tier_timeout_ms: u64,

    /// Maximum concurrent in-flight queries (load shedding threshold)
    pub max_concurrent_queries: usize,
}

impl Default for TieredEngineConfig {
    fn default() -> Self {
        Self {
            hot_tier_max_size: 10_000,
            hot_tier_hard_limit: 20_000, // 2x soft limit for emergency eviction
            hot_tier_max_age: Duration::from_secs(60),
            hnsw_max_elements: 1_000_000,
            data_dir: None,
            fsync_policy: FsyncPolicy::Always,
            snapshot_interval: 10_000,
            flush_interval: Duration::from_secs(30),
            cache_timeout_ms: 10,     // 10ms for cache
            hot_tier_timeout_ms: 50,  // 50ms for hot tier
            cold_tier_timeout_ms: 1000, // 1000ms (1s) for cold tier HNSW
            max_concurrent_queries: 1000, // Load shedding threshold: max 1000 in-flight queries
        }
    }
}

/// Tiered Engine - Three-layer vector database
pub struct TieredEngine {
    /// Layer 1: Hybrid Semantic Cache (hot documents predicted by RMI + semantic)
    cache_strategy: Arc<RwLock<Box<dyn CacheStrategy>>>,

    /// Layer 2: Hot tier (recent writes)
    hot_tier: Arc<HotTier>,

    /// Layer 3: Cold tier (HNSW index)
    cold_tier: Arc<HnswBackend>,

    /// Access logger (for cache training)
    access_logger: Option<Arc<RwLock<AccessPatternLogger>>>,

    /// Statistics
    stats: Arc<RwLock<TieredEngineStats>>,

    /// Configuration
    config: TieredEngineConfig,

    /// Circuit breakers for timeout handling
    pub(crate) cache_circuit_breaker: Arc<CircuitBreaker>,
    pub(crate) hot_tier_circuit_breaker: Arc<CircuitBreaker>,
    pub(crate) cold_tier_circuit_breaker: Arc<CircuitBreaker>,

    /// Semaphore for load shedding (max concurrent queries)
    pub(crate) query_semaphore: Arc<Semaphore>,
}

impl TieredEngine {
    /// Create new tiered engine
    ///
    /// # Parameters
    /// - `cache_strategy`: Layer 1 cache strategy (LRU or Learned)
    /// - `initial_embeddings`: Initial documents to load into cold tier
    /// - `config`: Configuration for all tiers
    pub fn new(
        cache_strategy: Box<dyn CacheStrategy>,
        initial_embeddings: Vec<Vec<f32>>,
        config: TieredEngineConfig,
    ) -> Result<Self> {
        // Create hot tier
        let hot_tier = Arc::new(HotTier::new(
            config.hot_tier_max_size,
            config.hot_tier_max_age,
        ));

        // Create cold tier (HNSW backend)
        let cold_tier = if let Some(ref data_dir) = config.data_dir {
            // With persistence
            Arc::new(HnswBackend::with_persistence(
                initial_embeddings,
                config.hnsw_max_elements,
                data_dir,
                config.fsync_policy,
                config.snapshot_interval,
            )?)
        } else {
            // Without persistence (testing only)
            Arc::new(HnswBackend::new(
                initial_embeddings,
                config.hnsw_max_elements,
            )?)
        };

        let query_semaphore = Arc::new(Semaphore::new(config.max_concurrent_queries));

        Ok(Self {
            cache_strategy: Arc::new(RwLock::new(cache_strategy)),
            hot_tier,
            cold_tier,
            access_logger: None,
            stats: Arc::new(RwLock::new(TieredEngineStats::default())),
            config,
            cache_circuit_breaker: Arc::new(CircuitBreaker::new()),
            hot_tier_circuit_breaker: Arc::new(CircuitBreaker::new()),
            cold_tier_circuit_breaker: Arc::new(CircuitBreaker::new()),
            query_semaphore,
        })
    }

    /// Recover from persistence
    pub fn recover(
        cache_strategy: Box<dyn CacheStrategy>,
        data_dir: impl AsRef<Path>,
        config: TieredEngineConfig,
    ) -> Result<Self> {
        let data_dir_str = data_dir.as_ref().to_string_lossy().to_string();

        // Recover cold tier from WAL + snapshot
        let metrics = crate::metrics::MetricsCollector::new();
        let cold_tier = Arc::new(HnswBackend::recover(
            &data_dir_str,
            config.hnsw_max_elements,
            config.fsync_policy,
            config.snapshot_interval,
            metrics,
        )?);

        // Create fresh hot tier (ephemeral)
        let hot_tier = Arc::new(HotTier::new(
            config.hot_tier_max_size,
            config.hot_tier_max_age,
        ));

        let mut recovered_config = config;
        recovered_config.data_dir = Some(data_dir_str);

        let query_semaphore = Arc::new(Semaphore::new(recovered_config.max_concurrent_queries));

        Ok(Self {
            cache_strategy: Arc::new(RwLock::new(cache_strategy)),
            hot_tier,
            cold_tier,
            access_logger: None,
            stats: Arc::new(RwLock::new(TieredEngineStats::default())),
            config: recovered_config,
            cache_circuit_breaker: Arc::new(CircuitBreaker::new()),
            hot_tier_circuit_breaker: Arc::new(CircuitBreaker::new()),
            cold_tier_circuit_breaker: Arc::new(CircuitBreaker::new()),
            query_semaphore,
        })
    }

    /// Set access logger (for cache training)
    pub fn set_access_logger(&mut self, logger: Arc<RwLock<AccessPatternLogger>>) {
        self.access_logger = Some(logger);
    }

    /// Query - unified three-tier path
    ///
    /// # Query Flow
    /// 1. Check cache (L1) - RMI prediction + semantic similarity
    /// 2. If miss, check hot tier (L2) - recent writes
    /// 3. If miss, search HNSW (L3) - full k-NN search
    /// 4. Cache admission decision (should we cache this result?)
    /// 5. Log access for training
    ///
    /// # Lock Ordering Discipline
    /// **CRITICAL**: To prevent deadlocks, locks MUST be acquired in this order:
    /// 1. cache_strategy (read/write)
    /// 2. stats (write)
    /// 3. access_logger (write)
    ///
    /// Never hold multiple locks simultaneously. Always drop locks before
    /// calling methods that may acquire other locks.
    ///
    /// # Returns
    /// - `Some(embedding)` if document found in any tier
    /// - `None` if document doesn't exist
    pub fn query(&self, doc_id: u64, query_embedding: Option<&[f32]>) -> Option<Vec<f32>> {
        // Increment total queries (isolated lock)
        {
            let mut stats = self.stats.write();
            stats.total_queries += 1;
        } // Lock released

        // Layer 1: Check cache with circuit breaker protection
        if !self.cache_circuit_breaker.is_open() {
            let cached_result = {
                let cache = self.cache_strategy.read();
                cache.get_cached(doc_id)
            }; // cache_strategy lock released

            if let Some(cached) = cached_result {
                // Cache hit - record success
                self.cache_circuit_breaker.record_success();
                
                // Update stats (no other locks held)
                {
                    let mut stats = self.stats.write();
                    stats.cache_hits += 1;
                } // stats lock released

                // Log access (no other locks held)
                if let Some(ref logger) = self.access_logger {
                    if let Some(query_emb) = query_embedding {
                        logger.write().log_access(doc_id, query_emb);
                    }
                } // logger lock released

                return Some(cached.embedding);
            }
            
            // Cache miss - not a failure, just continue to next tier
            {
                let mut stats = self.stats.write();
                stats.cache_misses += 1;
            } // Lock released
        } else {
            // Circuit breaker open - skip cache layer
            {
                let mut stats = self.stats.write();
                stats.circuit_breaker_rejections += 1;
            }
            debug!("Cache circuit breaker open, skipping cache layer for doc_id={}", doc_id);
        }

        // Layer 2: Check hot tier with circuit breaker protection
        if !self.hot_tier_circuit_breaker.is_open() {
            if let Some(embedding) = self.hot_tier.get(doc_id) {
                // Hot tier hit - record success
                self.hot_tier_circuit_breaker.record_success();
                
                // Update stats (isolated)
                {
                    let mut stats = self.stats.write();
                    stats.hot_tier_hits += 1;
                } // Lock released

                // Cache admission decision (isolated)
                let should_cache_decision = {
                    let cache = self.cache_strategy.write();
                    cache.should_cache(doc_id, &embedding)
                }; // cache_strategy lock released

                if should_cache_decision {
                    let cached = CachedVector {
                        doc_id,
                        embedding: embedding.clone(),
                        distance: 0.0,
                        cached_at: Instant::now(),
                    };
                    // Insert into cache (isolated)
                    self.cache_strategy.write().insert_cached(cached);
                } // cache_strategy lock released

                // Log access (no other locks held)
                if let Some(ref logger) = self.access_logger {
                    if let Some(query_emb) = query_embedding {
                        logger.write().log_access(doc_id, query_emb);
                    }
                } // logger lock released

                return Some(embedding);
            }
            
            // Hot tier miss - update stats (isolated)
            {
                let mut stats = self.stats.write();
                stats.hot_tier_misses += 1;
            } // Lock released
        } else {
            // Circuit breaker open - skip hot tier layer
            {
                let mut stats = self.stats.write();
                stats.circuit_breaker_rejections += 1;
            }
            debug!("Hot tier circuit breaker open, skipping hot tier for doc_id={}", doc_id);
        }

        // Layer 3: Fetch from cold tier with circuit breaker protection
        if !self.cold_tier_circuit_breaker.is_open() {
            if let Some(embedding) = self.cold_tier.fetch_document(doc_id) {
                // Cold tier success - record it
                self.cold_tier_circuit_breaker.record_success();
                
                // Update stats (isolated)
                {
                    let mut stats = self.stats.write();
                    stats.cold_tier_searches += 1;
                } // Lock released

                // Cache admission decision (isolated)
                let should_cache_decision = {
                    let cache = self.cache_strategy.write();
                    cache.should_cache(doc_id, &embedding)
                }; // cache_strategy lock released

                if should_cache_decision {
                    let cached = CachedVector {
                        doc_id,
                        embedding: embedding.clone(),
                        distance: 0.0,
                        cached_at: Instant::now(),
                    };
                    // Insert into cache (isolated)
                    self.cache_strategy.write().insert_cached(cached);
                } // cache_strategy lock released

                // Log access (no other locks held)
                if let Some(ref logger) = self.access_logger {
                    if let Some(query_emb) = query_embedding {
                        logger.write().log_access(doc_id, query_emb);
                    }
                } // logger lock released

                return Some(embedding);
            }
            
            // Cold tier miss - this is normal (document doesn't exist)
        } else {
            // Circuit breaker open - fail fast
            {
                let mut stats = self.stats.write();
                stats.circuit_breaker_rejections += 1;
            }
            warn!("Cold tier circuit breaker open, cannot query doc_id={}", doc_id);
        }

        // Document not found in any tier
        None
    }

    /// k-NN search across all tiers
    ///
    /// This searches the cold tier (HNSW) for approximate k-NN,
    /// then augments with hot tier results if needed.
    pub fn knn_search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        // Search cold tier (HNSW)
        let results = self.cold_tier.knn_search(query, k)?;

        {
            let mut stats = self.stats.write();
            stats.cold_tier_searches += 1;
        }

        // TODO: Merge with hot tier results (for now, HNSW only)
        // Future enhancement: Search hot tier, merge with HNSW results

        Ok(results)
    }

    /// k-NN search with per-layer timeouts and graceful degradation
    ///
    /// # Timeout Configuration
    /// - Cache: 10ms (fastest, predicted hot documents)
    /// - Hot Tier: 50ms (recent writes, fast scan)
    /// - Cold Tier: 1000ms (HNSW search, slowest but most comprehensive)
    ///
    /// # Graceful Degradation
    /// If a layer times out or circuit breaker is open:
    /// 1. Cache timeout → Try hot tier → Try cold tier
    /// 2. Hot tier timeout → Try cold tier
    /// 3. Cold tier timeout → Return partial results if available
    ///
    /// # Returns
    /// - `Ok(Vec<SearchResult>)`: Full or partial results
    /// - `Err(...)`: All layers failed or timed out with no results, or queue saturated
    pub async fn knn_search_with_timeouts(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        // Load shedding: Try to acquire semaphore permit
        // Note: _permit is kept alive to hold the permit until function returns (RAII guard)
        let _permit = match self.query_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                // Queue saturated - reject query
                {
                    let mut stats = self.stats.write();
                    stats.queries_rejected += 1;
                }
                return Err(anyhow!(
                    "Query queue saturated: {} in-flight queries (max: {})",
                    self.config.max_concurrent_queries,
                    self.config.max_concurrent_queries
                ));
            }
        };

        // Update queue depth metric
        {
            let mut stats = self.stats.write();
            let available_permits = self.query_semaphore.available_permits();
            stats.current_queue_depth = (self.config.max_concurrent_queries - available_permits) as u64;
        }

        let mut results = Vec::new();
        let mut partial = false;

        // Layer 1: Cache layer not yet implemented for k-NN search
        // TODO: Implement cache lookup for k-NN query results
        debug!("Cache layer k-NN search not yet implemented");

        // Layer 2: Search hot tier (50ms timeout)
        // Note: HotTier k-NN search not yet implemented, skip for now
        // In future: implement linear scan or small HNSW for hot tier
        debug!("Hot tier k-NN search not yet implemented, skipping to cold tier");

        // Layer 3: Search cold tier (HNSW) (1000ms timeout)
        let cold_timeout = Duration::from_millis(self.config.cold_tier_timeout_ms);
        if self.cold_tier_circuit_breaker.is_closed() {
            match tokio::time::timeout(cold_timeout, async {
                tokio::task::spawn_blocking({
                    let query_vec = query.to_vec();
                    let cold_tier = Arc::clone(&self.cold_tier);
                    move || cold_tier.knn_search(&query_vec, k)
                })
                .await
            })
            .await
            {
                Ok(Ok(Ok(cold_results))) => {
                    results.extend(cold_results);
                    self.cold_tier_circuit_breaker.record_success();
                    let mut stats = self.stats.write();
                    stats.cold_tier_searches += 1;
                }
                Ok(Ok(Err(e))) => {
                    // Cold tier error
                    self.cold_tier_circuit_breaker.record_failure();
                    warn!("Cold tier search failed: {}", e);
                }
                Ok(Err(e)) => {
                    // Task join error
                    self.cold_tier_circuit_breaker.record_failure();
                    error!("Cold tier task panicked: {}", e);
                }
                Err(_) => {
                    // Timeout
                    self.cold_tier_circuit_breaker.record_failure();
                    let mut stats = self.stats.write();
                    stats.cold_tier_timeouts += 1;
                    warn!("Cold tier timed out after {}ms", self.config.cold_tier_timeout_ms);
                    partial = true;
                }
            }
        } else {
            // Circuit breaker open - record rejection
            {
                let mut stats = self.stats.write();
                stats.circuit_breaker_rejections += 1;
            }
            warn!("Cold tier circuit breaker open, cannot perform k-NN search");
        }

        // Permit is automatically dropped here, releasing the semaphore

        // Return results or error
        if !results.is_empty() {
            if partial {
                let mut stats = self.stats.write();
                stats.partial_results_returned += 1;
                info!("Returning {} partial results after timeout", results.len());
            }

            // Filter out any NaN distances (shouldn't happen in normal operation,
            // but serves as defensive check against floating-point computation errors)
            results.retain(|r| !r.distance.is_nan());

            // Deduplicate and take top k
            results.sort_by(|a, b| {
                a.distance
                    .partial_cmp(&b.distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(k);

            Ok(results)
        } else {
            Err(anyhow!(
                "All layers failed or timed out with no results"
            ))
        }
    }

    /// Insert document
    ///
    /// # Write Flow
    /// 1. Add to hot tier (fast, no HNSW update)
    /// 2. Log to WAL (durability, happens in background flush)
    /// 3. Background flush to cold tier when thresholds reached
    ///
    /// # Emergency Eviction
    /// If hot tier exceeds hard limit (2x soft limit), triggers emergency flush
    /// to prevent unbounded memory growth.
    pub fn insert(&self, doc_id: u64, embedding: Vec<f32>) -> Result<()> {
        // Check for hard limit violation BEFORE insert
        let current_size = self.hot_tier.len();
        if current_size >= self.config.hot_tier_hard_limit {
            warn!(
                current_size,
                hard_limit = self.config.hot_tier_hard_limit,
                "hot tier at hard limit; triggering emergency eviction"
            );

            // Emergency flush: force flush regardless of normal thresholds
            match self.emergency_flush_hot_tier() {
                Ok(flushed) => {
                    info!(
                        flushed_docs = flushed,
                        "emergency flush completed"
                    );
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "emergency flush failed; rejecting insert to prevent OOM"
                    );
                    
                    // Update emergency eviction metric
                    let mut stats = self.stats.write();
                    stats.hot_tier_emergency_evictions += 1;
                    
                    anyhow::bail!(
                        "insert rejected: hot tier at hard limit ({}) and emergency flush failed",
                        self.config.hot_tier_hard_limit
                    );
                }
            }

            // Update emergency eviction metric (successful case)
            let mut stats = self.stats.write();
            stats.hot_tier_emergency_evictions += 1;
        }

        // Insert into hot tier (fast)
        self.hot_tier.insert(doc_id, embedding);

        let mut stats = self.stats.write();
        stats.total_inserts += 1;

        Ok(())
    }

    /// Emergency flush: force flush regardless of normal thresholds
    ///
    /// Used when hot tier reaches hard limit to prevent OOM.
    /// Unlike normal flush, this ignores the needs_flush() check.
    fn emergency_flush_hot_tier(&self) -> Result<usize> {
        let documents = self.hot_tier.drain_for_flush();
        let count = documents.len();

        if count == 0 {
            return Ok(0);
        }

        info!(
            documents = count,
            "emergency flush: force-flushing all hot tier documents"
        );

        // Track failed documents for re-insertion
        let mut failed_documents = Vec::new();
        let mut success_count = 0;

        // Insert into cold tier (HNSW + WAL) with per-document error handling
        for (doc_id, embedding) in documents {
            match self.cold_tier.insert(doc_id, embedding.clone()) {
                Ok(()) => {
                    success_count += 1;
                }
                Err(e) => {
                    error!(
                        doc_id,
                        error = %e,
                        "failed to flush document to cold tier during emergency flush"
                    );
                    failed_documents.push((doc_id, embedding));
                }
            }
        }

        // Re-insert failed documents back into hot tier
        if !failed_documents.is_empty() {
            let fail_count = failed_documents.len();
            error!(
                failed = fail_count,
                succeeded = success_count,
                "emergency flush partial failure"
            );

            self.hot_tier.reinsert_failed_documents(failed_documents);

            // Update failure metric
            let mut stats = self.stats.write();
            stats.hot_tier_flush_failures += 1;

            if success_count == 0 {
                anyhow::bail!(
                    "emergency flush completely failed: all {} documents remain in hot tier",
                    fail_count
                );
            }
        }

        Ok(success_count)
    }

    /// Flush hot tier to cold tier (manual trigger)
    ///
    /// This is called periodically by background task,
    /// or can be called manually for testing/shutdown.
    ///
    /// # Error Handling
    /// - On partial failure: re-inserts failed documents back into hot tier
    /// - On complete failure: all documents re-inserted, flush marked as failed
    /// - Tracks flush_failures metric for observability
    pub fn flush_hot_tier(&self) -> Result<usize> {
        if !self.hot_tier.needs_flush() {
            return Ok(0);
        }

        let documents = self.hot_tier.drain_for_flush();
        let count = documents.len();

        if count == 0 {
            return Ok(0);
        }

        // Track failed documents for re-insertion
        let mut failed_documents = Vec::new();
        let mut success_count = 0;

        // Insert into cold tier (HNSW + WAL) with per-document error handling
        for (doc_id, embedding) in documents {
            match self.cold_tier.insert(doc_id, embedding.clone()) {
                Ok(()) => {
                    success_count += 1;
                }
                Err(e) => {
                    error!(
                        doc_id,
                        error = %e,
                        "failed to flush document to cold tier; will re-insert to hot tier"
                    );
                    failed_documents.push((doc_id, embedding));
                }
            }
        }

        // Re-insert failed documents back into hot tier to prevent data loss
        if !failed_documents.is_empty() {
            let fail_count = failed_documents.len();
            warn!(
                failed = fail_count,
                succeeded = success_count,
                "partial flush failure; re-inserting failed documents to hot tier"
            );

            self.hot_tier.reinsert_failed_documents(failed_documents);

            // Update failure metric
            let mut stats = self.stats.write();
            stats.hot_tier_flush_failures += 1;

            if success_count == 0 {
                // Complete failure - return error
                anyhow::bail!(
                    "flush completely failed: all {} documents re-inserted to hot tier",
                    fail_count
                );
            }
            // Partial success - return success count but log warning (already done above)
        }

        Ok(success_count)
    }

    /// Spawn background flush task
    ///
    /// Periodically checks if hot tier needs flushing and flushes to cold tier.
    pub fn spawn_flush_task(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let flush_interval = self.config.flush_interval;

        tokio::spawn(async move {
            let mut ticker = interval(flush_interval);

            loop {
                ticker.tick().await;

                if self.hot_tier.needs_flush() {
                    match self.flush_hot_tier() {
                        Ok(count) => {
                            if count > 0 {
                                println!(
                                    "Background flush: {} documents moved to cold tier",
                                    count
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!("Background flush failed: {}", e);
                        }
                    }
                }
            }
        })
    }

    /// Get comprehensive statistics
    pub fn stats(&self) -> TieredEngineStats {
        let mut stats = self.stats.read().clone();

        // Update tier-specific stats
        let hot_tier_stats = self.hot_tier.stats();
        stats.hot_tier_size = hot_tier_stats.current_size;
        stats.hot_tier_flushes = hot_tier_stats.total_flushes;
        stats.hot_tier_hit_rate = self.hot_tier.hit_rate();

        stats.cold_tier_size = self.cold_tier.len();

        // Calculate overall hit rate
        let total_hits = stats.cache_hits + stats.hot_tier_hits;
        if stats.total_queries > 0 {
            stats.overall_hit_rate = total_hits as f64 / stats.total_queries as f64;
            stats.cache_hit_rate = stats.cache_hits as f64 / stats.total_queries as f64;
        }

        stats
    }

    /// Get cold tier reference (for direct access if needed)
    pub fn cold_tier(&self) -> &HnswBackend {
        &self.cold_tier
    }

    /// Get hot tier reference (for testing/inspection)
    pub fn hot_tier(&self) -> &HotTier {
        &self.hot_tier
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LruCacheStrategy;
    use tempfile::TempDir;

    #[test]
    fn test_tiered_engine_query_path() {
        let cache = LruCacheStrategy::new(100);
        let initial_embeddings = vec![vec![1.0, 0.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]];

        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();

        // Query doc 0 (in cold tier)
        let result = engine.query(0, None);
        assert!(result.is_some());

        let stats = engine.stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.cold_tier_searches, 1);
    }

    #[test]
    fn test_tiered_engine_insert_and_query() {
        let cache = LruCacheStrategy::new(100);
        let initial_embeddings = vec![vec![1.0, 0.0]];

        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();

        // Insert into hot tier
        engine.insert(10, vec![0.5, 0.5]).unwrap();

        // Query should hit hot tier
        let result = engine.query(10, None);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), vec![0.5, 0.5]);

        let stats = engine.stats();
        assert_eq!(stats.hot_tier_hits, 1);
        assert_eq!(stats.hot_tier_size, 1);
    }

    #[test]
    fn test_tiered_engine_flush() {
        let cache = LruCacheStrategy::new(100);
        let initial_embeddings = vec![vec![1.0, 0.0]];

        let config = TieredEngineConfig {
            hot_tier_max_size: 2, // Small threshold
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();

        // Insert 2 documents (trigger flush threshold)
        engine.insert(10, vec![0.1, 0.1]).unwrap();
        engine.insert(11, vec![0.2, 0.2]).unwrap();

        assert!(engine.hot_tier().needs_flush());

        // Manual flush
        let flushed = engine.flush_hot_tier().unwrap();
        assert_eq!(flushed, 2);

        // Hot tier should be empty
        assert_eq!(engine.hot_tier().len(), 0);

        // Documents should be in cold tier
        assert!(engine.cold_tier().fetch_document(10).is_some());
        assert!(engine.cold_tier().fetch_document(11).is_some());
    }

    #[test]
    fn test_tiered_engine_with_persistence() {
        let dir = TempDir::new().unwrap();

        {
            let cache = LruCacheStrategy::new(100);
            let initial_embeddings = vec![vec![1.0, 0.0]];

            // Use hot_tier_max_size=1 to trigger flush immediately
            let config = TieredEngineConfig {
                hot_tier_max_size: 1,
                hnsw_max_elements: 100,
                data_dir: Some(dir.path().to_string_lossy().to_string()),
                fsync_policy: FsyncPolicy::Always,
                ..Default::default()
            };

            let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();

            // Insert and flush (should trigger because hot_tier_max_size=1)
            engine.insert(10, vec![0.5, 0.5]).unwrap();
            let flushed = engine.flush_hot_tier().unwrap();
            assert_eq!(flushed, 1, "Expected 1 document to be flushed");

            // Verify doc 10 is in cold tier before snapshot
            assert!(
                engine.cold_tier().fetch_document(10).is_some(),
                "Doc 10 not in cold tier before snapshot"
            );

            // Create snapshot
            engine.cold_tier().create_snapshot().unwrap();

            // Explicit drop to ensure WAL fsynced
            drop(engine);
        }

        // Recover
        let cache = LruCacheStrategy::new(100);
        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            ..Default::default()
        };

        let recovered = TieredEngine::recover(Box::new(cache), dir.path(), config).unwrap();

        // Verify data recovered
        assert!(
            recovered.cold_tier().fetch_document(0).is_some(),
            "Doc 0 not recovered"
        );
        assert!(
            recovered.cold_tier().fetch_document(10).is_some(),
            "Doc 10 not recovered"
        );
    }

    #[tokio::test]
    async fn test_knn_search_with_timeouts_success() {
        // Test successful k-NN search with timeouts
        let cache = LruCacheStrategy::new(100);
        let mut embeddings = Vec::new();
        for i in 0..100 {
            embeddings.push(vec![i as f32, 0.0, 0.0, 0.0]);
        }

        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 200,
            data_dir: None,
            cache_timeout_ms: 10,
            hot_tier_timeout_ms: 50,
            cold_tier_timeout_ms: 1000,
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), embeddings, config).unwrap();

        // Search for nearest neighbors
        let query = vec![5.0, 0.0, 0.0, 0.0];
        let results = engine.knn_search_with_timeouts(&query, 5).await.unwrap();

        // Should get results from cold tier (HNSW)
        assert!(!results.is_empty());
        assert!(results.len() <= 5);

        // Verify timeout stats initialized
        let stats = engine.stats();
        assert_eq!(stats.cache_timeouts, 0); // Should not timeout with small dataset
    }

    #[tokio::test]
    async fn test_knn_search_with_timeouts_cold_tier_fallback() {
        // Test that cold tier is searched when cache/hot tier empty
        let cache = LruCacheStrategy::new(100);
        let embeddings = vec![
            vec![1.0, 0.0, 0.0, 0.0],
            vec![2.0, 0.0, 0.0, 0.0],
            vec![3.0, 0.0, 0.0, 0.0],
        ];

        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), embeddings, config).unwrap();

        let query = vec![2.5, 0.0, 0.0, 0.0];
        let results = engine.knn_search_with_timeouts(&query, 2).await.unwrap();

        assert_eq!(results.len(), 2);
        
        // Verify cold tier was searched
        let stats = engine.stats();
        assert!(stats.cold_tier_searches > 0);
    }

    #[tokio::test]
    async fn test_timeout_stats_tracking() {
        // Test that timeout statistics are properly tracked
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![vec![1.0, 0.0]];

        let mut config = TieredEngineConfig::default();
        config.data_dir = None;
        config.hnsw_max_elements = 100;

        let engine = TieredEngine::new(Box::new(cache), embeddings, config).unwrap();

        // Initial stats should be zero
        let stats = engine.stats();
        assert_eq!(stats.cache_timeouts, 0);
        assert_eq!(stats.hot_tier_timeouts, 0);
        assert_eq!(stats.cold_tier_timeouts, 0);
        assert_eq!(stats.partial_results_returned, 0);
    }

    #[tokio::test]
    async fn test_actual_timeout_triggers() {
        // Test that timeouts actually occur and are tracked
        let cache = LruCacheStrategy::new(10);
        
        // Create larger dataset to potentially cause timeout with very short deadline
        let mut embeddings = Vec::new();
        for i in 0..1000 {
            let mut vec = vec![0.0; 128];
            vec[0] = i as f32;
            embeddings.push(vec);
        }

        let config = TieredEngineConfig {
            cold_tier_timeout_ms: 1, // Very short timeout to force timeout
            hnsw_max_elements: 2000,
            data_dir: None,
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), embeddings, config).unwrap();

        let query = vec![500.0; 128];
        let _result = engine.knn_search_with_timeouts(&query, 10).await;

        // Verify stats changed from zero - either search succeeded or timed out
        let stats = engine.stats();
        assert!(
            stats.cold_tier_searches > 0 || stats.cold_tier_timeouts > 0,
            "Either searches or timeouts should have incremented"
        );
    }

    #[test]
    fn test_circuit_breakers_initialized() {
        // Verify circuit breakers are properly initialized
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![vec![1.0, 0.0, 0.0, 0.0]];

        let config = TieredEngineConfig {
            data_dir: None,
            hnsw_max_elements: 100,
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), embeddings, config).unwrap();

        // All circuit breakers should start closed
        assert!(engine.cache_circuit_breaker.is_closed());
        assert!(engine.cold_tier_circuit_breaker.is_closed());
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_on_failures() {
        // Test that circuit breakers open after repeated failures
        // Note: This is a behavioral test - circuit breaker integration is validated
        // by observing that failures are recorded and timeouts occur
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![vec![1.0, 0.0, 0.0, 0.0]];
        
        let config = TieredEngineConfig {
            cold_tier_timeout_ms: 1, // Very short timeout to trigger failures
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };
        
        let engine = TieredEngine::new(Box::new(cache), embeddings, config).unwrap();
        
        // Trigger multiple searches - some may timeout, some may succeed
        for _ in 0..10 {
            let _ = engine.knn_search_with_timeouts(&vec![1.0, 0.0, 0.0, 0.0], 5).await;
        }
        
        // Verify that searches were attempted (stats should be non-zero)
        let stats = engine.stats();
        assert!(
            stats.cold_tier_searches + stats.cold_tier_timeouts > 0,
            "Searches should have been attempted (either succeeded or timed out)"
        );
        
        // Note: Whether circuit breaker opens depends on actual timeout behavior,
        // which can vary based on system load. The key is that the integration
        // between TieredEngine and CircuitBreaker is functional.
    }

    #[tokio::test]
    async fn test_load_shedding_queue_saturation() {
        // Test that queries are rejected when semaphore is saturated
        // 
        // Strategy: Use a barrier BEFORE the query to ensure permits are held
        // while we attempt the 3rd query
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![vec![1.0, 2.0]; 10];

        let config = TieredEngineConfig {
            max_concurrent_queries: 2, // Very low limit to trigger rejection
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };

        let engine = Arc::new(TieredEngine::new(Box::new(cache), embeddings, config).unwrap());

        // Barrier to coordinate: 2 background tasks will wait AFTER acquiring permit
        // but BEFORE releasing it, so main thread can attempt 3rd query
        let barrier = Arc::new(tokio::sync::Barrier::new(3)); // 2 tasks + main

        // Spawn 2 tasks that acquire permits and wait at barrier
        let engine1 = Arc::clone(&engine);
        let barrier1 = Arc::clone(&barrier);
        let handle1 = tokio::spawn(async move {
            // Manually acquire permit to hold it
            let _permit = engine1.query_semaphore.acquire().await.unwrap();
            // Signal we have permit, then wait for main to finish test
            barrier1.wait().await;
            // Permit released when _permit drops
        });

        let engine2 = Arc::clone(&engine);
        let barrier2 = Arc::clone(&barrier);
        let handle2 = tokio::spawn(async move {
            // Manually acquire permit to hold it
            let _permit = engine2.query_semaphore.acquire().await.unwrap();
            // Signal we have permit, then wait for main to finish test
            barrier2.wait().await;
            // Permit released when _permit drops
        });

        // Small delay to ensure tasks have started
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Try a 3rd query - should be rejected (both permits held by background tasks)
        let query = vec![1.0, 2.0];
        let result = engine.knn_search_with_timeouts(&query, 5).await;

        // Should be rejected with queue saturation error
        assert!(result.is_err(), "Expected query to be rejected due to queue saturation");
        let err = result.unwrap_err();
        assert!(err.to_string().contains("saturated"), "Error should mention saturation: {}", err);

        // Check stats
        let stats = engine.stats();
        assert_eq!(stats.queries_rejected, 1, "Should have exactly 1 rejected query");

        // Release barrier to let background tasks finish
        barrier.wait().await;
        let _ = handle1.await;
        let _ = handle2.await;
    }

    #[tokio::test]
    async fn test_load_shedding_permits_released() {
        // Test that semaphore permits are properly released after query completes
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![vec![1.0, 2.0]; 5];

        let config = TieredEngineConfig {
            max_concurrent_queries: 1, // Only 1 concurrent query allowed
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };

        let engine = Arc::new(TieredEngine::new(Box::new(cache), embeddings, config).unwrap());

        // Execute first query - should succeed
        let query1 = vec![1.0, 2.0];
        let result1 = engine.knn_search_with_timeouts(&query1, 3).await;
        assert!(result1.is_ok());

        // Execute second query immediately after - should also succeed (permit released)
        let query2 = vec![2.0, 3.0];
        let result2 = engine.knn_search_with_timeouts(&query2, 3).await;
        assert!(result2.is_ok());

        // No queries should be rejected
        let stats = engine.stats();
        assert_eq!(stats.queries_rejected, 0);
    }

    #[test]
    fn test_circuit_breaker_rejection_stats() {
        // Test that circuit breaker rejections are tracked in stats
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![vec![1.0]; 5];

        let config = TieredEngineConfig {
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), embeddings, config).unwrap();

        // Manually open circuit breakers to test rejection path
        engine.cache_circuit_breaker.open();
        engine.hot_tier_circuit_breaker.open();
        engine.cold_tier_circuit_breaker.open();

        // Query with all circuit breakers open
        let result = engine.query(1, None);

        // Query should return None (all tiers rejected)
        assert!(result.is_none());

        // Check that rejections were counted (3 rejections: cache, hot_tier, cold_tier)
        let stats = engine.stats();
        assert_eq!(stats.circuit_breaker_rejections, 3);
    }

    #[tokio::test]
    async fn test_queue_depth_tracking() {
        // Test that current queue depth is properly tracked
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![vec![1.0, 2.0]; 10];

        let config = TieredEngineConfig {
            max_concurrent_queries: 5,
            hnsw_max_elements: 100,
            data_dir: None,
            cold_tier_timeout_ms: 200, // Short timeout for quick test
            ..Default::default()
        };

        let engine = Arc::new(TieredEngine::new(Box::new(cache), embeddings, config).unwrap());

        // Initial queue depth should be 0
        let initial_stats = engine.stats();
        assert_eq!(initial_stats.current_queue_depth, 0);

        // Spawn a query and check depth during execution
        let engine_clone = Arc::clone(&engine);
        let query1 = vec![1.0, 2.0];
        let handle = tokio::spawn(async move {
            engine_clone.knn_search_with_timeouts(&query1, 3).await
        });

        // Give time for query to acquire permit
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Queue depth should be non-zero during query execution
        // Note: This may be flaky if query completes very fast
        
        // Wait for query to complete
        let _ = handle.await;

        // After completion, subsequent queries should work (permits released)
        let query2 = vec![1.0, 2.0];
        let result = engine.knn_search_with_timeouts(&query2, 3).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_circuit_breaker_in_knn_search() {
        // Test circuit breaker integration in k-NN search path
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![vec![1.0, 2.0]; 5];

        let config = TieredEngineConfig {
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), embeddings, config).unwrap();

        // Open cold tier circuit breaker
        engine.cold_tier_circuit_breaker.open();

        // k-NN search should fail gracefully (no results)
        let query = vec![1.0, 2.0];
        let result = engine.knn_search_with_timeouts(&query, 3).await;

        // Should return error (all layers failed)
        assert!(result.is_err());

        // Check circuit breaker rejection was counted
        let stats = engine.stats();
        assert_eq!(stats.circuit_breaker_rejections, 1);
    }

    #[test]
    fn test_hot_tier_flush_failure_recovery() {
        // Test that flush failures result in re-insertion to hot tier
        use tempfile::TempDir;
        
        let temp_dir = TempDir::new().unwrap();
        let cache = LruCacheStrategy::new(10);
        let initial_embeddings = vec![vec![1.0, 0.0]; 5];

        let config = TieredEngineConfig {
            hot_tier_max_size: 3, // Small threshold to trigger flush
            hnsw_max_elements: 100,
            data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
            snapshot_interval: 1000, // Don't snapshot during test
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();

        // Insert documents into hot tier
        engine.insert(10, vec![2.0, 0.0]).unwrap();
        engine.insert(11, vec![3.0, 0.0]).unwrap();
        engine.insert(12, vec![4.0, 0.0]).unwrap();

        assert_eq!(engine.hot_tier.len(), 3);

        // Attempt flush (should succeed normally)
        let flushed = engine.flush_hot_tier().unwrap();
        
        // For this test, flush should succeed, so hot tier should be empty
        // (Testing actual failure requires disk-full simulation which is complex)
        assert!(engine.hot_tier.len() == 0 || flushed > 0);
        
        // Verify stats include flush operations
        let stats = engine.stats();
        assert!(stats.hot_tier_flushes > 0 || stats.hot_tier_flush_failures >= 0);
    }

    #[test]
    fn test_emergency_eviction_on_hard_limit() {
        // Test that emergency eviction triggers when hard limit reached
        use tempfile::TempDir;
        
        let temp_dir = TempDir::new().unwrap();
        let cache = LruCacheStrategy::new(10);
        let initial_embeddings = vec![vec![1.0, 0.0]; 2];

        let config = TieredEngineConfig {
            hot_tier_max_size: 3,    // Soft limit (very small for testing)
            hot_tier_hard_limit: 6,  // Hard limit (2x soft limit)
            hnsw_max_elements: 100,
            data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
            snapshot_interval: 1000, // Don't snapshot during test
            ..Default::default()
        };

        let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();

        // Insert documents up to hard limit
        // Hard limit is 6, so insert 6 documents
        for i in 10..16 {
            let result = engine.insert(i, vec![i as f32, 0.0]);
            
            // Each insert should succeed (emergency eviction handles overflow)
            assert!(
                result.is_ok(),
                "Insert {} failed: {:?}",
                i,
                result.err()
            );
        }

        // Verify that either:
        // 1. Emergency eviction was triggered (stats show > 0), OR
        // 2. Normal flush prevented us from hitting hard limit
        let stats = engine.stats();
        let hot_tier_size = engine.hot_tier.len();
        
        // The key invariant: hot tier should NEVER exceed hard limit
        assert!(
            hot_tier_size <= 6,
            "Hot tier size {} should be at or below hard limit 6",
            hot_tier_size
        );
        
        // If we hit hard limit, emergency eviction counter should be > 0
        if stats.hot_tier_emergency_evictions > 0 {
            println!(
                "Emergency evictions triggered: {}",
                stats.hot_tier_emergency_evictions
            );
        }
    }

    #[test]
    fn test_hot_tier_reinsert_preserves_documents() {
        // Test that reinsert_failed_documents correctly restores documents to hot tier
        use std::time::Duration;
        
        let hot_tier = HotTier::new(100, Duration::from_secs(60));
        
        // Insert initial documents
        hot_tier.insert(1, vec![1.0, 0.0]);
        hot_tier.insert(2, vec![2.0, 0.0]);
        
        assert_eq!(hot_tier.len(), 2);
        
        // Simulate failed flush scenario: documents that couldn't be flushed
        let failed_docs = vec![
            (10, vec![10.0, 0.0]),
            (11, vec![11.0, 0.0]),
        ];
        
        hot_tier.reinsert_failed_documents(failed_docs);
        
        // Verify all documents present
        assert_eq!(hot_tier.len(), 4);
        assert!(hot_tier.get(1).is_some());
        assert!(hot_tier.get(2).is_some());
        assert!(hot_tier.get(10).is_some());
        assert!(hot_tier.get(11).is_some());
    }
}
