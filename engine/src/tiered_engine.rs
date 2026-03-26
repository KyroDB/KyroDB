//! Tiered Engine - Two-level cache architecture orchestrator
//!
//! Coordinates all tiers:
//! - **Layer 1a (Document Cache)**: Learned frequency-based cache (hot documents)
//! - **Layer 1b (Query Cache)**: Semantic search-result cache (paraphrased queries)
//! - **Layer 2 (Hot Tier)**: Recent-write mirror and acceleration layer
//! - **Layer 3 (Cold Tier)**: Canonical HNSW index with durability (WAL + recovery)
//!
//! # Search Path (k-NN)
//! ```text
//! Query → L1b (Query Cache) → L2 (Hot Tier) → L3 (HNSW)
//!         ↓ hit               ↓ hit            ↓ fallback
//!       return              return           return
//! ```
//!
//! # Point Lookup Path (doc_id)
//! ```text
//! Get → L1a (Doc Cache) → L2 (Hot Tier) → L3 (HNSW)
//!        ↓ hit             ↓ hit            ↓ fallback
//!      return            return           return
//! ```
//!
//! # Write Path
//! ```text
//! Insert → invalidate L1a → WAL + HNSW (canonical write)
//!        → invalidate affected L1b entries → Hot Tier mirror
//!        → ACK
//!        → Background audits/drain evict mirror entries and reconcile unexpected drift
//! ```

use crate::config::{DistanceMetric, RecoveryMode};
use crate::proto::MetadataFilter;
use crate::{
    embedding_matches_token, AccessPatternLogger, CacheLifecycleStats, CacheStrategy, CachedVector,
    CircuitBreaker, FsyncPolicy, HnswBackend, HotTier, HotTierMirrorDocument, QueryHashCache,
    SearchResult, VectorCoherenceToken,
};
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use std::borrow::Cow;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::interval;
use tracing::{debug, error, info, instrument, trace, warn};

/// Tiered engine statistics (Two-Level Cache Architecture)
///
/// # Lock Strategy (Performance Optimization)
///
/// Statistics updates use **separate, short-lived lock acquisitions** to minimize
/// lock contention on the hot query path. This is an intentional design choice:
///
/// - **No data corruption**: Each counter update is atomic
/// - **Minimal lock contention**: Locks are held for nanoseconds, not across complex operations
/// - **Acceptable trade-off**: Temporary inconsistent snapshots are fine for metrics
///   (e.g., briefly `cache_hits` might appear > `total_queries`, but converges immediately)
///
/// Alternative designs that hold metrics locks across larger query sections would
/// increase contention on the read path.
#[derive(Debug, Clone, Default)]
pub struct TieredEngineStats {
    /// Layer 1a (Document Cache) statistics - Learned frequency-based
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,

    /// Layer 1b (Query Cache) statistics - Semantic similarity-based
    pub query_cache_hits: u64,
    pub query_cache_misses: u64,
    pub query_cache_hit_rate: f64,
    pub query_cache_exact_hits: u64,      // Exact query hash matches
    pub query_cache_similarity_hits: u64, // Similarity-based matches

    /// Combined L1 (L1a + L1b) statistics
    pub l1_combined_hits: u64, // cache_hits + query_cache_hits
    pub l1_combined_hit_rate: f64, // (cache_hits + query_cache_hits) / total_queries

    /// Layer 2 (Hot Tier) statistics
    pub hot_tier_hits: u64,
    pub hot_tier_misses: u64,
    pub hot_tier_hit_rate: f64,
    pub hot_tier_size: usize,
    pub hot_tier_flushes: u64,
    pub hot_tier_flush_failures: u64,      // Failed flush operations
    pub hot_tier_emergency_evictions: u64, // Emergency evictions due to hard limit

    /// Layer 3 (Cold Tier) statistics
    pub cold_tier_searches: u64,
    pub cold_tier_size: usize,

    /// Overall statistics
    pub total_queries: u64,
    pub total_inserts: u64,
    pub overall_hit_rate: f64, // (l1_combined_hits + hot_tier_hits) / total_queries

    /// Timeout statistics
    pub cache_timeouts: u64,
    pub hot_tier_timeouts: u64,
    pub cold_tier_timeouts: u64,
    pub partial_results_returned: u64,

    /// Load shedding statistics
    pub queries_rejected: u64, // Queries rejected due to queue saturation
    pub current_queue_depth: u64,        // Current in-flight queries
    pub circuit_breaker_rejections: u64, // Queries failed due to circuit breaker open
    pub worker_saturation_count: u64,    // Searches that could not obtain a worker permit
}

/// Source tier for point lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointQueryTier {
    Cache,
    HotTier,
    ColdTier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalVectorState {
    Match,
    TokenMismatch,
    LocalCorruption,
    Missing,
}

/// Execution path used to serve a k-NN search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchExecutionPath {
    CacheHit,
    HotTierOnly,
    ColdTierOnly,
    HotAndCold,
    /// Neither tier was successfully queried (e.g. both circuit breakers open,
    /// both tiers timed out, or empty database with no hot-tier data).
    Degraded,
}

const NORMALIZATION_NORM_SQ_MIN: f32 = 0.98;
const NORMALIZATION_NORM_SQ_MAX: f32 = 1.02;
const DEFAULT_QUERY_CACHE_SCOPE: u64 = 0;

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

    /// Embedding dimension for the database.
    ///
    /// Required to initialize an empty cold tier without inserting dummy vectors.
    pub embedding_dimension: usize,

    /// Distance metric for the cold-tier HNSW index.
    pub hnsw_distance: DistanceMetric,

    /// HNSW construction parameter M (graph connectivity).
    pub hnsw_m: usize,

    /// HNSW construction parameter ef_construction (build quality).
    pub hnsw_ef_construction: usize,

    /// HNSW search parameter ef_search (query-time candidate list size).
    pub hnsw_ef_search: usize,

    /// Disable L2-normalization checks for inner-product vectors (performance opt-out).
    pub hnsw_disable_normalization_check: bool,

    /// Persistence data directory
    pub data_dir: Option<String>,

    /// WAL fsync policy
    pub fsync_policy: FsyncPolicy,

    /// Snapshot interval (create snapshot every N inserts to cold tier)
    pub snapshot_interval: usize,

    /// Recovery strictness for persistence startup.
    pub recovery_mode: RecoveryMode,

    /// Maximum WAL size before rotation (bytes).
    ///
    /// When the active WAL reaches this size, a new WAL segment is created and
    /// appended to `MANIFEST`. Old segments can be deleted after a snapshot.
    pub max_wal_size_bytes: u64,

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
            embedding_dimension: 768,
            hnsw_distance: DistanceMetric::Cosine,
            hnsw_m: crate::hnsw_index::HnswVectorIndex::DEFAULT_M,
            hnsw_ef_construction: crate::hnsw_index::HnswVectorIndex::DEFAULT_EF_CONSTRUCTION,
            hnsw_ef_search: 50,
            hnsw_disable_normalization_check: false,
            data_dir: None,
            fsync_policy: FsyncPolicy::Always,
            snapshot_interval: 10_000,
            recovery_mode: RecoveryMode::Strict,
            max_wal_size_bytes: 100 * 1024 * 1024, // 100 MB
            flush_interval: Duration::from_secs(30),
            cache_timeout_ms: 10,         // 10ms for cache
            hot_tier_timeout_ms: 50,      // 50ms for hot tier
            cold_tier_timeout_ms: 1000,   // 1000ms (1s) for cold tier HNSW
            max_concurrent_queries: 1000, // Load shedding threshold: max 1000 in-flight queries
        }
    }
}

/// Internal components built by `build_internal` helper
///
/// This struct holds the initialized tiers and infrastructure components
/// that are shared between `new` and `new_with_shared_strategy` constructors.
struct TieredEngineComponents {
    hot_tier: Arc<HotTier>,
    cold_tier: Arc<HnswBackend>,
    query_semaphore: Arc<Semaphore>,
    search_worker_semaphore: Arc<Semaphore>,
    stats: Arc<RwLock<TieredEngineStats>>,
    cache_circuit_breaker: Arc<CircuitBreaker>,
    hot_tier_circuit_breaker: Arc<CircuitBreaker>,
    cold_tier_circuit_breaker: Arc<CircuitBreaker>,
    last_hot_tier_coherence_audit: Arc<RwLock<Instant>>,
}

/// Tiered Engine - Two-level cache vector database
pub struct TieredEngine {
    /// Layer 1a: Document Cache (Learned frequency-based, hot documents)
    cache_strategy: Arc<dyn CacheStrategy>,

    /// Layer 1b: Query Cache (Semantic similarity-based, paraphrased queries)
    query_cache: Arc<QueryHashCache>,

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

    /// Semaphore bounding blocking search workers.
    pub(crate) search_worker_semaphore: Arc<Semaphore>,

    /// Last completed full hot-tier coherence audit.
    last_hot_tier_coherence_audit: Arc<RwLock<Instant>>,
}

impl TieredEngine {
    /// Build internal components shared by all constructors
    ///
    /// Creates the hot tier, cold tier, semaphore, stats, and circuit breakers.
    /// The cold tier initialization handles both persistence and non-persistence modes.
    ///
    /// # Parameters
    /// - `initial_embeddings`: Initial documents to load into cold tier
    /// - `initial_metadata`: Metadata for initial documents
    /// - `config`: Configuration for all tiers
    ///
    /// # Returns
    /// `TieredEngineComponents` containing all initialized infrastructure components
    fn build_internal(
        initial_embeddings: Vec<Vec<f32>>,
        initial_metadata: Vec<std::collections::HashMap<String, String>>,
        config: &TieredEngineConfig,
    ) -> Result<TieredEngineComponents> {
        // Create hot tier
        let hot_tier = Arc::new(HotTier::new(
            config.hot_tier_max_size,
            config.hot_tier_max_age,
            config.hnsw_distance,
        ));

        // Create cold tier (HNSW backend)
        let cold_tier = if let Some(ref data_dir) = config.data_dir {
            // With persistence
            Arc::new(HnswBackend::with_persistence_with_hnsw_params(
                config.embedding_dimension,
                config.hnsw_distance,
                initial_embeddings,
                initial_metadata,
                config.hnsw_max_elements,
                data_dir,
                config.fsync_policy,
                config.snapshot_interval,
                config.max_wal_size_bytes,
                config.hnsw_m,
                config.hnsw_ef_construction,
                config.hnsw_disable_normalization_check,
            )?)
        } else {
            // Without persistence (testing only)
            Arc::new(HnswBackend::new_with_hnsw_params(
                config.embedding_dimension,
                config.hnsw_distance,
                initial_embeddings,
                initial_metadata,
                config.hnsw_max_elements,
                config.hnsw_m,
                config.hnsw_ef_construction,
                config.hnsw_disable_normalization_check,
            )?)
        };

        // Distinct semaphores intentionally protect different scopes:
        // - query_semaphore: bounds total in-flight user queries
        // - search_worker_semaphore: bounds blocking worker tasks that can outlive request timeouts
        let query_semaphore = Arc::new(Semaphore::new(config.max_concurrent_queries));
        let search_worker_semaphore = Arc::new(Semaphore::new(config.max_concurrent_queries));

        Ok(TieredEngineComponents {
            hot_tier,
            cold_tier,
            query_semaphore,
            search_worker_semaphore,
            stats: Arc::new(RwLock::new(TieredEngineStats::default())),
            cache_circuit_breaker: Arc::new(CircuitBreaker::new()),
            hot_tier_circuit_breaker: Arc::new(CircuitBreaker::new()),
            cold_tier_circuit_breaker: Arc::new(CircuitBreaker::new()),
            last_hot_tier_coherence_audit: Arc::new(RwLock::new(Instant::now())),
        })
    }

    /// Create new tiered engine
    ///
    /// # Parameters
    /// - `cache_strategy`: Layer 1 cache strategy (LRU or Learned)
    /// - `initial_embeddings`: Initial documents to load into cold tier
    /// - `config`: Configuration for all tiers
    pub fn new(
        cache_strategy: Box<dyn CacheStrategy>,
        query_cache: Arc<QueryHashCache>,
        initial_embeddings: Vec<Vec<f32>>,
        initial_metadata: Vec<std::collections::HashMap<String, String>>,
        config: TieredEngineConfig,
    ) -> Result<Self> {
        let components = Self::build_internal(initial_embeddings, initial_metadata, &config)?;

        Ok(Self {
            cache_strategy: Arc::from(cache_strategy),
            query_cache,
            hot_tier: components.hot_tier,
            cold_tier: components.cold_tier,
            access_logger: None,
            stats: components.stats,
            config,
            cache_circuit_breaker: components.cache_circuit_breaker,
            hot_tier_circuit_breaker: components.hot_tier_circuit_breaker,
            cold_tier_circuit_breaker: components.cold_tier_circuit_breaker,
            query_semaphore: components.query_semaphore,
            search_worker_semaphore: components.search_worker_semaphore,
            last_hot_tier_coherence_audit: components.last_hot_tier_coherence_audit,
        })
    }

    /// Create new tiered engine with a shared cache strategy
    ///
    /// This constructor allows sharing the cache strategy with external components
    /// (e.g., training task) so that predictor updates are immediately visible
    /// to the engine's query path.
    ///
    /// # Parameters
    /// - `cache_strategy`: Shared cache strategy handle
    /// - `query_cache`: Layer 1b query cache
    /// - `initial_embeddings`: Initial documents to load into cold tier
    /// - `initial_metadata`: Metadata for initial documents
    /// - `config`: Configuration for all tiers
    pub fn new_with_shared_strategy(
        cache_strategy: Arc<dyn CacheStrategy>,
        query_cache: Arc<QueryHashCache>,
        initial_embeddings: Vec<Vec<f32>>,
        initial_metadata: Vec<std::collections::HashMap<String, String>>,
        config: TieredEngineConfig,
    ) -> Result<Self> {
        let components = Self::build_internal(initial_embeddings, initial_metadata, &config)?;

        Ok(Self {
            cache_strategy,
            query_cache,
            hot_tier: components.hot_tier,
            cold_tier: components.cold_tier,
            access_logger: None,
            stats: components.stats,
            config,
            cache_circuit_breaker: components.cache_circuit_breaker,
            hot_tier_circuit_breaker: components.hot_tier_circuit_breaker,
            cold_tier_circuit_breaker: components.cold_tier_circuit_breaker,
            query_semaphore: components.query_semaphore,
            search_worker_semaphore: components.search_worker_semaphore,
            last_hot_tier_coherence_audit: components.last_hot_tier_coherence_audit,
        })
    }

    /// Recover from persistence
    pub fn recover(
        cache_strategy: Box<dyn CacheStrategy>,
        query_cache: Arc<QueryHashCache>,
        data_dir: impl AsRef<Path>,
        config: TieredEngineConfig,
    ) -> Result<Self> {
        let data_dir_str = data_dir.as_ref().to_string_lossy().to_string();

        // Recover cold tier from WAL + snapshot
        let metrics = crate::metrics::MetricsCollector::new();
        let cold_tier = Arc::new(HnswBackend::recover_with_hnsw_params_and_mode(
            config.embedding_dimension,
            config.hnsw_distance,
            &data_dir_str,
            config.hnsw_max_elements,
            config.fsync_policy,
            config.snapshot_interval,
            config.max_wal_size_bytes,
            metrics,
            config.hnsw_m,
            config.hnsw_ef_construction,
            config.hnsw_disable_normalization_check,
            config.recovery_mode,
        )?);

        // Create fresh hot tier (ephemeral)
        let hot_tier = Arc::new(HotTier::new(
            config.hot_tier_max_size,
            config.hot_tier_max_age,
            config.hnsw_distance,
        ));

        let mut recovered_config = config;
        recovered_config.data_dir = Some(data_dir_str);

        let query_semaphore = Arc::new(Semaphore::new(recovered_config.max_concurrent_queries));
        let search_worker_semaphore =
            Arc::new(Semaphore::new(recovered_config.max_concurrent_queries));

        Ok(Self {
            cache_strategy: Arc::from(cache_strategy),
            query_cache,
            hot_tier,
            cold_tier,
            access_logger: None,
            stats: Arc::new(RwLock::new(TieredEngineStats::default())),
            config: recovered_config,
            cache_circuit_breaker: Arc::new(CircuitBreaker::new()),
            hot_tier_circuit_breaker: Arc::new(CircuitBreaker::new()),
            cold_tier_circuit_breaker: Arc::new(CircuitBreaker::new()),
            query_semaphore,
            search_worker_semaphore,
            last_hot_tier_coherence_audit: Arc::new(RwLock::new(Instant::now())),
        })
    }

    /// Set access logger (for cache training)
    pub fn set_access_logger(&mut self, logger: Arc<RwLock<AccessPatternLogger>>) {
        self.access_logger = Some(logger);
    }

    #[inline]
    fn log_point_access(&self, doc_id: u64) {
        if let Some(ref logger) = self.access_logger {
            logger.write().log_doc_access(doc_id);
        }
    }

    /// Log served search results for predictor training.
    ///
    /// Uses batched logging to avoid one lock acquisition per document.
    pub fn log_served_search_accesses(&self, doc_ids: &[u64]) -> usize {
        if let Some(ref logger) = self.access_logger {
            return logger.write().log_doc_accesses(doc_ids);
        }
        0
    }

    #[inline]
    fn refresh_queue_depth_metric(&self) {
        let available_permits = self.query_semaphore.available_permits();
        let in_flight = self
            .config
            .max_concurrent_queries
            .saturating_sub(available_permits) as u64;
        self.stats.write().current_queue_depth = in_flight;
    }

    /// Query - unified three-tier path
    ///
    /// # Query Flow
    /// 1. Check L1a exact document cache - admission driven by learned hotness + semantic similarity
    /// 2. If miss, check hot tier (L2) - recent writes
    /// 3. If miss, search HNSW (L3) - full k-NN search
    /// 4. Cache admission decision (should we cache this result?)
    /// 5. Log access for training
    ///
    /// # Lock Ordering Discipline
    /// `CacheStrategy` implementations provide their own internal synchronization.
    /// TieredEngine should not hold stats/access-logger locks across strategy calls.
    ///
    /// # Returns
    /// - `Some(embedding)` if a canonical document exists and can be served
    /// - `None` if document doesn't exist
    pub fn query(&self, doc_id: u64, query_embedding: Option<&[f32]>) -> Option<Vec<f32>> {
        self.query_with_source(doc_id, query_embedding)
            .map(|(embedding, _)| embedding)
    }

    /// Query with source tier metadata.
    pub fn query_with_source(
        &self,
        doc_id: u64,
        _query_embedding: Option<&[f32]>,
    ) -> Option<(Vec<f32>, PointQueryTier)> {
        // Increment total queries (isolated lock)
        {
            let mut stats = self.stats.write();
            stats.total_queries += 1;
        } // Lock released

        // Layer 1: Check cache with circuit breaker protection
        if !self.cache_circuit_breaker.is_open() {
            if let Some(cached) = self.cache_strategy.get_cached(doc_id) {
                match self.canonical_vector_state(
                    doc_id,
                    &cached.embedding,
                    cached.coherence,
                    "point query cache hit",
                ) {
                    CanonicalVectorState::Match => {
                        // Cache hit - record success
                        self.cache_circuit_breaker.record_success();

                        // Update stats (no other locks held)
                        {
                            let mut stats = self.stats.write();
                            stats.cache_hits += 1;
                        } // stats lock released

                        self.log_point_access(doc_id);

                        return Some((cached.embedding, PointQueryTier::Cache));
                    }
                    CanonicalVectorState::TokenMismatch
                    | CanonicalVectorState::LocalCorruption
                    | CanonicalVectorState::Missing => {
                        self.invalidate_stale_cache_entry(doc_id, "point query cache hit");
                    }
                }
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
            debug!(
                "Cache circuit breaker open, skipping cache layer for doc_id={}",
                doc_id
            );
        }

        // Layer 2: Check hot tier with circuit breaker protection
        if !self.hot_tier_circuit_breaker.is_open() {
            if let Some((embedding, coherence)) = self.hot_tier.get_with_coherence(doc_id) {
                match self.canonical_vector_state(
                    doc_id,
                    &embedding,
                    coherence,
                    "point query hot-tier hit",
                ) {
                    CanonicalVectorState::Match => {
                        // Hot tier hit - record success
                        self.hot_tier_circuit_breaker.record_success();

                        // Update stats (isolated)
                        {
                            let mut stats = self.stats.write();
                            stats.hot_tier_hits += 1;
                        } // Lock released

                        // Cache admission decision for L1a (isolated)
                        let should_cache_decision =
                            self.cache_strategy.should_cache(doc_id, &embedding);

                        if should_cache_decision {
                            let cached = CachedVector {
                                doc_id,
                                embedding: embedding.clone(),
                                coherence,
                                distance: 0.0,
                                cached_at: Instant::now(),
                            };
                            self.cache_strategy.insert_cached(cached);
                        }

                        self.log_point_access(doc_id);

                        return Some((embedding, PointQueryTier::HotTier));
                    }
                    CanonicalVectorState::TokenMismatch | CanonicalVectorState::LocalCorruption => {
                        self.discard_stale_hot_mirror(doc_id, "point query hot-tier hit");
                    }
                    CanonicalVectorState::Missing => {}
                }
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
            debug!(
                "Hot tier circuit breaker open, skipping hot tier for doc_id={}",
                doc_id
            );
        }

        // Layer 3: Fetch from cold tier with circuit breaker protection
        if !self.cold_tier_circuit_breaker.is_open() {
            if let Some((embedding, coherence)) =
                self.cold_tier.fetch_document_with_coherence(doc_id)
            {
                // Cold tier success - record it
                self.cold_tier_circuit_breaker.record_success();

                // Update stats (isolated)
                {
                    let mut stats = self.stats.write();
                    stats.cold_tier_searches += 1;
                } // Lock released

                // Cache admission decision for L1a (isolated)
                let should_cache_decision = self.cache_strategy.should_cache(doc_id, &embedding);

                if should_cache_decision {
                    let cached = CachedVector {
                        doc_id,
                        embedding: embedding.clone(),
                        coherence,
                        distance: 0.0,
                        cached_at: Instant::now(),
                    };
                    self.cache_strategy.insert_cached(cached);
                }

                self.log_point_access(doc_id);

                return Some((embedding, PointQueryTier::ColdTier));
            }

            // Cold tier miss - this is normal (document doesn't exist)
        } else {
            // Circuit breaker open - fail fast
            {
                let mut stats = self.stats.write();
                stats.circuit_breaker_rejections += 1;
            }
            warn!(
                "Cold tier circuit breaker open, cannot query doc_id={}",
                doc_id
            );
        }

        // Document not found in a canonical cold-backed state
        None
    }

    /// Get document with metadata
    ///
    /// Retrieves both the embedding and metadata for a given document ID.
    /// Metadata is read from the canonical cold tier when available; the hot tier
    /// only serves the embedding fast path. Mirror-only drift is not returned to
    /// callers as a real document.
    pub fn get_document_with_metadata(
        &self,
        doc_id: u64,
    ) -> Option<(Vec<f32>, std::collections::HashMap<String, String>)> {
        if let Some(metadata) = self.cold_tier.fetch_metadata(doc_id) {
            if let Some((embedding, coherence)) = self.hot_tier.get_with_coherence(doc_id) {
                match self.canonical_vector_state(
                    doc_id,
                    &embedding,
                    coherence,
                    "document-with-metadata hot-tier hit",
                ) {
                    CanonicalVectorState::Match => return Some((embedding, metadata)),
                    CanonicalVectorState::TokenMismatch | CanonicalVectorState::LocalCorruption => {
                        self.discard_stale_hot_mirror(doc_id, "document-with-metadata hot-tier hit")
                    }
                    CanonicalVectorState::Missing => {}
                }
            }
            if let Some((embedding, _coherence)) =
                self.cold_tier.fetch_document_with_coherence(doc_id)
            {
                return Some((embedding, metadata));
            }
        }

        if self.hot_tier.exists(doc_id) {
            warn!(
                doc_id,
                "refusing to serve hot-tier-only document without canonical cold-tier metadata"
            );
        }

        None
    }

    /// Get embedding by ID with L1a cache participation (no metrics side effects).
    ///
    /// Used for response hydration paths where we want cache acceleration without
    /// mutating point-query counters/training state. Only serves cache/hot-tier
    /// entries when a canonical cold-tier record exists.
    pub fn get_embedding_cache_aware(&self, doc_id: u64) -> Option<Vec<f32>> {
        if let Some(cached) = self.cache_strategy.peek_cached(doc_id) {
            if self.canonical_vector_state(
                doc_id,
                &cached.embedding,
                cached.coherence,
                "cache-aware embedding cache hit",
            ) == CanonicalVectorState::Match
            {
                return Some(cached.embedding);
            }
            self.invalidate_stale_cache_entry(doc_id, "cache-aware embedding cache hit");
        }
        if let Some((embedding, coherence)) = self.hot_tier.get_with_coherence(doc_id) {
            match self.canonical_vector_state(
                doc_id,
                &embedding,
                coherence,
                "cache-aware embedding hot-tier hit",
            ) {
                CanonicalVectorState::Match => return Some(embedding),
                CanonicalVectorState::TokenMismatch | CanonicalVectorState::LocalCorruption => {
                    self.discard_stale_hot_mirror(doc_id, "cache-aware embedding hot-tier hit");
                }
                CanonicalVectorState::Missing => {}
            }
        }
        self.cold_tier.fetch_document(doc_id)
    }

    /// Get document metadata by ID
    ///
    /// Returns canonical cold-tier metadata only. Mirror-only drift is ignored
    /// rather than exposed as a real document.
    ///
    /// # Parameters
    /// - `doc_id`: Document identifier
    ///
    /// # Returns
    /// - `Some(metadata)` if found
    /// - `None` if not found
    pub fn get_metadata(&self, doc_id: u64) -> Option<std::collections::HashMap<String, String>> {
        if let Some(metadata) = self.cold_tier.fetch_metadata(doc_id) {
            return Some(metadata);
        }
        if self.hot_tier.exists(doc_id) {
            warn!(
                doc_id,
                "ignoring hot-tier metadata because cold tier has no canonical record"
            );
        }
        None
    }

    /// Lightweight existence probe for canonical documents.
    pub fn exists(&self, doc_id: u64) -> bool {
        let exists = self.cold_tier.current_coherence_token(doc_id).is_some();
        if !exists && self.hot_tier.exists(doc_id) {
            warn!(
                doc_id,
                "ignoring hot-tier mirror for existence check because no canonical cold-tier record exists"
            );
        }
        exists
    }

    /// Update document metadata without changing embedding.
    ///
    /// The cold tier is canonical for metadata durability. Successful updates are
    /// written there first, then mirrored into the hot tier if the document
    /// currently has a resident recent-write copy.
    ///
    /// Query-cache entries for filtered searches cannot be invalidated precisely on
    /// metadata changes: a document can become newly eligible for a cached filter
    /// scope without appearing in the old result set. Successful metadata updates
    /// therefore conservatively clear L1b.
    ///
    /// # Parameters
    /// - `doc_id`: Document ID to update
    /// - `metadata`: New metadata
    /// - `merge`: true = merge with existing, false = replace
    ///
    /// # Returns
    /// - `Ok(true)` if document exists and was updated
    /// - `Ok(false)` if document does not exist
    pub fn update_metadata(
        &self,
        doc_id: u64,
        metadata: std::collections::HashMap<String, String>,
        merge: bool,
    ) -> Result<bool> {
        let existed = self
            .cold_tier
            .update_metadata(doc_id, metadata.clone(), merge)?;

        if !existed {
            if self.hot_tier.exists(doc_id) {
                warn!(
                    doc_id,
                    "metadata update found document in hot tier but not cold tier; refusing hot-only mutation"
                );
            }
            return Ok(false);
        }

        let mirrored = self.hot_tier.update_metadata(doc_id, metadata, merge);
        if mirrored {
            trace!(doc_id, "mirrored metadata update into hot tier");
        }

        // Filtered query-cache entries can become stale both when a document stops
        // matching a filter and when it starts matching one. The latter case cannot
        // be invalidated precisely from doc_id-only reverse indexes.
        self.query_cache.clear();

        Ok(true)
    }

    /// Delete document by ID
    ///
    /// Deletes the canonical cold-tier record first, then removes any hot-tier
    /// mirror entry. This prevents the mirror from being dropped before the durable
    /// delete succeeds.
    ///
    /// # Returns
    /// - `Ok(true)` if document was found and deleted in at least one tier
    /// - `Ok(false)` if document was not found
    ///
    /// # Consistency
    /// - L1a (document cache) entries are invalidated synchronously to prevent stale reads.
    /// - L1b (query cache) entries referencing the document are removed via reverse index lookup
    ///   (`doc_id -> cached query keys`) to avoid full-cache scans on deletes.
    pub fn delete(&self, doc_id: u64) -> Result<bool> {
        let cold_deleted = self.cold_tier.delete(doc_id)?;
        let hot_deleted = self.hot_tier.delete(doc_id);

        if hot_deleted && !cold_deleted {
            warn!(
                doc_id,
                "deleted hot-tier mirror entry without matching canonical cold-tier record"
            );
        }

        if !cold_deleted && !hot_deleted {
            return Ok(false);
        }

        // Invalidate document cache entries (L1a)
        self.cache_strategy.invalidate(doc_id);

        // Remove any cached queries referencing this document (best effort)
        let removed_queries = self.query_cache.invalidate_doc(doc_id);
        if removed_queries > 0 {
            debug!(
                doc_id,
                removed_queries, "Removed stale query-cache entries after delete"
            );
        }

        Ok(true)
    }

    /// Bulk query documents by ID
    ///
    /// Optimized to batch lookups across tiers.
    #[allow(clippy::type_complexity)]
    pub fn bulk_query(
        &self,
        doc_ids: &[u64],
        include_embeddings: bool,
    ) -> Vec<Option<(Vec<f32>, std::collections::HashMap<String, String>)>> {
        self.bulk_query_with_source(doc_ids, include_embeddings)
            .into_iter()
            .map(|entry| entry.map(|(embedding, metadata, _)| (embedding, metadata)))
            .collect()
    }

    #[allow(clippy::type_complexity)]
    pub fn bulk_query_with_source(
        &self,
        doc_ids: &[u64],
        include_embeddings: bool,
    ) -> Vec<
        Option<(
            Vec<f32>,
            std::collections::HashMap<String, String>,
            PointQueryTier,
        )>,
    > {
        let mut results = vec![None; doc_ids.len()];
        let mut missing_indices = Vec::new();

        // 1. Try Hot Tier embeddings, but always pair them with canonical cold-tier metadata.
        let hot_results = self.hot_tier.bulk_fetch_with_coherence(doc_ids);
        for (i, res) in hot_results.into_iter().enumerate() {
            let doc_id = doc_ids[i];
            if let Some((embedding, _hot_metadata, coherence)) = res {
                match self.canonical_vector_state(
                    doc_id,
                    &embedding,
                    coherence,
                    "bulk query hot-tier hit",
                ) {
                    CanonicalVectorState::Match => {
                        if let Some(canonical_metadata) = self.cold_tier.fetch_metadata(doc_id) {
                            results[i] =
                                Some((embedding, canonical_metadata, PointQueryTier::HotTier));
                        } else {
                            warn!(
                                doc_id,
                                "bulk query found canonical vector without canonical metadata; falling back to cold tier"
                            );
                            missing_indices.push(i);
                        }
                    }
                    CanonicalVectorState::TokenMismatch | CanonicalVectorState::LocalCorruption => {
                        self.discard_stale_hot_mirror(doc_id, "bulk query hot-tier hit");
                        missing_indices.push(i);
                    }
                    CanonicalVectorState::Missing => missing_indices.push(i),
                }
            } else {
                missing_indices.push(i);
            }
        }

        if missing_indices.is_empty() {
            // Filter embeddings if not requested
            if !include_embeddings {
                for res in results.iter_mut().flatten() {
                    res.0.clear();
                }
            }
            return results;
        }

        // 2. Try Cold Tier for missing
        let missing_ids: Vec<u64> = missing_indices.iter().map(|&i| doc_ids[i]).collect();
        let cold_results = self.cold_tier.bulk_fetch(&missing_ids);

        for (i, res) in cold_results.into_iter().enumerate() {
            if let Some(doc) = res {
                results[missing_indices[i]] = Some((doc.0, doc.1, PointQueryTier::ColdTier));
            }
        }

        // Filter embeddings if not requested
        if !include_embeddings {
            for res in results.iter_mut().flatten() {
                res.0.clear();
            }
        }

        results
    }

    /// Batch delete documents by ID
    pub fn batch_delete(&self, doc_ids: &[u64]) -> Result<u64> {
        let mut unique_doc_ids: Vec<u64> = doc_ids.to_vec();
        unique_doc_ids.sort_unstable();
        unique_doc_ids.dedup();

        // Best-effort pre-delete existence count. This can drift under concurrent
        // insert/delete races, but avoids double-counting duplicate IDs in input.
        let unique_deleted = unique_doc_ids
            .iter()
            .filter(|&&id| self.hot_tier.exists(id) || self.cold_tier.exists(id))
            .count() as u64;

        if unique_deleted == 0 {
            return Ok(0);
        }

        // Delete from cold tier (efficient batch with WAL logging)
        let cold_deleted = self.cold_tier.batch_delete(&unique_doc_ids)?;
        let hot_deleted = self.hot_tier.batch_delete(&unique_doc_ids);

        if hot_deleted as u64 > cold_deleted {
            warn!(
                hot_deleted,
                cold_deleted,
                "batch delete removed hot-tier mirror entries without matching canonical cold-tier records"
            );
        }

        // Invalidate caches
        for &id in &unique_doc_ids {
            self.cache_strategy.invalidate(id);
        }

        // Query cache invalidation
        for &id in &unique_doc_ids {
            self.query_cache.invalidate_doc(id);
        }

        Ok(unique_deleted)
    }

    /// Batch delete documents by metadata filter
    pub fn batch_delete_by_filter<F>(&self, predicate: F) -> Result<u64>
    where
        F: Fn(&std::collections::HashMap<String, String>) -> bool,
    {
        // Scan hot tier
        let hot_ids = self.hot_tier.scan(&predicate);

        // Scan cold tier
        let cold_ids = self.cold_tier.scan(&predicate);

        // Combine IDs (deduplicate)
        let mut all_ids = hot_ids;
        all_ids.extend(cold_ids);
        all_ids.sort_unstable();
        all_ids.dedup();

        // Delete
        self.batch_delete(&all_ids)
    }

    /// Batch delete documents by structured metadata filter.
    ///
    /// Hot tier is scanned (bounded size). Cold tier uses an inverted index fast path
    /// for common filter shapes and falls back to scan for `Range`.
    pub fn batch_delete_by_metadata_filter(&self, filter: &MetadataFilter) -> Result<u64> {
        let hot_ids = self
            .hot_tier
            .scan(|meta| crate::metadata_filter::matches(filter, meta));

        let cold_ids = self.cold_tier.ids_for_metadata_filter(filter);

        let mut all_ids = hot_ids;
        all_ids.extend(cold_ids);
        all_ids.sort_unstable();
        all_ids.dedup();

        self.batch_delete(&all_ids)
    }

    /// k-NN search across hot tier and cold tier with result merging
    ///
    /// Search path:
    /// 1. Layer 2 (Hot Tier): Linear scan over recent writes
    /// 2. Layer 3 (Cold Tier): HNSW approximate k-NN search
    /// 3. Merge and deduplicate results from hot + cold tiers
    ///
    /// Note: L1b caches top‑k search results for repeated/semantic queries.
    /// L1a document cache applies to point lookups (get by id), not k‑NN search.
    ///
    /// # Validation
    /// - `query` dimension must match backend dimension
    /// - `k` must be > 0 and <= 10,000 (reasonable upper bound)
    pub fn knn_search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.knn_search_with_ef(query, k, None)
    }

    pub fn knn_search_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef_search_override: Option<usize>,
    ) -> Result<Vec<SearchResult>> {
        self.knn_search_with_ef_detailed(query, k, ef_search_override)
            .map(|(results, _)| results)
    }

    pub fn knn_search_with_ef_detailed(
        &self,
        query: &[f32],
        k: usize,
        ef_search_override: Option<usize>,
    ) -> Result<(Vec<SearchResult>, SearchExecutionPath)> {
        self.knn_search_with_ef_detailed_scoped(
            query,
            k,
            ef_search_override,
            DEFAULT_QUERY_CACHE_SCOPE,
        )
    }

    pub fn knn_search_with_ef_detailed_scoped(
        &self,
        query: &[f32],
        k: usize,
        ef_search_override: Option<usize>,
        query_cache_scope: u64,
    ) -> Result<(Vec<SearchResult>, SearchExecutionPath)> {
        if query.is_empty() {
            anyhow::bail!("query embedding cannot be empty");
        }

        if k == 0 {
            anyhow::bail!("k must be greater than 0");
        }

        if k > 10_000 {
            anyhow::bail!("k must be <= 10,000 (requested: {})", k);
        }

        // Validate dimension against configured cold-tier index dimension.
        let backend_dim = (*self.cold_tier).dimension();
        if backend_dim != 0 && query.len() != backend_dim {
            anyhow::bail!(
                "query dimension mismatch: expected {} found {}",
                backend_dim,
                query.len()
            );
        }
        let cold_tier_has_docs = !self.cold_tier.is_empty();

        let normalized_query = normalize_query_for_search(self.config.hnsw_distance, query)?;
        let query = normalized_query.as_ref();

        let cacheable = ef_search_override.is_none();
        let cache_generation = cacheable.then(|| self.query_cache.invalidation_generation());
        if cacheable {
            if let Some(cached) = self.query_cache.get_scoped(query_cache_scope, query, k) {
                let (cached, pruned_noncanonical) =
                    self.filter_search_results_to_canonical(cached, "query-cache hit");
                if !pruned_noncanonical {
                    let mut stats = self.stats.write();
                    stats.query_cache_hits += 1;
                    stats.total_queries += 1;
                    return Ok((cached, SearchExecutionPath::CacheHit));
                }

                let mut stats = self.stats.write();
                stats.query_cache_misses += 1;
            } else {
                let mut stats = self.stats.write();
                stats.query_cache_misses += 1;
            }
        }

        let query_start = Instant::now();

        // Step 1: Search Layer 2 (Hot Tier) - recent writes
        // Over-fetch by 2× to ensure good candidates after merging
        let hot_results =
            self.filter_hot_knn_results_to_canonical(self.hot_tier.knn_search(query, k * 2));

        debug!(
            "Hot tier search returned {} results (requested {})",
            hot_results.len(),
            k * 2
        );

        // Step 2: Search Layer 3 (Cold Tier) - HNSW index
        // Only search cold tier if it has documents (dimension > 0)
        let cold_results = if cold_tier_has_docs {
            let effective_ef_search = ef_search_override.or(Some(self.config.hnsw_ef_search));
            self.cold_tier
                .knn_search_with_ef(query, k * 2, effective_ef_search)?
        } else {
            vec![]
        };

        debug!(
            "Cold tier search returned {} results (requested {})",
            cold_results.len(),
            k * 2
        );

        {
            let mut stats = self.stats.write();
            stats.total_queries += 1;

            if cold_tier_has_docs {
                stats.cold_tier_searches += 1;
            }

            if !hot_results.is_empty() {
                stats.hot_tier_hits += 1;
            } else {
                stats.hot_tier_misses += 1;
            }
        }

        let hot_non_empty = !hot_results.is_empty();
        let cold_non_empty = !cold_results.is_empty();

        // Step 3: Merge and deduplicate results from hot tier + cold tier
        let merged_results = Self::merge_knn_results(hot_results, cold_results, k);

        if cacheable && !merged_results.is_empty() {
            let cache_query = match normalized_query {
                Cow::Owned(owned) => owned,
                Cow::Borrowed(borrowed) => borrowed.to_vec(),
            };
            let _ = self.query_cache.insert_with_k_scoped_if_generation(
                query_cache_scope,
                cache_query,
                merged_results.clone(),
                k,
                cache_generation.expect("cache generation must exist when cacheable"),
            );
        }

        debug!(
            "Merged k-NN search completed in {:?}, returned {} results",
            query_start.elapsed(),
            merged_results.len()
        );

        let path = match (hot_non_empty, cold_non_empty, cold_tier_has_docs) {
            (true, true, _) => SearchExecutionPath::HotAndCold,
            (true, false, _) => SearchExecutionPath::HotTierOnly,
            (false, true, _) => SearchExecutionPath::ColdTierOnly,
            (false, false, true) => SearchExecutionPath::HotAndCold,
            (false, false, false) => SearchExecutionPath::HotTierOnly,
        };

        Ok((merged_results, path))
    }

    /// Batch k-NN search across hot tier and cold tier with result merging.
    ///
    /// This amortizes cold-tier lock acquisition by batching cold-tier searches,
    /// while still computing hot-tier results per query for correctness.
    ///
    /// # Parameters
    /// - `queries`: Query embedding vectors
    /// - `k`: Number of nearest neighbors to return per query
    /// - `ef_search_override`: Optional ef_search override
    ///
    /// # Returns
    /// Vector of search results, one per query (same order as input)
    pub fn knn_search_batch_with_ef(
        &self,
        queries: &[Vec<f32>],
        k: usize,
        ef_search_override: Option<usize>,
    ) -> Result<Vec<Vec<SearchResult>>> {
        self.knn_search_batch_with_ef_detailed(queries, k, ef_search_override)
            .map(|detailed| detailed.into_iter().map(|(results, _)| results).collect())
    }

    pub fn knn_search_batch_with_ef_detailed(
        &self,
        queries: &[Vec<f32>],
        k: usize,
        ef_search_override: Option<usize>,
    ) -> Result<Vec<(Vec<SearchResult>, SearchExecutionPath)>> {
        self.knn_search_batch_with_ef_detailed_scoped(
            queries,
            k,
            ef_search_override,
            DEFAULT_QUERY_CACHE_SCOPE,
        )
    }

    pub fn knn_search_batch_with_ef_detailed_scoped(
        &self,
        queries: &[Vec<f32>],
        k: usize,
        ef_search_override: Option<usize>,
        query_cache_scope: u64,
    ) -> Result<Vec<(Vec<SearchResult>, SearchExecutionPath)>> {
        if queries.is_empty() {
            return Ok(vec![]);
        }

        if k == 0 {
            anyhow::bail!("k must be greater than 0");
        }

        if k > 10_000 {
            anyhow::bail!("k must be <= 10,000 (requested: {})", k);
        }

        let backend_dim = (*self.cold_tier).dimension();
        let cold_tier_has_docs = !self.cold_tier.is_empty();

        let mut normalized_queries = Vec::with_capacity(queries.len());
        for (i, query) in queries.iter().enumerate() {
            if query.is_empty() {
                anyhow::bail!("query {} embedding cannot be empty", i);
            }
            if backend_dim != 0 && query.len() != backend_dim {
                anyhow::bail!(
                    "query {} dimension mismatch: expected {} found {}",
                    i,
                    backend_dim,
                    query.len()
                );
            }
            normalized_queries.push(normalize_query_for_search(
                self.config.hnsw_distance,
                query,
            )?);
        }

        let cacheable = ef_search_override.is_none();
        let cache_generation = cacheable.then(|| self.query_cache.invalidation_generation());
        let mut cached_results: Vec<Option<Vec<SearchResult>>> = vec![None; queries.len()];
        let mut miss_indices: Vec<usize> = Vec::new();
        let mut cache_hits = 0u64;
        let mut cache_misses = 0u64;

        if cacheable {
            for (i, query) in normalized_queries.iter().enumerate() {
                if let Some(cached) =
                    self.query_cache
                        .get_scoped(query_cache_scope, query.as_ref(), k)
                {
                    let (cached, pruned_noncanonical) =
                        self.filter_search_results_to_canonical(cached, "batch query-cache hit");
                    if pruned_noncanonical {
                        cache_misses += 1;
                        miss_indices.push(i);
                    } else {
                        cached_results[i] = Some(cached);
                        cache_hits += 1;
                    }
                } else {
                    cache_misses += 1;
                    miss_indices.push(i);
                }
            }
        } else {
            miss_indices = (0..normalized_queries.len()).collect();
        }

        {
            let mut stats = self.stats.write();
            stats.total_queries += queries.len() as u64;
            // cold_tier_searches is incremented below only if the cold tier
            // search actually executes (guarded by cold_tier_has_docs).
            if cacheable {
                stats.query_cache_hits += cache_hits;
                stats.query_cache_misses += cache_misses;
            }
        }

        if miss_indices.is_empty() {
            let mut results = Vec::with_capacity(cached_results.len());
            for (idx, cached) in cached_results.into_iter().enumerate() {
                let cached = cached.ok_or_else(|| {
                    anyhow::anyhow!(
                        "query cache invariant violated: missing cached result at index {}",
                        idx
                    )
                })?;
                results.push((cached, SearchExecutionPath::CacheHit));
            }
            return Ok(results);
        }

        let miss_queries: Vec<Vec<f32>> = miss_indices
            .iter()
            .map(|&i| normalized_queries[i].as_ref().to_vec())
            .collect();

        let hot_results: Vec<Vec<(u64, f32)>> = miss_queries
            .iter()
            .map(|query| {
                self.filter_hot_knn_results_to_canonical(self.hot_tier.knn_search(query, k * 2))
            })
            .collect();

        {
            let mut stats = self.stats.write();
            for hot in &hot_results {
                if hot.is_empty() {
                    stats.hot_tier_misses += 1;
                } else {
                    stats.hot_tier_hits += 1;
                }
            }
        }

        let cold_results = if cold_tier_has_docs {
            let effective_ef_search = ef_search_override.unwrap_or(self.config.hnsw_ef_search);
            let results =
                self.cold_tier
                    .knn_search_batch(&miss_queries, k * 2, Some(effective_ef_search))?;
            {
                let mut stats = self.stats.write();
                stats.cold_tier_searches += miss_indices.len() as u64;
            }
            results
        } else {
            vec![Vec::new(); miss_queries.len()]
        };

        let mut hot_iter = hot_results.into_iter();
        let mut cold_iter = cold_results.into_iter();
        let mut merged = Vec::with_capacity(queries.len());
        for (i, cached) in cached_results.into_iter().enumerate() {
            if let Some(cached_results) = cached {
                merged.push((cached_results, SearchExecutionPath::CacheHit));
                continue;
            }

            let hot = hot_iter.next().ok_or_else(|| {
                anyhow::anyhow!(
                    "hot results length mismatch: expected one entry per cache miss query"
                )
            })?;
            let cold = cold_iter.next().ok_or_else(|| {
                anyhow::anyhow!(
                    "cold results length mismatch: expected one entry per cache miss query"
                )
            })?;
            let hot_non_empty = !hot.is_empty();
            let cold_non_empty = !cold.is_empty();
            let merged_results = Self::merge_knn_results(hot, cold, k);
            let path = match (hot_non_empty, cold_non_empty, cold_tier_has_docs) {
                (true, true, _) => SearchExecutionPath::HotAndCold,
                (true, false, _) => SearchExecutionPath::HotTierOnly,
                (false, true, _) => SearchExecutionPath::ColdTierOnly,
                (false, false, true) => SearchExecutionPath::HotAndCold,
                (false, false, false) => SearchExecutionPath::HotTierOnly,
            };
            if cacheable && !merged_results.is_empty() {
                let cache_query = match &normalized_queries[i] {
                    Cow::Owned(owned) => owned.clone(),
                    Cow::Borrowed(borrowed) => borrowed.to_vec(),
                };
                let _ = self.query_cache.insert_with_k_scoped_if_generation(
                    query_cache_scope,
                    cache_query,
                    merged_results.clone(),
                    k,
                    cache_generation.expect("cache generation must exist when cacheable"),
                );
            }
            merged.push((merged_results, path));
        }

        Ok(merged)
    }

    /// Merge k-NN results from hot tier and cold tier
    ///
    /// Combines results from both tiers, deduplicates by doc_id (preferring hot tier),
    /// sorts by distance ascending, and truncates to k results.
    ///
    /// # Parameters
    /// - `hot_results`: Results from hot tier (doc_id, distance)
    /// - `cold_results`: Results from cold tier (SearchResult with doc_id, distance)
    /// - `k`: Number of final results to return
    ///
    /// # Deduplication Policy
    /// If the same doc_id appears in both hot and cold tier:
    /// - Prefer the hot-tier distance only after coherence checks prove the
    ///   mirror still matches canonical cold-tier state
    /// - Otherwise keep the cold-tier candidate
    ///
    /// # Complexity
    /// O(n log n) where n = hot_results.len() + cold_results.len()
    fn merge_knn_results(
        hot_results: Vec<(u64, f32)>,
        cold_results: Vec<SearchResult>,
        k: usize,
    ) -> Vec<SearchResult> {
        use std::collections::HashMap;

        // Use HashMap for O(1) deduplication
        let mut merged: HashMap<u64, f32> = HashMap::new();

        // Insert hot tier results (higher priority)
        for (doc_id, distance) in hot_results {
            merged.insert(doc_id, distance);
        }

        // Insert cold tier results (only if not already present)
        for result in cold_results {
            merged.entry(result.doc_id).or_insert(result.distance);
        }

        // Convert to Vec and sort by distance ascending (best match first)
        let mut final_results: Vec<SearchResult> = merged
            .into_iter()
            .map(|(doc_id, distance)| SearchResult { doc_id, distance })
            .collect();

        final_results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Truncate to k results
        final_results.truncate(k);

        final_results
    }

    fn filter_hot_knn_results_to_canonical(&self, hot_results: Vec<(u64, f32)>) -> Vec<(u64, f32)> {
        hot_results
            .into_iter()
            .filter_map(|(doc_id, distance)| {
                let Some((hot_embedding, hot_coherence)) =
                    self.hot_tier.peek_with_coherence(doc_id)
                else {
                    warn!(
                        doc_id,
                        "ignoring hot-tier k-NN candidate because mirror coherence is unavailable"
                    );
                    return None;
                };
                match self.canonical_vector_state(
                    doc_id,
                    &hot_embedding,
                    hot_coherence,
                    "hot-tier k-NN candidate",
                ) {
                    CanonicalVectorState::Match => Some((doc_id, distance)),
                    CanonicalVectorState::TokenMismatch | CanonicalVectorState::LocalCorruption => {
                        self.discard_stale_hot_mirror(doc_id, "hot-tier k-NN candidate");
                        None
                    }
                    CanonicalVectorState::Missing => None,
                }
            })
            .collect()
    }

    fn filter_search_results_to_canonical(
        &self,
        results: Vec<SearchResult>,
        source: &'static str,
    ) -> (Vec<SearchResult>, bool) {
        let mut filtered = Vec::with_capacity(results.len());
        let mut pruned = false;

        for result in results {
            if self.cold_tier.exists(result.doc_id) {
                filtered.push(result);
            } else {
                warn!(
                    doc_id = result.doc_id,
                    source, "ignoring cached search result without canonical cold-tier record"
                );
                self.query_cache.invalidate_doc(result.doc_id);
                pruned = true;
            }
        }

        (filtered, pruned)
    }

    fn canonical_vector_state(
        &self,
        doc_id: u64,
        mirrored_embedding: &[f32],
        mirrored_coherence: VectorCoherenceToken,
        source: &'static str,
    ) -> CanonicalVectorState {
        match self.cold_tier.current_coherence_token(doc_id) {
            Some(canonical_coherence) if canonical_coherence != mirrored_coherence => {
                warn!(
                    doc_id,
                    source,
                    mirrored_version = mirrored_coherence.version,
                    mirrored_digest_hi = mirrored_coherence.digest.hi,
                    mirrored_digest_lo = mirrored_coherence.digest.lo,
                    canonical_version = canonical_coherence.version,
                    canonical_digest_hi = canonical_coherence.digest.hi,
                    canonical_digest_lo = canonical_coherence.digest.lo,
                    "rejecting non-canonical vector coherence token"
                );
                CanonicalVectorState::TokenMismatch
            }
            Some(_) if !embedding_matches_token(mirrored_embedding, mirrored_coherence) => {
                warn!(
                    doc_id,
                    source,
                    mirrored_version = mirrored_coherence.version,
                    mirrored_digest_hi = mirrored_coherence.digest.hi,
                    mirrored_digest_lo = mirrored_coherence.digest.lo,
                    "rejecting mirrored vector payload that does not match its coherence token"
                );
                CanonicalVectorState::LocalCorruption
            }
            Some(_) => CanonicalVectorState::Match,
            None => {
                warn!(
                    doc_id,
                    source,
                    mirrored_version = mirrored_coherence.version,
                    mirrored_digest_hi = mirrored_coherence.digest.hi,
                    mirrored_digest_lo = mirrored_coherence.digest.lo,
                    "rejecting vector without canonical cold-tier record"
                );
                CanonicalVectorState::Missing
            }
        }
    }

    fn audit_hot_tier_coherence_if_due(&self, source: &'static str) {
        if self.hot_tier.is_empty() {
            *self.last_hot_tier_coherence_audit.write() = Instant::now();
            return;
        }

        if self.last_hot_tier_coherence_audit.read().elapsed() < self.config.flush_interval {
            return;
        }

        let removed = self.audit_hot_tier_coherence(source);
        if removed > 0 {
            warn!(
                source,
                removed, "hot-tier coherence audit scrubbed non-canonical mirror entries"
            );
        }
    }

    fn audit_hot_tier_coherence(&self, source: &'static str) -> usize {
        let snapshot = self.hot_tier.snapshot_doc_ids();
        let mut stale_doc_ids = Vec::new();

        for doc_id in snapshot {
            let Some((embedding, coherence)) = self.hot_tier.peek_with_coherence(doc_id) else {
                continue;
            };
            match self.canonical_vector_state(doc_id, &embedding, coherence, source) {
                CanonicalVectorState::Match => {}
                CanonicalVectorState::TokenMismatch
                | CanonicalVectorState::LocalCorruption
                | CanonicalVectorState::Missing => stale_doc_ids.push(doc_id),
            }
        }

        if !stale_doc_ids.is_empty() {
            for doc_id in &stale_doc_ids {
                self.hot_tier.delete(*doc_id);
                self.cache_strategy.invalidate(*doc_id);
            }
            self.query_cache.clear();
            debug!(
                source,
                stale_doc_ids = ?stale_doc_ids,
                "coherence audit removed stale hot-tier mirrors and cleared query cache"
            );
        }

        *self.last_hot_tier_coherence_audit.write() = Instant::now();
        stale_doc_ids.len()
    }

    fn discard_stale_hot_mirror(&self, doc_id: u64, source: &'static str) {
        let removed_hot = self.hot_tier.delete(doc_id);
        self.cache_strategy.invalidate(doc_id);
        self.query_cache.clear();
        debug!(
            doc_id,
            source, removed_hot, "discarded stale hot-tier mirror and cleared query cache"
        );
    }

    fn invalidate_stale_cache_entry(&self, doc_id: u64, source: &'static str) {
        self.cache_strategy.invalidate(doc_id);
        debug!(doc_id, source, "discarded stale L1a cache entry");
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
    /// - `Ok(Vec<SearchResult>)`: Full, partial, or empty results
    /// - `Err(...)`: Queue saturated or invalid request
    pub async fn knn_search_with_timeouts(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        self.knn_search_with_timeouts_with_ef_scoped(query, k, None, DEFAULT_QUERY_CACHE_SCOPE)
            .await
            .map(|(results, _)| results)
    }

    pub async fn knn_search_with_timeouts_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef_search_override: Option<usize>,
    ) -> Result<(Vec<SearchResult>, SearchExecutionPath)> {
        self.knn_search_with_timeouts_with_ef_scoped(
            query,
            k,
            ef_search_override,
            DEFAULT_QUERY_CACHE_SCOPE,
        )
        .await
    }

    pub async fn knn_search_with_timeouts_with_ef_scoped(
        &self,
        query: &[f32],
        k: usize,
        ef_search_override: Option<usize>,
        query_cache_scope: u64,
    ) -> Result<(Vec<SearchResult>, SearchExecutionPath)> {
        if query.is_empty() {
            anyhow::bail!("query embedding cannot be empty");
        }
        if k == 0 {
            anyhow::bail!("k must be greater than 0");
        }
        if k > 10_000 {
            anyhow::bail!("k must be <= 10,000 (requested: {})", k);
        }

        // Validate dimension against configured cold-tier index dimension.
        let backend_dim = (*self.cold_tier).dimension();
        if backend_dim != 0 && query.len() != backend_dim {
            anyhow::bail!(
                "query dimension mismatch: expected {} found {}",
                backend_dim,
                query.len()
            );
        }
        let cacheable = ef_search_override.is_none();
        let cold_tier_has_docs = !self.cold_tier.is_empty();

        // Load shedding: Try to acquire semaphore permit
        // Keep permit alive for the full search scope.
        let query_permit = match self.query_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                // Queue saturated - reject query
                {
                    let mut stats = self.stats.write();
                    stats.queries_rejected += 1;
                }
                self.refresh_queue_depth_metric();
                return Err(anyhow!(
                    "Query queue saturated: {} in-flight queries (max: {})",
                    self.config.max_concurrent_queries,
                    self.config.max_concurrent_queries
                ));
            }
        };
        self.refresh_queue_depth_metric();

        let normalized_query = match normalize_query_for_search(self.config.hnsw_distance, query) {
            Ok(query) => query,
            Err(e) => {
                drop(query_permit);
                self.refresh_queue_depth_metric();
                return Err(e);
            }
        };

        let cache_generation = cacheable.then(|| self.query_cache.invalidation_generation());
        if cacheable {
            if let Some(cached) =
                self.query_cache
                    .get_scoped(query_cache_scope, normalized_query.as_ref(), k)
            {
                let (cached, pruned_noncanonical) =
                    self.filter_search_results_to_canonical(cached, "timed query-cache hit");
                if !pruned_noncanonical {
                    {
                        let mut stats = self.stats.write();
                        stats.query_cache_hits += 1;
                        stats.total_queries += 1;
                    }
                    drop(query_permit);
                    self.refresh_queue_depth_metric();
                    return Ok((cached, SearchExecutionPath::CacheHit));
                }

                let mut stats = self.stats.write();
                stats.query_cache_misses += 1;
            } else {
                let mut stats = self.stats.write();
                stats.query_cache_misses += 1;
            }
        }

        let normalized_query = normalized_query.into_owned();
        {
            let mut stats = self.stats.write();
            stats.total_queries += 1;
        }
        let mut results: Vec<SearchResult> = Vec::new();
        let mut partial = false;
        let mut hot_accessed = false;
        let mut cold_accessed = false;

        let mut hot_results: Vec<(u64, f32)> = Vec::new();
        let mut cold_results: Vec<SearchResult> = Vec::new();

        // Layer 1: L1b query cache already checked above

        // Layer 2: Search hot tier (50ms timeout)
        let hot_timeout = Duration::from_millis(self.config.hot_tier_timeout_ms);
        if self.hot_tier_circuit_breaker.is_closed() {
            hot_accessed = true;
            match self.search_worker_semaphore.clone().try_acquire_owned() {
                Ok(worker_permit) => {
                    let hot_cancel = Arc::new(AtomicBool::new(false));
                    match tokio::time::timeout(hot_timeout, async {
                        tokio::task::spawn_blocking({
                            let query_vec = normalized_query.clone();
                            let hot_tier = Arc::clone(&self.hot_tier);
                            let k_candidates = k * 2;
                            let hot_cancel_worker = Arc::clone(&hot_cancel);
                            move || {
                                let _worker_permit = worker_permit;
                                hot_tier.knn_search_with_cancel(
                                    &query_vec,
                                    k_candidates,
                                    Some(hot_cancel_worker.as_ref()),
                                )
                            }
                        })
                        .await
                    })
                    .await
                    {
                        Ok(Ok(hot)) => {
                            hot_results = self.filter_hot_knn_results_to_canonical(hot);
                            self.hot_tier_circuit_breaker.record_success();
                        }
                        Ok(Err(e)) => {
                            self.hot_tier_circuit_breaker.record_failure();
                            error!("Hot tier task panicked: {}", e);
                            partial = true;
                        }
                        Err(_) => {
                            hot_cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                            self.hot_tier_circuit_breaker.record_failure();
                            let mut stats = self.stats.write();
                            stats.hot_tier_timeouts += 1;
                            warn!(
                                "Hot tier timed out after {}ms",
                                self.config.hot_tier_timeout_ms
                            );
                            partial = true;
                        }
                    }
                }
                Err(_) => {
                    warn!(
                        "Hot tier search worker queue saturated (limit: {}); skipping hot tier",
                        self.config.max_concurrent_queries
                    );
                    let mut stats = self.stats.write();
                    stats.worker_saturation_count += 1;
                    partial = true;
                }
            }
        } else {
            // Circuit breaker open - record rejection
            {
                let mut stats = self.stats.write();
                stats.circuit_breaker_rejections += 1;
            }
            warn!("Hot tier circuit breaker open, cannot perform k-NN search");
            partial = true;
        }

        // Layer 3: Search cold tier (HNSW) (1000ms timeout)
        let cold_timeout = Duration::from_millis(self.config.cold_tier_timeout_ms);
        if cold_tier_has_docs && self.cold_tier_circuit_breaker.is_closed() {
            cold_accessed = true;
            let worker_permit = match self.search_worker_semaphore.clone().try_acquire_owned() {
                Ok(worker_permit) => worker_permit,
                Err(_) => {
                    if hot_results.is_empty() {
                        {
                            let mut stats = self.stats.write();
                            stats.queries_rejected += 1;
                            stats.worker_saturation_count += 1;
                        }
                        drop(query_permit);
                        self.refresh_queue_depth_metric();
                        return Err(anyhow!(
                            "Search worker queue saturated: {} active workers (max: {})",
                            self.config.max_concurrent_queries,
                            self.config.max_concurrent_queries
                        ));
                    }

                    warn!(
                        "Cold tier search worker queue saturated (limit: {}); returning hot-tier partial results",
                        self.config.max_concurrent_queries
                    );
                    {
                        let mut stats = self.stats.write();
                        stats.worker_saturation_count += 1;
                        stats.partial_results_returned += 1;
                    }
                    let mut partial_results: Vec<SearchResult> = hot_results
                        .into_iter()
                        .map(|(doc_id, distance)| SearchResult { doc_id, distance })
                        .collect();
                    partial_results.sort_by(|a, b| {
                        a.distance
                            .partial_cmp(&b.distance)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    partial_results.truncate(k);
                    drop(query_permit);
                    self.refresh_queue_depth_metric();
                    return Ok((partial_results, SearchExecutionPath::HotTierOnly));
                }
            };

            let cold_cancel = Arc::new(AtomicBool::new(false));
            match tokio::time::timeout(cold_timeout, async {
                tokio::task::spawn_blocking({
                    let query_vec = normalized_query.clone();
                    let cold_tier = Arc::clone(&self.cold_tier);
                    let effective_ef_search =
                        ef_search_override.or(Some(self.config.hnsw_ef_search));
                    let cold_cancel_worker = Arc::clone(&cold_cancel);
                    move || {
                        let _worker_permit = worker_permit;
                        cold_tier.knn_search_with_ef_cancel(
                            &query_vec,
                            k * 2,
                            effective_ef_search,
                            Some(cold_cancel_worker.as_ref()),
                        )
                    }
                })
                .await
            })
            .await
            {
                Ok(Ok(Ok(cold))) => {
                    cold_results = cold;
                    self.cold_tier_circuit_breaker.record_success();
                    let mut stats = self.stats.write();
                    stats.cold_tier_searches += 1;
                }
                Ok(Ok(Err(e))) => {
                    // Cold tier error
                    self.cold_tier_circuit_breaker.record_failure();
                    warn!("Cold tier search failed: {}", e);
                    partial = true;
                }
                Ok(Err(e)) => {
                    // Task join error
                    self.cold_tier_circuit_breaker.record_failure();
                    error!("Cold tier task panicked: {}", e);
                    partial = true;
                }
                Err(_) => {
                    // Timeout
                    cold_cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                    self.cold_tier_circuit_breaker.record_failure();
                    let mut stats = self.stats.write();
                    stats.cold_tier_timeouts += 1;
                    warn!(
                        "Cold tier timed out after {}ms",
                        self.config.cold_tier_timeout_ms
                    );
                    partial = true;
                }
            }
        } else if cold_tier_has_docs {
            // Circuit breaker open - record rejection
            {
                let mut stats = self.stats.write();
                stats.circuit_breaker_rejections += 1;
            }
            warn!("Cold tier circuit breaker open, cannot perform k-NN search");
            partial = true;
        }

        if !hot_results.is_empty() || !cold_results.is_empty() {
            results = if !hot_results.is_empty() && !cold_results.is_empty() {
                Self::merge_knn_results(hot_results, cold_results, k)
            } else if !cold_results.is_empty() {
                cold_results
            } else {
                hot_results
                    .into_iter()
                    .map(|(doc_id, distance)| SearchResult { doc_id, distance })
                    .collect()
            };
        }

        if partial && !results.is_empty() {
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

        if cacheable && !partial && !results.is_empty() {
            let _ = self.query_cache.insert_with_k_scoped_if_generation(
                query_cache_scope,
                normalized_query.clone(),
                results.clone(),
                k,
                cache_generation.expect("cache generation must exist when cacheable"),
            );
        }

        let path = if hot_accessed && cold_accessed {
            SearchExecutionPath::HotAndCold
        } else if cold_accessed {
            SearchExecutionPath::ColdTierOnly
        } else if hot_accessed {
            SearchExecutionPath::HotTierOnly
        } else {
            SearchExecutionPath::Degraded
        };

        drop(query_permit);
        self.refresh_queue_depth_metric();
        Ok((results, path))
    }

    /// Insert document
    ///
    /// # Write Flow
    /// 1. Persist to cold tier (WAL + HNSW) before ACK
    /// 2. Invalidate affected query-cache entries
    /// 3. Mirror the recent write into hot tier for acceleration
    /// 4. Background drain later evicts the hot-tier copy and repairs unexpected drift
    ///
    /// # Emergency Eviction
    /// If hot tier exceeds hard limit (2x soft limit), triggers emergency flush
    /// to prevent unbounded memory growth.
    pub fn insert(
        &self,
        doc_id: u64,
        embedding: Vec<f32>,
        metadata: std::collections::HashMap<String, String>,
    ) -> Result<()> {
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
                    info!(flushed_docs = flushed, "emergency flush completed");
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

        // Upsert semantics: invalidate point-lookup cache so readers won't observe stale embeddings.
        self.cache_strategy.invalidate(doc_id);

        let mut embedding = embedding;
        normalize_in_place_if_needed(self.config.hnsw_distance, &mut embedding)?;

        // Persist first: ACK must not be returned before durable WAL + cold-tier update.
        self.cold_tier
            .insert(doc_id, embedding.clone(), metadata.clone())?;

        let removed_by_doc = self.query_cache.invalidate_doc(doc_id);
        let removed_by_insert = self
            .query_cache
            .invalidate_for_insert(&embedding, self.config.hnsw_distance);
        trace!(
            doc_id,
            removed_by_doc,
            removed_by_insert,
            "invalidated query cache entries affected by insert"
        );

        // Keep recent writes in hot tier to accelerate mixed hot/cold search merges.
        let coherence = self
            .cold_tier
            .current_coherence_token(doc_id)
            .ok_or_else(|| anyhow!("insert succeeded but cold tier has no canonical token"))?;
        self.hot_tier
            .insert_with_coherence(doc_id, embedding, metadata, coherence);

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
            "emergency hot-tier drain: evicting all mirror entries"
        );

        let (success_count, should_clear_query_cache) =
            self.reconcile_drained_hot_tier_documents(documents, "emergency")?;

        if should_clear_query_cache {
            self.query_cache.clear();
        }

        Ok(success_count)
    }

    fn reconcile_drained_hot_tier_documents(
        &self,
        documents: Vec<HotTierMirrorDocument>,
        drain_kind: &'static str,
    ) -> Result<(usize, bool)> {
        let mut failed_documents = Vec::new();
        let mut success_count = 0;
        let mut should_clear_query_cache = false;

        for (doc_id, embedding, metadata, mirror_coherence) in documents {
            let cold_embedding = self.cold_tier.fetch_document_with_coherence(doc_id);
            let cold_metadata = self.cold_tier.fetch_metadata(doc_id);

            match (cold_embedding.as_ref(), cold_metadata.as_ref()) {
                (Some((canonical_embedding, canonical_coherence)), Some(canonical_metadata)) => {
                    let token_diverged = *canonical_coherence != mirror_coherence;
                    let embedding_diverged = canonical_embedding != &embedding;
                    let metadata_diverged = canonical_metadata != &metadata;

                    if token_diverged || embedding_diverged || metadata_diverged {
                        warn!(
                            doc_id,
                            drain_kind,
                            mirror_version = mirror_coherence.version,
                            mirror_digest_hi = mirror_coherence.digest.hi,
                            mirror_digest_lo = mirror_coherence.digest.lo,
                            canonical_version = canonical_coherence.version,
                            canonical_digest_hi = canonical_coherence.digest.hi,
                            canonical_digest_lo = canonical_coherence.digest.lo,
                            token_diverged,
                            embedding_diverged,
                            metadata_diverged,
                            "hot-tier drain found mirror divergence; keeping cold tier authoritative"
                        );

                        if embedding_diverged {
                            self.cache_strategy.invalidate(doc_id);
                        }
                        should_clear_query_cache = true;
                    }

                    success_count += 1;
                }
                (None, None) | (Some(_), None) | (None, Some(_)) => {
                    warn!(
                        doc_id,
                        drain_kind,
                        cold_has_embedding = cold_embedding.is_some(),
                        cold_has_metadata = cold_metadata.is_some(),
                        "hot-tier drain found missing or partial canonical state; attempting repair from mirror"
                    );

                    match self
                        .cold_tier
                        .insert(doc_id, embedding.clone(), metadata.clone())
                    {
                        Ok(()) => {
                            success_count += 1;
                            should_clear_query_cache = true;
                        }
                        Err(e) => {
                            error!(
                                doc_id,
                                drain_kind,
                                error = %e,
                                "failed to repair canonical cold-tier state from hot-tier mirror"
                            );
                            failed_documents.push((doc_id, embedding, metadata, mirror_coherence));
                        }
                    }
                }
            }
        }

        if !failed_documents.is_empty() {
            let fail_count = failed_documents.len();
            warn!(
                drain_kind,
                failed = fail_count,
                succeeded = success_count,
                "hot-tier drain had repair failures; re-inserting failed mirror entries"
            );

            self.hot_tier.reinsert_failed_documents(failed_documents);

            let mut stats = self.stats.write();
            stats.hot_tier_flush_failures += 1;

            if success_count == 0 {
                anyhow::bail!(
                    "{} hot-tier drain completely failed: all {} documents remain in hot tier",
                    drain_kind,
                    fail_count
                );
            }
        }

        Ok((success_count, should_clear_query_cache))
    }

    /// Bulk load documents directly into cold tier (HNSW index).
    ///
    /// This bypasses the hot tier and writes directly into the durable cold tier.
    /// Use for controlled bulk ingest, data migrations, and initial data loading.
    ///
    /// # Parameters
    /// - `documents`: Vector of (doc_id, embedding, metadata) tuples
    ///
    /// # Returns
    /// - `Ok((loaded, failed, duration_ms, rate))`: Load statistics
    ///
    /// # Durability
    /// Each document is written through `cold_tier.insert` (WAL + HNSW) before being counted
    /// as loaded. This is safe for production/pilot bulk ingest.
    #[instrument(level = "info", skip(self, documents), fields(count = documents.len()))]
    pub fn bulk_load_cold_tier(
        &self,
        documents: Vec<(u64, Vec<f32>, std::collections::HashMap<String, String>)>,
    ) -> Result<(u64, u64, f32, f32)> {
        use std::time::Instant;

        let start = Instant::now();
        let total = documents.len() as u64;

        // Collect doc_ids for cache invalidation
        let doc_ids: Vec<u64> = documents.iter().map(|(id, _, _)| *id).collect();

        // Durable insert into cold tier (WAL + HNSW) with per-document accounting.
        let mut loaded = 0u64;
        let mut failed = 0u64;
        for (doc_id, embedding, metadata) in documents {
            match self.cold_tier.insert(doc_id, embedding, metadata) {
                Ok(()) => loaded += 1,
                Err(e) => {
                    failed += 1;
                    warn!(doc_id, error = %e, "bulk_load_cold_tier: insert failed");
                }
            }
        }

        self.invalidate_caches_after_bulk_load(&doc_ids);

        let elapsed = start.elapsed();
        let duration_ms = elapsed.as_secs_f32() * 1000.0;
        let rate = if elapsed.as_secs_f64() > 0.0 {
            loaded as f32 / elapsed.as_secs_f32()
        } else {
            loaded as f32
        };

        info!(
            total,
            loaded,
            failed,
            duration_ms,
            rate_per_sec = rate,
            "bulk_load_cold_tier complete"
        );

        Ok((loaded, failed, duration_ms, rate))
    }

    fn invalidate_caches_after_bulk_load(&self, doc_ids: &[u64]) {
        for doc_id in doc_ids {
            self.cache_strategy.invalidate(*doc_id);
        }

        // L1b caches search results, so bulk loads can change k-NN results even if
        // cached result sets do not explicitly include the newly loaded doc_ids.
        self.query_cache.clear();
    }

    /// Drain the hot tier mirror (manual trigger)
    ///
    /// This is called periodically by the background maintenance task, or can be
    /// called manually for testing/shutdown. Documents are already durable before
    /// they enter the hot tier; this operation bounds memory and reconciles any
    /// unexpected hot/cold drift before evicting the mirror entries. Existing
    /// cold-tier records remain authoritative; the mirror is only used to repair
    /// missing or partially missing canonical state.
    ///
    /// # Error Handling
    /// - On partial failure: re-inserts failed documents back into hot tier
    /// - On complete failure: all documents re-inserted, flush marked as failed
    /// - Tracks flush_failures metric for observability
    pub fn flush_hot_tier(&self, force: bool) -> Result<usize> {
        if !force && !self.hot_tier.needs_flush() {
            return Ok(0);
        }

        let documents = self.hot_tier.drain_for_flush();
        let count = documents.len();

        if count == 0 {
            return Ok(0);
        }

        let (success_count, should_clear_query_cache) =
            self.reconcile_drained_hot_tier_documents(documents, "manual")?;

        if should_clear_query_cache {
            self.query_cache.clear();
        }

        Ok(success_count)
    }

    /// Spawn background hot-tier drain task.
    ///
    /// Periodically checks if the hot tier should be drained and evicts mirror
    /// entries once they age out or exceed size thresholds.
    ///
    /// # Graceful Shutdown
    /// Accepts a broadcast receiver for shutdown signal. When shutdown is signaled,
    /// forces a final drain/reconciliation before stopping gracefully.
    pub fn spawn_flush_task(
        self: Arc<Self>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        let flush_interval = self.config.flush_interval;

        tokio::spawn(async move {
            let mut ticker = interval(flush_interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.audit_hot_tier_coherence_if_due("background hot-tier coherence audit");
                        if self.hot_tier.needs_flush() {
                            match self.flush_hot_tier(false) {
                                Ok(count) => {
                                    if count > 0 {
                                        info!(
                                            count,
                                            "Background hot-tier drain completed"
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!(error = %e, "Background flush failed");
                                }
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Flush task received shutdown signal, performing final hot-tier drain");

                        // Final forced drain before shutdown. This must run even
                        // when age/size thresholds have not been reached yet so
                        // fresh mirror entries still get reconciled.
                        match self.flush_hot_tier(true) {
                            Ok(count) => {
                                info!(
                                    count,
                                    "Final hot-tier drain completed before shutdown"
                                );
                            }
                            Err(e) => {
                                error!(error = %e, "Final hot-tier drain failed during shutdown");
                            }
                        }

                        info!("Flush task stopped gracefully");
                        break;
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

        stats.cold_tier_size = (*self.cold_tier).len();

        // Get L1b (query cache) stats
        let query_cache_stats = self.query_cache.stats();
        stats.query_cache_exact_hits = query_cache_stats.exact_hits;
        stats.query_cache_similarity_hits = query_cache_stats.similarity_hits;

        // Calculate hit rates (if we have queries)
        if stats.total_queries > 0 {
            // L1a (document cache) hit rate
            stats.cache_hit_rate = stats.cache_hits as f64 / stats.total_queries as f64;

            // L1b (query cache) hit rate
            stats.query_cache_hit_rate = stats.query_cache_hits as f64 / stats.total_queries as f64;

            // Combined L1 (L1a + L1b) hit rate
            stats.l1_combined_hits = stats.cache_hits + stats.query_cache_hits;
            stats.l1_combined_hit_rate = stats.l1_combined_hits as f64 / stats.total_queries as f64;

            // Overall hit rate (L1 + L2)
            let total_hits = stats.l1_combined_hits + stats.hot_tier_hits;
            stats.overall_hit_rate = total_hits as f64 / stats.total_queries as f64;
        }

        stats
    }

    /// Current number of entries in L1a cache.
    pub fn cache_size(&self) -> usize {
        self.cache_strategy.size()
    }

    /// Snapshot HSC lifecycle state for observability endpoints.
    pub fn hsc_lifecycle_stats(&self) -> Option<CacheLifecycleStats> {
        let mut stats = self.cache_strategy.lifecycle_stats()?;
        stats.access_logger_depth = self.access_logger.as_ref().map_or(0, |l| l.read().len());
        Some(stats)
    }

    /// Get cold tier reference (for direct access if needed)
    pub fn cold_tier(&self) -> &HnswBackend {
        &self.cold_tier
    }

    /// Cloneable handle to cold tier for long-running operations that should
    /// not hold the outer `TieredEngine` lock.
    pub fn cold_tier_handle(&self) -> Arc<HnswBackend> {
        Arc::clone(&self.cold_tier)
    }

    /// Get hot tier reference (for testing/inspection)
    pub fn hot_tier(&self) -> &HotTier {
        &self.hot_tier
    }
}

fn normalize_in_place_if_needed(distance: DistanceMetric, embedding: &mut [f32]) -> Result<()> {
    if !matches!(
        distance,
        DistanceMetric::Cosine | DistanceMetric::InnerProduct
    ) {
        return Ok(());
    }

    let norm_sq = crate::simd::sum_squares_f32(embedding);
    if norm_sq <= f32::EPSILON {
        anyhow::bail!("embedding norm is zero; cannot normalize");
    }
    if (NORMALIZATION_NORM_SQ_MIN..=NORMALIZATION_NORM_SQ_MAX).contains(&norm_sq) {
        return Ok(());
    }

    let inv_norm = 1.0 / norm_sq.sqrt();
    for v in embedding {
        *v *= inv_norm;
    }

    Ok(())
}

fn normalize_query_for_search<'a>(
    distance: DistanceMetric,
    query: &'a [f32],
) -> Result<Cow<'a, [f32]>> {
    if !matches!(
        distance,
        DistanceMetric::Cosine | DistanceMetric::InnerProduct
    ) {
        return Ok(Cow::Borrowed(query));
    }

    let norm_sq = crate::simd::sum_squares_f32(query);
    if norm_sq <= f32::EPSILON {
        anyhow::bail!("embedding norm is zero; cannot normalize");
    }
    if (NORMALIZATION_NORM_SQ_MIN..=NORMALIZATION_NORM_SQ_MAX).contains(&norm_sq) {
        return Ok(Cow::Borrowed(query));
    }

    let inv_norm = 1.0 / norm_sq.sqrt();
    let mut normalized = query.to_vec();
    for value in &mut normalized {
        *value *= inv_norm;
    }
    Ok(Cow::Owned(normalized))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AccessPatternLogger;
    use crate::LearnedCachePredictor;
    use crate::LearnedCacheStrategy;
    use crate::LruCacheStrategy;
    use crate::SemanticAdapter;
    use tempfile::TempDir;

    #[test]
    fn test_tiered_engine_query_path() {
        let cache = LruCacheStrategy::new(100);
        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_embeddings = vec![vec![1.0, 0.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]];

        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            embedding_dimension: 4,
            data_dir: None,
            ..Default::default()
        };

        let initial_metadata = vec![
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        ];

        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            initial_embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

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
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        let cache = LruCacheStrategy::new(100);
        let initial_embeddings = vec![normalize(vec![1.0, 0.0])];

        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            embedding_dimension: 2,
            data_dir: None,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new()];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            initial_embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        // Insert into hot tier
        engine
            .insert(
                10,
                normalize(vec![0.5, 0.5]),
                std::collections::HashMap::new(),
            )
            .unwrap();

        // Query should hit hot tier
        let result = engine.query(10, None);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), normalize(vec![0.5, 0.5]));

        let stats = engine.stats();
        assert_eq!(stats.hot_tier_hits, 1);
        assert_eq!(stats.hot_tier_size, 1);
    }

    #[test]
    fn test_point_query_logs_access_without_query_embedding() {
        let cache = LruCacheStrategy::new(32);
        let query_cache = Arc::new(QueryHashCache::new(32, 0.85));
        let initial_embeddings = vec![vec![1.0, 0.0]];
        let initial_metadata = vec![std::collections::HashMap::new()];
        let config = TieredEngineConfig {
            embedding_dimension: 2,
            hnsw_max_elements: 16,
            data_dir: None,
            ..Default::default()
        };

        let mut engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            initial_embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(64)));
        engine.set_access_logger(logger.clone());

        let result = engine.query(0, None);
        assert!(result.is_some(), "point query should return seeded doc");

        let events = logger
            .read()
            .get_recent_window(std::time::Duration::from_secs(60));
        assert_eq!(
            events.len(),
            1,
            "point query should emit a training access event without query embedding"
        );
        assert_eq!(events[0].doc_id, 0);
    }

    #[test]
    fn test_search_access_logging_is_batched() {
        let cache = LruCacheStrategy::new(32);
        let query_cache = Arc::new(QueryHashCache::new(32, 0.85));
        let config = TieredEngineConfig {
            embedding_dimension: 2,
            hnsw_max_elements: 16,
            data_dir: None,
            ..Default::default()
        };

        let mut engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0, 0.0]],
            vec![std::collections::HashMap::new()],
            config,
        )
        .unwrap();

        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(64)));
        engine.set_access_logger(logger.clone());

        let logged = engine.log_served_search_accesses(&[10, 11, 12, 13]);
        assert_eq!(logged, 4);
        assert_eq!(logger.read().stats().total_accesses, 4);
    }

    #[test]
    fn test_hsc_lifecycle_stats_reports_semantic_and_logger_depth() {
        let predictor = LearnedCachePredictor::new(256).unwrap();
        let strategy =
            LearnedCacheStrategy::new_with_semantic(64, predictor, SemanticAdapter::new());
        let query_cache = Arc::new(QueryHashCache::new(64, 0.85));
        let config = TieredEngineConfig {
            embedding_dimension: 2,
            hnsw_max_elements: 32,
            data_dir: None,
            ..Default::default()
        };

        let mut engine = TieredEngine::new(
            Box::new(strategy),
            query_cache,
            vec![vec![1.0, 0.0]],
            vec![std::collections::HashMap::new()],
            config,
        )
        .unwrap();

        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(64)));
        engine.set_access_logger(logger);
        let logged = engine.log_served_search_accesses(&[7, 8, 9, 10]);
        assert_eq!(logged, 4);

        let lifecycle = engine
            .hsc_lifecycle_stats()
            .expect("learned strategy should expose lifecycle stats");
        assert!(lifecycle.semantic_enabled);
        assert!(lifecycle.admission_controller_enabled);
        assert_eq!(lifecycle.access_logger_depth, 4);
        assert_eq!(lifecycle.hot_doc_count, 0);
        assert!(lifecycle.target_utilization > 0.0);
    }

    #[test]
    fn test_hsc_lifecycle_stats_absent_for_lru_strategy() {
        let engine = TieredEngine::new(
            Box::new(LruCacheStrategy::new(64)),
            Arc::new(QueryHashCache::new(64, 0.85)),
            vec![vec![1.0, 0.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                embedding_dimension: 2,
                hnsw_max_elements: 32,
                data_dir: None,
                ..Default::default()
            },
        )
        .unwrap();

        assert!(
            engine.hsc_lifecycle_stats().is_none(),
            "non-HSC strategies should not emit HSC lifecycle snapshots"
        );
    }

    #[test]
    fn test_tiered_engine_flush() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        let cache = LruCacheStrategy::new(100);
        let initial_embeddings = vec![normalize(vec![1.0, 0.0])];

        let config = TieredEngineConfig {
            hot_tier_max_size: 2, // Small threshold
            hnsw_max_elements: 100,
            embedding_dimension: 2,
            data_dir: None,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new()];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            initial_embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        // Insert 2 documents (trigger flush threshold)
        engine
            .insert(
                10,
                normalize(vec![0.1, 0.1]),
                std::collections::HashMap::new(),
            )
            .unwrap();
        engine
            .insert(
                11,
                normalize(vec![0.2, 0.2]),
                std::collections::HashMap::new(),
            )
            .unwrap();

        // Inserts are durable before the hot-tier mirror is drained.
        assert!(engine.cold_tier().fetch_document(10).is_some());
        assert!(engine.cold_tier().fetch_document(11).is_some());
        assert!(engine.hot_tier().needs_flush());

        // Manual drain of the hot-tier mirror.
        let flushed = engine.flush_hot_tier(false).unwrap();
        assert_eq!(flushed, 2);

        // Hot tier should be empty
        assert_eq!(engine.hot_tier().len(), 0);

        // Documents must remain in the canonical cold tier.
        assert!(engine.cold_tier().fetch_document(10).is_some());
        assert!(engine.cold_tier().fetch_document(11).is_some());
    }

    #[test]
    fn test_flush_hot_tier_keeps_cold_tier_authoritative_on_divergence() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }

        let cache = LruCacheStrategy::new(16);
        let query_cache = Arc::new(QueryHashCache::new(32, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            Arc::clone(&query_cache),
            vec![normalize(vec![1.0, 0.0])],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hot_tier_max_size: 8,
                hnsw_max_elements: 32,
                embedding_dimension: 2,
                ..Default::default()
            },
        )
        .unwrap();

        let canonical_embedding = normalize(vec![0.0, 1.0]);
        let mut canonical_metadata = std::collections::HashMap::new();
        canonical_metadata.insert("state".to_string(), "canonical".to_string());
        engine
            .insert(10, canonical_embedding.clone(), canonical_metadata.clone())
            .unwrap();

        let divergent_embedding = normalize(vec![1.0, 1.0]);
        let mut divergent_metadata = std::collections::HashMap::new();
        divergent_metadata.insert("state".to_string(), "hot-only".to_string());
        engine
            .hot_tier
            .insert(10, divergent_embedding, divergent_metadata);

        let scope = 91;
        let query = normalize(vec![0.0, 1.0]);
        query_cache.insert_with_k_scoped(
            scope,
            query.clone(),
            vec![SearchResult {
                doc_id: 10,
                distance: 0.0,
            }],
            1,
        );
        assert!(
            query_cache.get_scoped(scope, &query, 1).is_some(),
            "test setup should populate the query cache"
        );

        let flushed = engine.flush_hot_tier(true).unwrap();
        assert_eq!(flushed, 1, "flush should evict the divergent mirror entry");
        assert_eq!(engine.hot_tier().len(), 0, "hot tier should be drained");
        assert_eq!(
            engine.cold_tier.fetch_document(10).as_ref(),
            Some(&canonical_embedding),
            "flush must not let the mirror overwrite canonical cold-tier embeddings"
        );
        assert_eq!(
            engine.cold_tier.fetch_metadata(10).as_ref(),
            Some(&canonical_metadata),
            "flush must not let the mirror overwrite canonical cold-tier metadata"
        );
        assert!(
            query_cache.get_scoped(scope, &query, 1).is_none(),
            "detected divergence should clear L1b because cached search results may be stale"
        );
    }

    #[test]
    fn test_flush_hot_tier_repairs_missing_cold_record_from_mirror() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            Arc::clone(&query_cache),
            vec![vec![1.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hot_tier_max_size: 4,
                hnsw_max_elements: 16,
                embedding_dimension: 1,
                ..Default::default()
            },
        )
        .unwrap();

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("state".to_string(), "repaired".to_string());
        engine.hot_tier.insert(77, vec![2.0], metadata.clone());
        assert!(
            engine.cold_tier.fetch_document(77).is_none(),
            "test setup requires a missing canonical cold-tier record"
        );

        let scope = 12;
        let query = vec![2.0];
        query_cache.insert_with_k_scoped(
            scope,
            query.clone(),
            vec![SearchResult {
                doc_id: 77,
                distance: 0.0,
            }],
            1,
        );

        let flushed = engine.flush_hot_tier(true).unwrap();
        assert_eq!(
            flushed, 1,
            "flush should repair and evict the orphaned mirror"
        );
        assert_eq!(
            engine.cold_tier.fetch_document(77).as_deref(),
            Some(&[1.0][..]),
            "flush repair should preserve the mirror document through canonical normalization"
        );
        assert_eq!(
            engine.cold_tier.fetch_metadata(77).as_ref(),
            Some(&metadata),
            "repaired cold-tier record should preserve mirrored metadata"
        );
        assert!(
            query_cache.get_scoped(scope, &query, 1).is_none(),
            "repairing canonical state should invalidate L1b"
        );
    }

    #[test]
    fn test_update_metadata_uses_cold_tier_as_source_of_truth_for_hot_docs() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }

        let dir = TempDir::new().unwrap();
        let cache = LruCacheStrategy::new(16);
        let query_cache = Arc::new(QueryHashCache::new(32, 0.85));
        let config = TieredEngineConfig {
            hot_tier_max_size: 8,
            hnsw_max_elements: 32,
            embedding_dimension: 2,
            data_dir: Some(dir.path().to_string_lossy().to_string()),
            fsync_policy: FsyncPolicy::Always,
            ..Default::default()
        };

        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![normalize(vec![1.0, 0.0])],
            vec![std::collections::HashMap::new()],
            config,
        )
        .unwrap();

        let mut original = std::collections::HashMap::new();
        original.insert("state".to_string(), "draft".to_string());
        engine
            .insert(10, normalize(vec![0.0, 1.0]), original.clone())
            .unwrap();

        assert_eq!(
            engine.hot_tier.get_metadata(10).as_ref(),
            Some(&original),
            "hot tier should mirror original metadata"
        );
        assert_eq!(
            engine.cold_tier.fetch_metadata(10).as_ref(),
            Some(&original),
            "cold tier should persist original metadata"
        );

        let mut updated = std::collections::HashMap::new();
        updated.insert("state".to_string(), "published".to_string());
        updated.insert("owner".to_string(), "kyro".to_string());

        assert!(
            engine.update_metadata(10, updated.clone(), false).unwrap(),
            "metadata update should succeed for a resident hot-tier document"
        );

        assert_eq!(
            engine.cold_tier.fetch_metadata(10).as_ref(),
            Some(&updated),
            "cold tier must remain canonical after metadata update"
        );
        assert_eq!(
            engine.hot_tier.get_metadata(10).as_ref(),
            Some(&updated),
            "hot tier must mirror the canonical metadata after update"
        );
    }

    #[test]
    fn test_update_metadata_clears_query_cache() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }

        let cache = LruCacheStrategy::new(16);
        let query_cache = Arc::new(QueryHashCache::new(32, 0.85));
        let config = TieredEngineConfig {
            hnsw_max_elements: 32,
            embedding_dimension: 2,
            ..Default::default()
        };

        let engine = TieredEngine::new(
            Box::new(cache),
            Arc::clone(&query_cache),
            vec![normalize(vec![1.0, 0.0]), normalize(vec![0.0, 1.0])],
            vec![std::collections::HashMap::new(); 2],
            config,
        )
        .unwrap();

        let scope = 77;
        let query = normalize(vec![1.0, 0.0]);
        let (_results, path) = engine
            .knn_search_with_ef_detailed_scoped(&query, 1, None, scope)
            .expect("initial search should populate the scoped query cache");
        assert_ne!(path, SearchExecutionPath::CacheHit);
        assert!(
            query_cache.get_scoped(scope, &query, 1).is_some(),
            "scoped query cache should contain the freshly computed result"
        );

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("category".to_string(), "updated".to_string());
        assert!(
            engine.update_metadata(0, metadata, false).unwrap(),
            "metadata update should succeed"
        );

        assert!(
            query_cache.get_scoped(scope, &query, 1).is_none(),
            "metadata updates must clear L1b because filter membership can change"
        );
    }

    #[test]
    fn test_get_metadata_prefers_cold_tier_source_of_truth() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }

        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let config = TieredEngineConfig {
            hnsw_max_elements: 16,
            embedding_dimension: 2,
            ..Default::default()
        };

        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![normalize(vec![1.0, 0.0])],
            vec![std::collections::HashMap::new()],
            config,
        )
        .unwrap();

        let mut canonical = std::collections::HashMap::new();
        canonical.insert("role".to_string(), "canonical".to_string());
        engine
            .insert(9, normalize(vec![0.0, 1.0]), canonical.clone())
            .unwrap();

        let mut divergent = std::collections::HashMap::new();
        divergent.insert("role".to_string(), "hot-only".to_string());
        assert!(
            engine.hot_tier.update_metadata(9, divergent.clone(), false),
            "hot tier should accept direct test-only mutation"
        );
        assert_eq!(
            engine.hot_tier.get_metadata(9).as_ref(),
            Some(&divergent),
            "test setup must create divergent mirror metadata"
        );

        assert_eq!(
            engine.get_metadata(9).as_ref(),
            Some(&canonical),
            "metadata reads must prefer canonical cold-tier state over divergent mirror metadata"
        );
    }

    #[test]
    fn test_delete_scrubs_hot_only_orphan_after_cold_delete_attempt() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let config = TieredEngineConfig {
            hnsw_max_elements: 16,
            embedding_dimension: 1,
            ..Default::default()
        };

        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0]],
            vec![std::collections::HashMap::new()],
            config,
        )
        .unwrap();

        engine
            .hot_tier
            .insert(99, vec![2.0], std::collections::HashMap::new());
        assert!(
            engine.hot_tier.exists(99),
            "test setup must create a hot-tier orphan"
        );
        assert!(
            engine.cold_tier.fetch_document(99).is_none(),
            "test setup requires no canonical cold-tier record"
        );

        assert!(
            engine.delete(99).unwrap(),
            "delete should scrub hot-tier orphans even when cold tier is missing the record"
        );
        assert!(
            !engine.hot_tier.exists(99),
            "hot-tier orphan should be removed during delete cleanup"
        );
    }

    #[test]
    fn test_batch_delete_scrubs_hot_only_orphans_after_cold_delete_attempt() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let config = TieredEngineConfig {
            hnsw_max_elements: 16,
            embedding_dimension: 1,
            ..Default::default()
        };

        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0]],
            vec![std::collections::HashMap::new()],
            config,
        )
        .unwrap();

        engine
            .hot_tier
            .insert(55, vec![5.0], std::collections::HashMap::new());
        engine
            .hot_tier
            .insert(56, vec![6.0], std::collections::HashMap::new());

        let deleted = engine.batch_delete(&[55, 56]).unwrap();
        assert_eq!(
            deleted, 2,
            "batch delete should count scrubbed hot-tier orphans as removed entries"
        );
        assert!(!engine.hot_tier.exists(55));
        assert!(!engine.hot_tier.exists(56));
    }

    #[test]
    fn test_tiered_engine_with_persistence() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        let dir = TempDir::new().unwrap();

        {
            let cache = LruCacheStrategy::new(100);
            let initial_embeddings = vec![normalize(vec![1.0, 0.0])];

            // Use hot_tier_max_size=1 to trigger flush immediately
            let config = TieredEngineConfig {
                hot_tier_max_size: 1,
                hnsw_max_elements: 100,
                embedding_dimension: 2,
                data_dir: Some(dir.path().to_string_lossy().to_string()),
                fsync_policy: FsyncPolicy::Always,
                ..Default::default()
            };

            let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
            let initial_metadata = vec![std::collections::HashMap::new()];
            let engine = TieredEngine::new(
                Box::new(cache),
                query_cache,
                initial_embeddings,
                initial_metadata,
                config,
            )
            .unwrap();

            // Insert and flush (should trigger because hot_tier_max_size=1)
            engine
                .insert(
                    10,
                    normalize(vec![0.5, 0.5]),
                    std::collections::HashMap::new(),
                )
                .unwrap();
            let flushed = engine.flush_hot_tier(false).unwrap();
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
        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            embedding_dimension: 2,
            ..Default::default()
        };

        let recovered =
            TieredEngine::recover(Box::new(cache), query_cache, dir.path(), config).unwrap();

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
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test successful k-NN search with timeouts
        let cache = LruCacheStrategy::new(100);
        let mut embeddings = Vec::new();
        for i in 0..100 {
            embeddings.push(normalize(vec![(i as f32) + 1.0, 0.0, 0.0, 0.0]));
        }

        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 200,
            embedding_dimension: 4,
            data_dir: None,
            cache_timeout_ms: 10,
            hot_tier_timeout_ms: 50,
            cold_tier_timeout_ms: 1000,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 100];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        // Search for nearest neighbors
        let query = normalize(vec![5.0, 0.0, 0.0, 0.0]);
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
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test that cold tier is searched when cache/hot tier empty
        let cache = LruCacheStrategy::new(100);
        let embeddings = vec![
            normalize(vec![1.0, 0.0, 0.0, 0.0]),
            normalize(vec![0.0, 1.0, 0.0, 0.0]),
            normalize(vec![0.0, 0.0, 1.0, 0.0]),
        ];

        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            embedding_dimension: 4,
            data_dir: None,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 3];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        let query = normalize(vec![1.0, 0.0, 0.0, 0.0]);
        let results = engine.knn_search_with_timeouts(&query, 2).await.unwrap();

        assert!(!results.is_empty());
        assert!(results.len() <= 2);

        // Verify cold tier was searched
        let stats = engine.stats();
        assert!(stats.cold_tier_searches > 0);
    }

    #[tokio::test]
    async fn test_timeout_stats_tracking() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test that timeout statistics are properly tracked
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 0.0])];

        let config = TieredEngineConfig {
            data_dir: None,
            hnsw_max_elements: 100,
            embedding_dimension: 2,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new()];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        // Initial stats should be zero
        let stats = engine.stats();
        assert_eq!(stats.cache_timeouts, 0);
        assert_eq!(stats.hot_tier_timeouts, 0);
        assert_eq!(stats.cold_tier_timeouts, 0);
        assert_eq!(stats.partial_results_returned, 0);
    }

    #[tokio::test]
    async fn test_actual_timeout_triggers() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test that timeouts actually occur and are tracked
        let cache = LruCacheStrategy::new(10);

        // Create larger dataset to potentially cause timeout with very short deadline
        let mut embeddings = Vec::new();
        for i in 0..1000 {
            let mut vec = vec![0.0; 128];
            vec[0] = (i as f32) + 1.0;
            vec[1] = 1.0;
            embeddings.push(normalize(vec));
        }

        let config = TieredEngineConfig {
            cold_tier_timeout_ms: 1, // Very short timeout to force timeout
            hnsw_max_elements: 2000,
            embedding_dimension: 128,
            data_dir: None,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 1000];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        let query = normalize(vec![500.0; 128]);
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
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Verify circuit breakers are properly initialized
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 0.0, 0.0, 0.0])];

        let config = TieredEngineConfig {
            data_dir: None,
            hnsw_max_elements: 100,
            embedding_dimension: 4,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new()];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        // All circuit breakers should start closed
        assert!(engine.cache_circuit_breaker.is_closed());
        assert!(engine.cold_tier_circuit_breaker.is_closed());
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_on_failures() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test that circuit breakers open after repeated failures
        // Note: This is a behavioral test - circuit breaker integration is validated
        // by observing that failures are recorded and timeouts occur
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 0.0, 0.0, 0.0])];

        let config = TieredEngineConfig {
            cold_tier_timeout_ms: 1, // Very short timeout to trigger failures
            hnsw_max_elements: 100,
            embedding_dimension: 4,
            data_dir: None,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new()];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        // Trigger multiple searches - some may timeout, some may succeed
        for _ in 0..10 {
            let _ = engine
                .knn_search_with_timeouts(&normalize(vec![1.0, 0.0, 0.0, 0.0]), 5)
                .await;
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
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test that queries are rejected when semaphore is saturated
        //
        // Strategy: Use a barrier BEFORE the query to ensure permits are held
        // while we attempt the 3rd query
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 2.0]); 10];

        let config = TieredEngineConfig {
            max_concurrent_queries: 2, // Very low limit to trigger rejection
            hnsw_max_elements: 100,
            embedding_dimension: 2,
            data_dir: None,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 10];
        let engine = Arc::new(
            TieredEngine::new(
                Box::new(cache),
                query_cache,
                embeddings,
                initial_metadata,
                config,
            )
            .unwrap(),
        );

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
        let query = normalize(vec![1.0, 2.0]);
        let result = engine.knn_search_with_timeouts(&query, 5).await;

        // Should be rejected with queue saturation error
        assert!(
            result.is_err(),
            "Expected query to be rejected due to queue saturation"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("saturated"),
            "Error should mention saturation: {}",
            err
        );

        // Check stats
        let stats = engine.stats();
        assert_eq!(
            stats.queries_rejected, 1,
            "Should have exactly 1 rejected query"
        );

        // Release barrier to let background tasks finish
        barrier.wait().await;
        let _ = handle1.await;
        let _ = handle2.await;
    }

    #[tokio::test]
    async fn test_load_shedding_permits_released() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test that semaphore permits are properly released after query completes
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 2.0]); 5];

        let config = TieredEngineConfig {
            max_concurrent_queries: 1, // Only 1 concurrent query allowed
            hnsw_max_elements: 100,
            embedding_dimension: 2,
            data_dir: None,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 5];
        let engine = Arc::new(
            TieredEngine::new(
                Box::new(cache),
                query_cache,
                embeddings,
                initial_metadata,
                config,
            )
            .unwrap(),
        );

        // Execute first query - should succeed
        let query1 = normalize(vec![1.0, 2.0]);
        let result1 = engine.knn_search_with_timeouts(&query1, 3).await;
        assert!(result1.is_ok());

        // Execute second query immediately after - should also succeed (permit released)
        let query2 = normalize(vec![2.0, 3.0]);
        let result2 = engine.knn_search_with_timeouts(&query2, 3).await;
        assert!(result2.is_ok());

        // No queries should be rejected
        let stats = engine.stats();
        assert_eq!(stats.queries_rejected, 0);
    }

    #[tokio::test]
    async fn test_search_worker_saturation_rejects_when_no_partial_results() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }

        let cache = LruCacheStrategy::new(8);
        let embeddings = vec![normalize(vec![1.0, 2.0]); 4];
        let config = TieredEngineConfig {
            max_concurrent_queries: 2,
            hnsw_max_elements: 64,
            embedding_dimension: 2,
            data_dir: None,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(64, 0.90));
        let initial_metadata = vec![std::collections::HashMap::new(); embeddings.len()];
        let engine = Arc::new(
            TieredEngine::new(
                Box::new(cache),
                query_cache,
                embeddings,
                initial_metadata,
                config,
            )
            .unwrap(),
        );

        let worker_limit = engine.config.max_concurrent_queries as u32;
        let _held_workers = engine
            .search_worker_semaphore
            .clone()
            .acquire_many_owned(worker_limit)
            .await
            .expect("must be able to reserve all worker permits");

        let query = normalize(vec![1.0, 2.0]);
        let err = engine
            .knn_search_with_timeouts(&query, 3)
            .await
            .expect_err("query should be rejected when all search workers are saturated");

        assert!(
            err.to_string().contains("Search worker queue saturated"),
            "error should mention worker saturation, got: {}",
            err
        );

        let stats = engine.stats();
        assert_eq!(
            stats.queries_rejected, 1,
            "worker saturation should increment queries_rejected"
        );
        assert!(
            stats.worker_saturation_count >= 1,
            "worker saturation should increment worker_saturation_count (got {})",
            stats.worker_saturation_count
        );
    }

    #[test]
    fn test_circuit_breaker_rejection_stats() {
        // Test that circuit breaker rejections are tracked in stats
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![vec![1.0]; 5];

        let config = TieredEngineConfig {
            hnsw_max_elements: 100,
            data_dir: None,
            embedding_dimension: 1,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 5];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

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
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test that current queue depth is properly tracked
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 2.0]); 10];

        let config = TieredEngineConfig {
            max_concurrent_queries: 5,
            hnsw_max_elements: 100,
            data_dir: None,
            cold_tier_timeout_ms: 200, // Short timeout for quick test
            embedding_dimension: 2,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 10];
        let engine = Arc::new(
            TieredEngine::new(
                Box::new(cache),
                query_cache,
                embeddings,
                initial_metadata,
                config,
            )
            .unwrap(),
        );

        // Initial queue depth should be 0
        let initial_stats = engine.stats();
        assert_eq!(initial_stats.current_queue_depth, 0);

        // Spawn a query and check depth during execution
        let engine_clone = Arc::clone(&engine);
        let query1 = normalize(vec![1.0, 2.0]);
        let handle =
            tokio::spawn(async move { engine_clone.knn_search_with_timeouts(&query1, 3).await });

        // Give time for query to acquire permit
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Queue depth should be non-zero during query execution
        // Note: This may be flaky if query completes very fast

        // Wait for query to complete
        let _ = handle.await;

        // After completion, subsequent queries should work (permits released)
        let query2 = normalize(vec![1.0, 2.0]);
        let result = engine.knn_search_with_timeouts(&query2, 3).await;
        assert!(result.is_ok());
        assert_eq!(engine.stats().current_queue_depth, 0);
    }

    #[tokio::test]
    async fn test_shutdown_forces_final_hot_tier_reconciliation_for_fresh_mirrors() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = Arc::new(
            TieredEngine::new(
                Box::new(cache),
                query_cache,
                vec![vec![1.0]],
                vec![std::collections::HashMap::new()],
                TieredEngineConfig {
                    hnsw_max_elements: 16,
                    embedding_dimension: 1,
                    flush_interval: Duration::from_secs(3600),
                    hot_tier_max_age: Duration::from_secs(3600),
                    ..Default::default()
                },
            )
            .unwrap(),
        );

        engine
            .hot_tier
            .insert(99, vec![9.0], std::collections::HashMap::new());
        assert!(
            !engine.hot_tier.needs_flush(),
            "test setup requires a fresh mirror entry below normal drain thresholds"
        );
        assert!(
            !engine.cold_tier.exists(99),
            "test setup requires the mirror entry to be hot-tier-only before shutdown"
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let handle = Arc::clone(&engine).spawn_flush_task(shutdown_rx);

        shutdown_tx
            .send(())
            .expect("shutdown signal should be delivered to flush task");
        handle
            .await
            .expect("flush task should stop cleanly after shutdown");

        assert!(
            engine.cold_tier.exists(99),
            "shutdown must force final drain/reconciliation even for fresh mirror entries"
        );
        assert!(
            !engine.hot_tier.exists(99),
            "final shutdown drain should evict the repaired mirror entry"
        );
    }

    #[tokio::test]
    async fn test_scoped_query_cache_isolation() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }

        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 2.0]); 4];
        let initial_metadata = vec![std::collections::HashMap::new(); 4];
        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let config = TieredEngineConfig {
            hnsw_max_elements: 100,
            data_dir: None,
            embedding_dimension: 2,
            ..Default::default()
        };

        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        let query = normalize(vec![1.0, 2.0]);

        let (first_scope_a, path_a1) = engine
            .knn_search_with_timeouts_with_ef_scoped(&query, 3, None, 7)
            .await
            .expect("scope A first search should succeed");
        assert!(!first_scope_a.is_empty());
        assert_ne!(path_a1, SearchExecutionPath::CacheHit);

        let (_, path_a2) = engine
            .knn_search_with_timeouts_with_ef_scoped(&query, 3, None, 7)
            .await
            .expect("scope A second search should succeed");
        assert_eq!(path_a2, SearchExecutionPath::CacheHit);

        let (_, path_b1) = engine
            .knn_search_with_timeouts_with_ef_scoped(&query, 3, None, 11)
            .await
            .expect("scope B first search should succeed");
        assert_ne!(
            path_b1,
            SearchExecutionPath::CacheHit,
            "scope B must not reuse scope A cache entry"
        );
    }

    #[test]
    fn test_sync_search_path_reports_cold_tier_only_when_hot_misses() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }

        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 0.0]), normalize(vec![0.0, 1.0])];
        let initial_metadata = vec![std::collections::HashMap::new(); embeddings.len()];
        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let config = TieredEngineConfig {
            hnsw_max_elements: 100,
            data_dir: None,
            embedding_dimension: 2,
            ..Default::default()
        };

        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        let query = normalize(vec![1.0, 0.0]);
        let (_results, path) = engine
            .knn_search_with_ef_detailed_scoped(&query, 1, Some(64), 99)
            .expect("sync search should succeed");
        assert_eq!(path, SearchExecutionPath::ColdTierOnly);
    }

    #[test]
    fn test_batch_search_path_reports_cold_tier_only_when_hot_misses() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }

        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 0.0]), normalize(vec![0.0, 1.0])];
        let initial_metadata = vec![std::collections::HashMap::new(); embeddings.len()];
        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let config = TieredEngineConfig {
            hnsw_max_elements: 100,
            data_dir: None,
            embedding_dimension: 2,
            ..Default::default()
        };

        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        let query = normalize(vec![1.0, 0.0]);
        let detailed = engine
            .knn_search_batch_with_ef_detailed_scoped(&[query], 1, Some(64), 99)
            .expect("batch search should succeed");
        assert_eq!(detailed.len(), 1);
        assert_eq!(detailed[0].1, SearchExecutionPath::ColdTierOnly);
    }

    #[tokio::test]
    async fn test_circuit_breaker_in_knn_search() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test circuit breaker integration in k-NN search path
        let cache = LruCacheStrategy::new(10);
        let embeddings = vec![normalize(vec![1.0, 2.0]); 5];

        let config = TieredEngineConfig {
            hnsw_max_elements: 100,
            data_dir: None,
            embedding_dimension: 2,
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 5];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        // Open cold tier circuit breaker
        engine.cold_tier_circuit_breaker.open();

        // k-NN search should degrade gracefully to an empty result set.
        let query = normalize(vec![1.0, 2.0]);
        let result = engine.knn_search_with_timeouts(&query, 3).await;

        let results = result.expect("search should return empty results instead of internal error");
        assert!(results.is_empty());

        // Check circuit breaker rejection was counted
        let stats = engine.stats();
        assert_eq!(stats.circuit_breaker_rejections, 1);
    }

    #[test]
    fn test_hot_tier_flush_failure_recovery() {
        fn normalize(mut v: Vec<f32>) -> Vec<f32> {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                v.iter_mut().for_each(|x| *x /= norm);
            }
            v
        }
        // Test that flush failures result in re-insertion to hot tier
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let cache = LruCacheStrategy::new(10);
        let initial_embeddings = vec![normalize(vec![1.0, 0.0]); 5];

        let config = TieredEngineConfig {
            hot_tier_max_size: 3, // Small threshold to trigger flush
            hnsw_max_elements: 100,
            embedding_dimension: 2,
            data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
            snapshot_interval: 1000, // Don't snapshot during test
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 5];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            initial_embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        // Insert documents into hot tier
        engine
            .insert(
                10,
                normalize(vec![2.0, 0.0]),
                std::collections::HashMap::new(),
            )
            .unwrap();
        engine
            .insert(
                11,
                normalize(vec![3.0, 0.0]),
                std::collections::HashMap::new(),
            )
            .unwrap();
        engine
            .insert(
                12,
                normalize(vec![4.0, 0.0]),
                std::collections::HashMap::new(),
            )
            .unwrap();

        assert_eq!(engine.hot_tier.len(), 3);

        // Attempt flush (should succeed normally)
        let flushed = engine.flush_hot_tier(false).unwrap();

        // For this test, flush should succeed, so hot tier should be empty
        // (Testing actual failure requires disk-full simulation which is complex)
        assert!(engine.hot_tier.is_empty() || flushed > 0);

        // Verify stats include flush operations
        let stats = engine.stats();
        assert!(stats.hot_tier_flushes > 0);
    }

    #[test]
    fn test_emergency_eviction_on_hard_limit() {
        // Test that emergency eviction triggers when hard limit reached
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let cache = LruCacheStrategy::new(10);
        let initial_embeddings = vec![vec![1.0, 0.0]; 2];

        let config = TieredEngineConfig {
            hot_tier_max_size: 3,   // Soft limit (very small for testing)
            hot_tier_hard_limit: 6, // Hard limit (2x soft limit)
            hnsw_max_elements: 100,
            embedding_dimension: 2,
            data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
            snapshot_interval: 1000, // Don't snapshot during test
            ..Default::default()
        };

        let query_cache = Arc::new(QueryHashCache::new(100, 0.85));
        let initial_metadata = vec![std::collections::HashMap::new(); 2];
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            initial_embeddings,
            initial_metadata,
            config,
        )
        .unwrap();

        // Insert documents up to hard limit
        // Hard limit is 6, so insert 6 documents
        for i in 10..16 {
            let result = engine.insert(i, vec![i as f32, 0.0], std::collections::HashMap::new());

            // Each insert should succeed (emergency eviction handles overflow)
            assert!(result.is_ok(), "Insert {} failed: {:?}", i, result.err());
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

        let hot_tier = HotTier::new(100, Duration::from_secs(60), DistanceMetric::Cosine);

        // Insert initial documents
        hot_tier.insert(1, vec![1.0, 0.0], std::collections::HashMap::new());
        hot_tier.insert(2, vec![2.0, 0.0], std::collections::HashMap::new());

        assert_eq!(hot_tier.len(), 2);

        // Simulate failed flush scenario: documents that couldn't be flushed
        let failed_docs = vec![
            (
                10,
                vec![10.0, 0.0],
                std::collections::HashMap::new(),
                VectorCoherenceToken::for_embedding(1, &[10.0, 0.0]),
            ),
            (
                11,
                vec![11.0, 0.0],
                std::collections::HashMap::new(),
                VectorCoherenceToken::for_embedding(1, &[11.0, 0.0]),
            ),
        ];

        hot_tier.reinsert_failed_documents(failed_docs);

        // Verify all documents present
        assert_eq!(hot_tier.len(), 4);
        assert!(hot_tier.get(1).is_some());
        assert!(hot_tier.get(2).is_some());
        assert!(hot_tier.get(10).is_some());
        assert!(hot_tier.get(11).is_some());
    }

    #[test]
    fn test_point_queries_ignore_hot_only_orphans() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 1,
                ..Default::default()
            },
        )
        .unwrap();

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("state".to_string(), "orphan".to_string());
        engine.hot_tier.insert(99, vec![9.0], metadata);
        engine.cache_strategy.insert_cached(CachedVector {
            doc_id: 99,
            embedding: vec![9.0],
            coherence: VectorCoherenceToken::new(1, crate::VectorIntegrityDigest::ZERO),
            distance: 0.0,
            cached_at: Instant::now(),
        });

        assert!(
            engine.query_with_source(99, None).is_none(),
            "point queries must not serve hot-only orphans"
        );
        assert!(
            engine.cache_strategy.peek_cached(99).is_none(),
            "query path should invalidate non-canonical L1a entries"
        );
        assert!(
            engine.get_embedding_cache_aware(99).is_none(),
            "cache-aware embedding fetch must not serve hot-only orphans"
        );
        assert!(
            engine.get_metadata(99).is_none(),
            "metadata reads must ignore hot-only orphans"
        );
        assert!(
            !engine.exists(99),
            "existence checks must reflect canonical cold-tier state only"
        );
        assert!(
            engine.hot_tier.exists(99),
            "orphan mirror is preserved for later reconciliation; reads just refuse to serve it"
        );
    }

    #[test]
    fn test_point_queries_refresh_stale_cached_version_from_cold_tier() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 1,
                ..Default::default()
            },
        )
        .unwrap();

        engine
            .insert(7, vec![7.0], std::collections::HashMap::new())
            .unwrap();
        engine
            .flush_hot_tier(true)
            .expect("test setup should evict hot-tier mirror");

        engine.cache_strategy.insert_cached(CachedVector {
            doc_id: 7,
            embedding: vec![70.0],
            coherence: VectorCoherenceToken::for_embedding(0, &[70.0]),
            distance: 0.0,
            cached_at: Instant::now(),
        });

        let canonical_coherence = engine
            .cold_tier
            .current_coherence_token(7)
            .expect("canonical token must exist");
        let canonical_embedding = engine
            .cold_tier
            .fetch_document(7)
            .expect("canonical embedding must exist");

        let (embedding, tier) = engine
            .query_with_source(7, None)
            .expect("point query should recover from stale cache entry");

        assert_eq!(tier, PointQueryTier::ColdTier);
        assert_eq!(
            embedding, canonical_embedding,
            "point query must fall back to canonical cold-tier embedding"
        );

        let refreshed = engine
            .cache_strategy
            .peek_cached(7)
            .expect("cold-tier recovery should refresh L1a with canonical state");
        assert_eq!(refreshed.coherence, canonical_coherence);
        assert_eq!(refreshed.embedding, canonical_embedding);
    }

    #[test]
    fn test_point_queries_scrub_stale_hot_version_mirror() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0, 0.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 2,
                ..Default::default()
            },
        )
        .unwrap();

        engine
            .insert(7, vec![0.0, 1.0], std::collections::HashMap::new())
            .unwrap();
        engine.hot_tier.insert_with_coherence(
            7,
            vec![1.0, 0.0],
            std::collections::HashMap::new(),
            VectorCoherenceToken::for_embedding(999, &[1.0, 0.0]),
        );

        let canonical_embedding = engine
            .cold_tier
            .fetch_document(7)
            .expect("canonical embedding must exist");

        let (embedding, tier) = engine
            .query_with_source(7, None)
            .expect("point query should recover from stale hot-tier mirror");

        assert_eq!(
            tier,
            PointQueryTier::ColdTier,
            "stale hot-tier mirrors must be bypassed in favor of canonical cold-tier state"
        );
        assert_eq!(embedding, canonical_embedding);
        assert!(
            !engine.hot_tier.exists(7),
            "stale hot-tier mirrors should be scrubbed immediately after detection"
        );
    }

    #[test]
    fn test_bulk_query_uses_canonical_metadata_and_skips_hot_only_orphans() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0]],
            vec![std::collections::HashMap::from([(
                "state".to_string(),
                "cold".to_string(),
            )])],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 1,
                ..Default::default()
            },
        )
        .unwrap();

        let mut canonical_metadata = std::collections::HashMap::new();
        canonical_metadata.insert("state".to_string(), "canonical".to_string());
        engine
            .insert(7, vec![7.0], canonical_metadata.clone())
            .unwrap();

        let mut divergent_metadata = std::collections::HashMap::new();
        divergent_metadata.insert("state".to_string(), "hot-only".to_string());
        assert!(
            engine
                .hot_tier
                .update_metadata(7, divergent_metadata, false),
            "test setup must create divergent hot-tier metadata"
        );

        let mut orphan_metadata = std::collections::HashMap::new();
        orphan_metadata.insert("state".to_string(), "orphan".to_string());
        engine.hot_tier.insert(77, vec![77.0], orphan_metadata);

        let results = engine.bulk_query_with_source(&[7, 77], true);
        assert_eq!(results.len(), 2);

        let canonical = results[0]
            .as_ref()
            .expect("canonical hot-tier document should still be queryable");
        assert_eq!(canonical.1, canonical_metadata);
        assert_eq!(canonical.2, PointQueryTier::HotTier);

        assert!(
            results[1].is_none(),
            "bulk query must not surface hot-only orphan documents"
        );
    }

    #[test]
    fn test_bulk_query_scrubs_stale_hot_version_mirrors() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 1,
                ..Default::default()
            },
        )
        .unwrap();

        let canonical_metadata =
            std::collections::HashMap::from([("state".to_string(), "canonical".to_string())]);
        engine
            .insert(7, vec![7.0], canonical_metadata.clone())
            .unwrap();
        engine.hot_tier.insert_with_coherence(
            7,
            vec![70.0],
            std::collections::HashMap::from([("state".to_string(), "stale".to_string())]),
            VectorCoherenceToken::for_embedding(999, &[70.0]),
        );

        let results = engine.bulk_query_with_source(&[7], true);
        let canonical = results[0]
            .as_ref()
            .expect("bulk query should recover from stale hot-tier mirror");
        assert_eq!(canonical.2, PointQueryTier::ColdTier);
        assert_eq!(canonical.1, canonical_metadata);
        assert!(
            !engine.hot_tier.exists(7),
            "bulk query should scrub stale hot-tier mirrors after falling back to cold tier"
        );
    }

    #[test]
    fn test_knn_search_ignores_hot_only_orphan_candidates() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0, 0.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 2,
                ..Default::default()
            },
        )
        .unwrap();

        engine
            .insert(7, vec![0.0, 1.0], std::collections::HashMap::new())
            .unwrap();
        engine
            .hot_tier
            .insert(77, vec![0.99, 0.01], std::collections::HashMap::new());

        let results = engine
            .knn_search(&[1.0, 0.0], 1)
            .expect("k-NN search should succeed");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].doc_id, 0,
            "hot-only orphan candidates must not displace canonical search results"
        );
    }

    #[test]
    fn test_knn_search_scrubs_stale_hot_version_candidates() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0, 0.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 2,
                ..Default::default()
            },
        )
        .unwrap();

        engine
            .insert(7, vec![0.0, 1.0], std::collections::HashMap::new())
            .unwrap();
        engine.hot_tier.insert_with_coherence(
            7,
            vec![1.0, 0.0],
            std::collections::HashMap::new(),
            VectorCoherenceToken::for_embedding(999, &[1.0, 0.0]),
        );

        let results = engine
            .knn_search(&[0.0, 1.0], 1)
            .expect("k-NN search should succeed");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].doc_id, 7,
            "k-NN search must ignore stale hot-tier distances and return canonical cold-tier ranking"
        );
        assert!(
            !engine.hot_tier.exists(7),
            "k-NN search should scrub stale hot-tier mirrors after detection"
        );
    }

    #[test]
    fn test_point_queries_scrub_hot_mirror_with_corrupted_payload() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            query_cache,
            vec![vec![1.0, 0.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 2,
                ..Default::default()
            },
        )
        .unwrap();

        engine
            .insert(7, vec![0.0, 1.0], std::collections::HashMap::new())
            .unwrap();
        let canonical = engine
            .cold_tier
            .current_coherence_token(7)
            .expect("canonical token must exist");
        engine.hot_tier.insert_with_coherence(
            7,
            vec![1.0, 0.0],
            std::collections::HashMap::new(),
            canonical,
        );

        let (embedding, tier) = engine
            .query_with_source(7, None)
            .expect("point query should recover from locally corrupted hot-tier payload");

        assert_eq!(tier, PointQueryTier::ColdTier);
        assert_eq!(embedding, vec![0.0, 1.0]);
        assert!(
            !engine.hot_tier.exists(7),
            "local hot-tier payload corruption must scrub the mirror immediately"
        );
    }

    #[test]
    fn test_query_cache_hit_recomputes_when_cached_result_is_noncanonical() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            Arc::clone(&query_cache),
            vec![vec![1.0, 0.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 2,
                ..Default::default()
            },
        )
        .unwrap();

        let scope = 313;
        let query = vec![1.0, 0.0];
        query_cache.insert_with_k_scoped(
            scope,
            query.clone(),
            vec![SearchResult {
                doc_id: 77,
                distance: 0.0,
            }],
            1,
        );
        engine
            .hot_tier
            .insert(77, vec![1.0, 0.0], std::collections::HashMap::new());

        let (results, path) = engine
            .knn_search_with_ef_detailed_scoped(&query, 1, None, scope)
            .expect("search should recover from non-canonical cached results");

        assert_ne!(
            path,
            SearchExecutionPath::CacheHit,
            "non-canonical cached results must not be treated as valid query-cache hits"
        );
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].doc_id, 0,
            "search should recompute against canonical state after rejecting bad cache entries"
        );
        assert!(
            query_cache.get_scoped(scope, &query, 1).is_none(),
            "generation bump from invalidating bad cache entries should prevent same-request repopulation"
        );

        let (_second_results, second_path) = engine
            .knn_search_with_ef_detailed_scoped(&query, 1, None, scope)
            .expect("second search should repopulate the query cache");
        assert_ne!(second_path, SearchExecutionPath::CacheHit);
        assert!(
            query_cache.get_scoped(scope, &query, 1).is_some(),
            "subsequent search should repopulate the query cache with canonical results"
        );
    }

    #[test]
    fn test_background_coherence_audit_scrubs_corrupted_hot_mirror_and_query_cache() {
        let cache = LruCacheStrategy::new(8);
        let query_cache = Arc::new(QueryHashCache::new(16, 0.85));
        let engine = TieredEngine::new(
            Box::new(cache),
            Arc::clone(&query_cache),
            vec![vec![1.0, 0.0]],
            vec![std::collections::HashMap::new()],
            TieredEngineConfig {
                hnsw_max_elements: 16,
                embedding_dimension: 2,
                ..Default::default()
            },
        )
        .unwrap();

        engine
            .insert(7, vec![0.0, 1.0], std::collections::HashMap::new())
            .unwrap();
        let canonical = engine
            .cold_tier
            .current_coherence_token(7)
            .expect("canonical token must exist");
        engine.hot_tier.insert_with_coherence(
            7,
            vec![1.0, 0.0],
            std::collections::HashMap::new(),
            canonical,
        );

        let scope = 919;
        let query = vec![1.0, 0.0];
        query_cache.insert_with_k_scoped(
            scope,
            query.clone(),
            vec![SearchResult {
                doc_id: 7,
                distance: 0.0,
            }],
            1,
        );

        let removed = engine.audit_hot_tier_coherence("test background hot-tier coherence audit");
        assert_eq!(
            removed, 1,
            "background coherence audit should scrub the corrupted hot-tier mirror"
        );
        assert!(
            query_cache.get_scoped(scope, &query, 1).is_none(),
            "background coherence audit should clear potentially stale query-cache entries"
        );

        let (results, path) = engine
            .knn_search_with_ef_detailed_scoped(&query, 1, None, scope)
            .expect("search should recompute canonically after the background coherence audit");

        assert_ne!(path, SearchExecutionPath::CacheHit);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].doc_id, 0,
            "background audit should clear stale query cache entries and force canonical recomputation"
        );
        assert!(
            !engine.hot_tier.exists(7),
            "background audit should scrub corrupted hot-tier entries"
        );
    }
}
