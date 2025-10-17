//! KyroDB - High-performance vector database for RAG workloads
//!
//! **Hybrid Semantic Cache**: Combines RMI-based frequency prediction with semantic similarity
//! for intelligent cache admission decisions in RAG workloads.
//!
//! See Implementation.md for roadmap and IMPLEMENTATION_UPDATE_ANALYSIS.md for current status.

// ===== Core modules =====

// Vector search: HNSW k-NN index
pub mod hnsw_index;

// HNSW backend: Integration layer for cache + HNSW
pub mod hnsw_backend;

// Learned index: RMI (Recursive Model Index) for cache prediction
pub mod rmi_core;

// Cache prediction: Hybrid frequency + semantic similarity
pub mod learned_cache;

// Access logging: Ring buffer for training data collection
pub mod access_logger;

// A/B testing: Framework for cache strategy comparison
pub mod ab_stats; // Metrics persistence (CSV format)
pub mod cache_strategy; // CacheStrategy trait + LRU/Learned implementations + A/B splitter
pub mod training_task; // Background RMI training task (tokio::spawn, 60-second interval)
pub mod vector_cache; // In-memory vector cache with LRU eviction

// Semantic layer: Hybrid cache decisions (frequency + similarity)
pub mod semantic_adapter;

// Quality metrics: NDCG@10, MRR, Recall@k for ranking validation
pub mod ndcg;

// Memory profiling: jemalloc-based cross-platform profiler
pub mod memory_profiler;

// Persistence: WAL + snapshots for durability
pub mod persistence;

// Backup and restore: Full/incremental backups, PITR, retention policies
pub mod backup;

// Hot tier: Recent writes buffer (Layer 2)
pub mod hot_tier;

// Tiered engine: Three-layer architecture orchestrator
pub mod tiered_engine;

// Metrics and observability: Prometheus metrics, health checks, SLO monitoring
pub mod metrics;

// Configuration management: YAML/TOML parsing, env vars, validation
pub mod config;

// Error recovery: Circuit breaker pattern for fault tolerance
pub mod circuit_breaker;

// Authentication & multi-tenancy: API key auth, tenant isolation, rate limiting
pub mod auth;
pub mod rate_limiter;
pub mod tenant;
pub mod usage_tracker;

// ===== Global Allocator (jemalloc-profiling feature) =====

#[cfg(feature = "jemalloc-profiling")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// ===== Public API =====

// Vector search components
pub use hnsw_index::{HnswVectorIndex, SearchResult};

// HNSW backend (cache integration)
pub use hnsw_backend::HnswBackend;

// Learned index components (RMI for cache prediction)
pub use rmi_core::{LocalLinearModel, RmiIndex, RmiSegment};

// Cache predictor components
pub use learned_cache::{AccessEvent, AccessType, CachePredictorStats, LearnedCachePredictor};

// Access logger components
pub use access_logger::{hash_embedding, AccessLoggerStats, AccessPatternLogger};

// A/B testing components
pub use ab_stats::{AbStatsPersister, AbTestMetric, AbTestSummary};
pub use cache_strategy::{AbTestSplitter, CacheStrategy, LearnedCacheStrategy, LruCacheStrategy};
pub use training_task::{spawn_training_task, TrainingConfig};
pub use vector_cache::{CacheStats, CachedVector, VectorCache};

// Quality metrics components
pub use ndcg::{
    calculate_dcg, calculate_idcg, calculate_mean_ndcg, calculate_mrr, calculate_ndcg,
    calculate_recall_at_k, CacheQualityMetrics, RankingResult,
};

// Memory profiling components
pub use memory_profiler::{
    detect_memory_leak, dump_heap_profile, get_memory_stats, MemoryProfiler, MemoryStats,
    MemoryStatsDelta,
};

// Semantic adapter components (hybrid cache: frequency + similarity)
pub use semantic_adapter::{SemanticAdapter, SemanticConfig, SemanticStats};

// Persistence components (WAL + snapshots)
pub use persistence::{FsyncPolicy, Manifest, Snapshot, WalEntry, WalOp, WalReader, WalWriter};

// Backup and restore components
pub use backup::{BackupManager, BackupMetadata, BackupType, ClearDirectoryOptions, RestoreManager, RetentionPolicy, compute_backup_checksum};

// Hot tier components (Layer 2)
pub use hot_tier::{HotTier, HotTierStats};

// Tiered engine components (three-layer architecture)
pub use tiered_engine::{TieredEngine, TieredEngineConfig, TieredEngineStats};

// Circuit breaker components (error recovery)
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStats};

// Metrics and observability components
pub use metrics::{ErrorCategory, HealthStatus, MetricsCollector, SloStatus};

// Configuration components
pub use config::{
    AuthConfig, CacheConfig, CacheStrategy as ConfigCacheStrategy, DistanceMetric,
    FsyncPolicy as ConfigFsyncPolicy, HnswConfig, KyroDbConfig, LogFormat, LogLevel,
    LoggingConfig, PersistenceConfig, RateLimitConfig, ServerConfig, SloConfig,
};

// Authentication and authorization components
pub use auth::{ApiKey, AuthManager, TenantInfo};
pub use rate_limiter::{RateLimiter, TokenBucket};
pub use tenant::{filter_tenant_results, SearchResult as TenantSearchResult, TenantManager};
pub use usage_tracker::{TenantUsage, UsageSnapshot, UsageTracker};
