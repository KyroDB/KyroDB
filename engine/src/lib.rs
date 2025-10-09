//! KyroDB - High-performance vector database for RAG workloads
//!
//! **Hybrid Semantic Cache**: Combines RMI-based frequency prediction with semantic similarity
//! for intelligent cache admission decisions in RAG workloads.
//!
//! See Implementation.md for roadmap and IMPLEMENTATION_UPDATE_ANALYSIS.md for current status.

// ===== Core modules =====

// Vector search: HNSW k-NN index
pub mod hnsw_index;

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

// ===== Global Allocator (jemalloc-profiling feature) =====

#[cfg(feature = "jemalloc-profiling")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// ===== Public API =====

// Vector search components
pub use hnsw_index::{HnswVectorIndex, SearchResult};

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
