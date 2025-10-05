//! KyroDB - High-performance vector database for RAG workloads
//!
//! Phase 0 roadmap:
//! - Week 1-2: HNSW vector search prototype (hnswlib-rs wrapper)
//! - Week 3-8: Learned cache with RMI (predicts cache hotness)
//! - Week 9-12: Basic persistence (WAL + snapshots for vectors)
//!
//! This is a clean slate - building vector DB from scratch, NOT porting KV store.

// ===== Phase 0 modules =====

// Week 1-2: HNSW vector search (CURRENT - Phase 0 Week 1-2)
pub mod hnsw_index;

// Week 3-8: RMI core for learned cache (predicts doc_id → hotness_score)
pub mod rmi_core;

// Week 3-4: Learned cache predictor (Phase 0 Week 3-4) ✅
pub mod learned_cache;

// Week 5-8: Access pattern logger (Phase 0 Week 5-8) ✅
pub mod access_logger;

// Phase 0 Week 9-12: A/B Testing Framework (CURRENT)
pub mod ab_stats; // A/B test metrics persistence (CSV format)
pub mod cache_strategy; // CacheStrategy trait + LRU/Learned implementations + A/B splitter
pub mod training_task;
pub mod vector_cache; // In-memory vector cache with LRU eviction // Background RMI training task (tokio::spawn, 10-minute interval)

// ===== Public API =====

// Re-export HNSW components (Phase 0 Week 1-2)
pub use hnsw_index::{HnswVectorIndex, SearchResult};

// Re-export RMI core components for learned cache (Phase 0 Week 3-8)
pub use rmi_core::{LocalLinearModel, RmiIndex, RmiSegment};

// Re-export learned cache components (Phase 0 Week 3-4)
pub use learned_cache::{AccessEvent, AccessType, CachePredictorStats, LearnedCachePredictor};

// Re-export access logger components (Phase 0 Week 5-8)
pub use access_logger::{hash_embedding, AccessLoggerStats, AccessPatternLogger};

// Re-export A/B testing components (Phase 0 Week 9-12)
pub use ab_stats::{AbStatsPersister, AbTestMetric, AbTestSummary};
pub use cache_strategy::{AbTestSplitter, CacheStrategy, LearnedCacheStrategy, LruCacheStrategy};
pub use training_task::{spawn_training_task, TrainingConfig};
pub use vector_cache::{CacheStats, CachedVector, VectorCache};
