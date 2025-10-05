//! KyroDB - High-performance vector database for RAG workloads
//!
//! Phase 0 roadmap:
//! - Week 1-2: HNSW vector search prototype (hnswlib-rs wrapper)
//! - Week 3-8: Learned cache with RMI (predicts cache hotness)
//! - Week 9-12: Basic persistence (WAL + snapshots for vectors)
//!
//! This is a clean slate - building vector DB from scratch, NOT porting KV store.

// ===== Phase 0 modules =====

// Week 3-8: RMI core for learned cache (predicts doc_id → hotness_score)
pub mod rmi_core;

// Week 1-2: HNSW vector search (to be implemented next)
// pub mod hnsw_index;

// Week 3-8: Learned cache predictor (to be implemented)
// pub mod learned_cache;

// Week 9-12: Document storage and persistence (to be implemented)
// pub mod storage;

// ===== Public API =====

// Re-export RMI core components for learned cache
pub use rmi_core::{LocalLinearModel, RmiIndex, RmiSegment};
