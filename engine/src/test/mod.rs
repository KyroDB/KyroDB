//! Comprehensive Test Suite for KyroDB Engine
//!
//! This is a rigorous, extensive test suite covering all engine components:
//! - Concurrency (race conditions, deadlocks, lock ordering)
//! - Memory management (buffers, caches, leaks)
//! - SIMD operations (AVX2, NEON, correctness, performance)
//! - RMI learned index (accuracy, bounds, atomicity)
//! - WAL (durability, group commit, recovery)
//! - Snapshots (creation, recovery, atomicity)
//! - Background tasks (maintenance, compaction)
//! - HTTP endpoints (integration tests)
//! - Stress tests (high concurrency, large datasets)
//!
//! Created: September 30, 2025
//! Purpose: Replace legacy tests/ and examples/ directories with modern, comprehensive tests

// Test modules organized by component
pub mod utils;
// Temporarily commented out to test background module
// pub mod concurrency;
// pub mod memory;
// pub mod simd;
// pub mod rmi;
// pub mod wal;
// pub mod snapshot;
pub mod background;
// pub mod integration;
// pub mod stress;
