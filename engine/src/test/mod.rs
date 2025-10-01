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

// Test modules organized by component
pub mod utils;
pub mod utils_tests;  // Tests for test utilities themselves
pub mod concurrency;
pub mod memory;
pub mod simd;  // SIMD runtime detection and correctness tests
pub mod rmi;
pub mod wal;
pub mod snapshot;
pub mod background;
pub mod background_worker_test;  // Tests for zero-lock background worker
pub mod build_rmi_sync_test;     // Tests for build_rmi() synchronization with background worker
pub mod integration;
pub mod stress;
