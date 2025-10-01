//! Write-Ahead Log Tests
//!
//! Comprehensive tests for WAL operations, durability, recovery, and group commit

pub mod append_correctness;
pub mod compaction;
pub mod corruption;
pub mod durability;
pub mod group_commit;
pub mod recovery;
