//! RMI (learned index) tests for accuracy, bounded search, and rebuild atomicity.

#[cfg(feature = "learned-index")]
pub mod index_accuracy;

#[cfg(feature = "learned-index")]
pub mod bounded_search;

#[cfg(feature = "learned-index")]
pub mod rebuild_atomicity;

#[cfg(feature = "learned-index")]
pub mod segment_adaptation;
