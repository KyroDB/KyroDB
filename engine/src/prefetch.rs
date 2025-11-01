//! Predictive prefetching based on co-access pattern learning
//!
//! Learns which documents are frequently accessed together and proactively
//! prefetches likely-to-be-accessed documents into cache, reducing latency
//! for subsequent queries.


use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

/// Minimum co-access count to establish a prefetch relationship
const MIN_COOCCURRENCE_COUNT: u32 = 3;

/// Maximum co-access entries per document to prevent memory explosion
const MAX_COOCCURRENCE_PER_DOC: usize = 20;

/// Prefetch if hotness score exceeds this threshold
const DEFAULT_PREFETCH_THRESHOLD: f32 = 0.05;

/// Time window for co-access pattern detection
const COOCCURRENCE_WINDOW: Duration = Duration::from_secs(60);

/// Co-access pattern: document A frequently accessed with document B
#[derive(Clone, Debug)]
struct CoAccessPattern {
    target_doc_id: u64,
    count: u32,
    last_access: SystemTime,
    confidence: f32,
}

impl CoAccessPattern {
    fn new(target_doc_id: u64) -> Self {
        Self {
            target_doc_id,
            count: 1,
            last_access: SystemTime::now(),
            confidence: 0.0,
        }
    }

    fn update(&mut self) {
        self.count += 1;
        self.last_access = SystemTime::now();
    }

    fn compute_confidence(&mut self, total_accesses: u32) {
        if total_accesses > 0 {
            self.confidence = self.count as f32 / total_accesses as f32;
        }
    }
}

/// Co-access graph for learning document relationships
pub struct CoAccessGraph {
    graph: Arc<RwLock<HashMap<u64, Vec<CoAccessPattern>>>>,
    doc_access_counts: Arc<RwLock<HashMap<u64, u32>>>,
    prefetch_threshold: f32,
}

impl CoAccessGraph {
    pub fn new(prefetch_threshold: f32) -> Self {
        Self {
            graph: Arc::new(RwLock::new(HashMap::new())),
            doc_access_counts: Arc::new(RwLock::new(HashMap::new())),
            prefetch_threshold: prefetch_threshold.clamp(0.0, 1.0),
        }
    }

    pub fn with_default_threshold() -> Self {
        Self::new(DEFAULT_PREFETCH_THRESHOLD)
    }

    /// Record co-access pattern between documents
    ///
    /// Should be called when document B is accessed shortly after document A
    pub fn record_cooccurrence(&self, source_doc: u64, target_doc: u64) {
        if source_doc == target_doc {
            return;
        }

        let mut graph = self.graph.write();
        let patterns = graph.entry(source_doc).or_insert_with(Vec::new);

        if let Some(pattern) = patterns.iter_mut().find(|p| p.target_doc_id == target_doc) {
            pattern.update();
        } else if patterns.len() < MAX_COOCCURRENCE_PER_DOC {
            patterns.push(CoAccessPattern::new(target_doc));
        }

        let mut counts = self.doc_access_counts.write();
        *counts.entry(source_doc).or_insert(0) += 1;
    }

    /// Get documents that should be prefetched for a given document
    ///
    /// Returns document IDs ranked by prefetch priority (confidence score)
    pub fn get_prefetch_candidates(&self, doc_id: u64) -> Vec<u64> {
        let graph = self.graph.read();
        let counts = self.doc_access_counts.read();

        let patterns = match graph.get(&doc_id) {
            Some(p) => p,
            None => return Vec::new(),
        };

        let total_accesses = counts.get(&doc_id).copied().unwrap_or(0);

        let mut candidates: Vec<(u64, f32)> = patterns
            .iter()
            .filter(|p| p.count >= MIN_COOCCURRENCE_COUNT)
            .map(|p| {
                let mut pattern = p.clone();
                pattern.compute_confidence(total_accesses);
                (pattern.target_doc_id, pattern.confidence)
            })
            .filter(|(_, confidence)| *confidence >= self.prefetch_threshold)
            .collect();

        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        candidates.into_iter().map(|(doc_id, _)| doc_id).collect()
    }

    /// Get statistics about the co-access graph
    pub fn stats(&self) -> CoAccessStats {
        let graph = self.graph.read();
        let counts = self.doc_access_counts.read();

        let total_source_docs = graph.len();
        let total_patterns: usize = graph.values().map(|v| v.len()).sum();
        let avg_patterns_per_doc = if total_source_docs > 0 {
            total_patterns as f64 / total_source_docs as f64
        } else {
            0.0
        };

        CoAccessStats {
            total_source_docs,
            total_patterns,
            avg_patterns_per_doc,
            total_tracked_docs: counts.len(),
            prefetch_threshold: self.prefetch_threshold,
        }
    }

    /// Prune stale co-access patterns
    pub fn prune_stale(&self, max_age: Duration) {
        let mut graph = self.graph.write();
        let cutoff = SystemTime::now() - max_age;

        for patterns in graph.values_mut() {
            patterns.retain(|p| p.last_access > cutoff);
        }

        graph.retain(|_, patterns| !patterns.is_empty());
    }

    /// Clear all co-access data
    pub fn clear(&self) {
        let mut graph = self.graph.write();
        graph.clear();

        let mut counts = self.doc_access_counts.write();
        counts.clear();
    }
}

/// Prefetcher statistics
#[derive(Debug, Clone)]
pub struct CoAccessStats {
    pub total_source_docs: usize,
    pub total_patterns: usize,
    pub avg_patterns_per_doc: f64,
    pub total_tracked_docs: usize,
    pub prefetch_threshold: f32,
}

/// Prefetch engine with background task
pub struct Prefetcher {
    coaccesses: Arc<CoAccessGraph>,
    recent_accesses: Arc<RwLock<Vec<(u64, SystemTime)>>>,
    prefetches_executed: Arc<AtomicU64>,
    prefetch_hits: Arc<AtomicU64>,
}

impl Prefetcher {
    pub fn new(prefetch_threshold: f32) -> Self {
        Self {
            coaccesses: Arc::new(CoAccessGraph::new(prefetch_threshold)),
            recent_accesses: Arc::new(RwLock::new(Vec::new())),
            prefetches_executed: Arc::new(AtomicU64::new(0)),
            prefetch_hits: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_default_threshold() -> Self {
        Self::new(DEFAULT_PREFETCH_THRESHOLD)
    }

    /// Record a document access for co-access learning
    pub fn record_access(&self, doc_id: u64) {
        let now = SystemTime::now();
        let mut recent = self.recent_accesses.write();

        let cutoff = now - COOCCURRENCE_WINDOW;
        recent.retain(|(_, timestamp)| *timestamp > cutoff);

        for (prev_doc_id, _) in recent.iter() {
            self.coaccesses.record_cooccurrence(*prev_doc_id, doc_id);
        }

        recent.push((doc_id, now));

        if recent.len() > 100 {
            recent.drain(..50);
        }
    }

    /// Get prefetch candidates for a document
    pub fn get_prefetch_candidates(&self, doc_id: u64) -> Vec<u64> {
        self.coaccesses.get_prefetch_candidates(doc_id)
    }

    /// Check if a document should be prefetched
    ///
    /// Returns true if the document has strong co-access patterns
    /// (frequently accessed with other documents).
    pub fn should_prefetch(&self, doc_id: u64) -> bool {
        let candidates = self.get_prefetch_candidates(doc_id);
        !candidates.is_empty()
    }

    /// Record that a prefetch was executed
    pub fn record_prefetch(&self) {
        self.prefetches_executed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a prefetched document was actually used
    pub fn record_prefetch_hit(&self) {
        self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Get prefetcher statistics
    pub fn stats(&self) -> PrefetcherStats {
        let prefetches = self.prefetches_executed.load(Ordering::Relaxed);
        let hits = self.prefetch_hits.load(Ordering::Relaxed);
        let hit_rate = if prefetches > 0 {
            hits as f64 / prefetches as f64
        } else {
            0.0
        };

        PrefetcherStats {
            prefetches_executed: prefetches,
            prefetch_hits: hits,
            prefetch_hit_rate: hit_rate,
            coaccesses: self.coaccesses.stats(),
        }
    }

    /// Clear all prefetch data
    pub fn clear(&self) {
        self.coaccesses.clear();
        let mut recent = self.recent_accesses.write();
        recent.clear();
        self.prefetches_executed.store(0, Ordering::Relaxed);
        self.prefetch_hits.store(0, Ordering::Relaxed);
    }
}

/// Prefetcher statistics
#[derive(Debug, Clone)]
pub struct PrefetcherStats {
    pub prefetches_executed: u64,
    pub prefetch_hits: u64,
    pub prefetch_hit_rate: f64,
    pub coaccesses: CoAccessStats,
}

/// Configuration for prefetch background task
#[derive(Clone, Debug)]
pub struct PrefetchConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub max_prefetch_per_doc: usize,
    pub prefetch_threshold: f32,
    pub prune_interval: Duration,
    pub max_pattern_age: Duration,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(5),
            max_prefetch_per_doc: 5,
            prefetch_threshold: DEFAULT_PREFETCH_THRESHOLD,
            prune_interval: Duration::from_secs(300),
            max_pattern_age: Duration::from_secs(3600),
        }
    }
}

/// Spawn prefetch background task
///
/// Periodically prunes stale co-access patterns and could trigger
/// proactive prefetching in future enhancements
pub async fn spawn_prefetch_task(
    prefetcher: Arc<Prefetcher>,
    config: PrefetchConfig,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !config.enabled {
            return;
        }

        let mut prune_counter = 0;
        let prune_every = (config.prune_interval.as_secs() / config.interval.as_secs()).max(1);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    break;
                }
                _ = sleep(config.interval) => {
                    prune_counter += 1;

                    if prune_counter >= prune_every {
                        prefetcher.coaccesses.prune_stale(config.max_pattern_age);
                        prune_counter = 0;
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coaccesses_basic() {
        let graph = CoAccessGraph::with_default_threshold();

        for _ in 0..5 {
            graph.record_cooccurrence(1, 2);
        }

        let candidates = graph.get_prefetch_candidates(1);
        assert!(candidates.contains(&2));
    }

    #[test]
    fn test_coaccesses_threshold() {
        let graph = CoAccessGraph::new(0.5);

        graph.record_cooccurrence(1, 2);
        graph.record_cooccurrence(1, 3);
        graph.record_cooccurrence(1, 3);

        let candidates = graph.get_prefetch_candidates(1);
        assert!(!candidates.contains(&2));
    }

    #[test]
    fn test_coaccesses_same_doc() {
        let graph = CoAccessGraph::with_default_threshold();
        graph.record_cooccurrence(1, 1);

        let stats = graph.stats();
        assert_eq!(stats.total_patterns, 0);
    }

    #[test]
    fn test_coaccesses_max_per_doc() {
        let graph = CoAccessGraph::with_default_threshold();

        for i in 0..MAX_COOCCURRENCE_PER_DOC + 10 {
            for _ in 0..MIN_COOCCURRENCE_COUNT {
                graph.record_cooccurrence(1, i as u64 + 100);
            }
        }

        let stats = graph.stats();
        assert!(stats.total_patterns <= MAX_COOCCURRENCE_PER_DOC);
    }

    #[test]
    fn test_prefetcher_record_access() {
        let prefetcher = Prefetcher::with_default_threshold();

        prefetcher.record_access(1);
        prefetcher.record_access(2);
        prefetcher.record_access(3);

        let candidates = prefetcher.get_prefetch_candidates(1);
        assert!(candidates.len() <= 2);
    }

    #[test]
    fn test_prefetcher_stats() {
        let prefetcher = Prefetcher::with_default_threshold();

        prefetcher.record_prefetch();
        prefetcher.record_prefetch_hit();

        let stats = prefetcher.stats();
        assert_eq!(stats.prefetches_executed, 1);
        assert_eq!(stats.prefetch_hits, 1);
        assert!((stats.prefetch_hit_rate - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_coaccesses_clear() {
        let graph = CoAccessGraph::with_default_threshold();

        for _ in 0..5 {
            graph.record_cooccurrence(1, 2);
        }

        let stats_before = graph.stats();
        assert!(stats_before.total_patterns > 0);

        graph.clear();

        let stats_after = graph.stats();
        assert_eq!(stats_after.total_patterns, 0);
        assert_eq!(stats_after.total_source_docs, 0);
    }

    #[tokio::test]
    async fn test_spawn_prefetch_task_disabled() {
        let prefetcher = Arc::new(Prefetcher::with_default_threshold());
        let config = PrefetchConfig {
            enabled: false,
            ..Default::default()
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let handle = spawn_prefetch_task(prefetcher, config, shutdown_rx).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        handle.abort();
    }

    #[tokio::test]
    async fn test_spawn_prefetch_task_shutdown() {
        let prefetcher = Arc::new(Prefetcher::with_default_threshold());
        let config = PrefetchConfig::default();

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let handle = spawn_prefetch_task(prefetcher, config, shutdown_rx).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = shutdown_tx.send(());
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(handle.is_finished());
    }
}
