//! A/B Test Statistics Persistence
//!
//! A/B test metrics persistence: CSV-based stats for experiment tracking
//!
//! Persists cache hit/miss stats to CSV file, allowing server restarts
//! without losing evaluation data.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// A/B test metric record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbTestMetric {
    pub timestamp_ms: u64,
    pub strategy: String,
    pub event_type: String, // "hit", "miss", "eviction"
    pub doc_id: u64,
    pub latency_ns: u64,
}

/// A/B test stats persister
///
/// Appends metrics to CSV file for durability across restarts.
pub struct AbStatsPersister {
    #[allow(dead_code)] // Keep for future use (file rotation, path queries)
    file_path: PathBuf,
    writer: RwLock<Option<csv::Writer<File>>>,
}

impl AbStatsPersister {
    /// Create new stats persister
    ///
    /// Creates CSV file if it doesn't exist, appends if it does.
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self> {
        let file_path = file_path.as_ref().to_path_buf();

        // Create parent directory if needed
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).context("Failed to create stats directory")?;
        }

        // Open file in append mode
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .context("Failed to open stats file")?;

        let writer = csv::WriterBuilder::new()
            .has_headers(false) // Don't write headers on every open
            .from_writer(file);

        Ok(Self {
            file_path,
            writer: RwLock::new(Some(writer)),
        })
    }

    /// Log cache hit
    pub async fn log_hit(&self, strategy: &str, doc_id: u64, latency_ns: u64) -> Result<()> {
        self.log_event(strategy, "hit", doc_id, latency_ns).await
    }

    /// Log cache miss
    pub async fn log_miss(&self, strategy: &str, doc_id: u64, latency_ns: u64) -> Result<()> {
        self.log_event(strategy, "miss", doc_id, latency_ns).await
    }

    /// Log cache eviction
    pub async fn log_eviction(&self, strategy: &str, doc_id: u64) -> Result<()> {
        self.log_event(strategy, "eviction", doc_id, 0).await
    }

    /// Log generic event
    async fn log_event(
        &self,
        strategy: &str,
        event_type: &str,
        doc_id: u64,
        latency_ns: u64,
    ) -> Result<()> {
        let timestamp_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;

        let metric = AbTestMetric {
            timestamp_ms,
            strategy: strategy.to_string(),
            event_type: event_type.to_string(),
            doc_id,
            latency_ns,
        };

        let mut writer_guard = self.writer.write().await;
        if let Some(writer) = writer_guard.as_mut() {
            writer
                .serialize(&metric)
                .context("Failed to serialize metric")?;
            writer.flush().context("Failed to flush CSV writer")?;
        }

        Ok(())
    }

    /// Flush writer (ensure all data written to disk)
    pub async fn flush(&self) -> Result<()> {
        let mut writer_guard = self.writer.write().await;
        if let Some(writer) = writer_guard.as_mut() {
            writer.flush().context("Failed to flush CSV writer")?;
        }
        Ok(())
    }

    /// Load historical metrics from file
    ///
    /// Reads all metrics from CSV file. Useful for analysis and visualization.
    pub fn load_metrics<P: AsRef<Path>>(file_path: P) -> Result<Vec<AbTestMetric>> {
        let file =
            File::open(file_path.as_ref()).context("Failed to open stats file for reading")?;

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        let mut metrics = Vec::new();
        for result in reader.deserialize() {
            let metric: AbTestMetric = result.context("Failed to deserialize metric")?;
            metrics.push(metric);
        }

        Ok(metrics)
    }

    /// Analyze metrics and compute summary statistics
    pub fn analyze_metrics(metrics: &[AbTestMetric]) -> AbTestSummary {
        let mut lru_hits = 0u64;
        let mut lru_misses = 0u64;
        let mut learned_hits = 0u64;
        let mut learned_misses = 0u64;

        let mut lru_latencies: Vec<u64> = Vec::new();
        let mut learned_latencies: Vec<u64> = Vec::new();

        for metric in metrics {
            match metric.strategy.as_str() {
                "lru_baseline" => match metric.event_type.as_str() {
                    "hit" => {
                        lru_hits += 1;
                        lru_latencies.push(metric.latency_ns);
                    }
                    "miss" => lru_misses += 1,
                    _ => {}
                },
                "learned_rmi" => match metric.event_type.as_str() {
                    "hit" => {
                        learned_hits += 1;
                        learned_latencies.push(metric.latency_ns);
                    }
                    "miss" => learned_misses += 1,
                    _ => {}
                },
                _ => {}
            }
        }

        let lru_hit_rate = if lru_hits + lru_misses > 0 {
            lru_hits as f64 / (lru_hits + lru_misses) as f64
        } else {
            0.0
        };

        let learned_hit_rate = if learned_hits + learned_misses > 0 {
            learned_hits as f64 / (learned_hits + learned_misses) as f64
        } else {
            0.0
        };

        let lru_avg_latency = if !lru_latencies.is_empty() {
            lru_latencies.iter().sum::<u64>() / lru_latencies.len() as u64
        } else {
            0
        };

        let learned_avg_latency = if !learned_latencies.is_empty() {
            learned_latencies.iter().sum::<u64>() / learned_latencies.len() as u64
        } else {
            0
        };

        AbTestSummary {
            lru_hits,
            lru_misses,
            lru_hit_rate,
            lru_avg_latency_ns: lru_avg_latency,
            learned_hits,
            learned_misses,
            learned_hit_rate,
            learned_avg_latency_ns: learned_avg_latency,
            total_events: metrics.len(),
        }
    }
}

/// A/B test summary statistics
#[derive(Debug, Clone)]
pub struct AbTestSummary {
    pub lru_hits: u64,
    pub lru_misses: u64,
    pub lru_hit_rate: f64,
    pub lru_avg_latency_ns: u64,
    pub learned_hits: u64,
    pub learned_misses: u64,
    pub learned_hit_rate: f64,
    pub learned_avg_latency_ns: u64,
    pub total_events: usize,
}

impl AbTestSummary {
    /// Print human-readable summary
    pub fn print(&self) {
        println!("=== A/B Test Summary ===");
        println!("LRU Baseline:");
        println!("  Hits: {}", self.lru_hits);
        println!("  Misses: {}", self.lru_misses);
        println!("  Hit Rate: {:.2}%", self.lru_hit_rate * 100.0);
        println!("  Avg Latency: {}ns", self.lru_avg_latency_ns);

        println!("\nLearned Cache:");
        println!("  Hits: {}", self.learned_hits);
        println!("  Misses: {}", self.learned_misses);
        println!("  Hit Rate: {:.2}%", self.learned_hit_rate * 100.0);
        println!("  Avg Latency: {}ns", self.learned_avg_latency_ns);

        println!("\nImprovement:");
        let hit_rate_improvement =
            (self.learned_hit_rate - self.lru_hit_rate) / self.lru_hit_rate * 100.0;
        println!("  Hit Rate: {:+.2}%", hit_rate_improvement);

        println!("\nTotal Events: {}", self.total_events);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_persister_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("ab_test_stats.csv");

        let persister = AbStatsPersister::new(&stats_path).unwrap();

        // Log some events
        persister.log_hit("lru_baseline", 1, 100).await.unwrap();
        persister.log_miss("lru_baseline", 2, 200).await.unwrap();
        persister.log_hit("learned_rmi", 3, 50).await.unwrap();

        persister.flush().await.unwrap();

        // Verify file exists
        assert!(stats_path.exists());

        // Load and verify metrics
        let metrics = AbStatsPersister::load_metrics(&stats_path).unwrap();
        assert_eq!(metrics.len(), 3);
        assert_eq!(metrics[0].strategy, "lru_baseline");
        assert_eq!(metrics[0].event_type, "hit");
        assert_eq!(metrics[2].strategy, "learned_rmi");
    }

    #[tokio::test]
    async fn test_persister_survives_restart() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("ab_test_stats.csv");

        // First session
        {
            let persister = AbStatsPersister::new(&stats_path).unwrap();
            persister.log_hit("lru_baseline", 1, 100).await.unwrap();
            persister.flush().await.unwrap();
        }

        // Second session (simulates restart)
        {
            let persister = AbStatsPersister::new(&stats_path).unwrap();
            persister.log_hit("learned_rmi", 2, 50).await.unwrap();
            persister.flush().await.unwrap();
        }

        // Load all metrics
        let metrics = AbStatsPersister::load_metrics(&stats_path).unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].doc_id, 1);
        assert_eq!(metrics[1].doc_id, 2);
    }

    #[test]
    fn test_analyze_metrics() {
        let metrics = vec![
            AbTestMetric {
                timestamp_ms: 1000,
                strategy: "lru_baseline".to_string(),
                event_type: "hit".to_string(),
                doc_id: 1,
                latency_ns: 100,
            },
            AbTestMetric {
                timestamp_ms: 1001,
                strategy: "lru_baseline".to_string(),
                event_type: "miss".to_string(),
                doc_id: 2,
                latency_ns: 0,
            },
            AbTestMetric {
                timestamp_ms: 1002,
                strategy: "learned_rmi".to_string(),
                event_type: "hit".to_string(),
                doc_id: 3,
                latency_ns: 50,
            },
            AbTestMetric {
                timestamp_ms: 1003,
                strategy: "learned_rmi".to_string(),
                event_type: "hit".to_string(),
                doc_id: 4,
                latency_ns: 50,
            },
        ];

        let summary = AbStatsPersister::analyze_metrics(&metrics);

        assert_eq!(summary.lru_hits, 1);
        assert_eq!(summary.lru_misses, 1);
        assert!((summary.lru_hit_rate - 0.5).abs() < 0.01);

        assert_eq!(summary.learned_hits, 2);
        assert_eq!(summary.learned_misses, 0);
        assert!((summary.learned_hit_rate - 1.0).abs() < 0.01);

        assert_eq!(summary.total_events, 4);
    }

    #[test]
    fn test_summary_print() {
        let summary = AbTestSummary {
            lru_hits: 30,
            lru_misses: 70,
            lru_hit_rate: 0.3,
            lru_avg_latency_ns: 100,
            learned_hits: 80,
            learned_misses: 20,
            learned_hit_rate: 0.8,
            learned_avg_latency_ns: 50,
            total_events: 200,
        };

        // Just ensure it doesn't panic
        summary.print();
    }

    #[tokio::test]
    async fn test_concurrent_logging() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("ab_test_stats.csv");

        let persister = Arc::new(AbStatsPersister::new(&stats_path).unwrap());
        let mut handles = vec![];

        // Spawn 10 tasks logging concurrently
        for i in 0..10 {
            let persister_clone = Arc::clone(&persister);
            let handle = tokio::spawn(async move {
                for j in 0..10 {
                    let doc_id = (i * 10 + j) as u64;
                    persister_clone.log_hit("test", doc_id, 100).await.unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        persister.flush().await.unwrap();

        // Should have 100 events
        let metrics = AbStatsPersister::load_metrics(&stats_path).unwrap();
        assert_eq!(metrics.len(), 100);
    }
}
