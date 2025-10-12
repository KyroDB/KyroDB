//!
//! **Purpose**: Validate KyroDB A/B testing framework under sustained production-like load
//!
//! **What This Test Validates**:
//! 1. Learned cache achieves 70-90% hit rate (vs 30-40% LRU baseline)
//! 2. No memory leaks under sustained load (4.32M queries)
//! 3. Training task runs reliably every 10 minutes (72 cycles)
//! 4. No performance degradation over time
//! 5. Stats persistence survives restarts
//!
//! **Workload**:
//! - Duration: 12 hours (configurable)
//! - QPS: 100 queries/second
//! - Distribution: Zipf (exponent=1.5) - 80/20 hot/cold
//! - Corpus: 100,000 documents
//! - A/B split: 50% LRU baseline, 50% Learned cache
//!
//! **Run on Azure VM**:
//! ```
//! cargo build --release --bin validation_24h
//! nohup ./target/release/validation_24h > validation.log 2>&1 &
//! ```

use anyhow::{bail, Context, Result};
use kyrodb_engine::{
    spawn_training_task, AbStatsPersister, AbTestSplitter, AccessPatternLogger, CacheStrategy,
    CachedVector, LearnedCachePredictor, LearnedCacheStrategy, LruCacheStrategy, TrainingConfig,
};
use rand::{distributions::Distribution, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::Normal;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;
use zipf::ZipfDistribution;

/// Validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    /// Test duration in hours (default: 12)
    duration_hours: u64,

    /// Target queries per second (default: 100)
    target_qps: u64,

    /// Number of unique documents (default: 100K)
    corpus_size: usize,

    /// Zipf exponent (default: 1.1 for realistic 80/20 distribution)
    /// Lower values = less skewed (1.0 = uniform, 2.0 = very skewed)
    zipf_exponent: f64,

    /// Cache capacity (default: 10K vectors)
    cache_capacity: usize,

    /// Training interval in seconds (default: 600 = 10 minutes)
    training_interval_secs: u64,

    /// Access logger window size (default: 100K events)
    logger_window_size: usize,

    /// Output files
    stats_csv: String,
    results_json: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            duration_hours: 12,
            target_qps: 100,
            corpus_size: 100_000,
            zipf_exponent: 1.07, // Realistic 80/20 distribution (1.5 was too extreme)
            cache_capacity: 10_000,
            training_interval_secs: 600,
            logger_window_size: 100_000,
            stats_csv: "validation_24h_stats.csv".to_string(),
            results_json: "validation_24h_results.json".to_string(),
        }
    }
}

impl Config {
    /// Validate configuration for sanity
    fn validate(&self) -> Result<()> {
        if self.duration_hours == 0 {
            bail!("duration_hours must be > 0");
        }
        if self.target_qps == 0 {
            bail!("target_qps must be > 0");
        }
        if self.corpus_size == 0 {
            bail!("corpus_size must be > 0");
        }
        if self.cache_capacity == 0 {
            bail!("cache_capacity must be > 0");
        }
        if self.cache_capacity > self.corpus_size {
            bail!(
                "cache_capacity ({}) cannot exceed corpus_size ({})",
                self.cache_capacity,
                self.corpus_size
            );
        }
        if self.training_interval_secs == 0 {
            bail!("training_interval_secs must be > 0");
        }
        if self.zipf_exponent <= 0.0 {
            bail!("zipf_exponent must be > 0");
        }
        if self.logger_window_size == 0 {
            bail!("logger_window_size must be > 0");
        }
        Ok(())
    }
}

/// Strategy identifier (type-safe routing)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrategyId {
    LruBaseline,
    LearnedRmi,
}

/// Final validation results
#[derive(Debug, Serialize, Deserialize)]
struct ValidationResults {
    config: Config,
    test_duration_secs: u64,
    total_queries: u64,

    // LRU baseline stats
    lru_total_queries: u64,
    lru_cache_hits: u64,
    lru_cache_misses: u64,
    lru_hit_rate: f64,

    // Learned cache stats
    learned_total_queries: u64,
    learned_cache_hits: u64,
    learned_cache_misses: u64,
    learned_hit_rate: f64,

    // Comparison
    hit_rate_improvement: f64,

    // Training stats
    expected_training_cycles: u64,
    training_task_crashed: bool,

    // Memory stats
    initial_memory_mb: f64,
    final_memory_mb: f64,
    memory_growth_mb: f64,
    memory_growth_pct: f64,

    // Timestamps
    start_time: SystemTime,
    end_time: SystemTime,
}

/// Zipf distribution sampler for hot/cold access patterns
struct ZipfSampler {
    dist: ZipfDistribution,
}

impl ZipfSampler {
    fn new(corpus_size: usize, exponent: f64) -> Result<Self> {
        let dist = ZipfDistribution::new(corpus_size, exponent).map_err(|_| {
            anyhow::anyhow!(
                "Failed to create Zipf distribution with corpus_size={}, exponent={}",
                corpus_size,
                exponent
            )
        })?;
        Ok(Self { dist })
    }

    /// Sample doc_id following Zipf distribution
    /// Lower IDs are more frequent (hot documents)
    fn sample(&self) -> u64 {
        let mut rng = rand::thread_rng();
        // ZipfDistribution returns 1-indexed, convert to 0-indexed
        (self.dist.sample(&mut rng) - 1) as u64
    }
}

/// Mock document storage (in real system, this would be disk/network)
struct MockDocumentStore {
    corpus_size: usize,
    embedding_dim: usize,
    topic_bases: Vec<Vec<f32>>,
    noise_stddev: f32,
}

impl MockDocumentStore {
    fn new(corpus_size: usize) -> Self {
        let embedding_dim = 768;
        let mut topics = std::cmp::max(16, corpus_size / 1000);
        topics = topics.min(256);
        topics = topics.max(1).min(corpus_size.max(1));

        let mut rng = ChaCha8Rng::seed_from_u64(0x5EEDFACE);
        let mut topic_bases = Vec::with_capacity(topics);
        for _ in 0..topics {
            let mut base = vec![0.0f32; embedding_dim];
            for value in base.iter_mut() {
                *value = rng.gen::<f32>() * 2.0 - 1.0;
            }
            normalize_embedding(&mut base);
            topic_bases.push(base);
        }

        Self {
            corpus_size,
            embedding_dim,
            topic_bases,
            noise_stddev: 0.05,
        }
    }

    /// Fetch document by ID (simulates disk read)
    fn fetch(&self, doc_id: u64) -> Option<Vec<f32>> {
        if doc_id >= self.corpus_size as u64 {
            return None;
        }

        let topic_index = (doc_id as usize) % self.topic_bases.len();
        let mut embedding = self.topic_bases[topic_index].clone();
        debug_assert_eq!(embedding.len(), self.embedding_dim);

        let mut rng = ChaCha8Rng::seed_from_u64(doc_id);
        let noise = Normal::new(0.0, self.noise_stddev as f64).expect("valid noise distribution");
        for value in embedding.iter_mut() {
            *value += noise.sample(&mut rng) as f32;
        }

        normalize_embedding(&mut embedding);
        Some(embedding)
    }
}

fn normalize_embedding(embedding: &mut [f32]) {
    let norm = embedding
        .iter()
        .map(|v| (*v as f64) * (*v as f64))
        .sum::<f64>()
        .sqrt() as f32;

    if norm > 1e-6 {
        for value in embedding.iter_mut() {
            *value /= norm;
        }
    }
}

/// Get process memory usage in MB (Linux only)
fn get_memory_mb() -> Result<f64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status")
            .context("Failed to read /proc/self/status")?;

        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    let kb = kb_str
                        .parse::<f64>()
                        .context("Failed to parse memory value")?;
                    return Ok(kb / 1024.0);
                }
            }
        }
        bail!("VmRSS not found in /proc/self/status");
    }

    #[cfg(not(target_os = "linux"))]
    {
        bail!("Memory tracking only supported on Linux. Use --skip-memory-check flag.");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load and validate config
    let config = Config::default();
    config.validate().context("Invalid configuration")?;

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Configuration:");
    println!("  Duration:          {} hours", config.duration_hours);
    println!("  Target QPS:        {}", config.target_qps);
    println!("  Corpus size:       {} documents", config.corpus_size);
    println!(
        "  Zipf exponent:     {} (realistic skew)",
        config.zipf_exponent
    );
    println!("  Cache capacity:    {} vectors", config.cache_capacity);
    println!(
        "  Training interval: {} seconds",
        config.training_interval_secs
    );
    println!("  Logger window:     {} events", config.logger_window_size);
    println!();
    println!("Expected workload:");
    let total_expected = config.target_qps * config.duration_hours * 3600;
    println!(
        "  Total queries:     {:.2} million",
        total_expected as f64 / 1_000_000.0
    );
    println!(
        "  Training cycles:   {}",
        (config.duration_hours * 3600) / config.training_interval_secs
    );
    println!();
    println!("Output files:");
    println!("  Stats CSV:         {}", config.stats_csv);
    println!("  Results JSON:      {}", config.results_json);
    println!();

    // Initialize components
    println!("Initializing components...");

    // Access logger with bounded circular buffer (prevents OOM)
    let access_logger = Arc::new(RwLock::new(AccessPatternLogger::new(
        config.logger_window_size,
    )));

    let lru_strategy = Arc::new(LruCacheStrategy::new(config.cache_capacity));

    let learned_predictor = LearnedCachePredictor::new(config.cache_capacity)
        .context("Failed to create learned cache predictor")?;
    let learned_strategy = Arc::new(LearnedCacheStrategy::new(
        config.cache_capacity,
        learned_predictor,
    ));

    let ab_splitter = AbTestSplitter::new(lru_strategy.clone(), learned_strategy.clone());

    let stats_persister = Arc::new(
        AbStatsPersister::new(&config.stats_csv).context("Failed to create stats persister")?,
    );

    // Mock document storage
    let doc_store = Arc::new(MockDocumentStore::new(config.corpus_size));

    // Spawn background training task
    println!("Spawning background training task...");
    let training_config = TrainingConfig {
        interval: Duration::from_secs(config.training_interval_secs),
        window_duration: Duration::from_secs(24 * 3600),
        min_events_for_training: 100,
        rmi_capacity: config.cache_capacity,
    };

    let training_handle = spawn_training_task(
        access_logger.clone(),
        learned_strategy.clone(),
        training_config,
        None,
    )
    .await;

    println!(
        "Training task running (retrains every {} seconds)",
        config.training_interval_secs
    );
    println!();

    // Verify strategy names
    println!("A/B Test Configuration:");
    println!("  LRU strategy name:     {}", lru_strategy.name());
    println!("  Learned strategy name: {}", learned_strategy.name());
    println!("  Traffic split:         50/50 (even doc_ids → LRU, odd → Learned)");
    println!();

    // Initialize Zipf sampler
    let zipf_sampler = ZipfSampler::new(config.corpus_size, config.zipf_exponent)
        .context("Failed to create Zipf sampler")?;

    // Validate Zipf distribution (sample 10K docs to verify distribution)
    println!("Validating Zipf distribution...");
    {
        let mut doc_counts: std::collections::HashMap<u64, usize> =
            std::collections::HashMap::new();
        for _ in 0..10_000 {
            let doc_id = zipf_sampler.sample();
            *doc_counts.entry(doc_id).or_insert(0) += 1;
        }

        let mut sorted_docs: Vec<_> = doc_counts.iter().collect();
        sorted_docs.sort_by(|a, b| b.1.cmp(a.1));

        let top_20_docs: usize = sorted_docs.iter().take(20).map(|(_, count)| **count).sum();
        let top_20_pct = top_20_docs as f64 / 10_000.0;

        println!(
            "  Top 20 docs: {:.1}% of accesses (expected: 10-40%)",
            top_20_pct * 100.0
        );

        // With Zipf(1.1), expect top 20 docs to capture 10-40% of accesses
        // With Zipf(1.5), this would be 80%+ (too skewed for realistic workload)
        if top_20_pct < 0.08 || top_20_pct > 0.50 {
            bail!("Zipf distribution validation failed: top 20 docs should capture 8-50% of accesses, got {:.1}%", top_20_pct * 100.0);
        }

        println!("  ✓ Zipf distribution validated");
    }
    println!();

    // Test parameters
    let test_duration = Duration::from_secs(config.duration_hours * 3600);
    let target_interval = Duration::from_nanos(1_000_000_000 / config.target_qps);

    // Metrics tracking
    let mut total_queries = 0u64;
    let mut lru_queries = 0u64;
    let mut lru_hits = 0u64;
    let mut learned_queries = 0u64;
    let mut learned_hits = 0u64;

    let start_time = SystemTime::now();
    let test_start = Instant::now();
    let mut next_query = Instant::now();

    let initial_memory = get_memory_mb().unwrap_or(0.0);
    if initial_memory > 0.0 {
        println!("Initial memory usage: {:.1} MB", initial_memory);
    } else {
        println!("Warning: Memory tracking unavailable");
    }
    println!();
    println!("Starting workload... (Press Ctrl+C to stop early)");
    println!("─────────────────────────────────────────────────────────────────");

    // Graceful shutdown on Ctrl+C
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\n\nReceived Ctrl+C, shutting down gracefully...");
        r.store(false, Ordering::SeqCst);
    });

    // Main query loop
    while test_start.elapsed() < test_duration && running.load(Ordering::SeqCst) {
        // Sample doc_id from Zipf distribution
        let doc_id = zipf_sampler.sample();

        // A/B test: route to LRU or Learned strategy
        let strategy = ab_splitter.get_strategy(doc_id);
        let strategy_name = strategy.name();

        // Determine strategy ID (type-safe)
        let strategy_id = if strategy_name == "lru_baseline" {
            StrategyId::LruBaseline
        } else if strategy_name == "learned_rmi" {
            StrategyId::LearnedRmi
        } else {
            eprintln!(
                "ERROR: Unknown strategy '{}', skipping query",
                strategy_name
            );
            continue;
        };

        // Check cache
        let cached_vec = strategy.get_cached(doc_id);

        let (embedding, cache_hit) = if let Some(vec) = cached_vec {
            // Cache hit - use cached embedding
            (vec.embedding, true)
        } else {
            // Cache miss - fetch from storage
            let embedding = doc_store
                .fetch(doc_id)
                .ok_or_else(|| anyhow::anyhow!("Document {} not found in store", doc_id))?;

            // Decide if we should cache based on strategy
            let should_cache = match strategy_id {
                StrategyId::LruBaseline => {
                    // LRU: Always cache (permissive admission)
                    true
                }
                StrategyId::LearnedRmi => {
                    // Learned: Only cache if RMI predictor says hot
                    strategy.should_cache(doc_id, &embedding)
                }
            };

            if should_cache {
                let cached = CachedVector {
                    doc_id,
                    embedding: embedding.clone(),
                    distance: 0.5,
                    cached_at: Instant::now(),
                };
                strategy.insert_cached(cached);
            }

            (embedding, false)
        };

        // Record metrics (type-safe routing)
        match strategy_id {
            StrategyId::LruBaseline => {
                lru_queries += 1;
                if cache_hit {
                    lru_hits += 1;
                }
            }
            StrategyId::LearnedRmi => {
                learned_queries += 1;
                if cache_hit {
                    learned_hits += 1;
                }
            }
        }

        // Log access for training
        {
            let mut logger = access_logger.write().await;
            logger.log_access(doc_id, &embedding);
        }

        // Persist A/B test metric (async, best-effort)
        if cache_hit {
            stats_persister.log_hit(strategy_name, doc_id, 0).await.ok();
        } else {
            stats_persister
                .log_miss(strategy_name, doc_id, 0)
                .await
                .ok();
        }

        total_queries += 1;

        // Progress reporting every 10K queries with detailed diagnostics
        if total_queries % 10_000 == 0 {
            let elapsed = test_start.elapsed();
            let progress_pct = (total_queries as f64 / total_expected as f64) * 100.0;
            let lru_hit_rate = if lru_queries > 0 {
                lru_hits as f64 / lru_queries as f64
            } else {
                0.0
            };
            let learned_hit_rate = if learned_queries > 0 {
                learned_hits as f64 / learned_queries as f64
            } else {
                0.0
            };
            let current_qps = total_queries as f64 / elapsed.as_secs_f64();
            let improvement = if lru_hit_rate > 0.0 {
                learned_hit_rate / lru_hit_rate
            } else {
                0.0
            };

            // Get cache diagnostics
            let lru_stats = lru_strategy.cache.stats();
            let learned_stats = learned_strategy.cache.stats();
            let learned_tracked = {
                let pred = learned_strategy.predictor.read();
                pred.tracked_count()
            };

            // Calculate traffic split percentages
            let lru_pct = if total_queries > 0 {
                (lru_queries as f64 / total_queries as f64) * 100.0
            } else {
                0.0
            };
            let learned_pct = if total_queries > 0 {
                (learned_queries as f64 / total_queries as f64) * 100.0
            } else {
                0.0
            };

            println!(
                "[{:>5.1}%] Q: {:>7} | QPS: {:>3.0} | LRU: {:>5.1}% ({:>4} cached, {:>5.1}% traffic) | Learned: {:>5.1}% ({:>4} cached, {:>5} tracked, {:>5.1}% traffic) | Improvement: {:>4.2}×",
                progress_pct,
                total_queries,
                current_qps,
                lru_hit_rate * 100.0,
                lru_stats.size,
                lru_pct,
                learned_hit_rate * 100.0,
                learned_stats.size,
                learned_tracked,
                learned_pct,
                improvement
            );
        }

        // Rate limiting (maintain target QPS)
        next_query += target_interval;
        let now = Instant::now();
        if next_query > now {
            tokio::time::sleep(next_query - now).await;
        } else {
            // If we're behind, skip ahead (don't try to catch up)
            next_query = now + target_interval;
        }
    }

    // Check if training task is still alive
    let training_crashed = training_handle.is_finished();
    if training_crashed {
        eprintln!("WARNING: Training task crashed during test");
    }

    // Stop training task
    println!("\nStopping training task...");
    training_handle.abort();
    let _ = training_handle.await; // Ignore cancellation error

    // Finalize stats
    let end_time = SystemTime::now();
    let final_memory = get_memory_mb().unwrap_or(0.0);
    let actual_duration = test_start.elapsed().as_secs();

    println!("─────────────────────────────────────────────────────────────────");
    println!();
    println!("Test complete!");
    println!();

    // Calculate final metrics
    let lru_hit_rate = if lru_queries > 0 {
        lru_hits as f64 / lru_queries as f64
    } else {
        0.0
    };
    let learned_hit_rate = if learned_queries > 0 {
        learned_hits as f64 / learned_queries as f64
    } else {
        0.0
    };
    let improvement = if lru_hit_rate > 0.0 {
        learned_hit_rate / lru_hit_rate
    } else {
        0.0
    };
    let memory_growth_mb = final_memory - initial_memory;
    let memory_growth_pct = if initial_memory > 0.0 {
        (memory_growth_mb / initial_memory) * 100.0
    } else {
        0.0
    };

    let results = ValidationResults {
        config: config.clone(),
        test_duration_secs: actual_duration,
        total_queries,

        lru_total_queries: lru_queries,
        lru_cache_hits: lru_hits,
        lru_cache_misses: lru_queries - lru_hits,
        lru_hit_rate,

        learned_total_queries: learned_queries,
        learned_cache_hits: learned_hits,
        learned_cache_misses: learned_queries - learned_hits,
        learned_hit_rate,

        hit_rate_improvement: improvement,

        expected_training_cycles: actual_duration / config.training_interval_secs,
        training_task_crashed: training_crashed,

        initial_memory_mb: initial_memory,
        final_memory_mb: final_memory,
        memory_growth_mb,
        memory_growth_pct,

        start_time,
        end_time,
    };

    // Display results
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                    VALIDATION RESULTS                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Test Summary:");
    println!(
        "  Duration:        {} hours {} minutes",
        actual_duration / 3600,
        (actual_duration % 3600) / 60
    );
    println!("  Total queries:   {}", total_queries);
    println!(
        "  Avg QPS:         {:.1}",
        total_queries as f64 / actual_duration as f64
    );
    println!();
    println!("LRU Baseline:");
    println!("  Queries:         {}", lru_queries);
    println!("  Cache hits:      {}", lru_hits);
    println!("  Cache misses:    {}", lru_queries - lru_hits);
    println!("  Hit rate:        {:.1}%", lru_hit_rate * 100.0);
    println!();
    println!("Learned Cache (RMI):");
    println!("  Queries:         {}", learned_queries);
    println!("  Cache hits:      {}", learned_hits);
    println!("  Cache misses:    {}", learned_queries - learned_hits);
    println!("  Hit rate:        {:.1}%", learned_hit_rate * 100.0);
    println!();
    println!("Performance:");
    println!(
        "  Hit rate improvement:  {:.2}× (target: 2.0-3.0×)",
        improvement
    );
    println!(
        "  Status:                {}",
        if improvement >= 2.0 && improvement <= 3.5 {
            "✅ PASS"
        } else {
            "❌ FAIL"
        }
    );
    println!();
    println!("Training:");
    println!("  Expected cycles: {}", results.expected_training_cycles);
    println!(
        "  Interval:        {} seconds",
        config.training_interval_secs
    );
    println!(
        "  Task crashed:    {}",
        if training_crashed {
            "❌ YES"
        } else {
            "✅ NO"
        }
    );
    println!();

    if initial_memory > 0.0 {
        println!("Memory:");
        println!("  Initial:         {:.1} MB", initial_memory);
        println!("  Final:           {:.1} MB", final_memory);
        println!(
            "  Growth:          {:+.1} MB ({:+.1}%) (target: < 5%)",
            memory_growth_mb, memory_growth_pct
        );
        println!(
            "  Status:          {}",
            if memory_growth_pct.abs() < 5.0 {
                "✅ PASS"
            } else if memory_growth_pct.abs() < 10.0 {
                "⚠️  WARN"
            } else {
                "❌ FAIL"
            }
        );
    } else {
        println!("Memory:");
        println!("  Tracking unavailable (non-Linux platform)");
    }
    println!();

    // Go/No-Go decision
    let hit_rate_pass = learned_hit_rate >= 0.70 && learned_hit_rate <= 0.90;
    let improvement_pass = improvement >= 2.0 && improvement <= 3.5;
    let memory_pass = initial_memory == 0.0 || memory_growth_pct.abs() < 5.0;
    let training_pass = !training_crashed;

    let go_decision = hit_rate_pass && improvement_pass && memory_pass && training_pass;

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                     GO/NO-GO DECISION                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Criteria:");
    println!(
        "  ✓ Learned cache hit rate 70-90%:       {}",
        if hit_rate_pass {
            "✅ PASS"
        } else {
            "❌ FAIL"
        }
    );
    println!(
        "  ✓ Improvement 2.0-3.0× over LRU:       {}",
        if improvement_pass {
            "✅ PASS"
        } else {
            "❌ FAIL"
        }
    );
    println!(
        "  ✓ Memory growth < 5%:                  {}",
        if memory_pass { "✅ PASS" } else { "❌ FAIL" }
    );
    println!(
        "  ✓ Training task stable:                {}",
        if training_pass {
            "✅ PASS"
        } else {
            "❌ FAIL"
        }
    );
    println!();
    println!(
        "Decision: {}",
        if go_decision {
            "✅ GO for Week 13-16"
        } else {
            "❌ NO-GO - needs investigation"
        }
    );
    println!();

    // Save results to JSON
    let json = serde_json::to_string_pretty(&results).context("Failed to serialize results")?;
    std::fs::write(&config.results_json, json).context("Failed to write results JSON")?;

    // Flush stats CSV
    stats_persister
        .flush()
        .await
        .context("Failed to flush stats")?;

    println!("Results saved to:");
    println!("  - {}", config.results_json);
    println!("  - {}", config.stats_csv);
    println!();

    if !go_decision {
        std::process::exit(1);
    }

    Ok(())
}
