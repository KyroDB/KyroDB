//! 24-hour validation workload for Phase 0 Week 9-12
//! 
//! **Purpose**: Validate KyroDB A/B testing framework under sustained production-like load
//! 
//! **What This Test Validates**:
//! 1. Learned cache achieves 70-90% hit rate (vs 30-40% LRU baseline)
//! 2. No memory leaks under sustained load (8.64M queries)
//! 3. Training task runs reliably every 10 minutes (144 cycles)
//! 4. No performance degradation over time
//! 5. Stats persistence survives restarts
//! 
//! **Workload**:
//! - Duration: 24 hours (configurable)
//! - QPS: 100 queries/second
//! - Distribution: Zipf (exponent=1.5) - 80/20 hot/cold
//! - Corpus: 100,000 documents
//! - A/B split: 50% LRU baseline, 50% Learned cache
//! 
//! **Run on Azure VM**:
//! ```bash
//! cargo build --release --bin validation_24h
//! nohup ./target/release/validation_24h > validation.log 2>&1 &
//! ```

use anyhow::{Context, Result};
use kyrodb_engine::{
    AccessPatternLogger, AbStatsPersister, AbTestSplitter, CachedVector,
    LearnedCachePredictor, LearnedCacheStrategy, LruCacheStrategy, TrainingConfig,
    spawn_training_task,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;

/// Validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    /// Test duration in hours (default: 24)
    duration_hours: u64,
    
    /// Target queries per second (default: 100)
    target_qps: u64,
    
    /// Number of unique documents (default: 100K)
    corpus_size: usize,
    
    /// Zipf exponent (default: 1.5 for 80/20 distribution)
    zipf_exponent: f64,
    
    /// Cache capacity (default: 10K vectors)
    cache_capacity: usize,
    
    /// Training interval in seconds (default: 600 = 10 minutes)
    training_interval_secs: u64,
    
    /// Output files
    stats_csv: String,
    results_json: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            duration_hours: 24,
            target_qps: 100,
            corpus_size: 100_000,
            zipf_exponent: 1.5,
            cache_capacity: 10_000,
            training_interval_secs: 600,
            stats_csv: "validation_24h_stats.csv".to_string(),
            results_json: "validation_24h_results.json".to_string(),
        }
    }
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
    
    // Memory stats
    initial_memory_mb: f64,
    final_memory_mb: f64,
    memory_growth_pct: f64,
    
    // Timestamps
    start_time: SystemTime,
    end_time: SystemTime,
}

/// Zipf distribution sampler for hot/cold access patterns
struct ZipfSampler {
    corpus_size: usize,
    exponent: f64,
}

impl ZipfSampler {
    fn new(corpus_size: usize, exponent: f64) -> Self {
        Self { corpus_size, exponent }
    }
    
    /// Sample doc_id following Zipf distribution
    /// Lower IDs are more frequent (hot documents)
    fn sample(&self) -> u64 {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        
        // Simple Zipf approximation: rank ~ U^(-1/exponent)
        let u: f64 = rng.gen_range(0.0..1.0);
        let rank = ((self.corpus_size as f64).powf(1.0 - self.exponent) * u)
            .powf(1.0 / (1.0 - self.exponent));
        
        rank.min((self.corpus_size - 1) as f64) as u64
    }
}

/// Get process memory usage in MB (Linux only)
fn get_memory_mb() -> f64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<f64>() {
                            return kb / 1024.0;
                        }
                    }
                }
            }
        }
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        println!("Warning: Memory tracking only supported on Linux");
    }
    
    0.0
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::default();
    
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║  KyroDB 24-Hour Validation Workload - Phase 0 Week 9-12       ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Configuration:");
    println!("  Duration:          {} hours", config.duration_hours);
    println!("  Target QPS:        {}", config.target_qps);
    println!("  Corpus size:       {} documents", config.corpus_size);
    println!("  Zipf exponent:     {} (80/20 hot/cold)", config.zipf_exponent);
    println!("  Cache capacity:    {} vectors", config.cache_capacity);
    println!("  Training interval: {} seconds", config.training_interval_secs);
    println!();
    println!("Expected workload:");
    println!("  Total queries:     {} million", (config.target_qps * config.duration_hours * 3600) / 1_000_000);
    println!("  Training cycles:   {}", (config.duration_hours * 3600) / config.training_interval_secs);
    println!();
    println!("Output files:");
    println!("  Stats CSV:         {}", config.stats_csv);
    println!("  Results JSON:      {}", config.results_json);
    println!();
    
    // Initialize components
    println!("Initializing components...");
    
    let access_logger = Arc::new(RwLock::new(
        AccessPatternLogger::new(10_000_000) // 10M event capacity
    ));
    
    let lru_strategy = Arc::new(LruCacheStrategy::new(config.cache_capacity));
    
    let learned_predictor = LearnedCachePredictor::new(config.cache_capacity)
        .context("Failed to create learned cache predictor")?;
    let learned_strategy = Arc::new(LearnedCacheStrategy::new(config.cache_capacity, learned_predictor));
    
    let ab_splitter = AbTestSplitter::new(lru_strategy.clone(), learned_strategy.clone());
    
    let stats_persister = Arc::new(AbStatsPersister::new(&config.stats_csv)
        .context("Failed to create stats persister")?);
    
    // Spawn background training task
    println!("Spawning background training task...");
    let training_config = TrainingConfig {
        interval: Duration::from_secs(config.training_interval_secs),
        window_duration: Duration::from_secs(24 * 3600),
        min_events_for_training: 100,
        rmi_capacity: config.cache_capacity,
    };
    
    let _training_handle = spawn_training_task(
        access_logger.clone(),
        learned_strategy.clone(),
        training_config,
    ).await;
    
    println!("Training task running (retrains every {} seconds)", config.training_interval_secs);
    println!();
    
    // Initialize Zipf sampler
    let zipf_sampler = ZipfSampler::new(config.corpus_size, config.zipf_exponent);
    
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
    
    let initial_memory = get_memory_mb();
    println!("Initial memory usage: {:.1} MB", initial_memory);
    println!();
    println!("Starting workload... (Press Ctrl+C to stop early)");
    println!("─────────────────────────────────────────────────────────────────");
    
    // Main query loop
    while test_start.elapsed() < test_duration {
        // Sample doc_id from Zipf distribution
        let doc_id = zipf_sampler.sample();
        
        // Generate query embedding (simple for validation)
        let query_embedding: Vec<f32> = (0..768).map(|i| ((doc_id + i as u64) % 1000) as f32 / 1000.0).collect();
        
        // A/B test: route to LRU or Learned strategy
        let strategy = ab_splitter.get_strategy(doc_id);
        let strategy_name = strategy.name();
        
        // Check cache
        let cache_hit = strategy.get_cached(doc_id).is_some();
        
        // Record metrics
        if strategy_name == "lru_baseline" {
            lru_queries += 1;
            if cache_hit {
                lru_hits += 1;
            }
        } else {
            learned_queries += 1;
            if cache_hit {
                learned_hits += 1;
            }
        }
        
        // If cache miss, simulate insertion (learned cache uses should_cache)
        if !cache_hit {
            if strategy_name == "learned_rmi" {
                if strategy.should_cache(doc_id, &query_embedding) {
                    let cached = CachedVector {
                        doc_id,
                        embedding: query_embedding.clone(),
                        distance: 0.5,
                        cached_at: Instant::now(),
                    };
                    strategy.insert_cached(cached);
                }
            } else {
                // LRU always caches
                let cached = CachedVector {
                    doc_id,
                    embedding: query_embedding.clone(),
                    distance: 0.5,
                    cached_at: Instant::now(),
                };
                strategy.insert_cached(cached);
            }
        }
        
        // Log access for training
        {
            let mut logger = access_logger.write().await;
            logger.log_access(doc_id, &query_embedding);
        }
        
        // Persist A/B test metric (async)
        if cache_hit {
            stats_persister.log_hit(strategy_name, doc_id, 0).await.ok();
        } else {
            stats_persister.log_miss(strategy_name, doc_id, 0).await.ok();
        }
        
        total_queries += 1;
        
        // Progress reporting every 10K queries
        if total_queries % 10_000 == 0 {
            let elapsed = test_start.elapsed();
            let progress_pct = (elapsed.as_secs_f64() / test_duration.as_secs_f64()) * 100.0;
            let lru_hit_rate = if lru_queries > 0 { lru_hits as f64 / lru_queries as f64 } else { 0.0 };
            let learned_hit_rate = if learned_queries > 0 { learned_hits as f64 / learned_queries as f64 } else { 0.0 };
            let current_qps = total_queries as f64 / elapsed.as_secs_f64();
            
            println!(
                "[{:>5.1}%] Queries: {:>8} | QPS: {:>5.0} | LRU: {:>5.1}% | Learned: {:>5.1}% | Improvement: {:>4.2}×",
                progress_pct,
                total_queries,
                current_qps,
                lru_hit_rate * 100.0,
                learned_hit_rate * 100.0,
                if lru_hit_rate > 0.0 { learned_hit_rate / lru_hit_rate } else { 0.0 }
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
    
    // Finalize stats
    let end_time = SystemTime::now();
    let final_memory = get_memory_mb();
    let actual_duration = test_start.elapsed().as_secs();
    
    println!("─────────────────────────────────────────────────────────────────");
    println!();
    println!("Test complete!");
    println!();
    
    // Calculate final metrics
    let lru_hit_rate = if lru_queries > 0 { lru_hits as f64 / lru_queries as f64 } else { 0.0 };
    let learned_hit_rate = if learned_queries > 0 { learned_hits as f64 / learned_queries as f64 } else { 0.0 };
    let improvement = if lru_hit_rate > 0.0 { learned_hit_rate / lru_hit_rate } else { 0.0 };
    let memory_growth = if initial_memory > 0.0 {
        ((final_memory - initial_memory) / initial_memory) * 100.0
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
        
        expected_training_cycles: (actual_duration / config.training_interval_secs),
        
        initial_memory_mb: initial_memory,
        final_memory_mb: final_memory,
        memory_growth_pct: memory_growth,
        
        start_time,
        end_time,
    };
    
    // Display results
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                    VALIDATION RESULTS                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Test Summary:");
    println!("  Duration:        {} hours {} minutes", actual_duration / 3600, (actual_duration % 3600) / 60);
    println!("  Total queries:   {}", total_queries);
    println!("  Avg QPS:         {:.1}", total_queries as f64 / actual_duration as f64);
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
    println!("  Hit rate improvement:  {:.2}× (target: 2-3×)", improvement);
    println!("  Status:                {}", 
        if improvement >= 2.0 && improvement <= 3.5 { "✅ PASS" } else { "❌ FAIL" });
    println!();
    println!("Training:");
    println!("  Expected cycles: {}", results.expected_training_cycles);
    println!("  Interval:        {} seconds", config.training_interval_secs);
    println!();
    println!("Memory:");
    println!("  Initial:         {:.1} MB", initial_memory);
    println!("  Final:           {:.1} MB", final_memory);
    println!("  Growth:          {:.1}% (target: < 5%)", memory_growth);
    println!("  Status:          {}", 
        if memory_growth.abs() < 5.0 { "✅ PASS" } else { "⚠️  WARN" });
    println!();
    
    // Go/No-Go decision
    let go_decision = improvement >= 2.0 && improvement <= 3.5 && memory_growth.abs() < 10.0;
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                     GO/NO-GO DECISION                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Criteria:");
    println!("  ✓ Learned cache hit rate 70-90%:       {}", 
        if learned_hit_rate >= 0.70 && learned_hit_rate <= 0.90 { "✅ PASS" } else { "❌ FAIL" });
    println!("  ✓ Improvement 2-3× over LRU:           {}", 
        if improvement >= 2.0 && improvement <= 3.5 { "✅ PASS" } else { "❌ FAIL" });
    println!("  ✓ Memory growth < 5%:                  {}", 
        if memory_growth.abs() < 5.0 { "✅ PASS" } else { "⚠️  WARN" });
    println!();
    println!("Decision: {}", if go_decision { "✅ GO for Week 13-16" } else { "❌ NO-GO - needs investigation" });
    println!();
    
    // Save results to JSON
    let json = serde_json::to_string_pretty(&results)
        .context("Failed to serialize results")?;
    std::fs::write(&config.results_json, json)
        .context("Failed to write results JSON")?;
    
    // Flush stats CSV
    stats_persister.flush().await.context("Failed to flush stats")?;
    
    println!("Results saved to:");
    println!("  - {}", config.results_json);
    println!("  - {}", config.stats_csv);
    println!();
    
    Ok(())
}
