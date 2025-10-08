//! Enterprise-scale validation workload
//!
//! **Purpose**: Validate KyroDB hybrid semantic-learned cache against REALISTIC production RAG workloads
//!
//! **What This Test Validates**:
//! 1. Hybrid cache achieves 55-70% hit rate vs LRU 35-45% (1.5-2× improvement)
//! 2. No memory leaks under sustained load (4.32M queries)
//! 3. Training task runs reliably every 10 minutes (72 cycles)
//! 4. Handles temporal patterns (topic shifts, spikes) + 60% cold traffic
//! 5. Stats persistence survives restarts
//!
//! **Realistic Workload**:
//! - Corpus: 1M documents (enterprise-scale)
//! - Cache: 10K vectors (1% of corpus - industry standard)
//! - Duration: 12 hours
//! - QPS: 100 queries/second
//! - Distribution: Zipf 1.01 (real-world web traffic)
//! - Temporal patterns: Topic rotation + random spikes
//! - A/B split: 50% LRU baseline, 50% Learned cache
//!
//! **Run on Azure VM**:
//! ```
//! cargo build --release --bin validation_enterprise
//! nohup ./target/release/validation_enterprise > validation.log 2>&1 &
//! tail -f validation.log
//! ```

use anyhow::{bail, Context, Result};
use kyrodb_engine::{
    ab_stats::AbStatsPersister,
    access_logger::AccessPatternLogger,
    cache_strategy::{AbTestSplitter, LearnedCacheStrategy, LruCacheStrategy},
    learned_cache::LearnedCachePredictor,
    semantic_adapter::SemanticAdapter,
    training_task::{spawn_training_task, TrainingConfig},
    vector_cache::CachedVector,
};
use rand::{distributions::Distribution, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::Normal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::{fs, sync::RwLock};
use zipf::ZipfDistribution;

/// Enterprise validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    /// Test duration in hours (default: 12)
    duration_hours: u64,

    /// Target queries per second (default: 100)
    target_qps: u64,

    /// Number of unique documents (enterprise-scale)
    corpus_size: usize,

    /// Zipf exponent (1.01 = real-world web traffic)
    zipf_exponent: f64,

    /// Cache capacity (1% of corpus - industry standard)
    cache_capacity: usize,

    /// Training interval in seconds (default: 600 = 10 minutes)
    training_interval_secs: u64,

    /// Access logger window size (bounded circular buffer)
    logger_window_size: usize,

    /// Enable temporal patterns (topic shifts, spikes)
    enable_temporal_patterns: bool,

    /// Topic rotation interval in seconds (default: 2 hours)
    topic_rotation_interval_secs: u64,

    /// Spike probability (0.001 = 0.1% chance per query)
    spike_probability: f64,

    /// Spike duration in seconds (default: 5 minutes)
    spike_duration_secs: u64,

    /// Ratio of queries that target cold, one-off documents (uniform)
    cold_traffic_ratio: f64,

    /// Bias for sampling from rolling working set (post cold-traffic)
    working_set_bias: f64,

    /// Rolling working-set size multiplier relative to cache capacity
    working_set_multiplier: f64,

    /// Probability of rotating an item in the working set each query
    working_set_churn: f64,

    /// Output files
    stats_csv: String,
    results_json: String,

    /// Path to MS MARCO embeddings (optional, falls back to mock)
    /// Phase 0.5.1: Support real embeddings for semantic validation
    #[serde(default)]
    ms_marco_embeddings_path: Option<String>,

    /// Path to MS MARCO passages (optional)
    #[serde(default)]
    ms_marco_passages_path: Option<String>,

    /// Path to query embeddings (optional)
    /// Phase 0.5.2: Support semantic query generation for realistic RAG workloads
    #[serde(default)]
    query_embeddings_path: Option<String>,

    /// Path to query→doc mapping (optional)
    #[serde(default)]
    query_to_doc_path: Option<String>,

    /// Top-K queries per document for semantic sampling (default: 10)
    #[serde(default = "default_top_k_queries")]
    top_k_queries_per_doc: usize,
}

fn default_top_k_queries() -> usize {
    10
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Enterprise-scale corpus
            corpus_size: 1_000_000,

            // 1% cache size (industry standard)
            cache_capacity: 10_000,

            // Phase 0.5 Fix: Realistic Zipf exponent (1.4)
            // Models real RAG query distribution (moderate skew)
            // Reduces artificial LRU advantage from concentrated access
            zipf_exponent: 1.4,

            // Test parameters
            duration_hours: 12,
            target_qps: 100,
            training_interval_secs: 600,

            // Phase 0.5 Fix: Increased logger window (100K from 20K)
            // Captures longer-term patterns for learned cache training
            // Memory is not a concern (validated in Phase 0 Week 5-8)
            logger_window_size: 100_000,

            // Temporal patterns (realistic production)
            enable_temporal_patterns: true,
            topic_rotation_interval_secs: 7200, // 2 hours
            spike_probability: 0.001,           // 0.1% per query
            spike_duration_secs: 300,           // 5 minutes

            cold_traffic_ratio: 0.6,
            working_set_bias: 0.2,
            working_set_multiplier: 3.5,
            working_set_churn: 0.08,

            stats_csv: "validation_enterprise.csv".to_string(),
            results_json: "validation_enterprise.json".to_string(),

            // Phase 0.5.1: MS MARCO dataset paths (optional)
            ms_marco_embeddings_path: None,
            ms_marco_passages_path: None,

            // Phase 0.5.2: Query embeddings for semantic workload
            query_embeddings_path: None,
            query_to_doc_path: None,
            top_k_queries_per_doc: 10,
        }
    }
}

impl Config {
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
        if self.spike_probability < 0.0 || self.spike_probability > 1.0 {
            bail!("spike_probability must be in [0, 1]");
        }
        if self.cold_traffic_ratio < 0.0 || self.cold_traffic_ratio > 1.0 {
            bail!("cold_traffic_ratio must be in [0, 1]");
        }
        if self.working_set_bias < 0.0 || self.working_set_bias > 1.0 {
            bail!("working_set_bias must be in [0, 1]");
        }
        if self.working_set_multiplier < 1.0 {
            bail!("working_set_multiplier must be >= 1.0");
        }
        if self.working_set_churn <= 0.0 || self.working_set_churn > 1.0 {
            bail!("working_set_churn must be in (0, 1]");
        }
        Ok(())
    }

    /// Calculate expected working set size (docs that account for 95% of queries)
    fn expected_working_set_size(&self) -> usize {
        let n = self.corpus_size as f64;
        let exponent = 1.0 / self.zipf_exponent;
        let zipf_estimate = (n.powf(exponent) * 0.15) as usize;
        let rolling_estimate =
            (self.cache_capacity as f64 * self.working_set_multiplier).round() as usize;
        zipf_estimate.max(rolling_estimate).min(self.corpus_size)
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
    absolute_improvement: f64,

    // Training stats
    expected_training_cycles: u64,
    actual_training_cycles: u64,
    training_task_crashed: bool,

    // Temporal pattern stats
    topic_rotations: u64,
    spike_events: u64,

    // Memory stats
    initial_memory_mb: f64,
    final_memory_mb: f64,
    memory_growth_mb: f64,
    memory_growth_pct: f64,

    // Timestamps
    start_time: SystemTime,
    end_time: SystemTime,

    // Working set analysis
    expected_working_set_size: usize,
    cache_to_working_set_ratio: f64,

    // Workload mix stats
    cold_queries: u64,
    working_set_draws: u64,
}

/// Realistic temporal workload generator
/// Simulates: Zipfian base + topic shifts + random spikes
struct TemporalWorkloadGenerator {
    base_sampler: ZipfSampler,
    corpus_size: usize,
    working_set: Arc<RwLock<VecDeque<u64>>>,
    working_set_size: usize,
    working_set_churn: f64,
    working_set_bias: f64,
    cold_traffic_ratio: f64,
    cold_query_count: Arc<AtomicU64>,
    working_set_draws: Arc<AtomicU64>,

    // Topic rotation (hot documents shift every 2 hours)
    topic_rotation_interval: Duration,
    current_topic_offset: Arc<AtomicU64>,
    last_rotation: Arc<RwLock<Instant>>,
    rotation_count: Arc<AtomicU64>,

    // Random spikes (breaking news, product launches)
    spike_probability: f64,
    spike_duration: Duration,
    current_spike: Arc<RwLock<Option<SpikeEvent>>>,
    spike_count: Arc<AtomicU64>,

    enabled: bool,
}

#[derive(Debug, Clone)]
struct SpikeEvent {
    hot_doc_id: u64,
    started_at: Instant,
}

impl TemporalWorkloadGenerator {
    fn new(
        corpus_size: usize,
        zipf_exponent: f64,
        topic_rotation_interval: Duration,
        spike_probability: f64,
        spike_duration: Duration,
        enabled: bool,
        cache_capacity: usize,
        working_set_multiplier: f64,
        working_set_churn: f64,
        cold_traffic_ratio: f64,
        working_set_bias: f64,
    ) -> Result<Self> {
        let base_sampler = ZipfSampler::new(corpus_size, zipf_exponent)?;
        let working_set_size = (((cache_capacity as f64) * working_set_multiplier).ceil() as usize)
            .max(cache_capacity.max(1))
            .min(corpus_size.max(1));

        let mut rng = rand::thread_rng();
        let mut initial_set = VecDeque::with_capacity(working_set_size);
        for _ in 0..working_set_size {
            let doc = rng.gen_range(0..corpus_size) as u64;
            initial_set.push_back(doc);
        }

        Ok(Self {
            base_sampler,
            corpus_size,
            working_set: Arc::new(RwLock::new(initial_set)),
            working_set_size,
            working_set_churn,
            working_set_bias,
            cold_traffic_ratio,
            cold_query_count: Arc::new(AtomicU64::new(0)),
            working_set_draws: Arc::new(AtomicU64::new(0)),
            topic_rotation_interval,
            current_topic_offset: Arc::new(AtomicU64::new(0)),
            last_rotation: Arc::new(RwLock::new(Instant::now())),
            rotation_count: Arc::new(AtomicU64::new(0)),
            spike_probability,
            spike_duration,
            current_spike: Arc::new(RwLock::new(None)),
            spike_count: Arc::new(AtomicU64::new(0)),
            enabled,
        })
    }

    async fn sample(&self) -> u64 {
        if !self.enabled {
            return self.base_sampler.sample();
        }

        let mut rng = rand::thread_rng();

        // Phase 0.5 Fix: Check cold traffic FIRST (before spikes/rotations)
        // Bug: cold check after spike meant 50% spike queries skipped cold logic
        // Result: actual cold traffic was ~10% instead of configured 20%
        if rng.gen::<f64>() < self.cold_traffic_ratio {
            let doc = rng.gen_range(0..self.corpus_size) as u64;
            self.cold_query_count.fetch_add(1, Ordering::Relaxed);
            return doc;
        }

        // Check for active spike event (50% of queries hit spike during event)
        {
            let spike = self.current_spike.read().await;
            if let Some(event) = spike.as_ref() {
                if event.started_at.elapsed() < self.spike_duration {
                    if rng.gen::<f64>() < 0.5 {
                        return event.hot_doc_id;
                    }
                }
            }
        }

        // Trigger new spike (0.1% chance per query)
        if rng.gen::<f64>() < self.spike_probability {
            let spike_doc = rng.gen_range(0..self.corpus_size) as u64;
            let mut spike = self.current_spike.write().await;
            *spike = Some(SpikeEvent {
                hot_doc_id: spike_doc,
                started_at: Instant::now(),
            });
            self.spike_count.fetch_add(1, Ordering::Relaxed);
            return spike_doc;
        }

        // Check for topic rotation (every 2 hours)
        {
            let last_rotation = self.last_rotation.read().await;
            if last_rotation.elapsed() > self.topic_rotation_interval {
                drop(last_rotation);
                let mut last_rotation = self.last_rotation.write().await;
                if last_rotation.elapsed() > self.topic_rotation_interval {
                    *last_rotation = Instant::now();

                    // Shift hot topic window (20% of corpus)
                    let shift = rng.gen_range(0..self.corpus_size / 5);
                    self.current_topic_offset
                        .store(shift as u64, Ordering::Relaxed);
                    self.rotation_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Normal Zipfian sample with topic offset
        let base_sample = self.base_sampler.sample();
        let offset = self.current_topic_offset.load(Ordering::Relaxed);
        let candidate = (base_sample + offset) % self.corpus_size as u64;

        let sampled = {
            let mut working_set = self.working_set.write().await;

            if working_set.len() < self.working_set_size {
                working_set.push_back(candidate);
            } else if rng.gen::<f64>() < self.working_set_churn {
                working_set.pop_front();
                working_set.push_back(candidate);
            }

            if !working_set.is_empty() && rng.gen::<f64>() < self.working_set_bias {
                let idx = rng.gen_range(0..working_set.len());
                working_set.get(idx).copied()
            } else {
                None
            }
        };

        if let Some(doc) = sampled {
            self.working_set_draws.fetch_add(1, Ordering::Relaxed);
            doc
        } else {
            candidate
        }
    }

    fn get_rotation_count(&self) -> u64 {
        self.rotation_count.load(Ordering::Relaxed)
    }

    fn get_spike_count(&self) -> u64 {
        self.spike_count.load(Ordering::Relaxed)
    }

    fn get_cold_query_count(&self) -> u64 {
        self.cold_query_count.load(Ordering::Relaxed)
    }

    fn get_working_set_draws(&self) -> u64 {
        self.working_set_draws.load(Ordering::Relaxed)
    }
}

/// Basic Zipf sampler (for baseline)
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

    fn sample(&self) -> u64 {
        let mut rng = rand::thread_rng();
        (self.dist.sample(&mut rng) - 1) as u64
    }
}

/// Document store trait for unified access to embeddings
trait DocumentStore: Send + Sync {
    fn fetch(&self, doc_id: u64) -> Option<Vec<f32>>;
    fn corpus_size(&self) -> usize;
    
    /// Phase 0.5.2: Get all embeddings for semantic workload generator
    /// Returns None if store doesn't support bulk access (e.g., remote store)
    fn get_all_embeddings(&self) -> Option<Arc<Vec<Vec<f32>>>>;
}

/// Real document store backed by MS MARCO embeddings
/// Phase 0.5.1: Load pre-computed embeddings from numpy files
struct RealDocumentStore {
    embeddings: Arc<Vec<Vec<f32>>>,
    passages: Arc<Vec<String>>,
    embedding_dim: usize,
}

impl DocumentStore for RealDocumentStore {
    fn fetch(&self, doc_id: u64) -> Option<Vec<f32>> {
        self.embeddings.get(doc_id as usize).cloned()
    }

    fn corpus_size(&self) -> usize {
        self.embeddings.len()
    }
    
    fn get_all_embeddings(&self) -> Option<Arc<Vec<Vec<f32>>>> {
        Some(self.embeddings.clone())
    }
}

impl RealDocumentStore {
    fn new(embeddings_path: &str, passages_path: &str) -> Result<Self> {
        println!("Loading MS MARCO data...");
        println!("  Embeddings: {}", embeddings_path);
        println!("  Passages: {}", passages_path);

        let embeddings_data =
            std::fs::read(embeddings_path).context("Failed to read embeddings file")?;

        let embeddings = Self::parse_numpy_embeddings(&embeddings_data)
            .context("Failed to parse numpy embeddings")?;

        let passages_text =
            std::fs::read_to_string(passages_path).context("Failed to read passages file")?;
        let passages: Vec<String> = passages_text.lines().map(|s| s.to_string()).collect();

        if embeddings.len() != passages.len() {
            bail!(
                "Mismatch: {} embeddings but {} passages",
                embeddings.len(),
                passages.len()
            );
        }

        println!(
            "Loaded {} embeddings ({}×{}-dim), {:.1} MB",
            embeddings.len(),
            embeddings.len(),
            384,
            (embeddings.len() * 384 * 4) as f64 / 1024.0 / 1024.0
        );

        Ok(Self {
            embeddings: Arc::new(embeddings),
            passages: Arc::new(passages),
            embedding_dim: 384,
        })
    }

    fn get_passage(&self, doc_id: u64) -> Option<&str> {
        self.passages.get(doc_id as usize).map(|s| s.as_str())
    }

    pub fn parse_numpy_embeddings(data: &[u8]) -> Result<Vec<Vec<f32>>> {
        if data.len() < 128 {
            bail!("Invalid numpy file: too small ({}  bytes)", data.len());
        }

        let header_end = data
            .iter()
            .position(|&b| b == b'\n')
            .ok_or_else(|| anyhow::anyhow!("Invalid numpy header: no newline found"))?;

        if header_end > 1024 {
            bail!("Invalid numpy header: too long ({} bytes)", header_end);
        }

        let data_start = header_end + 1;

        if data_start >= data.len() {
            bail!("Invalid numpy file: no data after header");
        }

        let float_data = &data[data_start..];
        let num_floats = float_data.len() / 4;
        
        // Try common dimensions (384 for all-MiniLM-L6-v2, 768 for others)
        let expected_dim = if num_floats % 384 == 0 {
            384
        } else if num_floats % 768 == 0 {
            768
        } else {
            // Fallback: try to infer from first 10K floats
            384 // Default to 384
        };
        
        let num_vectors = num_floats / expected_dim;

        if num_floats % expected_dim != 0 {
            bail!(
                "Invalid numpy file: {} floats not divisible by detected dim={}",
                num_floats,
                expected_dim
            );
        }

        let mut embeddings = Vec::with_capacity(num_vectors);

        for i in 0..num_vectors {
            let mut embedding = Vec::with_capacity(expected_dim);
            for j in 0..expected_dim {
                let offset = (i * expected_dim + j) * 4;

                if offset + 4 > float_data.len() {
                    bail!("Invalid numpy file: unexpected EOF at vector {}", i);
                }

                let bytes = &float_data[offset..offset + 4];
                let value = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                embedding.push(value);
            }
            embeddings.push(embedding);
        }

        println!(
            "Parsed {} vectors of {}-dim from numpy file",
            embeddings.len(),
            expected_dim
        );

        Ok(embeddings)
    }
}

/// Mock document storage (fallback when MS MARCO not available)
struct MockDocumentStore {
    corpus_size: usize,
    embedding_dim: usize,
    topic_bases: Vec<Vec<f32>>,
    noise_stddev: f32,
}

impl DocumentStore for MockDocumentStore {
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

    fn corpus_size(&self) -> usize {
        self.corpus_size
    }
    
    fn get_all_embeddings(&self) -> Option<Arc<Vec<Vec<f32>>>> {
        None // MockDocumentStore doesn't pre-store embeddings
    }
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
}

/// Phase 0.5.2: Semantic workload generator with query paraphrasing
/// 
/// This generator simulates realistic RAG workloads where:
/// - Different queries are semantically similar but NOT identical
/// - Same document can be retrieved by multiple paraphrased queries
/// - Query embeddings != document embeddings (but are similar)
/// 
/// Example:
///   Query 1: "What is machine learning?" → embedding A → doc_id=42
///   Query 2: "Explain machine learning" → embedding B (≠A, but similar) → doc_id=42
///   
/// LRU cache: Will MISS on query 2 (different embedding hash)
/// Learned cache: Can HIT if it predicts doc_id=42 is hot based on semantic similarity
struct SemanticWorkloadGenerator {
    base_sampler: ZipfSampler,
    corpus_embeddings: Arc<Vec<Vec<f32>>>,
    query_embeddings: Arc<Vec<Vec<f32>>>,
    
    /// Precomputed mapping: doc_id → [top-K similar query indices]
    /// Built once at startup using parallel cosine similarity computation
    doc_to_queries: Arc<std::collections::HashMap<u64, Vec<usize>>>,
    
    rng: Arc<RwLock<ChaCha8Rng>>,
}

impl SemanticWorkloadGenerator {
    fn new(
        corpus_embeddings: Arc<Vec<Vec<f32>>>,
        query_embeddings: Arc<Vec<Vec<f32>>>,
        query_to_doc: &[u64],
        zipf_exponent: f64,
        top_k: usize,
    ) -> Result<Self> {
        let corpus_size = corpus_embeddings.len();
        if corpus_size == 0 {
            bail!("Corpus size cannot be 0");
        }
        
        let base_sampler = ZipfSampler::new(corpus_size, zipf_exponent)?;
        
        println!("\nPhase 0.5.2: Building semantic query map...");
        println!("  Documents: {}", corpus_size);
        println!("  Queries: {}", query_embeddings.len());
        println!("  Top-K per doc: {}", top_k);
        
        let start = Instant::now();
        let doc_to_queries = Arc::new(build_doc_to_queries_map(
            &corpus_embeddings,
            &query_embeddings,
            query_to_doc,
            top_k,
        ));
        let elapsed = start.elapsed();
        
        println!("  Built query map in {:.2}s", elapsed.as_secs_f64());
        
        Ok(Self {
            base_sampler,
            corpus_embeddings,
            query_embeddings,
            doc_to_queries,
            rng: Arc::new(RwLock::new(ChaCha8Rng::seed_from_u64(0xFACE))),
        })
    }
    
    /// Sample returns (query_embedding, target_doc_id)
    /// This simulates: User query → HNSW search → finds document
    async fn sample(&self) -> (Vec<f32>, u64) {
        // 1. Sample target doc_id using Zipf (hot documents get more queries)
        let doc_id = self.base_sampler.sample();
        
        // 2. Pick a semantically relevant query for this document
        let query_idx = if let Some(query_indices) = self.doc_to_queries.get(&doc_id) {
            // Random query from precomputed top-K relevant queries
            let mut rng = self.rng.write().await;
            query_indices[rng.gen_range(0..query_indices.len())]
        } else {
            // Fallback: random query (should rarely happen)
            let mut rng = self.rng.write().await;
            rng.gen_range(0..self.query_embeddings.len())
        };
        
        let query_embedding = self.query_embeddings[query_idx].clone();
        
        (query_embedding, doc_id)
    }
}

/// Build doc_id → [top-K query indices] mapping using parallel computation
/// 
/// For each document:
/// 1. Compute cosine similarity with ALL queries
/// 2. Sort by similarity descending
/// 3. Take top-K queries
/// 
/// Uses rayon for parallel processing across documents
fn build_doc_to_queries_map(
    docs: &[Vec<f32>],
    queries: &[Vec<f32>],
    query_to_doc: &[u64],
    top_k: usize,
) -> std::collections::HashMap<u64, Vec<usize>> {
    use rayon::prelude::*;
    use std::collections::HashMap;
    
    // First, build a reverse index: doc_id → [query_indices that target it]
    let mut doc_to_candidate_queries: HashMap<u64, Vec<usize>> = HashMap::new();
    for (query_idx, &doc_id) in query_to_doc.iter().enumerate() {
        doc_to_candidate_queries
            .entry(doc_id)
            .or_insert_with(Vec::new)
            .push(query_idx);
    }
    
    // For each document, rank its candidate queries by similarity and take top-K
    let map: HashMap<u64, Vec<usize>> = (0..docs.len())
        .into_par_iter()
        .filter_map(|doc_id_usize| {
            let doc_id = doc_id_usize as u64;
            let doc_emb = &docs[doc_id_usize];
            
            // Get candidate queries for this document
            let candidate_queries = doc_to_candidate_queries.get(&doc_id)?;
            
            if candidate_queries.is_empty() {
                return None;
            }
            
            // Compute cosine similarity with candidate queries
            let mut similarities: Vec<(usize, f32)> = candidate_queries
                .iter()
                .map(|&query_idx| {
                    let query_emb = &queries[query_idx];
                    let sim = cosine_similarity(doc_emb, query_emb);
                    (query_idx, sim)
                })
                .collect();
            
            // Sort by similarity descending, take top-K
            similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let top_queries: Vec<usize> = similarities
                .into_iter()
                .take(top_k.min(candidate_queries.len()))
                .map(|(idx, _)| idx)
                .collect();
            
            Some((doc_id, top_queries))
        })
        .collect();
    
    map
}

/// Cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if norm_a < 1e-6 || norm_b < 1e-6 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

/// Hash embedding vector to u64 for cache key
/// Uses first 8 float32 values XOR'd together
fn hash_embedding(embedding: &[f32]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    
    // Hash first 8 floats (or all if fewer than 8)
    for &value in embedding.iter().take(8) {
        value.to_bits().hash(&mut hasher);
    }
    
    hasher.finish()
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

async fn load_config_from_path(path: &str) -> Result<Config> {
    let contents = fs::read_to_string(path)
        .await
        .with_context(|| format!("Failed to read config file '{}':", path))?;

    let config: Config = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse config file '{}'", path))?;

    Ok(config)
}

fn print_usage(program_name: &str) {
    println!("Usage: {program_name} [config.json]");
    println!("       {program_name} --config <config.json>");
    println!("       {program_name} --help");
    println!();
    println!("When no configuration file is provided, the built-in enterprise workload defaults are used.");
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
        bail!("Memory tracking only supported on Linux");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut raw_args = std::env::args();
    let program_name = raw_args
        .next()
        .unwrap_or_else(|| "validation_enterprise".to_string());
    let mut args = raw_args;

    let config = match args.next() {
        Some(arg) if arg == "--help" || arg == "-h" => {
            print_usage(&program_name);
            return Ok(());
        }
        Some(flag) if flag == "--config" || flag == "-c" => {
            let path = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("Missing configuration path after '{}'", flag))?;
            let config = load_config_from_path(&path).await?;
            println!("Loaded configuration from {}", path);
            config
        }
        Some(path) => {
            let config = load_config_from_path(&path).await?;
            println!("Loaded configuration from {}", path);
            config
        }
        None => {
            println!("Using built-in default configuration (enterprise-scale workload).");
            Config::default()
        }
    };

    if args.next().is_some() {
        eprintln!(
            "Warning: extra arguments detected. Run '{} --help' for usage information.",
            program_name
        );
    }

    config.validate().context("Invalid configuration")?;

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║  KyroDB Enterprise Validation - Phase 0 Week 9-12             ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Configuration:");
    println!("  Duration:          {} hours", config.duration_hours);
    println!("  Target QPS:        {}", config.target_qps);
    println!(
        "  Corpus size:       {} documents (enterprise-scale)",
        config.corpus_size
    );
    println!(
        "  Cache capacity:    {} vectors ({}% of corpus)",
        config.cache_capacity,
        (config.cache_capacity as f64 / config.corpus_size as f64 * 100.0)
    );
    println!(
        "  Zipf exponent:     {} (real-world distribution)",
        config.zipf_exponent
    );

    let expected_ws = config.expected_working_set_size();
    println!(
        "  Expected working set: ~{} docs ({}% of corpus)",
        expected_ws,
        (expected_ws as f64 / config.corpus_size as f64 * 100.0)
    );
    println!(
        "  Cache/WS ratio:    {:.1}% (realistic constraint)",
        (config.cache_capacity as f64 / expected_ws as f64 * 100.0)
    );

    if config.enable_temporal_patterns {
        println!("\n  Temporal patterns: ENABLED");
        println!(
            "    Topic rotation:  Every {} hours",
            config.topic_rotation_interval_secs / 3600
        );
        println!(
            "    Spike probability: {:.1}%",
            config.spike_probability * 100.0
        );
        println!(
            "    Spike duration:  {} minutes",
            config.spike_duration_secs / 60
        );
    }

    println!(
        "\n  Training interval: {} seconds",
        config.training_interval_secs
    );
    println!("  Logger window:     {} events", config.logger_window_size);
    println!();

    let total_expected = config.target_qps * config.duration_hours * 3600;
    println!("Expected workload:");
    println!(
        "  Total queries:     {:.2} million",
        total_expected as f64 / 1_000_000.0
    );
    println!(
        "  Training cycles:   {}",
        (config.duration_hours * 3600) / config.training_interval_secs
    );
    if config.enable_temporal_patterns {
        println!(
            "  Topic rotations:   ~{}",
            (config.duration_hours * 3600) / config.topic_rotation_interval_secs
        );
        println!(
            "  Expected spikes:   ~{}",
            (total_expected as f64 * config.spike_probability) as u64
        );
    }
    println!();
    println!("Expected LRU hit rate: 35-45% (realistic baseline with 60% cold traffic)");
    println!("Target hybrid cache hit rate: 55-70% (1.5-2× improvement via semantic + frequency)");
    println!();
    println!("Output files:");
    println!("  Stats CSV:         {}", config.stats_csv);
    println!("  Results JSON:      {}", config.results_json);
    println!();

    // Initialize components
    println!("Initializing components...");

    let access_logger = Arc::new(RwLock::new(AccessPatternLogger::new(
        config.logger_window_size,
    )));

    let lru_strategy = Arc::new(LruCacheStrategy::new(config.cache_capacity));

    let learned_predictor = LearnedCachePredictor::new(config.cache_capacity)
        .context("Failed to create learned cache predictor")?;
    let semantic_adapter = SemanticAdapter::new();
    let learned_strategy = Arc::new(LearnedCacheStrategy::new_with_semantic(
        config.cache_capacity,
        learned_predictor,
        semantic_adapter,
    ));

    let ab_splitter = AbTestSplitter::new(lru_strategy.clone(), learned_strategy.clone());

    let stats_persister = Arc::new(
        AbStatsPersister::new(&config.stats_csv).context("Failed to create stats persister")?,
    );

    // Phase 0.5.1: Try to load real MS MARCO embeddings, fallback to mock
    let doc_store: Arc<dyn DocumentStore> = match (
        &config.ms_marco_embeddings_path,
        &config.ms_marco_passages_path,
    ) {
        (Some(embeddings_path), Some(passages_path))
            if std::path::Path::new(embeddings_path).exists()
                && std::path::Path::new(passages_path).exists() =>
        {
            println!("Using real MS MARCO embeddings (Phase 0.5.1)");
            Arc::new(
                RealDocumentStore::new(embeddings_path, passages_path)
                    .context("Failed to load MS MARCO data")?,
            )
        }
        (Some(_), Some(_)) => {
            println!("WARNING: MS MARCO paths configured but files not found");
            println!("Falling back to mock clustered embeddings");
            Arc::new(MockDocumentStore::new(config.corpus_size))
        }
        _ => {
            println!("Using mock clustered embeddings (no MS MARCO data configured)");
            Arc::new(MockDocumentStore::new(config.corpus_size))
        }
    };

    // Phase 0.5.2: Load query embeddings for semantic workload generation
    let (query_embeddings, query_to_doc) = match (
        &config.query_embeddings_path,
        &config.query_to_doc_path,
    ) {
        (Some(query_emb_path), Some(query_doc_path))
            if std::path::Path::new(query_emb_path).exists()
                && std::path::Path::new(query_doc_path).exists() =>
        {
            println!("Phase 0.5.2: Loading query embeddings for semantic workload...");
            
            // Load query embeddings (numpy format)
            let query_emb_data = std::fs::read(query_emb_path)
                .context("Failed to read query embeddings file")?;
            let query_embeddings = RealDocumentStore::parse_numpy_embeddings(&query_emb_data)?;
            
            // Load query→doc mapping
            let query_doc_text = std::fs::read_to_string(query_doc_path)
                .context("Failed to read query_to_doc file")?;
            let query_to_doc: Vec<u64> = query_doc_text
                .lines()
                .filter(|line| !line.trim().is_empty())
                .enumerate()
                .map(|(idx, line)| {
                    line.trim().parse::<u64>().with_context(|| {
                        format!("Failed to parse doc_id at line {}: '{}'", idx + 1, line)
                    })
                })
                .collect::<Result<Vec<u64>>>()?;
            
            if query_embeddings.len() != query_to_doc.len() {
                bail!(
                    "Query embedding count ({}) != query_to_doc count ({})",
                    query_embeddings.len(),
                    query_to_doc.len()
                );
            }
            
            println!(
                "  Loaded {} query embeddings ({}×{}-dim)",
                query_embeddings.len(),
                query_embeddings.len(),
                query_embeddings[0].len()
            );
            
            (Some(Arc::new(query_embeddings)), Some(query_to_doc))
        }
        (Some(_), Some(_)) => {
            println!("WARNING: Query embedding paths configured but files not found");
            println!("Falling back to ID-based sampling (no semantic variance)");
            (None, None)
        }
        _ => {
            println!("Phase 0.5.2: Query embeddings not configured, using ID-based sampling");
            (None, None)
        }
    };

    // Spawn background training task
    println!("Spawning background training task...");
    let training_config = TrainingConfig {
        interval: Duration::from_secs(config.training_interval_secs),
        window_duration: Duration::from_secs(3600),
        min_events_for_training: 100,
        rmi_capacity: config.cache_capacity,
    };

    let training_cycles = Arc::new(AtomicU64::new(0));

    let training_handle = spawn_training_task(
        access_logger.clone(),
        learned_strategy.clone(),
        training_config,
        Some(training_cycles.clone()),
    )
    .await;

    println!(
        "Training task running (retrains every {} seconds)",
        config.training_interval_secs
    );
    println!();

    // Phase 0.5.2: Initialize workload generator (semantic if query embeddings available)
    enum WorkloadGenerator {
        Temporal(TemporalWorkloadGenerator),
        Semantic(SemanticWorkloadGenerator),
    }
    
    impl WorkloadGenerator {
        fn get_rotation_count(&self) -> u64 {
            match self {
                WorkloadGenerator::Temporal(gen) => gen.get_rotation_count(),
                WorkloadGenerator::Semantic(_) => 0, // Semantic gen doesn't have rotations
            }
        }
        
        fn get_spike_count(&self) -> u64 {
            match self {
                WorkloadGenerator::Temporal(gen) => gen.get_spike_count(),
                WorkloadGenerator::Semantic(_) => 0, // Semantic gen doesn't have spikes
            }
        }
        
        fn get_cold_query_count(&self) -> u64 {
            match self {
                WorkloadGenerator::Temporal(gen) => gen.get_cold_query_count(),
                WorkloadGenerator::Semantic(_) => 0, // Semantic gen doesn't track cold queries
            }
        }
        
        fn get_working_set_draws(&self) -> u64 {
            match self {
                WorkloadGenerator::Temporal(gen) => gen.get_working_set_draws(),
                WorkloadGenerator::Semantic(_) => 0, // Semantic gen doesn't have working set
            }
        }
    }
    
    let workload_gen = if let (Some(query_embs), Some(query_doc_map)) = (&query_embeddings, &query_to_doc) {
        // Get corpus embeddings for semantic workload
        if let Some(corpus_embs) = doc_store.get_all_embeddings() {
            println!("Using SemanticWorkloadGenerator (Phase 0.5.2)");
            let semantic_gen = SemanticWorkloadGenerator::new(
                corpus_embs,
                query_embs.clone(),
                query_doc_map,
                config.zipf_exponent,
                config.top_k_queries_per_doc,
            )?;
            WorkloadGenerator::Semantic(semantic_gen)
        } else {
            println!("WARNING: Query embeddings available but corpus embeddings not accessible");
            println!("Falling back to TemporalWorkloadGenerator");
            let temporal_gen = TemporalWorkloadGenerator::new(
                config.corpus_size,
                config.zipf_exponent,
                Duration::from_secs(config.topic_rotation_interval_secs),
                config.spike_probability,
                Duration::from_secs(config.spike_duration_secs),
                config.enable_temporal_patterns,
                config.cache_capacity,
                config.working_set_multiplier,
                config.working_set_churn,
                config.cold_traffic_ratio,
                config.working_set_bias,
            )?;
            WorkloadGenerator::Temporal(temporal_gen)
        }
    } else {
        println!("Using TemporalWorkloadGenerator (ID-based sampling)");
        let temporal_gen = TemporalWorkloadGenerator::new(
            config.corpus_size,
            config.zipf_exponent,
            Duration::from_secs(config.topic_rotation_interval_secs),
            config.spike_probability,
            Duration::from_secs(config.spike_duration_secs),
            config.enable_temporal_patterns,
            config.cache_capacity,
            config.working_set_multiplier,
            config.working_set_churn,
            config.cold_traffic_ratio,
            config.working_set_bias,
        )?;
        WorkloadGenerator::Temporal(temporal_gen)
    };

    // Test parameters
    let test_duration = Duration::from_secs(config.duration_hours * 3600);
    let target_interval = Duration::from_nanos(1_000_000_000 / config.target_qps);

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

    // Graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\n\nReceived Ctrl+C, shutting down gracefully...");
        r.store(false, Ordering::SeqCst);
    });

    // Main query loop
    while test_start.elapsed() < test_duration && running.load(Ordering::SeqCst) {
        // Phase 0.5.2: Sample either (doc_id) or (query_embedding, doc_id)
        let (cache_key, doc_id) = match &workload_gen {
            WorkloadGenerator::Temporal(gen) => {
                let doc_id = gen.sample().await;
                (doc_id, doc_id) // Cache by doc_id for ID-based sampling
            }
            WorkloadGenerator::Semantic(gen) => {
                let (query_embedding, doc_id) = gen.sample().await;
                // Phase 0.5.2: Cache by query embedding hash (not doc_id!)
                // This simulates real RAG: "What is ML?" and "Explain ML" have DIFFERENT cache keys
                let cache_key = hash_embedding(&query_embedding);
                (cache_key, doc_id)
            }
        };

        let strategy = ab_splitter.get_strategy(doc_id);
        let strategy_name = strategy.name();

        let strategy_id = if strategy_name == "lru_baseline" {
            StrategyId::LruBaseline
        } else if strategy_name == "learned_rmi" {
            StrategyId::LearnedRmi
        } else {
            eprintln!("ERROR: Unknown strategy '{}'", strategy_name);
            continue;
        };

        let cached_vec = strategy.get_cached(cache_key);

        let (embedding, cache_hit) = if let Some(vec) = cached_vec {
            (vec.embedding, true)
        } else {
            let embedding = doc_store
                .fetch(doc_id)
                .ok_or_else(|| anyhow::anyhow!("Document {} not found", doc_id))?;

            let should_cache = match strategy_id {
                StrategyId::LruBaseline => true,
                StrategyId::LearnedRmi => {
                    let cycles = training_cycles.load(Ordering::Relaxed);
                    if cycles == 0 {
                        // Bootstrap: use LRU policy before first training
                        true
                    } else {
                        // Use RMI predictor after training
                        strategy.should_cache(doc_id, &embedding)
                    }
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

        {
            let mut logger = access_logger.write().await;
            logger.log_access(doc_id, &embedding);
        }

        if cache_hit {
            stats_persister.log_hit(strategy_name, doc_id, 0).await.ok();
        } else {
            stats_persister
                .log_miss(strategy_name, doc_id, 0)
                .await
                .ok();
        }

        total_queries += 1;

        // Progress reporting every 10K queries
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

            let cycles = training_cycles.load(Ordering::Relaxed);
            let mode = if cycles == 0 {
                "BOOTSTRAP"
            } else {
                "LEARNED  "
            };

            println!(
                "[{:>5.1}%] Q:{:>7} | QPS:{:>3.0} | LRU:{:>5.1}% | Learned:{:>5.1}% ({}) | Cycles:{:>2} | Imp:{:>4.2}×",
                progress_pct,
                total_queries,
                current_qps,
                lru_hit_rate * 100.0,
                learned_hit_rate * 100.0,
                mode,
                cycles,
                improvement
            );
        }

        next_query += target_interval;
        let now = Instant::now();
        if next_query > now {
            tokio::time::sleep(next_query - now).await;
        } else {
            next_query = now + target_interval;
        }
    }

    // Finalize
    let training_crashed = training_handle.is_finished();
    if training_crashed {
        eprintln!("WARNING: Training task crashed");
    }

    println!("\nStopping training task...");
    training_handle.abort();
    let _ = training_handle.await;

    let end_time = SystemTime::now();
    let final_memory = get_memory_mb().unwrap_or(0.0);
    let actual_duration = test_start.elapsed().as_secs();

    println!("─────────────────────────────────────────────────────────────────");
    println!("\nTest complete!\n");

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
    let absolute_improvement = learned_hit_rate - lru_hit_rate;
    let memory_growth_mb = final_memory - initial_memory;
    let memory_growth_pct = if initial_memory > 0.0 {
        (memory_growth_mb / initial_memory) * 100.0
    } else {
        0.0
    };

    let actual_training_cycles = training_cycles.load(Ordering::Relaxed);
    let topic_rotations = workload_gen.get_rotation_count();
    let spike_events = workload_gen.get_spike_count();
    let cold_queries = workload_gen.get_cold_query_count();
    let working_set_draws = workload_gen.get_working_set_draws();

    let cache_to_ws_ratio = config.cache_capacity as f64 / expected_ws as f64;

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
        absolute_improvement,

        expected_training_cycles: actual_duration / config.training_interval_secs,
        actual_training_cycles,
        training_task_crashed: training_crashed,

        topic_rotations,
        spike_events,

        initial_memory_mb: initial_memory,
        final_memory_mb: final_memory,
        memory_growth_mb,
        memory_growth_pct,

        start_time,
        end_time,

        expected_working_set_size: expected_ws,
        cache_to_working_set_ratio: cache_to_ws_ratio,
        cold_queries,
        working_set_draws,
    };

    // Display results
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║              ENTERPRISE VALIDATION RESULTS                     ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Test Summary:");
    println!(
        "  Duration:        {} hours {} min",
        actual_duration / 3600,
        (actual_duration % 3600) / 60
    );
    println!(
        "  Total queries:   {} ({:.2}M)",
        total_queries,
        total_queries as f64 / 1_000_000.0
    );
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
        "  Hit rate improvement:  {:.2}× (target: 3.0-5.0×)",
        improvement
    );
    println!(
        "  Absolute improvement:  {:+.1} percentage points",
        absolute_improvement * 100.0
    );
    println!(
        "  Status:                {}",
        if improvement >= 3.0 && learned_hit_rate >= 0.60 {
            "PASS - EXCELLENT"
        } else if improvement >= 2.5 && learned_hit_rate >= 0.50 {
            "PASS"
        } else {
            "FAIL"
        }
    );
    println!();
    println!("Training:");
    println!("  Expected cycles: {}", results.expected_training_cycles);
    println!("  Actual cycles:   {}", actual_training_cycles);
    println!("  Interval:        {}s", config.training_interval_secs);
    println!(
        "  Task crashed:    {}",
        if training_crashed { "YES" } else { "NO" }
    );
    println!();

    if config.enable_temporal_patterns {
        println!("Temporal Patterns:");
        println!("  Topic rotations: {}", topic_rotations);
        println!("  Spike events:    {}", spike_events);
        println!();
    }

    let total_q = total_queries.max(1); // avoid div-by-zero for display
    println!("Workload Mix:");
    println!(
        "  Cold traffic:     {} ({:.1}% of total)",
        cold_queries,
        cold_queries as f64 / total_q as f64 * 100.0
    );
    println!(
        "  Working-set reuse:{} draws ({:.1}% of total)",
        working_set_draws,
        working_set_draws as f64 / total_q as f64 * 100.0
    );
    println!();

    if initial_memory > 0.0 {
        println!("Memory:");
        println!("  Initial:         {:.1} MB", initial_memory);
        println!("  Final:           {:.1} MB", final_memory);
        println!(
            "  Growth:          {:+.1} MB ({:+.1}%)",
            memory_growth_mb, memory_growth_pct
        );
        println!(
            "  Status:          {}",
            if memory_growth_pct.abs() < 5.0 {
                "PASS"
            } else if memory_growth_pct.abs() < 10.0 {
                "WARN"
            } else {
                "FAIL"
            }
        );
    }
    println!();

    // Go/No-Go decision
    let hit_rate_pass = learned_hit_rate >= 0.60;
    let improvement_pass = improvement >= 2.5;
    let memory_pass = initial_memory == 0.0 || memory_growth_pct.abs() < 10.0;
    let training_pass = !training_crashed;

    let go_decision = hit_rate_pass && improvement_pass && memory_pass && training_pass;

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                     GO/NO-GO DECISION                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Criteria:");
    println!(
        "  Learned cache hit rate ≥60% ............ {}",
        if hit_rate_pass { "PASS" } else { "FAIL" }
    );
    println!(
        "  Improvement ≥2.5× over LRU ............. {}",
        if improvement_pass { "PASS" } else { "FAIL" }
    );
    println!(
        "  Memory growth <10% ..................... {}",
        if memory_pass { "PASS" } else { "FAIL" }
    );
    println!(
        "  Training task stable ................... {}",
        if training_pass { "PASS" } else { "FAIL" }
    );
    println!();

    if go_decision {
        println!("Decision: GO FOR PRODUCTION (PASS)");
        println!();
        println!("Market Pitch:");
        println!(
            "  \"KyroDB learned cache achieves {:.0}% hit rate\"",
            learned_hit_rate * 100.0
        );
        println!(
            "  \"vs industry-standard LRU baseline of {:.0}%\"",
            lru_hit_rate * 100.0
        );
        println!(
            "  \"{:.1}× improvement = eliminate {:.0}% of database queries\"",
            improvement,
            absolute_improvement * 100.0
        );
        println!("  \"Enterprise-validated on 1M document corpus\"");
    } else {
        println!("Decision: NO-GO - needs investigation");
        println!();
        if !hit_rate_pass {
            println!(
                "  Issue: Learned hit rate ({:.1}%) below target (60%)",
                learned_hit_rate * 100.0
            );
        }
        if !improvement_pass {
            println!(
                "  Issue: Improvement ({:.2}×) below target (2.5×)",
                improvement
            );
        }
        if !memory_pass {
            println!(
                "  Issue: Memory growth ({:.1}%) above limit (10%)",
                memory_growth_pct
            );
        }
        if !training_pass {
            println!("  Issue: Training task crashed");
        }
    }
    println!();

    // Save results
    let json = serde_json::to_string_pretty(&results)?;
    std::fs::write(&config.results_json, json)?;
    stats_persister.flush().await?;

    println!("Results saved to:");
    println!("  - {}", config.results_json);
    println!("  - {}", config.stats_csv);
    println!();

    if !go_decision {
        std::process::exit(1);
    }

    Ok(())
}
