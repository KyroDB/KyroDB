//! Tiered Engine - Three-layer architecture orchestrator
//!
//! Coordinates all three tiers:
//! //! - **Layer 1 (Cache)**: Hybrid Semantic Cache (RMI frequency + semantic similarity)
//! - **Layer 2 (Hot Tier)**: Recent writes buffer (fast writes, periodic flush)
//! - **Layer 3 (Cold Tier)**: HNSW index (all documents, approximate k-NN search)
//!
//! # Query Path
//! ```text
//! Query → Cache (L1) → Hot Tier (L2) → HNSW (L3)
//!         ↓ hit         ↓ hit           ↓ always succeeds
//!       return        return          return k-NN results
//! ```
//!
//! # Write Path
//! ```text
//! Insert → WAL (durability) → Hot Tier → Background flush → HNSW + Snapshot
//! ```

use crate::{
    HnswBackend, CacheStrategy, HotTier, SearchResult, AccessPatternLogger,
    FsyncPolicy, CachedVector,
};
use tracing::{trace, debug, info, warn, error, instrument};
use anyhow::{Context, Result};
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;

/// Tiered engine statistics
#[derive(Debug, Clone, Default)]
pub struct TieredEngineStats {
    /// Layer 1 (Cache) statistics
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,
    
    /// Layer 2 (Hot Tier) statistics
    pub hot_tier_hits: u64,
    pub hot_tier_misses: u64,
    pub hot_tier_hit_rate: f64,
    pub hot_tier_size: usize,
    pub hot_tier_flushes: u64,
    
    /// Layer 3 (Cold Tier) statistics
    pub cold_tier_searches: u64,
    pub cold_tier_size: usize,
    
    /// Overall statistics
    pub total_queries: u64,
    pub total_inserts: u64,
    pub overall_hit_rate: f64, // (cache_hits + hot_tier_hits) / total_queries
}

/// Configuration for tiered engine
#[derive(Debug, Clone)]
pub struct TieredEngineConfig {
    /// Hot tier max size (documents)
    pub hot_tier_max_size: usize,
    
    /// Hot tier max age (duration before forced flush)
    pub hot_tier_max_age: Duration,
    
    /// HNSW max elements capacity
    pub hnsw_max_elements: usize,
    
    /// Persistence data directory
    pub data_dir: Option<String>,
    
    /// WAL fsync policy
    pub fsync_policy: FsyncPolicy,
    
    /// Snapshot interval (create snapshot every N inserts to cold tier)
    pub snapshot_interval: usize,
    
    /// Background flush interval (check hot tier every N seconds)
    pub flush_interval: Duration,
}

impl Default for TieredEngineConfig {
    fn default() -> Self {
        Self {
            hot_tier_max_size: 10_000,
            hot_tier_max_age: Duration::from_secs(60),
            hnsw_max_elements: 1_000_000,
            data_dir: None,
            fsync_policy: FsyncPolicy::Always,
            snapshot_interval: 10_000,
            flush_interval: Duration::from_secs(30),
        }
    }
}

/// Tiered Engine - Three-layer vector database
pub struct TieredEngine {
    /// Layer 1: Hybrid Semantic Cache (hot documents predicted by RMI + semantic)
    cache_strategy: Arc<RwLock<Box<dyn CacheStrategy>>>,
    
    /// Layer 2: Hot tier (recent writes)
    hot_tier: Arc<HotTier>,
    
    /// Layer 3: Cold tier (HNSW index)
    cold_tier: Arc<HnswBackend>,
    
    /// Access logger (for cache training)
    access_logger: Option<Arc<RwLock<AccessPatternLogger>>>,
    
    /// Statistics
    stats: Arc<RwLock<TieredEngineStats>>,
    
    /// Configuration
    config: TieredEngineConfig,
}

impl TieredEngine {
    /// Create new tiered engine
    ///
    /// # Parameters
    /// - `cache_strategy`: Layer 1 cache strategy (LRU or Learned)
    /// - `initial_embeddings`: Initial documents to load into cold tier
    /// - `config`: Configuration for all tiers
    pub fn new(
        cache_strategy: Box<dyn CacheStrategy>,
        initial_embeddings: Vec<Vec<f32>>,
        config: TieredEngineConfig,
    ) -> Result<Self> {
        // Create hot tier
        let hot_tier = Arc::new(HotTier::new(
            config.hot_tier_max_size,
            config.hot_tier_max_age,
        ));
        
        // Create cold tier (HNSW backend)
        let cold_tier = if let Some(ref data_dir) = config.data_dir {
            // With persistence
            Arc::new(HnswBackend::with_persistence(
                initial_embeddings,
                config.hnsw_max_elements,
                data_dir,
                config.fsync_policy,
                config.snapshot_interval,
            )?)
        } else {
            // Without persistence (testing only)
            Arc::new(HnswBackend::new(
                initial_embeddings,
                config.hnsw_max_elements,
            )?)
        };
        
        Ok(Self {
            cache_strategy: Arc::new(RwLock::new(cache_strategy)),
            hot_tier,
            cold_tier,
            access_logger: None,
            stats: Arc::new(RwLock::new(TieredEngineStats::default())),
            config,
        })
    }
    
    /// Recover from persistence
    pub fn recover(
        cache_strategy: Box<dyn CacheStrategy>,
        data_dir: impl AsRef<Path>,
        config: TieredEngineConfig,
    ) -> Result<Self> {
        let data_dir_str = data_dir.as_ref().to_string_lossy().to_string();
        
        // Recover cold tier from WAL + snapshot
        let cold_tier = Arc::new(HnswBackend::recover(
            &data_dir_str,
            config.hnsw_max_elements,
            config.fsync_policy,
            config.snapshot_interval,
        )?);
        
        // Create fresh hot tier (ephemeral)
        let hot_tier = Arc::new(HotTier::new(
            config.hot_tier_max_size,
            config.hot_tier_max_age,
        ));
        
        let mut recovered_config = config;
        recovered_config.data_dir = Some(data_dir_str);
        
        Ok(Self {
            cache_strategy: Arc::new(RwLock::new(cache_strategy)),
            hot_tier,
            cold_tier,
            access_logger: None,
            stats: Arc::new(RwLock::new(TieredEngineStats::default())),
            config: recovered_config,
        })
    }
    
    /// Set access logger (for cache training)
    pub fn set_access_logger(&mut self, logger: Arc<RwLock<AccessPatternLogger>>) {
        self.access_logger = Some(logger);
    }
    
    /// Query - unified three-tier path
    ///
    /// # Query Flow
    /// 1. Check cache (L1) - RMI prediction + semantic similarity
    /// 2. If miss, check hot tier (L2) - recent writes
    /// 3. If miss, search HNSW (L3) - full k-NN search
    /// 4. Cache admission decision (should we cache this result?)
    /// 5. Log access for training
    ///
    /// # Returns
    /// - `Some(embedding)` if document found in any tier
    /// - `None` if document doesn't exist
    pub fn query(&self, doc_id: u64, query_embedding: Option<&[f32]>) -> Option<Vec<f32>> {
        let mut stats = self.stats.write();
        stats.total_queries += 1;
        drop(stats);
        
        // Layer 1: Check cache
        {
            let cache = self.cache_strategy.read();
            if let Some(cached) = cache.get_cached(doc_id) {
                let mut stats = self.stats.write();
                stats.cache_hits += 1;
                drop(stats);
                
                // Log access (cache hit)
                if let Some(ref logger) = self.access_logger {
                    if let Some(query_emb) = query_embedding {
                        logger.write().log_access(doc_id, query_emb);
                    }
                }
                
                return Some(cached.embedding);
            }
        }
        
        // Cache miss
        {
            let mut stats = self.stats.write();
            stats.cache_misses += 1;
        }
        
        // Layer 2: Check hot tier
        if let Some(embedding) = self.hot_tier.get(doc_id) {
            let mut stats = self.stats.write();
            stats.hot_tier_hits += 1;
            drop(stats);
            
            // Cache admission decision: should_cache takes (doc_id, embedding)
            let should_cache_decision = {
                let mut cache = self.cache_strategy.write();
                cache.should_cache(doc_id, &embedding)
            };
            
            if should_cache_decision {
                let cached = CachedVector {
                    doc_id,
                    embedding: embedding.clone(),
                    distance: 0.0,
                    cached_at: Instant::now(),
                };
                self.cache_strategy.write().insert_cached(cached);
            }
            
            // Log access (hot tier hit) - use query embedding if available
            if let Some(ref logger) = self.access_logger {
                if let Some(query_emb) = query_embedding {
                    logger.write().log_access(doc_id, query_emb);
                }
            }
            
            return Some(embedding);
        }
        
        // Hot tier miss
        {
            let mut stats = self.stats.write();
            stats.hot_tier_misses += 1;
        }
        
        // Layer 3: Fetch from cold tier (HNSW)
        if let Some(embedding) = self.cold_tier.fetch_document(doc_id) {
            let mut stats = self.stats.write();
            stats.cold_tier_searches += 1;
            drop(stats);
            
            // Cache admission decision: should_cache takes (doc_id, embedding)
            let should_cache_decision = {
                let mut cache = self.cache_strategy.write();
                cache.should_cache(doc_id, &embedding)
            };
            
            if should_cache_decision {
                let cached = CachedVector {
                    doc_id,
                    embedding: embedding.clone(),
                    distance: 0.0,
                    cached_at: Instant::now(),
                };
                self.cache_strategy.write().insert_cached(cached);
            }
            
            // Log access (cold tier hit) - use query embedding if available
            if let Some(ref logger) = self.access_logger {
                if let Some(query_emb) = query_embedding {
                    logger.write().log_access(doc_id, query_emb);
                }
            }
            
            return Some(embedding);
        }
        
        // Document not found in any tier
        None
    }
    
    /// k-NN search across all tiers
    ///
    /// This searches the cold tier (HNSW) for approximate k-NN,
    /// then augments with hot tier results if needed.
    pub fn knn_search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        // Search cold tier (HNSW)
        let results = self.cold_tier.knn_search(query, k)?;
        
        {
            let mut stats = self.stats.write();
            stats.cold_tier_searches += 1;
        }
        
        // TODO: Merge with hot tier results (for now, HNSW only)
        // Future enhancement: Search hot tier, merge with HNSW results
        
        Ok(results)
    }
    
    /// Insert document
    ///
    /// # Write Flow
    /// 1. Add to hot tier (fast, no HNSW update)
    /// 2. Log to WAL (durability, happens in background flush)
    /// 3. Background flush to cold tier when thresholds reached
    pub fn insert(&self, doc_id: u64, embedding: Vec<f32>) -> Result<()> {
        // Insert into hot tier (fast)
        self.hot_tier.insert(doc_id, embedding);
        
        let mut stats = self.stats.write();
        stats.total_inserts += 1;
        
        Ok(())
    }
    
    /// Flush hot tier to cold tier (manual trigger)
    ///
    /// This is called periodically by background task,
    /// or can be called manually for testing/shutdown.
    pub fn flush_hot_tier(&self) -> Result<usize> {
        if !self.hot_tier.needs_flush() {
            return Ok(0);
        }
        
        let documents = self.hot_tier.drain_for_flush();
        let count = documents.len();
        
        if count == 0 {
            return Ok(0);
        }
        
        println!("Flushing {} documents from hot tier to cold tier...", count);
        
        // Insert into cold tier (HNSW + WAL)
        for (doc_id, embedding) in documents {
            self.cold_tier.insert(doc_id, embedding)
                .context("Failed to insert into cold tier during flush")?;
        }
        
        println!("Flush complete: {} documents moved to cold tier", count);
        
        Ok(count)
    }
    
    /// Spawn background flush task
    ///
    /// Periodically checks if hot tier needs flushing and flushes to cold tier.
    pub fn spawn_flush_task(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let flush_interval = self.config.flush_interval;
        
        tokio::spawn(async move {
            let mut ticker = interval(flush_interval);
            
            loop {
                ticker.tick().await;
                
                if self.hot_tier.needs_flush() {
                    match self.flush_hot_tier() {
                        Ok(count) => {
                            if count > 0 {
                                println!("Background flush: {} documents moved to cold tier", count);
                            }
                        }
                        Err(e) => {
                            eprintln!("Background flush failed: {}", e);
                        }
                    }
                }
            }
        })
    }
    
    /// Get comprehensive statistics
    pub fn stats(&self) -> TieredEngineStats {
        let mut stats = self.stats.read().clone();
        
        // Update tier-specific stats
        let hot_tier_stats = self.hot_tier.stats();
        stats.hot_tier_size = hot_tier_stats.current_size;
        stats.hot_tier_flushes = hot_tier_stats.total_flushes;
        stats.hot_tier_hit_rate = self.hot_tier.hit_rate();
        
        stats.cold_tier_size = self.cold_tier.len();
        
        // Calculate overall hit rate
        let total_hits = stats.cache_hits + stats.hot_tier_hits;
        if stats.total_queries > 0 {
            stats.overall_hit_rate = total_hits as f64 / stats.total_queries as f64;
            stats.cache_hit_rate = stats.cache_hits as f64 / stats.total_queries as f64;
        }
        
        stats
    }
    
    /// Get cold tier reference (for direct access if needed)
    pub fn cold_tier(&self) -> &HnswBackend {
        &self.cold_tier
    }
    
    /// Get hot tier reference (for testing/inspection)
    pub fn hot_tier(&self) -> &HotTier {
        &self.hot_tier
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LruCacheStrategy;
    use tempfile::TempDir;
    
    #[test]
    fn test_tiered_engine_query_path() {
        let cache = LruCacheStrategy::new(100);
        let initial_embeddings = vec![
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0, 0.0],
        ];
        
        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };
        
        let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();
        
        // Query doc 0 (in cold tier)
        let result = engine.query(0, None);
        assert!(result.is_some());
        
        let stats = engine.stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.cold_tier_searches, 1);
    }
    
    #[test]
    fn test_tiered_engine_insert_and_query() {
        let cache = LruCacheStrategy::new(100);
        let initial_embeddings = vec![vec![1.0, 0.0]];
        
        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };
        
        let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();
        
        // Insert into hot tier
        engine.insert(10, vec![0.5, 0.5]).unwrap();
        
        // Query should hit hot tier
        let result = engine.query(10, None);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), vec![0.5, 0.5]);
        
        let stats = engine.stats();
        assert_eq!(stats.hot_tier_hits, 1);
        assert_eq!(stats.hot_tier_size, 1);
    }
    
    #[test]
    fn test_tiered_engine_flush() {
        let cache = LruCacheStrategy::new(100);
        let initial_embeddings = vec![vec![1.0, 0.0]];
        
        let config = TieredEngineConfig {
            hot_tier_max_size: 2, // Small threshold
            hnsw_max_elements: 100,
            data_dir: None,
            ..Default::default()
        };
        
        let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();
        
        // Insert 2 documents (trigger flush threshold)
        engine.insert(10, vec![0.1, 0.1]).unwrap();
        engine.insert(11, vec![0.2, 0.2]).unwrap();
        
        assert!(engine.hot_tier().needs_flush());
        
        // Manual flush
        let flushed = engine.flush_hot_tier().unwrap();
        assert_eq!(flushed, 2);
        
        // Hot tier should be empty
        assert_eq!(engine.hot_tier().len(), 0);
        
        // Documents should be in cold tier
        assert!(engine.cold_tier().fetch_document(10).is_some());
        assert!(engine.cold_tier().fetch_document(11).is_some());
    }
    
    #[test]
    fn test_tiered_engine_with_persistence() {
        let dir = TempDir::new().unwrap();
        
        {
            let cache = LruCacheStrategy::new(100);
            let initial_embeddings = vec![vec![1.0, 0.0]];
            
            // Use hot_tier_max_size=1 to trigger flush immediately
            let config = TieredEngineConfig {
                hot_tier_max_size: 1,
                hnsw_max_elements: 100,
                data_dir: Some(dir.path().to_string_lossy().to_string()),
                fsync_policy: FsyncPolicy::Always,
                ..Default::default()
            };
            
            let engine = TieredEngine::new(Box::new(cache), initial_embeddings, config).unwrap();
            
            // Insert and flush (should trigger because hot_tier_max_size=1)
            println!("Inserting doc_id 10...");
            engine.insert(10, vec![0.5, 0.5]).unwrap();
            println!("Flushing hot tier...");
            let flushed = engine.flush_hot_tier().unwrap();
            println!("Flushed {} documents", flushed);
            assert_eq!(flushed, 1, "Expected 1 document to be flushed");
            
            // Verify doc 10 is in cold tier before snapshot
            assert!(engine.cold_tier().fetch_document(10).is_some(), "Doc 10 not in cold tier before snapshot");
            
            // Create snapshot
            println!("Creating snapshot...");
            engine.cold_tier().create_snapshot().unwrap();
            println!("Snapshot created");
            
            // Explicit drop to ensure WAL fsynced
            drop(engine);
        }
        
        // Recover
        println!("Recovering...");
        let cache = LruCacheStrategy::new(100);
        let config = TieredEngineConfig {
            hot_tier_max_size: 10,
            hnsw_max_elements: 100,
            ..Default::default()
        };
        
        let recovered = TieredEngine::recover(Box::new(cache), dir.path(), config).unwrap();
        
        // Verify data recovered
        println!("Verifying recovery...");
        assert!(recovered.cold_tier().fetch_document(0).is_some(), "Doc 0 not recovered");
        assert!(recovered.cold_tier().fetch_document(10).is_some(), "Doc 10 not recovered");
        println!("Persistence test passed!");
    }
}
