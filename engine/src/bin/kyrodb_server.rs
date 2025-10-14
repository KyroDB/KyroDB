//! KyroDB Production gRPC Server
//!
//! High-performance vector database server optimized for RAG workloads.
//!
//! # Architecture
//! - gRPC primary protocol (performance-critical)
//! - Three-tier query engine (Cache → Hot Tier → HNSW)
//! - Structured logging with tracing
//! - Prometheus metrics
//!
//! # Usage
//! ```bash
//! # Start server with default config
//! kyrodb_server
//!
//! # With custom config
//! KYRODB_DATA_DIR=./data KYRODB_PORT=50051 kyrodb_server
//!
//! # With verbose logging
//! RUST_LOG=kyrodb_engine=debug kyrodb_server
//! ```

use kyrodb_engine::{
    FsyncPolicy, LruCacheStrategy, SearchResult, TieredEngine, TieredEngineConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, instrument, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Generated protobuf code
pub mod kyrodb {
    tonic::include_proto!("kyrodb.v1");
}

use kyrodb::{
    kyro_db_service_server::{KyroDbService, KyroDbServiceServer},
    *,
};

// ============================================================================
// CONFIGURATION CONSTANTS
// ============================================================================

/// Maximum embedding dimension to prevent DoS attacks
const MAX_EMBEDDING_DIM: usize = 4096;

/// Maximum batch size for bulk operations to prevent memory exhaustion
const MAX_BATCH_SIZE: usize = 10000;

/// Minimum valid document ID (0 is reserved)
const MIN_DOC_ID: u64 = 1;

/// Maximum k value for k-NN search to prevent excessive computation
const MAX_KNN_K: u32 = 1000;

// ============================================================================
// SERVER STATE
// ============================================================================

/// Server state - holds engine and runtime metrics
struct ServerState {
    engine: Arc<RwLock<TieredEngine>>,
    start_time: Instant,
    config: TieredEngineConfig,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
}

/// gRPC service implementation
struct KyroDBServiceImpl {
    state: Arc<ServerState>,
}

// KyroDBServiceImpl is constructed directly in main() with existing engine Arc

#[tonic::async_trait]
impl KyroDbService for KyroDBServiceImpl {
    type BulkSearchStream = ReceiverStream<Result<SearchResponse, Status>>;
    // ============================================================================
    // WRITE OPERATIONS
    // ============================================================================

    #[instrument(skip(self, request), fields(doc_id))]
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Validate input
        if req.doc_id < MIN_DOC_ID {
            return Err(Status::invalid_argument(format!(
                "doc_id must be >= {}",
                MIN_DOC_ID
            )));
        }
        if req.embedding.is_empty() {
            return Err(Status::invalid_argument("embedding cannot be empty"));
        }
        if req.embedding.len() > MAX_EMBEDDING_DIM {
            return Err(Status::invalid_argument(format!(
                "embedding dimension {} exceeds maximum {}",
                req.embedding.len(),
                MAX_EMBEDDING_DIM
            )));
        }

        tracing::Span::current().record("doc_id", req.doc_id);

        let engine = self.state.engine.write().await;

        match engine.insert(req.doc_id, req.embedding) {
            Ok(_) => {
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                info!(
                    doc_id = req.doc_id,
                    latency_ms = latency_ms,
                    "Document inserted successfully"
                );

                // FIXME Phase 1: Tier reporting is currently hardcoded as HotTier
                // TieredEngine.insert() should return which tier was used
                // For now, all inserts go to hot tier by design
                Ok(Response::new(InsertResponse {
                    success: true,
                    error: String::new(),
                    inserted_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    tier: insert_response::Tier::HotTier as i32,
                    total_inserted: 1,
                    total_failed: 0,
                }))
            }
            Err(e) => {
                error!(
                    doc_id = req.doc_id,
                    error = %e,
                    "Insert failed"
                );
                Err(Status::internal(format!("Insert failed: {}", e)))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn bulk_insert(
        &self,
        request: Request<tonic::Streaming<InsertRequest>>,
    ) -> Result<Response<InsertResponse>, Status> {
        let start = Instant::now();
        let mut stream = request.into_inner();

        let mut total_inserted = 0u64;
        let mut total_failed = 0u64;
        let mut last_error = String::new();
        let mut batch_count = 0u64;

        // CRITICAL FIX: Acquire lock per-operation to prevent deadlock
        // Holding write lock across stream.message().await causes deadlock
        while let Some(req) = stream.message().await? {
            batch_count += 1;

            // Check batch size limit to prevent memory exhaustion
            if batch_count > MAX_BATCH_SIZE as u64 {
                last_error = format!("Batch size exceeds maximum {}", MAX_BATCH_SIZE);
                total_failed += 1;
                break;
            }

            // Validate (no lock needed)
            if req.doc_id < MIN_DOC_ID {
                total_failed += 1;
                last_error = format!("Invalid doc_id: {}", req.doc_id);
                continue;
            }
            if req.embedding.is_empty() {
                total_failed += 1;
                last_error = format!("Empty embedding for doc_id {}", req.doc_id);
                continue;
            }
            if req.embedding.len() > MAX_EMBEDDING_DIM {
                total_failed += 1;
                last_error = format!(
                    "Embedding dimension {} exceeds maximum {}",
                    req.embedding.len(),
                    MAX_EMBEDDING_DIM
                );
                continue;
            }

            // Short-lived lock per insert operation
            {
                let engine = self.state.engine.write().await;
                match engine.insert(req.doc_id, req.embedding) {
                    Ok(_) => total_inserted += 1,
                    Err(e) => {
                        total_failed += 1;
                        last_error = format!("{}", e);
                    }
                }
                // Lock released here
            }
        }

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        info!(
            total_inserted = total_inserted,
            total_failed = total_failed,
            latency_ms = latency_ms,
            throughput_docs_per_sec = (total_inserted as f64 / start.elapsed().as_secs_f64()),
            "Bulk insert completed"
        );

        // FIXME Phase 1: Tier reporting hardcoded (see insert() method comment)
        Ok(Response::new(InsertResponse {
            success: total_failed == 0,
            error: last_error,
            inserted_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            tier: insert_response::Tier::HotTier as i32,
            total_inserted,
            total_failed,
        }))
    }

    #[instrument(skip(self, _request))]
    async fn delete(
        &self,
        _request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        // Phase 1: Deletion not yet implemented (requires tombstone tracking)
        warn!("Delete operation not yet implemented");

        Err(Status::unimplemented(
            "Delete operation will be implemented in Phase 1 (requires WAL tombstones)",
        ))
    }

    // ============================================================================
    // READ OPERATIONS
    // ============================================================================

    #[instrument(skip(self, request), fields(doc_id))]
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        if req.doc_id == 0 {
            return Err(Status::invalid_argument("doc_id must be non-zero"));
        }

        tracing::Span::current().record("doc_id", req.doc_id);

        let engine = self.state.engine.read().await;

        match engine.query(req.doc_id, None) {
            Some(embedding) => {
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

                info!(
                    doc_id = req.doc_id,
                    latency_ms = latency_ms,
                    embedding_returned = req.include_embedding,
                    "Document found"
                );

                Ok(Response::new(QueryResponse {
                    found: true,
                    doc_id: req.doc_id,
                    embedding: if req.include_embedding {
                        embedding
                    } else {
                        vec![]
                    },
                    metadata: HashMap::new(), // TODO: Phase 1
                    served_from: query_response::Tier::Unknown as i32, // TODO: Track tier
                    error: String::new(),
                }))
            }
            None => {
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

                info!(
                    doc_id = req.doc_id,
                    latency_ms = latency_ms,
                    "Document not found"
                );

                Ok(Response::new(QueryResponse {
                    found: false,
                    doc_id: req.doc_id,
                    embedding: vec![],
                    metadata: HashMap::new(),
                    served_from: query_response::Tier::Unknown as i32,
                    error: String::new(),
                }))
            }
        }
    }

    #[instrument(skip(self, request), fields(k, query_dim))]
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Validate input
        if req.query_embedding.is_empty() {
            return Err(Status::invalid_argument("query_embedding cannot be empty"));
        }
        if req.query_embedding.len() > MAX_EMBEDDING_DIM {
            return Err(Status::invalid_argument(format!(
                "query_embedding dimension {} exceeds maximum {}",
                req.query_embedding.len(),
                MAX_EMBEDDING_DIM
            )));
        }
        if req.k == 0 {
            return Err(Status::invalid_argument("k must be greater than 0"));
        }
        if req.k > MAX_KNN_K {
            return Err(Status::invalid_argument(format!(
                "k must be <= {}",
                MAX_KNN_K
            )));
        }

        tracing::Span::current().record("k", req.k);
        tracing::Span::current().record("query_dim", req.query_embedding.len());

        let engine = self.state.engine.read().await;

        match engine.knn_search(&req.query_embedding, req.k as usize) {
            Ok(results) => {
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

                // Convert distance to similarity score
                // CRITICAL: Assumes cosine distance metric from HNSW
                // - Cosine distance: 0 = identical, 2 = opposite vectors
                // - Similarity score: 1 = identical, -1 = opposite (more intuitive for API)
                // - Formula: score = 1 - distance
                // TODO Phase 1: Support multiple distance metrics (L2, inner product)
                let convert_distance_to_score = |dist: f32| -> f32 {
                    // Clamp to valid range to handle floating point errors
                    (1.0 - dist).max(-1.0).min(1.0)
                };

                // Filter by min_score if specified
                let filtered_results: Vec<SearchResult> = if req.min_score > 0.0 {
                    results
                        .into_iter()
                        .filter(|r| convert_distance_to_score(r.distance) >= req.min_score)
                        .collect()
                } else {
                    results
                };

                // Get embeddings if requested (requires additional query)
                let search_results: Vec<kyrodb::SearchResult> = if req.include_embeddings {
                    // Need to query each doc_id to get embedding
                    filtered_results
                        .into_iter()
                        .map(|r| {
                            let embedding = engine.query(r.doc_id, None).unwrap_or_default();
                            kyrodb::SearchResult {
                                doc_id: r.doc_id,
                                score: convert_distance_to_score(r.distance),
                                embedding,
                                metadata: HashMap::new(), // TODO: Phase 1
                            }
                        })
                        .collect()
                } else {
                    filtered_results
                        .into_iter()
                        .map(|r| kyrodb::SearchResult {
                            doc_id: r.doc_id,
                            score: convert_distance_to_score(r.distance),
                            embedding: vec![],
                            metadata: HashMap::new(), // TODO: Phase 1
                        })
                        .collect()
                };

                info!(
                    k = req.k,
                    results_found = search_results.len(),
                    latency_ms = latency_ms,
                    min_score = req.min_score,
                    "Search completed successfully"
                );

                Ok(Response::new(SearchResponse {
                    results: search_results.clone(),
                    total_found: search_results.len() as u32,
                    search_latency_ms: latency_ms as f32,
                    search_path: search_response::SearchPath::Unknown as i32, // TODO: Track path
                    error: String::new(),
                }))
            }
            Err(e) => {
                error!(
                    error = %e,
                    k = req.k,
                    "Search failed"
                );
                Err(Status::internal(format!("Search failed: {}", e)))
            }
        }
    }

    #[instrument(skip(self, _request))]
    async fn bulk_search(
        &self,
        _request: Request<Streaming<SearchRequest>>,
    ) -> Result<Response<Self::BulkSearchStream>, Status> {
        // TODO: Phase 1 - implement bulk search with batching
        Err(Status::unimplemented("bulk_search not yet implemented"))
    }

    // ============================================================================
    // HEALTH & OBSERVABILITY
    // ============================================================================

    #[instrument(skip(self, request))]
    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let req = request.into_inner();

        let engine = self.state.engine.read().await;
        let stats = engine.stats();

        // Determine overall health
        let status = if stats.total_queries > 0 {
            health_response::Status::Healthy
        } else {
            health_response::Status::Degraded
        };

        // Component-level health
        let mut components = HashMap::new();

        if req.component.is_empty() || req.component == "cache" {
            components.insert(
                "cache".to_string(),
                format!(
                    "healthy ({}% hit rate)",
                    (stats.cache_hit_rate * 100.0) as u32
                ),
            );
        }

        if req.component.is_empty() || req.component == "hot_tier" {
            components.insert(
                "hot_tier".to_string(),
                format!("healthy ({} docs)", stats.hot_tier_size),
            );
        }

        if req.component.is_empty() || req.component == "cold_tier" {
            components.insert(
                "cold_tier".to_string(),
                format!("healthy ({} docs)", stats.cold_tier_size),
            );
        }

        info!(
            status = ?status,
            components = components.len(),
            "Health check completed"
        );

        Ok(Response::new(HealthResponse {
            status: status as i32,
            version: env!("CARGO_PKG_VERSION").to_string(),
            components,
            uptime_seconds: self.state.start_time.elapsed().as_secs(),
            git_commit: env!("GIT_COMMIT_HASH").to_string(),
        }))
    }

    #[instrument(skip(self, _request))]
    async fn metrics(
        &self,
        _request: Request<MetricsRequest>,
    ) -> Result<Response<MetricsResponse>, Status> {
        let engine = self.state.engine.read().await;
        let stats = engine.stats();

        info!(
            cache_hit_rate = stats.cache_hit_rate,
            total_queries = stats.total_queries,
            total_inserts = stats.total_inserts,
            "Metrics retrieved"
        );

        Ok(Response::new(MetricsResponse {
            // Cache metrics
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
            cache_hit_rate: stats.cache_hit_rate * 100.0,
            cache_size: 0, // TODO: Expose from cache strategy

            // Hot tier metrics
            hot_tier_hits: stats.hot_tier_hits,
            hot_tier_misses: stats.hot_tier_misses,
            hot_tier_hit_rate: stats.hot_tier_hit_rate * 100.0,
            hot_tier_size: stats.hot_tier_size as u64,
            hot_tier_flushes: stats.hot_tier_flushes,

            // Cold tier metrics
            cold_tier_searches: stats.cold_tier_searches,
            cold_tier_size: stats.cold_tier_size as u64,

            // Performance metrics (TODO: Add histogram tracking)
            p50_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            total_queries: stats.total_queries,
            total_inserts: stats.total_inserts,
            queries_per_second: 0.0, // TODO: Track windowed QPS
            inserts_per_second: 0.0, // TODO: Track windowed IPS

            // System metrics (TODO: Add jemalloc integration)
            memory_usage_bytes: 0,
            disk_usage_bytes: 0,
            cpu_usage_percent: 0.0,

            // Overall metrics
            overall_hit_rate: stats.overall_hit_rate * 100.0,
            collected_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }))
    }

    // ============================================================================
    // ADMIN OPERATIONS
    // ============================================================================

    #[instrument(skip(self, request))]
    async fn flush_hot_tier(
        &self,
        request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        info!(force = req.force, "Flush hot tier requested");

        let engine = self.state.engine.write().await;

        match engine.flush_hot_tier() {
            Ok(docs_flushed) => {
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

                info!(
                    docs_flushed = docs_flushed,
                    latency_ms = latency_ms,
                    "Hot tier flushed successfully"
                );

                Ok(Response::new(FlushResponse {
                    success: true,
                    error: String::new(),
                    documents_flushed: docs_flushed as u64,
                    flush_duration_ms: latency_ms as f32,
                }))
            }
            Err(e) => {
                error!(error = %e, "Flush failed");
                Err(Status::internal(format!("Flush failed: {}", e)))
            }
        }
    }

    #[instrument(skip(self, _request))]
    async fn create_snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        // TODO: Phase 1 - implement manual snapshots
        Err(Status::unimplemented("create_snapshot not yet implemented"))
    }

    #[instrument(skip(self, _request))]
    async fn get_config(
        &self,
        _request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let config = &self.state.config;

        Ok(Response::new(ConfigResponse {
            hot_tier_max_size: config.hot_tier_max_size as u64,
            hot_tier_max_age_seconds: config.hot_tier_max_age.as_secs(),
            hnsw_max_elements: config.hnsw_max_elements as u64,
            data_dir: config.data_dir.clone().unwrap_or_default(),
            fsync_policy: format!("{:?}", config.fsync_policy),
            snapshot_interval: config.snapshot_interval as u64,
            flush_interval_seconds: config.flush_interval.as_secs(),
            embedding_dimension: 384, // TODO: Track dynamically
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }
}

// ============================================================================
// SERVER INITIALIZATION
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kyrodb_engine=info,kyrodb_server=info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .json()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true),
        )
        .init();

    info!("Starting KyroDB gRPC server v{}", env!("CARGO_PKG_VERSION"));
    info!("Git commit: {}", env!("GIT_COMMIT_HASH"));
    info!("Build target: {}", env!("TARGET_TRIPLE"));

    // Parse configuration from environment
    let data_dir = std::env::var("KYRODB_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
    let port = std::env::var("KYRODB_PORT")
        .unwrap_or_else(|_| "50051".to_string())
        .parse::<u16>()?;
    let hot_tier_max_size = std::env::var("KYRODB_HOT_TIER_SIZE")
        .unwrap_or_else(|_| "10000".to_string())
        .parse::<usize>()?;

    info!(
        data_dir = %data_dir,
        port = port,
        hot_tier_max_size = hot_tier_max_size,
        "Configuration loaded"
    );

    // Create engine configuration
    let config = TieredEngineConfig {
        hot_tier_max_size,
        hot_tier_max_age: Duration::from_secs(300), // 5 minutes
        hnsw_max_elements: 10_000_000,              // 10M vectors
        data_dir: Some(data_dir.clone()),
        fsync_policy: FsyncPolicy::Periodic(5000), // Fsync every 5 seconds
        snapshot_interval: 10_000,
        flush_interval: Duration::from_secs(60),
    };

    // Initialize or recover engine
    info!("Initializing TieredEngine...");

    // Create cache strategy (LRU for now, Learned in Phase 1)
    let cache_strategy = Box::new(LruCacheStrategy::new(5000));

    let engine = if std::path::Path::new(&data_dir).exists() {
        info!("Data directory exists, attempting recovery...");
        match TieredEngine::recover(cache_strategy, &data_dir, config.clone()) {
            Ok(engine) => {
                info!("Recovery successful");
                engine
            }
            Err(e) => {
                warn!(error = %e, "Recovery failed, creating new engine");
                let cache_strategy = Box::new(LruCacheStrategy::new(5000));
                // WORKAROUND Phase 0: HnswBackend requires at least 1 embedding
                // Phase 1 TODO: Support true empty database initialization
                let dummy_embedding = vec![vec![0.0; 384]]; // 384-dim zero vector
                TieredEngine::new(cache_strategy, dummy_embedding, config.clone())?
            }
        }
    } else {
        info!("Creating new TieredEngine...");
        // WORKAROUND Phase 0: HnswBackend requires at least 1 embedding
        // Phase 1 TODO: Support true empty database initialization
        let dummy_embedding = vec![vec![0.0; 384]]; // 384-dim zero vector
        TieredEngine::new(cache_strategy, dummy_embedding, config.clone())?
    };

    info!("TieredEngine initialized successfully");

    // Wrap engine in Arc<RwLock> for concurrent access
    let engine_arc = Arc::new(RwLock::new(engine));

    // Create shutdown channel for graceful shutdown
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Spawn background flush task with graceful shutdown
    let flush_engine = engine_arc.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        info!("Background flush task started (60s interval)");

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Acquire write lock only for flush operation
                    let engine = flush_engine.write().await;
                    match engine.flush_hot_tier() {
                        Ok(count) if count > 0 => {
                            info!(docs_flushed = count, "Background flush completed");
                        }
                        Ok(_) => {}  // No docs to flush
                        Err(e) => {
                            error!(error = %e, "Background flush failed");
                        }
                    }
                    // Lock released here
                }
                _ = shutdown_rx.recv() => {
                    info!("Background flush task shutting down gracefully");
                    // Perform final flush before shutdown
                    let engine = flush_engine.write().await;
                    if let Ok(count) = engine.flush_hot_tier() {
                        if count > 0 {
                            info!(docs_flushed = count, "Final flush completed on shutdown");
                        }
                    }
                    break;
                }
            }
        }
    });

    // Create gRPC service with engine Arc reference
    let service = KyroDBServiceImpl {
        state: Arc::new(ServerState {
            engine: engine_arc,
            start_time: Instant::now(),
            config: config.clone(),
            shutdown_tx,
        }),
    };

    let addr = format!("0.0.0.0:{}", port).parse()?;

    info!("gRPC server listening on {}", addr);
    info!("Server ready to accept connections");

    // Start gRPC server
    Server::builder()
        .add_service(KyroDbServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
