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

use clap::Parser;
use kyrodb_engine::{
    ErrorCategory, FsyncPolicy, HealthStatus, LruCacheStrategy, MetricsCollector, SearchResult,
    TieredEngine, TieredEngineConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, instrument, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// HTTP server for observability endpoints
use axum::{
    body::Body,
    extract::State as AxumState,
    http::{Response as HttpResponse, StatusCode},
    routing::get,
    Router,
};
use tower_http::trace::TraceLayer;

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
    app_config: kyrodb_engine::config::KyroDbConfig,
    engine_config: TieredEngineConfig,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
    metrics: MetricsCollector,
}

/// gRPC service implementation
struct KyroDBServiceImpl {
    state: Arc<ServerState>,
}

// KyroDBServiceImpl is constructed directly in main() with existing engine Arc

/// RAII guard for connection tracking
struct ConnectionGuard {
    metrics: MetricsCollector,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metrics.decrement_connections();
    }
}

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

        // Track connection
        self.state.metrics.increment_connections();
        let _conn_guard = ConnectionGuard {
            metrics: self.state.metrics.clone(),
        };

        // Validate input
        if req.doc_id < MIN_DOC_ID {
            self.state.metrics.record_error(ErrorCategory::Validation);
            return Err(Status::invalid_argument(format!(
                "doc_id must be >= {}",
                MIN_DOC_ID
            )));
        }
        if req.embedding.is_empty() {
            self.state.metrics.record_error(ErrorCategory::Validation);
            return Err(Status::invalid_argument("embedding cannot be empty"));
        }
        if req.embedding.len() > MAX_EMBEDDING_DIM {
            self.state.metrics.record_error(ErrorCategory::Validation);
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
                let latency_ns = start.elapsed().as_nanos() as u64;
                self.state.metrics.record_insert(true);
                
                info!(
                    doc_id = req.doc_id,
                    latency_ns = latency_ns,
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
                self.state.metrics.record_insert(false);
                self.state.metrics.record_error(ErrorCategory::Internal);
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

        // Track connection
        self.state.metrics.increment_connections();
        let _conn_guard = ConnectionGuard {
            metrics: self.state.metrics.clone(),
        };

        // Validate input
        if req.query_embedding.is_empty() {
            self.state.metrics.record_error(ErrorCategory::Validation);
            return Err(Status::invalid_argument("query_embedding cannot be empty"));
        }
        if req.query_embedding.len() > MAX_EMBEDDING_DIM {
            self.state.metrics.record_error(ErrorCategory::Validation);
            return Err(Status::invalid_argument(format!(
                "query_embedding dimension {} exceeds maximum {}",
                req.query_embedding.len(),
                MAX_EMBEDDING_DIM
            )));
        }
        if req.k == 0 {
            self.state.metrics.record_error(ErrorCategory::Validation);
            return Err(Status::invalid_argument("k must be greater than 0"));
        }
        if req.k > MAX_KNN_K {
            self.state.metrics.record_error(ErrorCategory::Validation);
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
                let latency_ns = start.elapsed().as_nanos() as u64;
                
                // Record metrics
                self.state.metrics.record_query_latency(latency_ns);
                self.state.metrics.record_hnsw_search(latency_ns);

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

                let latency_ms = latency_ns as f64 / 1_000_000.0;

                info!(
                    k = req.k,
                    results_found = search_results.len(),
                    latency_ns = latency_ns,
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
                self.state.metrics.record_query_failure();
                self.state.metrics.record_error(ErrorCategory::Internal);
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
        let engine_config = &self.state.engine_config;
        let app_config = &self.state.app_config;

        Ok(Response::new(ConfigResponse {
            hot_tier_max_size: engine_config.hot_tier_max_size as u64,
            hot_tier_max_age_seconds: engine_config.hot_tier_max_age.as_secs(),
            hnsw_max_elements: engine_config.hnsw_max_elements as u64,
            data_dir: engine_config.data_dir.clone().unwrap_or_default(),
            fsync_policy: format!("{:?}", engine_config.fsync_policy),
            snapshot_interval: engine_config.snapshot_interval as u64,
            flush_interval_seconds: engine_config.flush_interval.as_secs(),
            embedding_dimension: app_config.hnsw.dimension as u64,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }
}

// ============================================================================
// HTTP OBSERVABILITY ENDPOINTS
// ============================================================================

/// Prometheus /metrics endpoint handler
async fn metrics_handler(
    AxumState(state): AxumState<Arc<ServerState>>,
) -> HttpResponse<Body> {
    let prometheus_text = state.metrics.export_prometheus();
    
    HttpResponse::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(Body::from(prometheus_text))
        .unwrap()
}

/// Health check /health endpoint (liveness probe)
async fn health_handler(
    AxumState(state): AxumState<Arc<ServerState>>,
) -> HttpResponse<Body> {
    let health = state.metrics.health_status();
    
    let (status_code, body) = match health {
        HealthStatus::Healthy => (
            StatusCode::OK,
            serde_json::json!({
                "status": "healthy",
                "uptime_seconds": state.metrics.uptime().as_secs(),
            }),
        ),
        HealthStatus::Starting => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "status": "starting",
                "uptime_seconds": state.metrics.uptime().as_secs(),
            }),
        ),
        HealthStatus::Degraded { reason } => (
            StatusCode::OK,
            serde_json::json!({
                "status": "degraded",
                "reason": reason,
                "uptime_seconds": state.metrics.uptime().as_secs(),
            }),
        ),
        HealthStatus::Unhealthy { reason } => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "status": "unhealthy",
                "reason": reason,
                "uptime_seconds": state.metrics.uptime().as_secs(),
            }),
        ),
    };
    
    HttpResponse::builder()
        .status(status_code)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// Readiness check /ready endpoint (readiness probe)
async fn ready_handler(
    AxumState(state): AxumState<Arc<ServerState>>,
) -> HttpResponse<Body> {
    let health = state.metrics.health_status();
    
    let (status_code, body) = match health {
        HealthStatus::Healthy | HealthStatus::Degraded { .. } => (
            StatusCode::OK,
            serde_json::json!({
                "ready": true,
                "status": "ready",
            }),
        ),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "ready": false,
                "status": "not_ready",
            }),
        ),
    };
    
    HttpResponse::builder()
        .status(status_code)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// SLO status endpoint for alerting systems
async fn slo_handler(
    AxumState(state): AxumState<Arc<ServerState>>,
) -> HttpResponse<Body> {
    let slo = state.metrics.slo_status();
    
    let any_breach = slo.p99_latency_breached
        || slo.cache_hit_rate_breached
        || slo.error_rate_breached
        || slo.availability_breached;
    
    let status_code = if any_breach {
        StatusCode::OK // Still return 200, but include breach details
    } else {
        StatusCode::OK
    };
    
    let body = serde_json::json!({
        "slo_breaches": {
            "p99_latency": slo.p99_latency_breached,
            "cache_hit_rate": slo.cache_hit_rate_breached,
            "error_rate": slo.error_rate_breached,
            "availability": slo.availability_breached,
        },
        "current_metrics": {
            "p99_latency_ns": slo.current_p99_ns,
            "cache_hit_rate": slo.current_cache_hit_rate,
            "error_rate": slo.current_error_rate,
            "availability": slo.current_availability,
        },
        "slo_thresholds": {
            "p99_latency_ns": 1_000_000, // 1ms
            "min_cache_hit_rate": 0.70,
            "max_error_rate": 0.001,
            "min_availability": 0.999,
        },
    });
    
    HttpResponse::builder()
        .status(status_code)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

// ============================================================================
// SERVER INITIALIZATION
// ============================================================================
// CLI ARGUMENTS
// ============================================================================

/// KyroDB - High-performance vector database for RAG workloads
#[derive(Parser, Debug)]
#[command(name = "kyrodb_server")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Production gRPC server for KyroDB vector database", long_about = None)]
struct CliArgs {
    /// Path to configuration file (YAML or TOML)
    #[arg(short, long, env = "KYRODB_CONFIG")]
    config: Option<String>,
    
    /// Override gRPC server port
    #[arg(short, long, env = "KYRODB_PORT")]
    port: Option<u16>,
    
    /// Override data directory
    #[arg(short, long, env = "KYRODB_DATA_DIR")]
    data_dir: Option<String>,
    
    /// Generate example config file (yaml or toml) and exit
    #[arg(long, value_name = "FORMAT")]
    generate_config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize deadlock detection in debug builds
    kyrodb_engine::init_deadlock_detection();
    
    // Parse command-line arguments
    let cli_args = CliArgs::parse();
    
    // Handle --generate-config early exit
    if let Some(format) = cli_args.generate_config {
        match format.to_lowercase().as_str() {
            "yaml" | "yml" => {
                println!("{}", kyrodb_engine::config::generate_example_yaml());
                return Ok(());
            }
            "toml" => {
                println!("{}", kyrodb_engine::config::generate_example_toml());
                return Ok(());
            }
            _ => {
                eprintln!("Error: Invalid format '{}'. Use 'yaml' or 'toml'.", format);
                std::process::exit(1);
            }
        }
    }
    
    // Load configuration with priority: CLI args > env vars > config file > defaults
    let mut config = kyrodb_engine::config::KyroDbConfig::load(cli_args.config.as_deref())?;
    
    // Apply CLI overrides (highest priority)
    if let Some(port) = cli_args.port {
        config.server.port = port;
    }
    if let Some(data_dir) = cli_args.data_dir {
        config.persistence.data_dir = data_dir.into();
    }
    
    // Validate final configuration
    config.validate()?;
    
    // Initialize structured logging based on config
    let (non_blocking, _guard) = if let Some(log_file) = &config.logging.file {
        match std::fs::File::create(log_file) {
            Ok(file) => tracing_appender::non_blocking(file),
            Err(e) => {
                eprintln!("Warning: Failed to create log file {:?}: {}. Falling back to stdout.", log_file, e);
                tracing_appender::non_blocking(std::io::stdout())
            }
        }
    } else {
        tracing_appender::non_blocking(std::io::stdout())
    };

    let log_level = config.logging.level.as_str();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| format!("kyrodb_engine={},kyrodb_server={}", log_level, log_level).into());

    match config.logging.format {
        kyrodb_engine::config::LogFormat::Json => {
            tracing_subscriber::registry()
                .with(env_filter)
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
        }
        kyrodb_engine::config::LogFormat::Text => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(non_blocking)
                        .with_target(true)
                        .with_thread_ids(true)
                        .with_file(true)
                        .with_line_number(true),
                )
                .init();
        }
    }

    info!("Starting KyroDB gRPC server v{}", env!("CARGO_PKG_VERSION"));
    info!("Git commit: {}", env!("GIT_COMMIT_HASH"));
    info!("Build target: {}", env!("TARGET_TRIPLE"));
    info!(
        grpc_port = config.server.port,
        http_port = config.http_port(),
        data_dir = %config.persistence.data_dir.display(),
        cache_capacity = config.cache.capacity,
        cache_strategy = ?config.cache.strategy,
        hnsw_max_elements = config.hnsw.max_elements,
        hnsw_m = config.hnsw.m,
        hnsw_ef_construction = config.hnsw.ef_construction,
        hnsw_ef_search = config.hnsw.ef_search,
        log_level = log_level,
        "Configuration loaded"
    );

    // Create engine configuration from loaded config
    let fsync_policy = match config.persistence.fsync_policy {
        kyrodb_engine::config::FsyncPolicy::None => FsyncPolicy::Never,
        kyrodb_engine::config::FsyncPolicy::DataOnly => {
            FsyncPolicy::Periodic(config.persistence.wal_flush_interval_ms)
        }
        kyrodb_engine::config::FsyncPolicy::Full => {
            FsyncPolicy::Always  // Most conservative for "full"
        }
    };
    
    let engine_config = TieredEngineConfig {
        hot_tier_max_size: config.cache.capacity,
        hot_tier_max_age: Duration::from_secs(config.cache.training_interval_secs),
        hnsw_max_elements: config.hnsw.max_elements,
        data_dir: Some(config.persistence.data_dir.to_string_lossy().to_string()),
        fsync_policy,
        snapshot_interval: config.persistence.snapshot_interval_secs as usize,
        flush_interval: config.wal_flush_interval(),
        cache_timeout_ms: config.timeouts.cache_ms,
        hot_tier_timeout_ms: config.timeouts.hot_tier_ms,
        cold_tier_timeout_ms: config.timeouts.cold_tier_ms,
        max_concurrent_queries: 1000, // Load shedding: max 1000 in-flight queries
    };

    // Initialize or recover engine
    info!("Initializing TieredEngine...");

    // Create cache strategy (LRU for now, Learned in Phase 1)
    let cache_strategy = Box::new(LruCacheStrategy::new(config.cache.capacity));

    let data_dir_path = config.persistence.data_dir.clone();
    let engine = if data_dir_path.exists() {
        info!("Data directory exists, attempting recovery...");
        match TieredEngine::recover(cache_strategy, data_dir_path.to_str().unwrap(), engine_config.clone()) {
            Ok(engine) => {
                info!("Recovery successful");
                engine
            }
            Err(e) => {
                warn!(error = %e, "Recovery failed, creating new engine");
                let cache_strategy = Box::new(LruCacheStrategy::new(config.cache.capacity));
                // WORKAROUND Phase 0: HnswBackend requires at least 1 embedding
                // Phase 1 TODO: Support true empty database initialization
                let dummy_embedding = vec![vec![0.0; config.hnsw.dimension]]; // Zero vector with configured dimension
                TieredEngine::new(cache_strategy, dummy_embedding, engine_config.clone())?
            }
        }
    } else {
        info!("Creating new TieredEngine...");
        // WORKAROUND Phase 0: HnswBackend requires at least 1 embedding
        // Phase 1 TODO: Support true empty database initialization
        let dummy_embedding = vec![vec![0.0; config.hnsw.dimension]]; // Zero vector with configured dimension
        TieredEngine::new(cache_strategy, dummy_embedding, engine_config.clone())?
    };

    info!("TieredEngine initialized successfully");

    // Wrap engine in Arc<RwLock> for concurrent access
    let engine_arc = Arc::new(RwLock::new(engine));

    // Create metrics collector
    let metrics = MetricsCollector::new();

    // Create shutdown channel for graceful shutdown
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Spawn background flush task with graceful shutdown
    let flush_engine = engine_arc.clone();
    let flush_metrics = metrics.clone();
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
                            flush_metrics.record_flush();
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
                            flush_metrics.record_flush();
                        }
                    }
                    break;
                }
            }
        }
    });

    // Create shared server state
    let state = Arc::new(ServerState {
        engine: engine_arc,
        start_time: Instant::now(),
        app_config: config.clone(),
        engine_config: engine_config.clone(),
        shutdown_tx,
        metrics: metrics.clone(),
    });

    // Create gRPC service with engine Arc reference
    let grpc_service = KyroDBServiceImpl {
        state: state.clone(),
    };

    // Build HTTP router for observability endpoints
    let http_app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/slo", get(slo_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    // HTTP port for observability (from config)
    let http_port = config.http_port();
    let http_addr = format!("{}:{}", config.server.host, http_port)
        .parse::<std::net::SocketAddr>()?;

    info!("HTTP observability server listening on http://{}", http_addr);
    info!("  GET /metrics  - Prometheus metrics");
    info!("  GET /health   - Liveness probe");
    info!("  GET /ready    - Readiness probe");
    info!("  GET /slo      - SLO breach status");

    // Spawn HTTP server with proper error handling
    let http_addr_clone = http_addr;
    let _http_handle = tokio::spawn(async move {
        match tokio::net::TcpListener::bind(http_addr_clone).await {
            Ok(listener) => {
                info!("HTTP listener bound successfully on {}", http_addr_clone);
                if let Err(e) = axum::serve(listener, http_app).await {
                    error!(error = %e, "HTTP observability server failed");
                    return Err(anyhow::anyhow!("HTTP server error: {}", e));
                }
                Ok(())
            }
            Err(e) => {
                error!(
                    error = %e,
                    addr = %http_addr_clone,
                    "Failed to bind HTTP observability listener - port may be in use"
                );
                Err(anyhow::anyhow!("Failed to bind HTTP listener: {}", e))
            }
        }
    });

    // Give HTTP server a moment to bind before marking ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Mark server as ready after initialization
    metrics.mark_ready();

    let grpc_addr = format!("{}:{}", config.server.host, config.server.port).parse()?;

    info!("gRPC server listening on {}", grpc_addr);
    info!("Server ready to accept connections");

    // Start gRPC server (blocking)
    Server::builder()
        .add_service(KyroDbServiceServer::new(grpc_service))
        .serve(grpc_addr)
        .await?;

    Ok(())
}
