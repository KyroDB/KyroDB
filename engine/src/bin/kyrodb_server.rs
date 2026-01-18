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

use anyhow::Context;
use clap::Parser;
use kyrodb_engine::{
    cache_strategy::{AbTestSplitter, LearnedCacheStrategy, LruCacheStrategy},
    AuthManager, ErrorCategory, FsyncPolicy, HealthStatus, LearnedCachePredictor, MetricsCollector,
    RateLimiter, TenantInfo, TieredEngine, TieredEngineConfig,
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
    http::{header::CONTENT_TYPE, HeaderValue, Response as HttpResponse, StatusCode},
    routing::get,
    Router,
};
use tower_http::trace::TraceLayer;

// Generated protobuf code
// Generated protobuf code

use kyrodb_engine::proto as kyrodb;

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

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ============================================================================
// AUTH / TENANCY
// ============================================================================

const API_KEY_HEADER: &str = "x-api-key";
const AUTHORIZATION_HEADER: &str = "authorization";

#[derive(Debug, Clone)]
struct TenantContext {
    tenant_id: String,
    tenant_index: u32,
    max_qps: u32,
}

/// Stable tenant-local → global doc_id mapping.
///
/// global_doc_id layout:
/// - high 32 bits: tenant_index
/// - low  32 bits: tenant-local doc_id
#[derive(Debug)]
struct TenantIdMapper {
    path: std::path::PathBuf,
    map: parking_lot::RwLock<std::collections::HashMap<String, u32>>,
}

impl TenantIdMapper {
    fn load_or_create(path: std::path::PathBuf, tenants: &[TenantInfo]) -> anyhow::Result<Self> {
        if path.exists() {
            let bytes = std::fs::read(&path).map_err(|e| {
                anyhow::anyhow!("Failed to read tenant map {}: {}", path.display(), e)
            })?;
            let map: std::collections::HashMap<String, u32> = serde_json::from_slice(&bytes)
                .map_err(|e| {
                    anyhow::anyhow!("Failed to parse tenant map {}: {}", path.display(), e)
                })?;
            return Ok(Self {
                path,
                map: parking_lot::RwLock::new(map),
            });
        }

        let mut tenant_ids = tenants
            .iter()
            .map(|t| t.tenant_id.clone())
            .collect::<Vec<_>>();
        tenant_ids.sort();
        tenant_ids.dedup();

        let mut map = std::collections::HashMap::with_capacity(tenant_ids.len());
        for (idx, tenant_id) in tenant_ids.into_iter().enumerate() {
            let idx_u32: u32 = idx
                .try_into()
                .map_err(|_| anyhow::anyhow!("Too many tenants for u32 tenant_index"))?;
            map.insert(tenant_id, idx_u32);
        }

        Self::persist_map_atomic(&path, &map)?;

        Ok(Self {
            path,
            map: parking_lot::RwLock::new(map),
        })
    }

    fn ensure_tenant(&self, tenant_id: &str) -> anyhow::Result<u32> {
        if let Some(idx) = self.map.read().get(tenant_id).copied() {
            return Ok(idx);
        }

        let mut map = self.map.write();
        if let Some(idx) = map.get(tenant_id).copied() {
            return Ok(idx);
        }

        let next_idx = map
            .len()
            .try_into()
            .map_err(|_| anyhow::anyhow!("tenant_index overflow"))?;
        map.insert(tenant_id.to_string(), next_idx);

        let map_snapshot = map.clone();
        let path = self.path.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(move || Self::persist_map_atomic(&path, &map_snapshot));
        } else {
            Self::persist_map_atomic(&path, &map_snapshot)?;
        }
        Ok(next_idx)
    }

    fn persist_map_atomic(
        path: &std::path::Path,
        map: &std::collections::HashMap<String, u32>,
    ) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec_pretty(map)
            .map_err(|e| anyhow::anyhow!("Failed to serialize tenant map: {}", e))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create tenant map dir {}: {}",
                    parent.display(),
                    e
                )
            })?;
        }

        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Tenant map path has no parent"))?;
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Tenant map path has no file name"))?;
        let tmp_name = format!(
            ".{}.tmp.{}",
            file_name.to_string_lossy(),
            std::process::id()
        );
        let tmp_path = parent.join(tmp_name);

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| anyhow::anyhow!("Failed to create temp tenant map: {}", e))?;

        if path.exists() {
            if let Ok(metadata) = std::fs::metadata(path) {
                let _ = std::fs::set_permissions(&tmp_path, metadata.permissions());
            }
        }

        use std::io::Write;
        file.write_all(&bytes)
            .map_err(|e| anyhow::anyhow!("Failed to write tenant map: {}", e))?;
        file.sync_all()
            .map_err(|e| anyhow::anyhow!("Failed to fsync tenant map: {}", e))?;
        drop(file);

        std::fs::rename(&tmp_path, path)
            .map_err(|e| anyhow::anyhow!("Failed to rename tenant map: {}", e))?;

        let dir = std::fs::File::open(parent)
            .map_err(|e| anyhow::anyhow!("Failed to open tenant map dir: {}", e))?;
        dir.sync_all()
            .map_err(|e| anyhow::anyhow!("Failed to fsync tenant map dir: {}", e))?;

        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn to_global_doc_id(tenant_index: u32, local_doc_id: u64) -> Result<u64, Status> {
        if local_doc_id > u32::MAX as u64 {
            return Err(Status::invalid_argument(
                "doc_id exceeds tenant-local max (u32)",
            ));
        }
        Ok(((tenant_index as u64) << 32) | local_doc_id)
    }

    fn is_tenant_doc_id(tenant_index: u32, global_doc_id: u64) -> bool {
        (global_doc_id >> 32) as u32 == tenant_index
    }

    fn to_local_doc_id(global_doc_id: u64) -> u64 {
        global_doc_id & 0xFFFF_FFFF
    }
}

// ============================================================================
// SERVER STATE
// ============================================================================

/// Server state - holds engine and runtime metrics
struct ServerState {
    engine: Arc<RwLock<TieredEngine>>,
    start_time: Instant,
    app_config: kyrodb_engine::config::KyroDbConfig,
    engine_config: TieredEngineConfig,
    #[allow(dead_code)] // Used in shutdown sequence, not via field access
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    metrics: MetricsCollector,

    auth: Option<AuthManager>,
    rate_limiter: Option<RateLimiter>,
    tenant_id_mapper: Option<TenantIdMapper>,
}

/// gRPC service implementation
struct KyroDBServiceImpl {
    state: Arc<ServerState>,
}

impl KyroDBServiceImpl {
    #[allow(clippy::result_large_err)]
    fn tenant_context<T>(&self, request: &Request<T>) -> Result<Option<TenantContext>, Status> {
        if !self.state.app_config.auth.enabled {
            return Ok(None);
        }

        request
            .extensions()
            .get::<TenantContext>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("missing tenant context"))
            .map(Some)
    }

    #[allow(clippy::result_large_err)]
    fn enforce_rate_limit(&self, tenant: Option<&TenantContext>) -> Result<(), Status> {
        let Some(tenant) = tenant else {
            return Ok(());
        };
        let limiter = self
            .state
            .rate_limiter
            .as_ref()
            .ok_or_else(|| Status::internal("rate limiter not initialized"))?;
        if limiter.check_limit(&tenant.tenant_id, tenant.max_qps) {
            Ok(())
        } else {
            Err(Status::resource_exhausted("rate limit exceeded"))
        }
    }

    #[allow(clippy::result_large_err)]
    fn map_doc_id(&self, tenant: Option<&TenantContext>, local_doc_id: u64) -> Result<u64, Status> {
        if let Some(tenant) = tenant {
            Ok(TenantIdMapper::to_global_doc_id(
                tenant.tenant_index,
                local_doc_id,
            )?)
        } else {
            Ok(local_doc_id)
        }
    }

    fn unmap_doc_id(&self, tenant: Option<&TenantContext>, global_doc_id: u64) -> u64 {
        if let Some(tenant) = tenant {
            if TenantIdMapper::is_tenant_doc_id(tenant.tenant_index, global_doc_id) {
                TenantIdMapper::to_local_doc_id(global_doc_id)
            } else {
                0
            }
        } else {
            global_doc_id
        }
    }

    async fn handle_search_request(
        &self,
        tenant: Option<&TenantContext>,
        req: SearchRequest,
    ) -> Result<SearchResponse, Status> {
        let start = Instant::now();
        self.enforce_rate_limit(tenant)?;

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

        if req.ef_search > 10_000 {
            self.state.metrics.record_error(ErrorCategory::Validation);
            return Err(Status::invalid_argument(
                "ef_search must be <= 10000 (0 = server default)",
            ));
        }

        // Adaptive oversampling based on filter complexity
        // Namespace filtering also requires oversampling since we filter post-fetch
        let _has_filter = req.filter.is_some();
        let has_namespace = !req.namespace.is_empty();
        let base_oversampling = if let Some(ref filter) = req.filter {
            kyrodb_engine::adaptive_oversampling::calculate_oversampling_factor(filter)
        } else {
            1
        };
        let oversampling_factor = if has_namespace {
            base_oversampling.saturating_mul(4).min(10)
        } else {
            base_oversampling
        };
        let search_k = (req.k as usize)
            .saturating_mul(oversampling_factor)
            .min(10_000);

        let engine = self.state.engine.read().await;

        let ef_search_override = if req.ef_search == 0 {
            None
        } else {
            Some(req.ef_search as usize)
        };

        let results = engine
            .knn_search_with_ef(&req.query_embedding, search_k, ef_search_override)
            .map_err(|e| {
                self.state.metrics.record_query_failure();
                self.state.metrics.record_error(ErrorCategory::Internal);
                Status::internal(format!("Search failed: {}", e))
            })?;

        let latency_ns = start.elapsed().as_nanos() as u64;
        self.state.metrics.record_query_latency(latency_ns);
        self.state.metrics.record_hnsw_search(latency_ns);

        // Convert engine distance to API score (higher is better).
        // - Cosine: distance in [0, 2], score = 1 - distance (in [-1, 1])
        // - Euclidean/L2: score = -distance (avoids clamping, preserves ordering and magnitude)
        // - Inner product: engine returns a distance-like value; use -distance for monotonicity
        let convert_distance_to_score = |dist: f32| -> f32 {
            use kyrodb_engine::config::DistanceMetric;

            match self.state.app_config.hnsw.distance {
                DistanceMetric::Cosine => 1.0 - dist,
                DistanceMetric::Euclidean | DistanceMetric::InnerProduct => -dist,
            }
        };

        let mut final_results = Vec::new();
        let mut candidates = results.into_iter();

        // Iterate candidates until we fill k results or run out
        while final_results.len() < req.k as usize {
            let candidate = match candidates.next() {
                Some(c) => c,
                None => break,
            };

            if let Some(tenant) = tenant {
                if !TenantIdMapper::is_tenant_doc_id(tenant.tenant_index, candidate.doc_id) {
                    continue;
                }
            }

            // Check min_score first (cheap)
            let score = convert_distance_to_score(candidate.distance);
            if req.min_score > 0.0 && score < req.min_score {
                continue;
            }

            // Fetch metadata (needed for filtering and/or response)
            let metadata = engine.get_metadata(candidate.doc_id).unwrap_or_default();

            if let Some(tenant) = tenant {
                let expected = tenant.tenant_index.to_string();
                if metadata.get("__tenant_idx__") != Some(&expected) {
                    continue;
                }
            }

            // Filter by namespace if specified
            if has_namespace {
                let doc_namespace = metadata
                    .get("__namespace__")
                    .map(|s| s.as_str())
                    .unwrap_or("");
                if doc_namespace != req.namespace {
                    continue;
                }
            }

            // Apply metadata filter if present
            if let Some(filter) = &req.filter {
                if !kyrodb_engine::metadata_filter::matches(filter, &metadata) {
                    continue;
                }
            }

            // Fetch embedding if requested (expensive)
            let embedding = if req.include_embeddings {
                engine.query(candidate.doc_id, None).unwrap_or_default()
            } else {
                vec![]
            };

            let local_doc_id = self.unmap_doc_id(tenant, candidate.doc_id);
            if tenant.is_some() && local_doc_id < MIN_DOC_ID {
                continue;
            }

            final_results.push(kyrodb::SearchResult {
                doc_id: local_doc_id,
                score,
                embedding,
                metadata,
            });
        }

        let latency_ms = latency_ns as f64 / 1_000_000.0;

        Ok(SearchResponse {
            results: final_results.clone(),
            total_found: final_results.len() as u32,
            search_latency_ms: latency_ms as f32,
            search_path: search_response::SearchPath::Unknown as i32,
            error: String::new(),
        })
    }
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
        let tenant = self.tenant_context(&request)?;
        self.enforce_rate_limit(tenant.as_ref())?;
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

        let global_doc_id = self.map_doc_id(tenant.as_ref(), req.doc_id)?;

        let engine = self.state.engine.write().await;

        // Store namespace in metadata for filtering during search
        // Namespace is stored as reserved key "__namespace__" to avoid conflicts with user metadata
        let mut metadata = req.metadata;
        if let Some(tenant) = &tenant {
            metadata.insert("__tenant_id__".to_string(), tenant.tenant_id.clone());
            metadata.insert(
                "__tenant_idx__".to_string(),
                tenant.tenant_index.to_string(),
            );
        }
        if !req.namespace.is_empty() {
            metadata.insert("__namespace__".to_string(), req.namespace.clone());
        }

        match engine.insert(global_doc_id, req.embedding, metadata) {
            Ok(_) => {
                let latency_ns = start.elapsed().as_nanos() as u64;
                self.state.metrics.record_insert(true);

                info!(
                    doc_id = req.doc_id,
                    latency_ns = latency_ns,
                    "Document inserted successfully"
                );

                // Note: Tier reporting is currently hardcoded as HotTier.
                // TieredEngine.insert() will be updated to return which tier was used in future iterations.
                // Currently, all inserts go to hot tier by design.
                Ok(Response::new(InsertResponse {
                    success: true,
                    error: String::new(),
                    inserted_at: unix_timestamp_secs(),
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
        let tenant = self.tenant_context(&request)?;
        let mut stream = request.into_inner();

        let mut total_inserted = 0u64;
        let mut total_failed = 0u64;
        let mut last_error = String::new();
        let mut batch_count = 0u64;

        // Acquire lock per-operation to prevent deadlock
        // Holding write lock across stream.message().await causes deadlock
        while let Some(req) = stream.message().await? {
            self.enforce_rate_limit(tenant.as_ref())?;
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
                let global_doc_id = match self.map_doc_id(tenant.as_ref(), req.doc_id) {
                    Ok(id) => id,
                    Err(e) => {
                        total_failed += 1;
                        last_error = e.message().to_string();
                        continue;
                    }
                };

                let mut metadata = req.metadata;
                if let Some(tenant) = &tenant {
                    metadata.insert("__tenant_id__".to_string(), tenant.tenant_id.clone());
                    metadata.insert(
                        "__tenant_idx__".to_string(),
                        tenant.tenant_index.to_string(),
                    );
                }
                if !req.namespace.is_empty() {
                    metadata.insert("__namespace__".to_string(), req.namespace.clone());
                }

                let engine = self.state.engine.write().await;
                match engine.insert(global_doc_id, req.embedding, metadata) {
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

        // Note: Tier reporting hardcoded (see insert() method comment)
        Ok(Response::new(InsertResponse {
            success: total_failed == 0,
            error: last_error,
            inserted_at: unix_timestamp_secs(),
            tier: insert_response::Tier::HotTier as i32,
            total_inserted,
            total_failed,
        }))
    }

    /// Bulk load directly to HNSW index (bypasses hot tier)
    ///
    /// This provides maximum indexing speed for benchmarks and data migrations.
    /// Warning: Data is NOT persisted to WAL during load.
    #[instrument(skip(self, request))]
    async fn bulk_load_hnsw(
        &self,
        request: Request<tonic::Streaming<InsertRequest>>,
    ) -> Result<Response<BulkLoadResponse>, Status> {
        let start = Instant::now();
        let tenant = self.tenant_context(&request)?;
        let mut stream = request.into_inner();

        // Collect all documents first (can't hold lock across stream)
        let mut documents: Vec<(u64, Vec<f32>, std::collections::HashMap<String, String>)> =
            Vec::new();
        let mut validation_errors = 0u64;
        let mut last_error = String::new();

        info!("BulkLoadHnsw: starting to collect documents");

        while let Some(req) = stream.message().await? {
            // Validate doc_id
            if req.doc_id < MIN_DOC_ID {
                validation_errors += 1;
                last_error = format!("Invalid doc_id: {} (must be >= {})", req.doc_id, MIN_DOC_ID);
                continue;
            }

            // Validate embedding
            if req.embedding.is_empty() {
                validation_errors += 1;
                last_error = format!("Empty embedding for doc_id {}", req.doc_id);
                continue;
            }

            if req.embedding.len() > MAX_EMBEDDING_DIM {
                validation_errors += 1;
                last_error = format!(
                    "Embedding dimension {} exceeds maximum {}",
                    req.embedding.len(),
                    MAX_EMBEDDING_DIM
                );
                continue;
            }

            // Map doc_id for multi-tenancy
            let global_doc_id = match self.map_doc_id(tenant.as_ref(), req.doc_id) {
                Ok(id) => id,
                Err(e) => {
                    validation_errors += 1;
                    last_error = e.message().to_string();
                    continue;
                }
            };

            // Build metadata with tenant info
            let mut metadata = req.metadata;
            if let Some(tenant) = &tenant {
                metadata.insert("__tenant_id__".to_string(), tenant.tenant_id.clone());
                metadata.insert(
                    "__tenant_idx__".to_string(),
                    tenant.tenant_index.to_string(),
                );
            }
            if !req.namespace.is_empty() {
                metadata.insert("__namespace__".to_string(), req.namespace.clone());
            }

            documents.push((global_doc_id, req.embedding, metadata));
        }

        let collection_time = start.elapsed();
        info!(
            doc_count = documents.len(),
            validation_errors,
            collection_ms = collection_time.as_millis() as u64,
            "BulkLoadHnsw: collected all documents, starting HNSW load"
        );

        if documents.is_empty() {
            return Ok(Response::new(BulkLoadResponse {
                success: validation_errors == 0,
                error: if validation_errors > 0 {
                    last_error
                } else {
                    String::new()
                },
                total_loaded: 0,
                total_failed: validation_errors,
                load_duration_ms: 0.0,
                avg_insert_rate: 0.0,
                peak_memory_bytes: 0,
            }));
        }

        // Now bulk load to HNSW (single lock acquisition)
        let engine = self.state.engine.write().await;
        let result = engine.bulk_load_cold_tier(documents);

        match result {
            Ok((loaded, failed, duration_ms, rate)) => {
                let total_time = start.elapsed();
                info!(
                    loaded,
                    failed,
                    validation_errors,
                    duration_ms,
                    rate_per_sec = rate,
                    total_ms = total_time.as_millis() as u64,
                    "BulkLoadHnsw: complete"
                );

                Ok(Response::new(BulkLoadResponse {
                    success: failed == 0 && validation_errors == 0,
                    error: if failed > 0 || validation_errors > 0 {
                        format!(
                            "Loaded {} docs; {} failed validation, {} failed insertion",
                            loaded, validation_errors, failed
                        )
                    } else {
                        String::new()
                    },
                    total_loaded: loaded,
                    total_failed: failed + validation_errors,
                    load_duration_ms: duration_ms,
                    avg_insert_rate: rate,
                    peak_memory_bytes: 0, // Could integrate sysinfo crate if needed
                }))
            }
            Err(e) => {
                error!(error = %e, "BulkLoadHnsw: failed");
                Err(Status::internal(format!("Bulk load failed: {}", e)))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let start = Instant::now();
        let tenant = self.tenant_context(&request)?;
        self.enforce_rate_limit(tenant.as_ref())?;
        let req = request.into_inner();

        if req.doc_id < MIN_DOC_ID {
            return Err(Status::invalid_argument(format!(
                "doc_id must be >= {}",
                MIN_DOC_ID
            )));
        }

        tracing::Span::current().record("doc_id", req.doc_id);

        let global_doc_id = self.map_doc_id(tenant.as_ref(), req.doc_id)?;

        let engine = self.state.engine.write().await;

        // Enforce namespace boundary (treat mismatch as not-found to avoid leakage).
        if !req.namespace.is_empty() {
            let metadata = match engine.get_metadata(global_doc_id) {
                Some(m) => m,
                None => {
                    return Ok(Response::new(DeleteResponse {
                        success: true,
                        error: String::new(),
                        existed: false,
                    }));
                }
            };

            let doc_namespace = metadata
                .get("__namespace__")
                .map(|s| s.as_str())
                .unwrap_or("");
            if doc_namespace != req.namespace {
                return Ok(Response::new(DeleteResponse {
                    success: true,
                    error: String::new(),
                    existed: false,
                }));
            }
        }

        match engine.delete(global_doc_id) {
            Ok(existed) => {
                let latency_ns = start.elapsed().as_nanos() as u64;

                if existed {
                    info!(
                        doc_id = req.doc_id,
                        latency_ns = latency_ns,
                        "Document deleted"
                    );
                } else {
                    info!(
                        doc_id = req.doc_id,
                        latency_ns = latency_ns,
                        "Document not found for deletion"
                    );
                }

                Ok(Response::new(DeleteResponse {
                    success: true,
                    error: String::new(),
                    existed,
                }))
            }
            Err(e) => {
                self.state.metrics.record_error(ErrorCategory::Internal);
                error!(
                    doc_id = req.doc_id,
                    error = %e,
                    "Delete failed"
                );
                Err(Status::internal(format!("Delete failed: {}", e)))
            }
        }
    }

    #[instrument(skip(self, request), fields(doc_id, merge))]
    async fn update_metadata(
        &self,
        request: Request<kyrodb::UpdateMetadataRequest>,
    ) -> Result<Response<kyrodb::UpdateMetadataResponse>, Status> {
        let start = Instant::now();
        let tenant = self.tenant_context(&request)?;
        self.enforce_rate_limit(tenant.as_ref())?;
        let req = request.into_inner();

        if req.doc_id == 0 {
            return Err(Status::invalid_argument("doc_id must be non-zero"));
        }

        tracing::Span::current().record("doc_id", req.doc_id);
        tracing::Span::current().record("merge", req.merge);

        let global_doc_id = self.map_doc_id(tenant.as_ref(), req.doc_id)?;

        let engine = self.state.engine.write().await;

        // Enforce namespace boundary (treat mismatch as not-found to avoid leakage).
        if !req.namespace.is_empty() {
            let metadata = match engine.get_metadata(global_doc_id) {
                Some(m) => m,
                None => {
                    return Ok(Response::new(kyrodb::UpdateMetadataResponse {
                        success: true,
                        error: String::new(),
                        existed: false,
                    }));
                }
            };
            let doc_namespace = metadata
                .get("__namespace__")
                .map(|s| s.as_str())
                .unwrap_or("");
            if doc_namespace != req.namespace {
                return Ok(Response::new(kyrodb::UpdateMetadataResponse {
                    success: true,
                    error: String::new(),
                    existed: false,
                }));
            }
        }

        let mut metadata = req.metadata;
        if let Some(tenant) = &tenant {
            metadata.insert("__tenant_id__".to_string(), tenant.tenant_id.clone());
            metadata.insert(
                "__tenant_idx__".to_string(),
                tenant.tenant_index.to_string(),
            );
        }
        if !req.namespace.is_empty() {
            metadata.insert("__namespace__".to_string(), req.namespace.clone());
        }

        match engine.update_metadata(global_doc_id, metadata, req.merge) {
            Ok(existed) => {
                let latency_ns = start.elapsed().as_nanos() as u64;
                // Record as query operation (generic operation metric)
                self.state.metrics.record_query_latency(latency_ns);

                if existed {
                    info!(
                        doc_id = req.doc_id,
                        merge = req.merge,
                        latency_ns = latency_ns,
                        "Metadata updated successfully"
                    );
                } else {
                    warn!(
                        doc_id = req.doc_id,
                        "Document not found for metadata update"
                    );
                }

                Ok(Response::new(kyrodb::UpdateMetadataResponse {
                    success: true,
                    error: String::new(),
                    existed,
                }))
            }
            Err(e) => {
                self.state.metrics.record_query_failure();
                self.state.metrics.record_error(ErrorCategory::Internal);
                error!(
                    doc_id = req.doc_id,
                    error = %e,
                    "Metadata update failed"
                );
                Err(Status::internal(format!("Metadata update failed: {}", e)))
            }
        }
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
        let tenant = self.tenant_context(&request)?;
        self.enforce_rate_limit(tenant.as_ref())?;
        let req = request.into_inner();

        if req.doc_id == 0 {
            return Err(Status::invalid_argument("doc_id must be non-zero"));
        }

        tracing::Span::current().record("doc_id", req.doc_id);

        let global_doc_id = self.map_doc_id(tenant.as_ref(), req.doc_id)?;

        let engine = self.state.engine.read().await;

        match engine.query(global_doc_id, None) {
            Some(embedding) => {
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

                // Fetch metadata for the document
                let metadata = engine.get_metadata(global_doc_id).unwrap_or_default();

                if let Some(tenant) = &tenant {
                    let expected = tenant.tenant_index.to_string();
                    if metadata.get("__tenant_idx__") != Some(&expected) {
                        // Hide cross-tenant existence.
                        return Ok(Response::new(QueryResponse {
                            found: false,
                            doc_id: req.doc_id,
                            embedding: vec![],
                            metadata: HashMap::new(),
                            served_from: query_response::Tier::Unknown as i32,
                            error: String::new(),
                        }));
                    }
                }

                if !req.namespace.is_empty() {
                    let doc_namespace = metadata
                        .get("__namespace__")
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    if doc_namespace != req.namespace {
                        // Hide cross-namespace existence.
                        return Ok(Response::new(QueryResponse {
                            found: false,
                            doc_id: req.doc_id,
                            embedding: vec![],
                            metadata: HashMap::new(),
                            served_from: query_response::Tier::Unknown as i32,
                            error: String::new(),
                        }));
                    }
                }

                info!(
                    doc_id = req.doc_id,
                    latency_ms = latency_ms,
                    embedding_returned = req.include_embedding,
                    metadata_keys = metadata.len(),
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
                    metadata,
                    served_from: query_response::Tier::Unknown as i32,
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
        let tenant = self.tenant_context(&request)?;
        let req = request.into_inner();

        // Track connection
        self.state.metrics.increment_connections();
        let _conn_guard = ConnectionGuard {
            metrics: self.state.metrics.clone(),
        };

        tracing::Span::current().record("k", req.k);
        tracing::Span::current().record("query_dim", req.query_embedding.len());

        let response = self.handle_search_request(tenant.as_ref(), req).await?;
        Ok(Response::new(response))
    }

    #[instrument(skip(self, request))]
    async fn bulk_search(
        &self,
        request: Request<Streaming<SearchRequest>>,
    ) -> Result<Response<Self::BulkSearchStream>, Status> {
        let tenant = self.tenant_context(&request)?;
        let mut stream = request.into_inner();

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<SearchResponse, Status>>(16);
        let service = KyroDBServiceImpl {
            state: Arc::clone(&self.state),
        };

        tokio::spawn(async move {
            let mut batch_count: u64 = 0;

            loop {
                tokio::select! {
                    _ = tx.closed() => {
                        // Client dropped the response stream.
                        return;
                    }
                    next = stream.message() => {
                        let req = match next {
                            Ok(Some(r)) => r,
                            Ok(None) => return,
                            Err(e) => {
                                let _ = tx
                                    .send(Err(Status::internal(format!("stream error: {}", e))))
                                    .await;
                                return;
                            }
                        };

                        batch_count += 1;
                        if batch_count > MAX_BATCH_SIZE as u64 {
                            let _ = tx
                                .send(Err(Status::resource_exhausted(format!(
                                    "bulk_search batch size exceeds maximum {}",
                                    MAX_BATCH_SIZE
                                ))))
                                .await;
                            return;
                        }

                        let resp = service.handle_search_request(tenant.as_ref(), req).await;
                        if tx.send(resp).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    #[instrument(skip(self, request))]
    async fn bulk_query(
        &self,
        request: Request<BulkQueryRequest>,
    ) -> Result<Response<BulkQueryResponse>, Status> {
        let start = Instant::now();
        let tenant = self.tenant_context(&request)?;
        self.enforce_rate_limit(tenant.as_ref())?;
        let req = request.into_inner();

        let total_requested = req.doc_ids.len() as u32;
        if total_requested > MAX_BATCH_SIZE as u32 {
            return Err(Status::invalid_argument(format!(
                "Batch size {} exceeds maximum {}",
                total_requested, MAX_BATCH_SIZE
            )));
        }

        let engine = self.state.engine.read().await;

        // Use the new bulk_query which returns metadata too
        let mapped_doc_ids = if let Some(tenant) = &tenant {
            let mut mapped = Vec::with_capacity(req.doc_ids.len());
            for id in &req.doc_ids {
                mapped.push(self.map_doc_id(Some(tenant), *id)?);
            }
            mapped
        } else {
            req.doc_ids.clone()
        };

        let results_vec = engine.bulk_query(&mapped_doc_ids, req.include_embeddings);

        if results_vec.len() != mapped_doc_ids.len() {
            self.state.metrics.record_query_failure();
            self.state.metrics.record_error(ErrorCategory::Internal);
            error!(
                requested = req.doc_ids.len(),
                returned = results_vec.len(),
                "Bulk query length mismatch"
            );
            return Err(Status::internal(
                "Internal error: bulk query result length mismatch",
            ));
        }

        let mut query_responses = Vec::with_capacity(results_vec.len());
        let mut total_found = 0;

        for (i, result) in results_vec.into_iter().enumerate() {
            let doc_id = req.doc_ids[i];
            let mut found = result.is_some();
            let (mut embedding, mut metadata) = result.unwrap_or_default();

            if found && !req.namespace.is_empty() {
                let doc_namespace = metadata
                    .get("__namespace__")
                    .map(|s| s.as_str())
                    .unwrap_or("");
                if doc_namespace != req.namespace {
                    found = false;
                    embedding.clear();
                    metadata.clear();
                }
            }

            if found {
                total_found += 1;
            }

            query_responses.push(QueryResponse {
                found,
                doc_id,
                embedding,
                metadata,
                served_from: query_response::Tier::Unknown as i32,
                error: String::new(),
            });
        }

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        info!(
            requested = total_requested,
            found = total_found,
            latency_ms = latency_ms,
            "Bulk query completed"
        );

        Ok(Response::new(BulkQueryResponse {
            results: query_responses,
            total_found,
            total_requested,
            error: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn batch_delete(
        &self,
        request: Request<BatchDeleteRequest>,
    ) -> Result<Response<BatchDeleteResponse>, Status> {
        let start = Instant::now();
        let tenant = self.tenant_context(&request)?;
        self.enforce_rate_limit(tenant.as_ref())?;
        let req = request.into_inner();

        let engine = self.state.engine.write().await;

        let result = match req.delete_criteria {
            Some(batch_delete_request::DeleteCriteria::Ids(id_list)) => {
                if id_list.doc_ids.len() > MAX_BATCH_SIZE {
                    return Err(Status::invalid_argument(format!(
                        "Batch size {} exceeds maximum {}",
                        id_list.doc_ids.len(),
                        MAX_BATCH_SIZE
                    )));
                }

                if let Some(tenant) = &tenant {
                    let mut mapped = Vec::with_capacity(id_list.doc_ids.len());
                    for id in &id_list.doc_ids {
                        mapped.push(self.map_doc_id(Some(tenant), *id)?);
                    }

                    if req.namespace.is_empty() {
                        engine.batch_delete(&mapped)
                    } else {
                        let namespace = req.namespace.as_str();
                        let mut filtered = Vec::with_capacity(mapped.len());
                        for global_id in mapped {
                            if let Some(meta) = engine.get_metadata(global_id) {
                                let doc_namespace =
                                    meta.get("__namespace__").map(|s| s.as_str()).unwrap_or("");
                                if doc_namespace == namespace {
                                    filtered.push(global_id);
                                }
                            }
                        }
                        engine.batch_delete(&filtered)
                    }
                } else if req.namespace.is_empty() {
                    engine.batch_delete(&id_list.doc_ids)
                } else {
                    let namespace = req.namespace.as_str();
                    let mut filtered = Vec::with_capacity(id_list.doc_ids.len());
                    for global_id in id_list.doc_ids {
                        if let Some(meta) = engine.get_metadata(global_id) {
                            let doc_namespace =
                                meta.get("__namespace__").map(|s| s.as_str()).unwrap_or("");
                            if doc_namespace == namespace {
                                filtered.push(global_id);
                            }
                        }
                    }
                    engine.batch_delete(&filtered)
                }
            }
            Some(batch_delete_request::DeleteCriteria::Filter(filter)) => {
                // Use the existing metadata filter logic
                if let Some(tenant) = &tenant {
                    let tenant_idx = tenant.tenant_index.to_string();
                    let namespace = req.namespace.clone();
                    engine.batch_delete_by_filter(move |meta| {
                        if meta.get("__tenant_idx__") != Some(&tenant_idx) {
                            return false;
                        }
                        if !namespace.is_empty() {
                            let doc_namespace =
                                meta.get("__namespace__").map(|s| s.as_str()).unwrap_or("");
                            if doc_namespace != namespace {
                                return false;
                            }
                        }
                        kyrodb_engine::metadata_filter::matches(&filter, meta)
                    })
                } else {
                    let namespace = req.namespace.clone();
                    engine.batch_delete_by_filter(move |meta| {
                        if !namespace.is_empty() {
                            let doc_namespace =
                                meta.get("__namespace__").map(|s| s.as_str()).unwrap_or("");
                            if doc_namespace != namespace {
                                return false;
                            }
                        }
                        kyrodb_engine::metadata_filter::matches(&filter, meta)
                    })
                }
            }
            None => {
                return Err(Status::invalid_argument("No delete criteria provided"));
            }
        };

        match result {
            Ok(count) => {
                let latency_ns = start.elapsed().as_nanos() as u64;
                // Record metrics for observability
                self.state.metrics.record_query_latency(latency_ns);

                info!(
                    deleted = count,
                    latency_ms = latency_ns as f64 / 1_000_000.0,
                    "Batch delete completed"
                );

                Ok(Response::new(BatchDeleteResponse {
                    success: true,
                    deleted_count: count,
                    error: String::new(),
                }))
            }
            Err(e) => {
                self.state.metrics.record_query_failure();
                self.state.metrics.record_error(ErrorCategory::Internal);
                error!(error = %e, "Batch delete failed");
                Err(Status::internal(format!("Batch delete failed: {}", e)))
            }
        }
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
            cache_size: 0,

            // Hot tier metrics
            hot_tier_hits: stats.hot_tier_hits,
            hot_tier_misses: stats.hot_tier_misses,
            hot_tier_hit_rate: stats.hot_tier_hit_rate * 100.0,
            hot_tier_size: stats.hot_tier_size as u64,
            hot_tier_flushes: stats.hot_tier_flushes,

            // Cold tier metrics
            cold_tier_searches: stats.cold_tier_searches,
            cold_tier_size: stats.cold_tier_size as u64,

            // Performance metrics
            p50_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            total_queries: stats.total_queries,
            total_inserts: stats.total_inserts,
            queries_per_second: 0.0,
            inserts_per_second: 0.0,

            // System metrics
            memory_usage_bytes: 0,
            disk_usage_bytes: 0,
            cpu_usage_percent: 0.0,

            // Overall metrics
            overall_hit_rate: stats.overall_hit_rate * 100.0,
            collected_at: unix_timestamp_secs(),
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

        match engine.flush_hot_tier(req.force) {
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
        // Manual snapshot triggering is not yet implemented
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
async fn metrics_handler(AxumState(state): AxumState<Arc<ServerState>>) -> HttpResponse<Body> {
    let prometheus_text = state.metrics.export_prometheus();

    let mut resp = HttpResponse::new(Body::from(prometheus_text));
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    resp
}

/// Health check /health endpoint (liveness probe)
async fn health_handler(AxumState(state): AxumState<Arc<ServerState>>) -> HttpResponse<Body> {
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
        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(Body::from(body.to_string()))
        .unwrap_or_else(|_| {
            let mut resp = HttpResponse::new(Body::from("{\"status\":\"internal_error\"}"));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp.headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            resp
        })
}

/// Readiness check /ready endpoint (readiness probe)
async fn ready_handler(AxumState(state): AxumState<Arc<ServerState>>) -> HttpResponse<Body> {
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
        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(Body::from(body.to_string()))
        .unwrap_or_else(|_| {
            let mut resp = HttpResponse::new(Body::from("{\"ready\":false}"));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp.headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            resp
        })
}

/// SLO status endpoint for alerting systems
async fn slo_handler(AxumState(state): AxumState<Arc<ServerState>>) -> HttpResponse<Body> {
    let slo = state.metrics.slo_status();

    // Always return 200 OK for SLO status (breaches reported in body)
    let status_code = StatusCode::OK;

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
        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(Body::from(body.to_string()))
        .unwrap_or_else(|_| {
            let mut resp = HttpResponse::new(Body::from("{\"slo_breaches\":{}}"));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp.headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            resp
        })
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
async fn main() -> anyhow::Result<()> {
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

    // Initialize auth/tenancy (must be ready before serving)
    let (auth, tenant_id_mapper, rate_limiter) = if config.auth.enabled {
        let api_keys_file = config
            .auth
            .api_keys_file
            .clone()
            .ok_or_else(|| anyhow::anyhow!("auth.enabled=true requires auth.api_keys_file"))?;

        let auth = AuthManager::new();
        auth.load_from_file(&api_keys_file)?;

        let tenants = auth.enabled_tenants();
        let tenant_map_path = config.persistence.data_dir.join("tenants.json");
        let tenant_id_mapper = TenantIdMapper::load_or_create(tenant_map_path, &tenants)?;

        (Some(auth), Some(tenant_id_mapper), Some(RateLimiter::new()))
    } else {
        (None, None, None)
    };

    // Initialize structured logging based on config
    let (non_blocking, _guard) = if let Some(log_file) = &config.logging.file {
        match std::fs::File::create(log_file) {
            Ok(file) => tracing_appender::non_blocking(file),
            Err(e) => {
                eprintln!(
                    "Warning: Failed to create log file {:?}: {}. Falling back to stdout.",
                    log_file, e
                );
                tracing_appender::non_blocking(std::io::stdout())
            }
        }
    } else {
        tracing_appender::non_blocking(std::io::stdout())
    };

    let log_level = config.logging.level.as_str();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        format!("kyrodb_engine={},kyrodb_server={}", log_level, log_level).into()
    });

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
            FsyncPolicy::Always // Most conservative for "full"
        }
    };

    let engine_config = TieredEngineConfig {
        hot_tier_max_size: config.cache.capacity,
        hot_tier_hard_limit: config.cache.capacity * 2, // 2x soft limit for emergency eviction
        hot_tier_max_age: Duration::from_secs(config.cache.training_interval_secs),
        hnsw_max_elements: config.hnsw.max_elements,
        embedding_dimension: config.hnsw.dimension,
        data_dir: Some(config.persistence.data_dir.to_string_lossy().to_string()),
        fsync_policy,
        snapshot_interval: config.persistence.snapshot_interval_secs as usize,
        flush_interval: config.wal_flush_interval(),
        cache_timeout_ms: config.timeouts.cache_ms,
        hot_tier_timeout_ms: config.timeouts.hot_tier_ms,
        cold_tier_timeout_ms: config.timeouts.cold_tier_ms,
        max_concurrent_queries: 1000, // Load shedding: max 1000 in-flight queries
    };

    // Create shutdown broadcast channel early for prefetch tasks
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(16);

    // Initialize or recover engine
    info!("Initializing TieredEngine with A/B testing (LRU vs learned strategy)...");

    // Helper to create cache strategy (Two-level cache architecture)
    let create_cache_strategy = || {
        let lru_strategy = Arc::new(LruCacheStrategy::new(config.cache.capacity));

        let learned_predictor = LearnedCachePredictor::new(config.cache.capacity)
            .context("Failed to create Hybrid Semantic Cache predictor")?;

        // NOTE: Semantic logic moved to QueryHashCache (L1b) in two-level architecture
        let learned_strategy = Arc::new(LearnedCacheStrategy::new(
            config.cache.capacity,
            learned_predictor,
        ));

        // NOTE: Query clustering and prefetching moved to separate layers in two-level architecture
        // Prefetching will be re-enabled as a separate service layer in future updates

        Ok::<Box<dyn kyrodb_engine::CacheStrategy>, anyhow::Error>(Box::new(AbTestSplitter::new(
            lru_strategy.clone(),
            learned_strategy.clone(),
        )))
    };

    info!(
        "Features: clustering={}, prefetching={}",
        config.cache.enable_query_clustering, config.cache.enable_prefetching
    );

    let cache_strategy = create_cache_strategy()?;

    // Create query cache (L1b) - semantic similarity-based
    let query_cache = Arc::new(kyrodb_engine::QueryHashCache::new(
        100,  // capacity: 100 query hashes
        0.82, // similarity threshold from SemanticConfig default
    ));

    let data_dir_path = config.persistence.data_dir.clone();
    let manifest_path = data_dir_path.join("MANIFEST");
    let should_attempt_recovery = config.persistence.enable_recovery && manifest_path.exists();

    let create_empty_engine =
        |cache_strategy: Box<dyn kyrodb_engine::CacheStrategy>,
         query_cache: Arc<kyrodb_engine::QueryHashCache>| {
            TieredEngine::new(
                cache_strategy,
                query_cache,
                Vec::new(),
                Vec::new(),
                engine_config.clone(),
            )
        };

    let engine = if should_attempt_recovery {
        info!("MANIFEST found, attempting recovery...");

        let data_dir_for_recover = data_dir_path.to_string_lossy().to_string();
        match TieredEngine::recover(
            cache_strategy,
            query_cache.clone(),
            data_dir_for_recover.as_str(),
            engine_config.clone(),
        ) {
            Ok(engine) => {
                info!("Recovery successful");
                engine
            }
            Err(e) => {
                error!(error = %e, "Recovery failed");
                if !config.persistence.allow_fresh_start_on_recovery_failure {
                    return Err(e).context(
                        "Recovery failed and allow_fresh_start_on_recovery_failure=false (fail-fast)",
                    );
                }

                // Operator explicitly allowed a fresh start. Quarantine the old on-disk state first.
                let ts = unix_timestamp_secs();
                let quarantine_path = data_dir_path.with_file_name(format!(
                    "{}_corrupt_{}",
                    data_dir_path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("kyrodb_data"),
                    ts
                ));

                if data_dir_path.exists() {
                    std::fs::rename(&data_dir_path, &quarantine_path).with_context(|| {
                        format!(
                            "Failed to quarantine corrupted data dir ({} -> {})",
                            data_dir_path.display(),
                            quarantine_path.display()
                        )
                    })?;
                    warn!(
                        old = %data_dir_path.display(),
                        quarantined = %quarantine_path.display(),
                        "Quarantined data directory after recovery failure"
                    );
                }

                let fallback_cache_strategy = create_cache_strategy()?;
                let query_cache_fallback = Arc::new(kyrodb_engine::QueryHashCache::new(100, 0.82));
                create_empty_engine(fallback_cache_strategy, query_cache_fallback)?
            }
        }
    } else {
        if config.persistence.enable_recovery {
            info!(
                data_dir = %data_dir_path.display(),
                "No MANIFEST found; initializing a new empty database"
            );
        } else {
            info!(
                data_dir = %data_dir_path.display(),
                "Recovery disabled; initializing a new empty database"
            );
        }
        create_empty_engine(cache_strategy, query_cache.clone())?
    };

    info!("TieredEngine initialized successfully with configured cache optimizations");

    // Wrap engine in Arc<RwLock> for concurrent access
    let engine_arc = Arc::new(RwLock::new(engine));

    // Create metrics collector
    let metrics = MetricsCollector::new();

    // Spawn background flush task with graceful shutdown
    let mut flush_shutdown_rx = shutdown_tx.subscribe();
    let engine_for_flush = engine_arc.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        info!("Background flush task started (60s interval)");

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let engine = engine_for_flush.write().await;
                    match engine.flush_hot_tier(false) {
                        Ok(count) if count > 0 => {
                            info!(docs_flushed = count, "Background flush completed");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            error!(error = %e, "Background flush failed");
                        }
                    }
                }
                _ = flush_shutdown_rx.recv() => {
                    info!("Background flush task shutting down gracefully");
                    let engine = engine_for_flush.write().await;
                    if let Ok(count) = engine.flush_hot_tier(true) { // Pass true for shutdown flush
                        if count > 0 {
                            info!(docs_flushed = count, "Final flush completed on shutdown");
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
        shutdown_tx: shutdown_tx.clone(),
        metrics: metrics.clone(),
        auth,
        rate_limiter,
        tenant_id_mapper,
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
    let http_addr =
        format!("{}:{}", config.server.host, http_port).parse::<std::net::SocketAddr>()?;

    info!(
        "HTTP observability server listening on http://{}",
        http_addr
    );
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

    // Setup signal handling for graceful shutdown
    let shutdown_signal = async {
        let ctrl_c = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                error!(error = %e, "Ctrl+C handler failed");
            }
        };

        #[cfg(unix)]
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sig) => {
                    sig.recv().await;
                }
                Err(e) => {
                    error!(error = %e, "SIGTERM handler failed");
                    std::future::pending::<()>().await;
                }
            }
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
            },
            _ = terminate => {
                info!("Received SIGTERM, initiating graceful shutdown");
            },
        }
    };

    // Build gRPC service with auth interceptor when enabled
    let auth_enabled = state.app_config.auth.enabled;
    let state_for_interceptor = state.clone();
    let service =
        KyroDbServiceServer::with_interceptor(grpc_service, move |mut req: Request<()>| {
            if !auth_enabled {
                return Ok(req);
            }

            let auth = state_for_interceptor
                .auth
                .as_ref()
                .ok_or_else(|| Status::internal("auth manager not initialized"))?;
            let tenant_id_mapper = state_for_interceptor
                .tenant_id_mapper
                .as_ref()
                .ok_or_else(|| Status::internal("tenant mapper not initialized"))?;

            let api_key = req
                .metadata()
                .get(API_KEY_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .or_else(|| {
                    req.metadata()
                        .get(AUTHORIZATION_HEADER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.strip_prefix("Bearer "))
                        .map(str::to_string)
                })
                .ok_or_else(|| Status::unauthenticated("missing api key"))?;

            let tenant = auth
                .validate(&api_key)
                .ok_or_else(|| Status::unauthenticated("invalid api key"))?;

            let tenant_index = tenant_id_mapper
                .ensure_tenant(&tenant.tenant_id)
                .map_err(|e| Status::internal(format!("tenant mapping error: {}", e)))?;

            req.extensions_mut().insert(TenantContext {
                tenant_id: tenant.tenant_id,
                tenant_index,
                max_qps: tenant.max_qps,
            });

            Ok(req)
        });

    // Start gRPC server with graceful shutdown
    let mut grpc_builder = Server::builder();
    if config.server.tls.enabled {
        let tls = &config.server.tls;
        let cert_path = tls.cert_path.as_ref().ok_or_else(|| {
            anyhow::anyhow!("server.tls.enabled=true requires server.tls.cert_path")
        })?;
        let key_path = tls.key_path.as_ref().ok_or_else(|| {
            anyhow::anyhow!("server.tls.enabled=true requires server.tls.key_path")
        })?;

        let cert = std::fs::read(cert_path)?;
        let key = std::fs::read(key_path)?;
        let identity = tonic::transport::Identity::from_pem(cert, key);

        let mut tls_cfg = tonic::transport::ServerTlsConfig::new().identity(identity);
        if tls.require_client_cert {
            let ca_path = tls.ca_cert_path.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "server.tls.require_client_cert=true requires server.tls.ca_cert_path"
                )
            })?;
            let ca = std::fs::read(ca_path)?;
            let ca = tonic::transport::Certificate::from_pem(ca);
            tls_cfg = tls_cfg.client_ca_root(ca);
        }

        grpc_builder = grpc_builder.tls_config(tls_cfg)?;
    }

    let grpc_server = grpc_builder
        .add_service(service)
        .serve_with_shutdown(grpc_addr, shutdown_signal);

    // Wait for shutdown
    let result = grpc_server.await;

    // Broadcast shutdown to all background tasks
    info!("Shutting down background tasks...");
    let _ = shutdown_tx.send(());

    // Give tasks time to stop gracefully
    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("KyroDB server stopped");

    result?;
    Ok(())
}
