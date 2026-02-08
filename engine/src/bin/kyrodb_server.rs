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
use kyrodb_engine::training_task::{spawn_training_task, TrainingConfig};
use kyrodb_engine::{
    access_logger::AccessPatternLogger,
    cache_strategy::{
        AbTestSplitter, LearnedCacheStrategy, LruCacheStrategy, SharedLearnedCacheStrategy,
    },
    config::ObservabilityAuthMode,
    AuthManager, ErrorCategory, FsyncPolicy, HealthStatus, LearnedCachePredictor, Manifest,
    MetricsCollector, PointQueryTier, RateLimiter, SearchExecutionPath, SloThresholds, TenantInfo,
    TieredEngine, TieredEngineConfig,
};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, instrument, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// HTTP server for observability endpoints
use axum::middleware::Next;
use axum::{
    body::Body,
    extract::State as AxumState,
    http::{header::CONTENT_TYPE, HeaderValue, Response as HttpResponse, StatusCode},
    middleware,
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

/// BulkSearch micro-batch size to amortize per-request overhead
const BULK_SEARCH_BATCH_SIZE: usize = 128;

/// Maximum time to wait for a BulkSearch micro-batch (milliseconds)
const BULK_SEARCH_MAX_WAIT_MS: u64 = 2;

/// Minimum valid document ID (0 is reserved)
const MIN_DOC_ID: u64 = 1;

/// Maximum total documents for a single bulk_load_hnsw stream to prevent
/// runaway CPU/disk usage. Configurable via max_total_documents if needed.
const MAX_TOTAL_BULK_LOAD_DOCUMENTS: u64 = 10_000_000;

/// Maximum k value for k-NN search to prevent excessive computation
const MAX_KNN_K: u32 = 1000;

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchErrorKind {
    Timeout,
    ResourceExhausted,
    Validation,
    Internal,
}

fn classify_search_error_message(message: &str) -> SearchErrorKind {
    let lower = message.to_ascii_lowercase();
    if lower.contains("timed out") || lower.contains("timeout") {
        SearchErrorKind::Timeout
    } else if lower.contains("saturated")
        || lower.contains("resource exhausted")
        || lower.contains("max in-flight")
    {
        SearchErrorKind::ResourceExhausted
    } else if lower.contains("invalid")
        || lower.contains("dimension mismatch")
        || lower.contains("cannot be empty")
        || lower.contains("must be")
    {
        SearchErrorKind::Validation
    } else {
        SearchErrorKind::Internal
    }
}

fn apply_wal_health_overrides(
    base: HealthStatus,
    wal_inconsistent: bool,
    wal_failed: u64,
) -> HealthStatus {
    if wal_inconsistent {
        return HealthStatus::Unhealthy {
            reason: "WAL inconsistent; writes disabled".to_string(),
        };
    }

    if wal_failed > 0 {
        return match base {
            HealthStatus::Healthy => HealthStatus::Degraded {
                reason: format!("{} failed WAL writes observed", wal_failed),
            },
            other => other,
        };
    }

    base
}

#[cfg(target_os = "linux")]
fn sample_process_memory_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let rss_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    Some(rss_pages.saturating_mul(page_size as u64))
}

#[cfg(not(target_os = "linux"))]
fn sample_process_memory_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn sample_process_cpu_usage_percent(uptime: Duration) -> Option<f64> {
    if uptime.is_zero() {
        return None;
    }

    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let close = stat.rfind(')')?;
    let rest = stat.get(close + 2..)?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() <= 12 {
        return None;
    }

    let utime_ticks: u64 = fields[11].parse().ok()?;
    let stime_ticks: u64 = fields[12].parse().ok()?;
    let ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks_per_sec <= 0 {
        return None;
    }

    let cpu_secs = (utime_ticks + stime_ticks) as f64 / ticks_per_sec as f64;
    Some((cpu_secs / uptime.as_secs_f64()) * 100.0)
}

#[cfg(not(target_os = "linux"))]
fn sample_process_cpu_usage_percent(_uptime: Duration) -> Option<f64> {
    None
}

#[cfg(unix)]
fn sample_filesystem_used_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return None;
    }

    #[allow(clippy::unnecessary_cast)]
    let blocks = stat.f_blocks as u64;
    #[allow(clippy::unnecessary_cast)]
    let free_blocks = stat.f_bfree as u64;
    #[allow(clippy::unnecessary_cast)]
    let frag_size = if stat.f_frsize > 0 {
        stat.f_frsize as u64
    } else {
        stat.f_bsize as u64
    };

    Some(blocks.saturating_sub(free_blocks).saturating_mul(frag_size))
}

#[cfg(not(unix))]
fn sample_filesystem_used_bytes(_path: &Path) -> Option<u64> {
    None
}

fn refresh_system_metrics(state: &ServerState) {
    if let Some(rss_bytes) = sample_process_memory_bytes() {
        state.metrics.set_memory_used(rss_bytes);
    }

    if let Some(data_dir) = state.engine_config.data_dir.as_deref() {
        if let Some(disk_used_bytes) = sample_filesystem_used_bytes(Path::new(data_dir)) {
            state.metrics.set_disk_used(disk_used_bytes);
        }
    }
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
    max_vectors: usize,
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
        match Self::persist_map_atomic(&path, &map_snapshot) {
            Ok(()) => Ok(next_idx),
            Err(e) => {
                map.remove(tenant_id);
                Err(e)
            }
        }
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
                "DOC_ID_OUT_OF_RANGE: doc_id exceeds tenant-local max (u32)",
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
    tenant_vector_counts: Option<parking_lot::RwLock<HashMap<String, usize>>>,
}

/// gRPC service implementation
struct KyroDBServiceImpl {
    state: Arc<ServerState>,
}

struct SearchPlan {
    search_k: usize,
    ef_search_override: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BatchSearchKey {
    search_k: usize,
    ef_search_override: Option<usize>,
    query_cache_scope: u64,
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

    fn map_query_tier(tier: PointQueryTier) -> i32 {
        match tier {
            PointQueryTier::Cache => query_response::Tier::Cache as i32,
            PointQueryTier::HotTier => query_response::Tier::HotTier as i32,
            PointQueryTier::ColdTier => query_response::Tier::ColdTier as i32,
        }
    }

    fn map_search_path(path: SearchExecutionPath) -> i32 {
        match path {
            SearchExecutionPath::CacheHit => search_response::SearchPath::CacheHit as i32,
            SearchExecutionPath::HotTierOnly => search_response::SearchPath::HotTierOnly as i32,
            SearchExecutionPath::ColdTierOnly => search_response::SearchPath::ColdTierOnly as i32,
            SearchExecutionPath::HotAndCold => search_response::SearchPath::HotAndCold as i32,
            SearchExecutionPath::Degraded => search_response::SearchPath::Unknown as i32,
        }
    }

    #[allow(clippy::result_large_err)]
    fn enforce_vector_quota(
        &self,
        tenant: Option<&TenantContext>,
        engine: &TieredEngine,
        global_doc_id: u64,
    ) -> Result<bool, Status> {
        let Some(tenant) = tenant else {
            return Ok(false);
        };

        let already_exists = engine.exists(global_doc_id);
        if already_exists {
            return Ok(true);
        }

        let counts = self
            .state
            .tenant_vector_counts
            .as_ref()
            .ok_or_else(|| Status::internal("tenant vector quota state not initialized"))?;
        let mut guard = counts.write();
        let entry = guard.entry(tenant.tenant_id.clone()).or_insert(0);
        if *entry >= tenant.max_vectors {
            return Err(Status::resource_exhausted(format!(
                "max_vectors quota exceeded: tenant={} limit={}",
                tenant.tenant_id, tenant.max_vectors
            )));
        }
        *entry = entry.saturating_add(1);

        Ok(false)
    }

    /// Adjust tenant vector counters for paths that do not reserve quota through
    /// `enforce_vector_quota` (for example, out-of-band bulk-load ingestion).
    fn increment_tenant_vectors(&self, tenant: Option<&TenantContext>, count: usize) {
        if count == 0 {
            return;
        }
        let Some(tenant) = tenant else {
            return;
        };
        if let Some(counts) = &self.state.tenant_vector_counts {
            let mut guard = counts.write();
            let entry = guard.entry(tenant.tenant_id.clone()).or_insert(0);
            *entry = entry.saturating_add(count);
        }
    }

    fn decrement_tenant_vectors(&self, tenant: Option<&TenantContext>, count: usize) {
        if count == 0 {
            return;
        }
        let Some(tenant) = tenant else {
            return;
        };
        if let Some(counts) = &self.state.tenant_vector_counts {
            let mut guard = counts.write();
            if let Some(entry) = guard.get_mut(&tenant.tenant_id) {
                *entry = entry.saturating_sub(count);
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn validate_search_request(
        &self,
        _tenant: Option<&TenantContext>,
        req: &SearchRequest,
    ) -> Result<SearchPlan, Status> {
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

        let ef_search_override = if req.ef_search == 0 {
            None
        } else {
            Some(req.ef_search as usize)
        };

        Ok(SearchPlan {
            search_k,
            ef_search_override,
        })
    }

    fn query_cache_scope(&self, tenant: Option<&TenantContext>, req: &SearchRequest) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        match tenant {
            Some(t) => t.tenant_index.hash(&mut hasher),
            None => u32::MAX.hash(&mut hasher),
        }
        req.namespace.hash(&mut hasher);

        match &req.filter {
            Some(filter) => {
                1u8.hash(&mut hasher);
                let mut encoded = Vec::new();
                if filter.encode(&mut encoded).is_ok() {
                    encoded.hash(&mut hasher);
                } else {
                    format!("{:?}", filter).hash(&mut hasher);
                }
            }
            None => 0u8.hash(&mut hasher),
        }

        hasher.finish()
    }

    fn map_search_error(&self, error: anyhow::Error) -> Status {
        let message = error.to_string();
        self.state.metrics.record_query_failure();

        match classify_search_error_message(&message) {
            SearchErrorKind::Timeout => {
                self.state.metrics.record_error(ErrorCategory::Timeout);
                Status::deadline_exceeded(format!("Search timed out: {}", message))
            }
            SearchErrorKind::ResourceExhausted => {
                self.state
                    .metrics
                    .record_error(ErrorCategory::ResourceExhausted);
                Status::resource_exhausted(format!("Search rejected: {}", message))
            }
            SearchErrorKind::Validation => {
                self.state.metrics.record_error(ErrorCategory::Validation);
                Status::invalid_argument(message)
            }
            SearchErrorKind::Internal => {
                self.state.metrics.record_error(ErrorCategory::Internal);
                Status::internal(format!("Search failed: {}", message))
            }
        }
    }

    fn record_search_path_metrics(&self, path: SearchExecutionPath) {
        match path {
            SearchExecutionPath::CacheHit => {
                self.state.metrics.record_cache_hit(true);
            }
            SearchExecutionPath::HotTierOnly => {
                self.state.metrics.record_cache_hit(false);
                self.state.metrics.record_tier_hit(true);
            }
            SearchExecutionPath::ColdTierOnly => {
                self.state.metrics.record_cache_hit(false);
                self.state.metrics.record_tier_hit(false);
            }
            SearchExecutionPath::HotAndCold => {
                self.state.metrics.record_cache_hit(false);
                self.state.metrics.record_tier_hit(true);
                self.state.metrics.record_tier_hit(false);
            }
            SearchExecutionPath::Degraded => {
                self.state.metrics.record_cache_hit(false);
            }
        }
    }

    fn convert_distance_to_score(&self, dist: f32) -> f32 {
        use kyrodb_engine::config::DistanceMetric;

        match self.state.app_config.hnsw.distance {
            DistanceMetric::Cosine => 1.0 - dist,
            DistanceMetric::InnerProduct => 1.0 - dist,
            DistanceMetric::Euclidean => 1.0 / (1.0 + dist.max(0.0)),
        }
    }

    fn build_search_response(
        &self,
        tenant: Option<&TenantContext>,
        req: &SearchRequest,
        results: Vec<kyrodb_engine::SearchResult>,
        engine: &TieredEngine,
        latency_ns: u64,
        search_path: SearchExecutionPath,
    ) -> SearchResponse {
        let total_found = results.len() as u32;
        let mut final_results = Vec::with_capacity(req.k as usize);
        let mut candidates = results.into_iter();

        let has_namespace = !req.namespace.is_empty();
        let needs_metadata = tenant.is_some() || has_namespace || req.filter.is_some();

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

            let score = self.convert_distance_to_score(candidate.distance);
            if req.min_score > 0.0 && score < req.min_score {
                continue;
            }

            let fetched_doc = if needs_metadata || req.include_embeddings {
                engine.get_document_with_metadata(candidate.doc_id)
            } else {
                None
            };

            let metadata = if needs_metadata {
                let metadata = match fetched_doc.as_ref() {
                    Some((_, metadata)) => metadata.clone(),
                    None => continue,
                };

                if let Some(tenant) = tenant {
                    let expected = tenant.tenant_index.to_string();
                    if metadata.get("__tenant_idx__") != Some(&expected) {
                        continue;
                    }
                }

                if has_namespace {
                    let doc_namespace = metadata
                        .get("__namespace__")
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    if doc_namespace != req.namespace {
                        continue;
                    }
                }

                if let Some(filter) = &req.filter {
                    if !kyrodb_engine::metadata_filter::matches(filter, &metadata) {
                        continue;
                    }
                }

                metadata
            } else {
                HashMap::new()
            };

            let embedding = if req.include_embeddings {
                fetched_doc
                    .map(|(embedding, _)| embedding)
                    .unwrap_or_default()
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

        SearchResponse {
            results: final_results,
            total_found,
            search_latency_ms: latency_ms as f32,
            search_path: Self::map_search_path(search_path),
            error: String::new(),
        }
    }

    async fn handle_search_request(
        &self,
        tenant: Option<&TenantContext>,
        req: SearchRequest,
    ) -> Result<SearchResponse, Status> {
        let start = Instant::now();
        self.enforce_rate_limit(tenant)?;
        let plan = self.validate_search_request(tenant, &req)?;
        let query_cache_scope = self.query_cache_scope(tenant, &req);

        let engine = self.state.engine.read().await;

        let (results, search_path) = engine
            .knn_search_with_timeouts_with_ef_scoped(
                &req.query_embedding,
                plan.search_k,
                plan.ef_search_override,
                query_cache_scope,
            )
            .await
            .map_err(|e| self.map_search_error(e))?;

        let latency_ns = start.elapsed().as_nanos() as u64;
        self.state.metrics.record_query_latency(latency_ns);
        self.state.metrics.record_hnsw_search(latency_ns);
        self.record_search_path_metrics(search_path);

        Ok(self.build_search_response(tenant, &req, results, &engine, latency_ns, search_path))
    }

    async fn handle_search_requests_batch(
        &self,
        tenant: Option<&TenantContext>,
        requests: Vec<SearchRequest>,
    ) -> Vec<Result<SearchResponse, Status>> {
        if requests.is_empty() {
            return Vec::new();
        }

        let mut results: Vec<Option<Result<SearchResponse, Status>>> = vec![None; requests.len()];
        let mut validated: Vec<(usize, SearchRequest, SearchPlan)> =
            Vec::with_capacity(requests.len());

        for (idx, req) in requests.into_iter().enumerate() {
            match self.validate_search_request(tenant, &req) {
                Ok(plan) => validated.push((idx, req, plan)),
                Err(e) => {
                    results[idx] = Some(Err(e));
                }
            }
        }

        let mut grouped: HashMap<BatchSearchKey, Vec<(usize, SearchRequest)>> = HashMap::new();
        for (idx, req, plan) in validated {
            let key = BatchSearchKey {
                search_k: plan.search_k,
                ef_search_override: plan.ef_search_override,
                query_cache_scope: self.query_cache_scope(tenant, &req),
            };
            grouped.entry(key).or_default().push((idx, req));
        }

        for (key, grouped_requests) in grouped {
            let group_start = Instant::now();
            let queries: Vec<Vec<f32>> = grouped_requests
                .iter()
                .map(|(_, req)| req.query_embedding.clone())
                .collect();

            let group_results: Vec<(usize, Result<SearchResponse, Status>)> = {
                let engine = self.state.engine.read().await;
                match engine.knn_search_batch_with_ef_detailed_scoped(
                    &queries,
                    key.search_k,
                    key.ef_search_override,
                    key.query_cache_scope,
                ) {
                    Ok(batch_results) => {
                        if batch_results.len() != grouped_requests.len() {
                            let status = Status::internal(format!(
                                "bulk_search internal mismatch: expected {} results, got {}",
                                grouped_requests.len(),
                                batch_results.len()
                            ));
                            grouped_requests
                                .into_iter()
                                .map(|(idx, _)| (idx, Err(status.clone())))
                                .collect()
                        } else {
                            let elapsed_ns = group_start.elapsed().as_nanos() as u64;
                            let per_query_latency_ns =
                                (elapsed_ns / grouped_requests.len().max(1) as u64).max(1);
                            grouped_requests
                                .into_iter()
                                .zip(batch_results.into_iter())
                                .map(|((idx, req), (candidates, search_path))| {
                                    self.state
                                        .metrics
                                        .record_query_latency(per_query_latency_ns);
                                    self.state.metrics.record_hnsw_search(per_query_latency_ns);
                                    self.record_search_path_metrics(search_path);
                                    let response = self.build_search_response(
                                        tenant,
                                        &req,
                                        candidates,
                                        &engine,
                                        per_query_latency_ns,
                                        search_path,
                                    );
                                    (idx, Ok(response))
                                })
                                .collect()
                        }
                    }
                    Err(e) => {
                        let status = self.map_search_error(e);
                        grouped_requests
                            .into_iter()
                            .map(|(idx, _)| (idx, Err(status.clone())))
                            .collect()
                    }
                }
            };

            for (idx, group_result) in group_results {
                results[idx] = Some(group_result);
            }
        }

        results
            .into_iter()
            .map(|r| r.unwrap_or_else(|| Err(Status::internal("missing batch result"))))
            .collect()
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
        let already_exists = self.enforce_vector_quota(tenant.as_ref(), &engine, global_doc_id)?;

        // Store namespace in metadata for filtering during search
        // Namespace is stored as reserved key "__namespace__" to avoid conflicts with user metadata
        let mut metadata = req.metadata;
        metadata.remove("__tenant_id__");
        metadata.remove("__tenant_idx__");
        metadata.remove("__namespace__");
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

                Ok(Response::new(InsertResponse {
                    success: true,
                    error: String::new(),
                    inserted_at: unix_timestamp_secs(),
                    tier: insert_response::Tier::ColdTier as i32,
                    total_inserted: 1,
                    total_failed: 0,
                }))
            }
            Err(e) => {
                if !already_exists {
                    self.decrement_tenant_vectors(tenant.as_ref(), 1);
                }
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
        self.enforce_rate_limit(tenant.as_ref())?;
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
                let engine = self.state.engine.write().await;
                let already_exists =
                    match self.enforce_vector_quota(tenant.as_ref(), &engine, global_doc_id) {
                        Ok(exists) => exists,
                        Err(status) => {
                            total_failed += 1;
                            last_error = status.message().to_string();
                            continue;
                        }
                    };

                let mut metadata = req.metadata;
                metadata.remove("__tenant_id__");
                metadata.remove("__tenant_idx__");
                metadata.remove("__namespace__");
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
                        total_inserted += 1;
                    }
                    Err(e) => {
                        if !already_exists {
                            self.decrement_tenant_vectors(tenant.as_ref(), 1);
                        }
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

        Ok(Response::new(InsertResponse {
            success: total_failed == 0,
            error: last_error,
            inserted_at: unix_timestamp_secs(),
            tier: insert_response::Tier::ColdTier as i32,
            total_inserted,
            total_failed,
        }))
    }

    /// Bulk load directly to HNSW index (bypasses hot tier).
    ///
    /// This path is durable: documents are ingested through the cold-tier write path
    /// (WAL + HNSW), so successful loads survive crashes.
    #[instrument(skip(self, request))]
    async fn bulk_load_hnsw(
        &self,
        request: Request<tonic::Streaming<InsertRequest>>,
    ) -> Result<Response<BulkLoadResponse>, Status> {
        let start = Instant::now();
        let tenant = self.tenant_context(&request)?;
        self.enforce_rate_limit(tenant.as_ref())?;
        let mut stream = request.into_inner();

        // Stream ingestion in bounded chunks.
        // We cannot hold the engine write lock across `.await` on the request stream.
        // Instead, we batch up to MAX_BATCH_SIZE docs, ingest synchronously, then continue.
        let mut documents: Vec<(u64, Vec<f32>, std::collections::HashMap<String, String>)> =
            Vec::with_capacity(MAX_BATCH_SIZE);
        let mut validation_errors = 0u64;
        let mut last_error = String::new();

        let mut total_loaded = 0u64;
        let mut total_failed_insertion = 0u64;
        let mut total_received = 0u64;

        info!("BulkLoadHnsw: starting streaming load");

        while let Some(req) = stream.message().await? {
            // Re-check rate limit per batch boundary to prevent long-running
            // streams from bypassing per-tenant limits.
            self.enforce_rate_limit(tenant.as_ref())?;

            total_received += 1;

            // Enforce total document cap to prevent runaway resource consumption
            if total_received > MAX_TOTAL_BULK_LOAD_DOCUMENTS {
                last_error = format!(
                    "Total document limit exceeded: received {} documents, max is {}",
                    total_received, MAX_TOTAL_BULK_LOAD_DOCUMENTS
                );
                return Err(Status::resource_exhausted(&last_error));
            }
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
            metadata.remove("__tenant_id__");
            metadata.remove("__tenant_idx__");
            metadata.remove("__namespace__");
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

            if documents.len() >= MAX_BATCH_SIZE {
                // Re-check rate limit before each batch ingestion
                self.enforce_rate_limit(tenant.as_ref())?;
                let batch = std::mem::take(&mut documents);
                let batch_len = batch.len() as u64;
                let engine = self.state.engine.write().await;
                let mut new_doc_ids = HashSet::new();
                if let Some(tenant_ctx) = tenant.as_ref() {
                    for (doc_id, _, _) in &batch {
                        if !engine.exists(*doc_id) {
                            new_doc_ids.insert(*doc_id);
                        }
                    }
                    if !new_doc_ids.is_empty() {
                        let counts = self.state.tenant_vector_counts.as_ref().ok_or_else(|| {
                            Status::internal("tenant vector quota state not initialized")
                        })?;
                        let current = counts
                            .read()
                            .get(&tenant_ctx.tenant_id)
                            .copied()
                            .unwrap_or(0);
                        if current.saturating_add(new_doc_ids.len()) > tenant_ctx.max_vectors {
                            return Err(Status::resource_exhausted(format!(
                                "max_vectors quota exceeded: tenant={} limit={}",
                                tenant_ctx.tenant_id, tenant_ctx.max_vectors
                            )));
                        }
                    }
                }
                match engine.bulk_load_cold_tier(batch) {
                    Ok((loaded, failed, _duration_ms, _rate)) => {
                        total_loaded += loaded;
                        total_failed_insertion += failed;
                        if !new_doc_ids.is_empty() {
                            let inserted_now = new_doc_ids
                                .iter()
                                .filter(|doc_id| engine.exists(**doc_id))
                                .count();
                            self.increment_tenant_vectors(tenant.as_ref(), inserted_now);
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "BulkLoadHnsw: batch load failed");
                        last_error = format!("Bulk load failed: {}", e);
                        total_failed_insertion += batch_len;
                        break;
                    }
                }
            }
        }

        if !documents.is_empty() {
            let batch = std::mem::take(&mut documents);
            let batch_len = batch.len() as u64;
            let engine = self.state.engine.write().await;
            let mut new_doc_ids = HashSet::new();
            if let Some(tenant_ctx) = tenant.as_ref() {
                for (doc_id, _, _) in &batch {
                    if !engine.exists(*doc_id) {
                        new_doc_ids.insert(*doc_id);
                    }
                }
                if !new_doc_ids.is_empty() {
                    let counts = self.state.tenant_vector_counts.as_ref().ok_or_else(|| {
                        Status::internal("tenant vector quota state not initialized")
                    })?;
                    let current = counts
                        .read()
                        .get(&tenant_ctx.tenant_id)
                        .copied()
                        .unwrap_or(0);
                    if current.saturating_add(new_doc_ids.len()) > tenant_ctx.max_vectors {
                        return Err(Status::resource_exhausted(format!(
                            "max_vectors quota exceeded: tenant={} limit={}",
                            tenant_ctx.tenant_id, tenant_ctx.max_vectors
                        )));
                    }
                }
            }
            match engine.bulk_load_cold_tier(batch) {
                Ok((loaded, failed, _duration_ms, _rate)) => {
                    total_loaded += loaded;
                    total_failed_insertion += failed;
                    if !new_doc_ids.is_empty() {
                        let inserted_now = new_doc_ids
                            .iter()
                            .filter(|doc_id| engine.exists(**doc_id))
                            .count();
                        self.increment_tenant_vectors(tenant.as_ref(), inserted_now);
                    }
                }
                Err(e) => {
                    error!(error = %e, "BulkLoadHnsw: final batch load failed");
                    last_error = format!("Bulk load failed: {}", e);
                    total_failed_insertion += batch_len;
                }
            }
        }

        let total_time = start.elapsed();
        let total_seconds = total_time.as_secs_f64().max(1e-9);
        let rate = (total_loaded as f64) / total_seconds;
        let total_failed = total_failed_insertion + validation_errors;

        info!(
            loaded = total_loaded,
            failed_insertion = total_failed_insertion,
            validation_errors,
            rate_per_sec = rate,
            total_ms = total_time.as_millis() as u64,
            "BulkLoadHnsw: complete"
        );

        Ok(Response::new(BulkLoadResponse {
            success: total_failed == 0,
            error: if total_failed > 0 {
                format!(
                    "Loaded {} docs; {} failed validation, {} failed insertion{}",
                    total_loaded,
                    validation_errors,
                    total_failed_insertion,
                    if last_error.is_empty() {
                        String::new()
                    } else {
                        format!("; last_error={}", last_error)
                    }
                )
            } else {
                String::new()
            },
            total_loaded,
            total_failed,
            load_duration_ms: (total_seconds * 1000.0) as f32,
            avg_insert_rate: rate as f32,
            peak_memory_bytes: 0,
        }))
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

        if let Some(tenant) = &tenant {
            if metadata.get("__tenant_idx__") != Some(&tenant.tenant_index.to_string()) {
                return Ok(Response::new(DeleteResponse {
                    success: true,
                    error: String::new(),
                    existed: false,
                }));
            }
        }

        // Enforce namespace boundary (treat mismatch as not-found to avoid leakage).
        if !req.namespace.is_empty() {
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
                    self.decrement_tenant_vectors(tenant.as_ref(), 1);
                }

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

        let existing_metadata = match engine.get_metadata(global_doc_id) {
            Some(m) => m,
            None => {
                return Ok(Response::new(kyrodb::UpdateMetadataResponse {
                    success: true,
                    error: String::new(),
                    existed: false,
                }));
            }
        };

        if let Some(tenant) = &tenant {
            let tenant_idx = tenant.tenant_index.to_string();
            if existing_metadata.get("__tenant_idx__") != Some(&tenant_idx) {
                return Ok(Response::new(kyrodb::UpdateMetadataResponse {
                    success: true,
                    error: String::new(),
                    existed: false,
                }));
            }
        }

        // Enforce namespace boundary (treat mismatch as not-found to avoid leakage).
        if !req.namespace.is_empty() {
            let doc_namespace = existing_metadata
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
        metadata.remove("__tenant_id__");
        metadata.remove("__tenant_idx__");
        metadata.remove("__namespace__");
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
        let tenant = self.tenant_context(&request)?;
        self.enforce_rate_limit(tenant.as_ref())?;
        let req = request.into_inner();

        if req.doc_id == 0 {
            return Err(Status::invalid_argument("doc_id must be non-zero"));
        }

        tracing::Span::current().record("doc_id", req.doc_id);

        let global_doc_id = self.map_doc_id(tenant.as_ref(), req.doc_id)?;

        let engine = self.state.engine.read().await;

        // Fetch metadata first to enforce tenant/namespace checks without
        // revealing existence via embedding lookup timing.
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

        let start = Instant::now();
        match engine.query_with_source(global_doc_id, None) {
            Some((embedding, served_from)) => {
                let latency_ns = start.elapsed().as_nanos() as u64;
                let latency_ms = latency_ns as f64 / 1_000_000.0;
                self.state.metrics.record_query_latency(latency_ns);
                match served_from {
                    PointQueryTier::Cache => self.state.metrics.record_cache_hit(true),
                    PointQueryTier::HotTier => {
                        self.state.metrics.record_cache_hit(false);
                        self.state.metrics.record_tier_hit(true);
                    }
                    PointQueryTier::ColdTier => {
                        self.state.metrics.record_cache_hit(false);
                        self.state.metrics.record_tier_hit(false);
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
                    served_from: Self::map_query_tier(served_from),
                    error: String::new(),
                }))
            }
            None => {
                let latency_ns = start.elapsed().as_nanos() as u64;
                let latency_ms = latency_ns as f64 / 1_000_000.0;
                self.state.metrics.record_query_latency(latency_ns);
                self.state.metrics.record_cache_hit(false);

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
            let mut total_count: usize = 0;
            let mut pending: Vec<SearchRequest> = Vec::with_capacity(BULK_SEARCH_BATCH_SIZE);

            loop {
                if tx.is_closed() {
                    return;
                }

                if pending.is_empty() {
                    tokio::select! {
                        _ = tx.closed() => {
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

                            if let Err(status) = service.enforce_rate_limit(tenant.as_ref()) {
                                let _ = tx.send(Err(status)).await;
                                return;
                            }

                            total_count += 1;
                            if total_count > MAX_BATCH_SIZE {
                                let _ = tx
                                    .send(Err(Status::resource_exhausted(format!(
                                        "bulk_search batch size exceeds maximum {}",
                                        MAX_BATCH_SIZE
                                    ))))
                                    .await;
                                return;
                            }

                            pending.push(req);
                        }
                    }
                } else {
                    let next = tokio::time::timeout(
                        Duration::from_millis(BULK_SEARCH_MAX_WAIT_MS),
                        stream.message(),
                    )
                    .await;

                    match next {
                        Ok(Ok(Some(req))) => {
                            if let Err(status) = service.enforce_rate_limit(tenant.as_ref()) {
                                let _ = tx.send(Err(status)).await;
                                return;
                            }
                            total_count += 1;
                            if total_count > MAX_BATCH_SIZE {
                                let _ = tx
                                    .send(Err(Status::resource_exhausted(format!(
                                        "bulk_search batch size exceeds maximum {}",
                                        MAX_BATCH_SIZE
                                    ))))
                                    .await;
                                return;
                            }
                            pending.push(req);
                        }
                        Ok(Ok(None)) => {
                            if !pending.is_empty() {
                                let batch = std::mem::take(&mut pending);
                                let responses = service
                                    .handle_search_requests_batch(tenant.as_ref(), batch)
                                    .await;
                                for resp in responses {
                                    if tx.send(resp).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            return;
                        }
                        Ok(Err(e)) => {
                            let _ = tx
                                .send(Err(Status::internal(format!("stream error: {}", e))))
                                .await;
                            return;
                        }
                        Err(_) => {
                            let batch = std::mem::take(&mut pending);
                            let responses = service
                                .handle_search_requests_batch(tenant.as_ref(), batch)
                                .await;
                            for resp in responses {
                                if tx.send(resp).await.is_err() {
                                    return;
                                }
                            }
                            continue;
                        }
                    }
                }

                if pending.len() >= BULK_SEARCH_BATCH_SIZE {
                    let batch = std::mem::take(&mut pending);
                    let responses = service
                        .handle_search_requests_batch(tenant.as_ref(), batch)
                        .await;
                    for resp in responses {
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

        // Use bulk query with source tier metadata.
        let mapped_doc_ids = if let Some(tenant) = &tenant {
            let mut mapped = Vec::with_capacity(req.doc_ids.len());
            for id in &req.doc_ids {
                mapped.push(self.map_doc_id(Some(tenant), *id)?);
            }
            mapped
        } else {
            req.doc_ids.clone()
        };

        let results_vec = engine.bulk_query_with_source(&mapped_doc_ids, req.include_embeddings);

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
            let (mut embedding, mut metadata, served_from) =
                result.unwrap_or((vec![], HashMap::new(), PointQueryTier::ColdTier));

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
                if let Some(tenant) = &tenant {
                    let tenant_idx = tenant.tenant_index.to_string();
                    if metadata.get("__tenant_idx__") != Some(&tenant_idx) {
                        found = false;
                        embedding.clear();
                        metadata.clear();
                    }
                }
            }

            if found {
                total_found += 1;
                match served_from {
                    PointQueryTier::Cache => self.state.metrics.record_cache_hit(true),
                    PointQueryTier::HotTier => {
                        self.state.metrics.record_cache_hit(false);
                        self.state.metrics.record_tier_hit(true);
                    }
                    PointQueryTier::ColdTier => {
                        self.state.metrics.record_cache_hit(false);
                        self.state.metrics.record_tier_hit(false);
                    }
                }
            } else {
                self.state.metrics.record_cache_hit(false);
            }

            query_responses.push(QueryResponse {
                found,
                doc_id,
                embedding,
                metadata,
                served_from: if found {
                    Self::map_query_tier(served_from)
                } else {
                    query_response::Tier::Unknown as i32
                },
                error: String::new(),
            });
        }

        let latency_ns = start.elapsed().as_nanos() as u64;
        let latency_ms = latency_ns as f64 / 1_000_000.0;
        self.state.metrics.record_query_latency(latency_ns);
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
                        let tenant_idx = tenant.tenant_index.to_string();
                        let mut filtered = Vec::with_capacity(mapped.len());
                        for global_id in mapped {
                            if let Some(meta) = engine.get_metadata(global_id) {
                                if meta.get("__tenant_idx__") == Some(&tenant_idx) {
                                    filtered.push(global_id);
                                }
                            }
                        }
                        engine.batch_delete(&filtered)
                    } else {
                        let namespace = req.namespace.as_str();
                        let tenant_idx = tenant.tenant_index.to_string();
                        let mut filtered = Vec::with_capacity(mapped.len());
                        for global_id in mapped {
                            if let Some(meta) = engine.get_metadata(global_id) {
                                if meta.get("__tenant_idx__") != Some(&tenant_idx) {
                                    continue;
                                }
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
                // Combine tenant/namespace constraints into a structured AND filter so the
                // cold-tier inverted index can accelerate common cases.
                use kyrodb_engine::proto::metadata_filter::FilterType;
                use kyrodb_engine::proto::{AndFilter, ExactMatch, MetadataFilter};

                let mut filters = Vec::new();

                if let Some(tenant) = &tenant {
                    filters.push(MetadataFilter {
                        filter_type: Some(FilterType::Exact(ExactMatch {
                            key: "__tenant_idx__".to_string(),
                            value: tenant.tenant_index.to_string(),
                        })),
                    });
                }

                if !req.namespace.is_empty() {
                    filters.push(MetadataFilter {
                        filter_type: Some(FilterType::Exact(ExactMatch {
                            key: "__namespace__".to_string(),
                            value: req.namespace.clone(),
                        })),
                    });
                }

                filters.push(filter);

                let combined = if filters.len() == 1 {
                    match filters.pop() {
                        Some(single) => single,
                        None => return Err(Status::internal("failed to combine delete filters")),
                    }
                } else {
                    MetadataFilter {
                        filter_type: Some(FilterType::AndFilter(AndFilter { filters })),
                    }
                };

                engine.batch_delete_by_metadata_filter(&combined)
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
                self.decrement_tenant_vectors(tenant.as_ref(), count as usize);

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

        let wal_inconsistent = engine.cold_tier().is_wal_inconsistent();
        let wal_failed = self
            .state
            .metrics
            .get_wal_writes_failed()
            .max(engine.cold_tier().wal_writes_failed());
        self.state.metrics.set_wal_writes_failed_floor(wal_failed);
        let overall_health = apply_wal_health_overrides(
            self.state.metrics.health_status(),
            wal_inconsistent,
            wal_failed,
        );
        let status = match overall_health {
            HealthStatus::Healthy => health_response::Status::Healthy,
            HealthStatus::Degraded { .. } => health_response::Status::Degraded,
            HealthStatus::Starting => health_response::Status::Degraded,
            HealthStatus::Unhealthy { .. } => health_response::Status::Unhealthy,
        };

        // Component-level health.
        let component_filter = req.component.trim();
        if !component_filter.is_empty()
            && component_filter != "cache"
            && component_filter != "hot_tier"
            && component_filter != "cold_tier"
            && component_filter != "wal"
        {
            return Err(Status::invalid_argument(
                "component must be one of: cache, hot_tier, cold_tier, wal",
            ));
        }

        let mut components = HashMap::new();

        if component_filter.is_empty() || component_filter == "cache" {
            components.insert(
                "cache".to_string(),
                format!(
                    "healthy ({}% hit rate)",
                    (stats.cache_hit_rate * 100.0) as u32
                ),
            );
        }

        if component_filter.is_empty() || component_filter == "hot_tier" {
            let hot_status = if stats.hot_tier_flush_failures > 0 {
                format!(
                    "degraded ({} docs, {} flush failures)",
                    stats.hot_tier_size, stats.hot_tier_flush_failures
                )
            } else {
                format!("healthy ({} docs)", stats.hot_tier_size)
            };
            components.insert("hot_tier".to_string(), hot_status);
        }

        if component_filter.is_empty() || component_filter == "cold_tier" {
            components.insert(
                "cold_tier".to_string(),
                format!("healthy ({} docs)", stats.cold_tier_size),
            );
        }

        if component_filter.is_empty() || component_filter == "wal" {
            let wal_status = if wal_inconsistent {
                "unhealthy (WAL inconsistent; writes disabled)".to_string()
            } else if wal_failed > 0 {
                format!("degraded ({} failed WAL writes)", wal_failed)
            } else {
                "healthy".to_string()
            };
            components.insert("wal".to_string(), wal_status);
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
        refresh_system_metrics(&self.state);
        let engine = self.state.engine.read().await;
        let stats = engine.stats();
        let cache_size = engine.cache_size() as u64;
        self.state.metrics.set_cache_size(cache_size as usize);

        let (p50_ns, p95_ns, p99_ns) = self.state.metrics.latency_percentiles_ns();
        let uptime_secs = self.state.metrics.uptime().as_secs_f64().max(1e-9);
        let cpu_usage_percent =
            sample_process_cpu_usage_percent(self.state.metrics.uptime()).unwrap_or(0.0);

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
            cache_size,

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
            p50_latency_ms: p50_ns as f64 / 1_000_000.0,
            p95_latency_ms: p95_ns as f64 / 1_000_000.0,
            p99_latency_ms: p99_ns as f64 / 1_000_000.0,
            total_queries: stats.total_queries,
            total_inserts: stats.total_inserts,
            queries_per_second: stats.total_queries as f64 / uptime_secs,
            inserts_per_second: stats.total_inserts as f64 / uptime_secs,

            // System metrics
            memory_usage_bytes: self.state.metrics.get_memory_used_bytes(),
            disk_usage_bytes: self.state.metrics.get_disk_used_bytes(),
            cpu_usage_percent,

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

    #[instrument(skip(self, request))]
    async fn create_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        let req = request.into_inner();
        if !req.path.trim().is_empty() {
            return Err(Status::invalid_argument(
                "custom snapshot path is not supported; leave path empty to use configured data_dir",
            ));
        }

        let engine = self.state.engine.read().await;
        engine
            .cold_tier()
            .create_snapshot()
            .map_err(|e| Status::internal(format!("snapshot creation failed: {}", e)))?;

        let data_dir = self
            .state
            .engine_config
            .data_dir
            .clone()
            .ok_or_else(|| Status::internal("data_dir not configured"))?;
        let manifest_path = std::path::Path::new(&data_dir).join("MANIFEST");
        let manifest = Manifest::load(&manifest_path)
            .map_err(|e| Status::internal(format!("failed to load MANIFEST: {}", e)))?;
        let snapshot_name = manifest.latest_snapshot.ok_or_else(|| {
            Status::internal("snapshot created but MANIFEST has no latest_snapshot")
        })?;
        let snapshot_path = std::path::Path::new(&data_dir).join(&snapshot_name);
        let snapshot_size_bytes = std::fs::metadata(&snapshot_path)
            .map(|m| m.len())
            .map_err(|e| Status::internal(format!("failed to stat snapshot: {}", e)))?;
        let documents_snapshotted = engine.cold_tier().len() as u64;

        Ok(Response::new(SnapshotResponse {
            success: true,
            error: String::new(),
            snapshot_path: snapshot_path.to_string_lossy().to_string(),
            documents_snapshotted,
            snapshot_size_bytes,
        }))
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
            hnsw_m: engine_config.hnsw_m as u64,
            hnsw_ef_construction: engine_config.hnsw_ef_construction as u64,
            hnsw_ef_search: engine_config.hnsw_ef_search as u64,
            hnsw_distance: format!("{:?}", engine_config.hnsw_distance),
            hnsw_disable_normalization_check: engine_config.hnsw_disable_normalization_check,
        }))
    }
}

// ============================================================================
// HTTP OBSERVABILITY ENDPOINTS
// ============================================================================

async fn wal_health_snapshot(state: &Arc<ServerState>) -> (bool, u64) {
    let (wal_inconsistent, backend_wal_failed) = {
        let engine = state.engine.read().await;
        (
            engine.cold_tier().is_wal_inconsistent(),
            engine.cold_tier().wal_writes_failed(),
        )
    };
    let wal_failed = state
        .metrics
        .get_wal_writes_failed()
        .max(backend_wal_failed);
    state.metrics.set_wal_writes_failed_floor(wal_failed);
    (wal_inconsistent, wal_failed)
}

/// Prometheus /metrics endpoint handler
async fn metrics_handler(AxumState(state): AxumState<Arc<ServerState>>) -> HttpResponse<Body> {
    let _ = wal_health_snapshot(&state).await;
    refresh_system_metrics(&state);
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
    let (wal_inconsistent, wal_failed) = wal_health_snapshot(&state).await;
    let health =
        apply_wal_health_overrides(state.metrics.health_status(), wal_inconsistent, wal_failed);

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
    let (wal_inconsistent, wal_failed) = wal_health_snapshot(&state).await;
    let health =
        apply_wal_health_overrides(state.metrics.health_status(), wal_inconsistent, wal_failed);

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
    let thresholds = state.metrics.slo_thresholds();

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
            "p99_latency_ns": thresholds.p99_latency_ns,
            "min_cache_hit_rate": thresholds.min_cache_hit_rate,
            "max_error_rate": thresholds.max_error_rate,
            "min_availability": thresholds.min_availability,
            "min_samples": thresholds.min_samples,
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

async fn observability_auth_middleware(
    AxumState(state): AxumState<Arc<ServerState>>,
    req: axum::http::Request<Body>,
    next: Next,
) -> HttpResponse<Body> {
    let mode = state.app_config.server.observability_auth;
    if mode == ObservabilityAuthMode::Disabled {
        return next.run(req).await;
    }

    let path = req.uri().path();
    let requires_auth = match mode {
        ObservabilityAuthMode::Disabled => false,
        ObservabilityAuthMode::MetricsAndSlo => matches!(path, "/metrics" | "/slo"),
        ObservabilityAuthMode::All => true,
    };

    if !requires_auth {
        return next.run(req).await;
    }

    let auth = match state.auth.as_ref() {
        Some(auth) => auth,
        None => {
            let mut resp = HttpResponse::new(Body::from("auth not initialized"));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            return resp;
        }
    };

    let api_key = req
        .headers()
        .get(API_KEY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| {
            req.headers()
                .get(AUTHORIZATION_HEADER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(str::to_string)
        });

    match api_key.and_then(|k| auth.validate(&k)) {
        Some(_tenant) => next.run(req).await,
        None => {
            let mut resp = HttpResponse::new(Body::from("unauthorized"));
            *resp.status_mut() = StatusCode::UNAUTHORIZED;
            resp
        }
    }
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

    info!(
        p99_latency_ms = config.slo.p99_latency_ms,
        cache_hit_rate = config.slo.cache_hit_rate,
        error_rate = config.slo.error_rate,
        availability = config.slo.availability,
        min_samples = config.slo.min_samples,
        "SLO thresholds configured"
    );

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

        let global_limit = if config.rate_limit.enabled {
            Some(config.rate_limit.max_qps_global as u32)
        } else {
            None
        };
        (
            Some(auth),
            Some(tenant_id_mapper),
            Some(RateLimiter::new_with_global(global_limit)),
        )
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
        hot_tier_max_age: Duration::from_secs(
            config
                .cache
                .hot_tier_max_age_secs
                .unwrap_or(config.cache.training_interval_secs),
        ),
        hnsw_max_elements: config.hnsw.max_elements,
        embedding_dimension: config.hnsw.dimension,
        hnsw_distance: config.hnsw.distance,
        hnsw_m: config.hnsw.m,
        hnsw_ef_construction: config.hnsw.ef_construction,
        hnsw_ef_search: config.hnsw.ef_search,
        hnsw_disable_normalization_check: config.hnsw.disable_normalization_check,
        data_dir: Some(config.persistence.data_dir.to_string_lossy().to_string()),
        fsync_policy,
        snapshot_interval: config.snapshot_interval_mutations(),
        max_wal_size_bytes: config.persistence.max_wal_size_bytes,
        flush_interval: config.wal_flush_interval(),
        cache_timeout_ms: config.timeouts.cache_ms,
        hot_tier_timeout_ms: config.timeouts.hot_tier_ms,
        cold_tier_timeout_ms: config.timeouts.cold_tier_ms,
        max_concurrent_queries: 1000, // Load shedding: max 1000 in-flight queries
    };

    // Create shutdown broadcast channel early for prefetch tasks
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(16);

    // Initialize or recover engine
    info!("Initializing TieredEngine with configured cache strategy...");

    // Helper to create cache strategy (L1a) and expose learned strategy for training.
    let create_cache_strategy = || {
        let lru_strategy = Arc::new(LruCacheStrategy::new(config.cache.capacity));

        let predictor_capacity = config
            .cache
            .capacity
            .saturating_mul(config.cache.predictor_capacity_multiplier.max(1))
            .min(config.hnsw.max_elements.max(1));

        let learned_predictor = LearnedCachePredictor::with_config(
            predictor_capacity,
            config.cache.admission_threshold,
            Duration::from_secs(config.cache.training_window_secs),
            Duration::from_secs(config.cache.recency_halflife_secs),
            Duration::from_secs(config.cache.training_interval_secs),
        )
        .context("Failed to create Hybrid Semantic Cache predictor")?;
        let mut learned_predictor = learned_predictor;
        learned_predictor.set_auto_tune(config.cache.auto_tune_threshold);
        learned_predictor.set_target_utilization(config.cache.target_utilization);

        // NOTE: L1b query cache handles semantic search‑result reuse.
        let learned_strategy = Arc::new(LearnedCacheStrategy::new(
            config.cache.capacity,
            learned_predictor,
        ));

        let (strategy_box, learned_for_training): (
            Box<dyn kyrodb_engine::CacheStrategy>,
            Option<Arc<LearnedCacheStrategy>>,
        ) = match config.cache.strategy {
            kyrodb_engine::config::CacheStrategy::Lru => {
                (Box::new(LruCacheStrategy::new(config.cache.capacity)), None)
            }
            kyrodb_engine::config::CacheStrategy::Learned => (
                Box::new(SharedLearnedCacheStrategy::new(learned_strategy.clone())),
                Some(learned_strategy.clone()),
            ),
            kyrodb_engine::config::CacheStrategy::AbTest => (
                Box::new(AbTestSplitter::new(
                    lru_strategy.clone(),
                    learned_strategy.clone(),
                )),
                Some(learned_strategy.clone()),
            ),
        };

        Ok::<
            (
                Box<dyn kyrodb_engine::CacheStrategy>,
                Option<Arc<LearnedCacheStrategy>>,
            ),
            anyhow::Error,
        >((strategy_box, learned_for_training))
    };

    let (cache_strategy, mut learned_strategy_for_training) = create_cache_strategy()?;

    // Create query cache (L1b) - semantic search‑result cache
    let query_cache = Arc::new(kyrodb_engine::QueryHashCache::new(
        config.cache.query_cache_capacity,
        config.cache.query_cache_similarity_threshold,
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

    let mut engine = if should_attempt_recovery {
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

                let (fallback_cache_strategy, fallback_learned_strategy_for_training) =
                    create_cache_strategy()?;
                learned_strategy_for_training = fallback_learned_strategy_for_training;
                let query_cache_fallback = Arc::new(kyrodb_engine::QueryHashCache::new(
                    config.cache.query_cache_capacity,
                    config.cache.query_cache_similarity_threshold,
                ));
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

    // Enable learned-cache training end-to-end:
    // - attach access logger so queries generate training events
    // - spawn periodic retraining task to update predictor
    let should_run_training = config.cache.enable_training_task
        && learned_strategy_for_training.is_some()
        && config.cache.training_interval_secs > 0;

    let access_logger = if should_run_training {
        Some(Arc::new(parking_lot::RwLock::new(
            AccessPatternLogger::new(config.cache.logger_window_size.max(1)),
        )))
    } else {
        None
    };

    if let Some(logger) = &access_logger {
        engine.set_access_logger(logger.clone());
    }

    // Wrap engine in Arc<RwLock> for concurrent access
    let engine_arc = Arc::new(RwLock::new(engine));

    // Create metrics collector with configured SLO thresholds.
    let metrics =
        MetricsCollector::new_with_slo_thresholds(SloThresholds::from_config(&config.slo));

    if should_run_training {
        match (learned_strategy_for_training, access_logger) {
            (Some(learned_strategy), Some(logger)) => {
                let training_config = TrainingConfig {
                    interval: Duration::from_secs(config.cache.training_interval_secs),
                    window_duration: Duration::from_secs(config.cache.training_window_secs),
                    recency_halflife: Duration::from_secs(config.cache.recency_halflife_secs),
                    min_events_for_training: config.cache.min_training_samples.max(1),
                    predictor_capacity: config
                        .cache
                        .capacity
                        .saturating_mul(config.cache.predictor_capacity_multiplier.max(1))
                        .min(config.hnsw.max_elements.max(1)),
                    admission_threshold: config.cache.admission_threshold,
                    auto_tune_enabled: config.cache.auto_tune_threshold,
                    target_utilization: config.cache.target_utilization,
                };

                let training_shutdown_rx = shutdown_tx.subscribe();
                let _training_handle = spawn_training_task(
                    logger,
                    learned_strategy,
                    training_config,
                    None,
                    Some(metrics.clone()),
                    training_shutdown_rx,
                )
                .await;

                info!(
                    interval_secs = config.cache.training_interval_secs,
                    window_secs = config.cache.training_window_secs,
                    logger_window_size = config.cache.logger_window_size,
                    "Learned cache background retraining enabled"
                );
            }
            _ => {
                warn!(
                    enabled = config.cache.enable_training_task,
                    strategy = ?config.cache.strategy,
                    "Learned cache training requested but dependencies were not initialized; training disabled"
                );
            }
        }
    } else {
        info!(
            enabled = config.cache.enable_training_task,
            strategy = ?config.cache.strategy,
            "Learned cache background retraining disabled"
        );
    }

    // Spawn background flush task with graceful shutdown
    let mut flush_shutdown_rx = shutdown_tx.subscribe();
    let engine_for_flush = engine_arc.clone();
    let flush_interval = if engine_config.flush_interval.is_zero() {
        Duration::from_secs(1)
    } else {
        engine_config.flush_interval
    };

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(flush_interval);
        info!(
            interval_ms = flush_interval.as_millis() as u64,
            "Background flush task started"
        );

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

    let tenant_vector_counts = if config.auth.enabled {
        let mut counts: HashMap<String, usize> = HashMap::new();
        if let (Some(auth_mgr), Some(mapper)) = (&auth, &tenant_id_mapper) {
            let tenants = auth_mgr.enabled_tenants();
            let engine = engine_arc.read().await;
            for tenant in tenants {
                let tenant_index = mapper
                    .ensure_tenant(&tenant.tenant_id)
                    .map_err(|e| anyhow::anyhow!("tenant mapping error: {}", e))?;
                let tenant_idx_str = tenant_index.to_string();
                let cold_filter = kyrodb::MetadataFilter {
                    filter_type: Some(kyrodb::metadata_filter::FilterType::Exact(
                        kyrodb::ExactMatch {
                            key: "__tenant_idx__".to_string(),
                            value: tenant_idx_str.clone(),
                        },
                    )),
                };
                let cold_count = engine
                    .cold_tier()
                    .ids_for_metadata_filter(&cold_filter)
                    .len();
                let hot_count = engine
                    .hot_tier()
                    .scan(|meta| meta.get("__tenant_idx__") == Some(&tenant_idx_str))
                    .len();
                counts.insert(
                    tenant.tenant_id.clone(),
                    cold_count.saturating_add(hot_count),
                );
            }
        }
        Some(parking_lot::RwLock::new(counts))
    } else {
        None
    };

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
        tenant_vector_counts,
    });

    // Create gRPC service with engine Arc reference
    let grpc_service = KyroDBServiceImpl {
        state: state.clone(),
    };

    // Build HTTP router for observability endpoints
    let mut http_app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/slo", get(slo_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    if config.server.observability_auth != ObservabilityAuthMode::Disabled {
        http_app = http_app.layer(middleware::from_fn_with_state(
            state.clone(),
            observability_auth_middleware,
        ));
    }

    // HTTP port for observability (from config)
    let http_port = config.http_port();
    let http_addr =
        format!("{}:{}", config.http_host(), http_port).parse::<std::net::SocketAddr>()?;

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

            let rate_limit_enabled = state_for_interceptor.app_config.rate_limit.enabled;
            let default_max_qps = state_for_interceptor
                .app_config
                .rate_limit
                .max_qps_per_connection as u32;
            let effective_max_qps = if tenant.max_qps == 0 {
                if rate_limit_enabled {
                    default_max_qps.max(1)
                } else {
                    u32::MAX
                }
            } else {
                tenant.max_qps
            };

            req.extensions_mut().insert(TenantContext {
                tenant_id: tenant.tenant_id,
                tenant_index,
                max_qps: effective_max_qps,
                max_vectors: tenant.max_vectors,
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

#[cfg(test)]
mod tests {
    use super::{
        apply_wal_health_overrides, classify_search_error_message, BatchSearchKey, HealthStatus,
        InsertRequest, KyroDBServiceImpl, KyroDbService, SearchErrorKind, SearchRequest,
        ServerState, TenantContext, TieredEngine, TieredEngineConfig,
    };
    use kyrodb_engine::{
        cache_strategy::LruCacheStrategy, config::KyroDbConfig, MetricsCollector, QueryHashCache,
        RateLimiter,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::RwLock;
    use tonic::{Code, Request};

    fn build_test_service_with_seed_data() -> Arc<KyroDBServiceImpl> {
        let engine_config = TieredEngineConfig {
            embedding_dimension: 16,
            ..TieredEngineConfig::default()
        };
        let engine = TieredEngine::new(
            Box::new(LruCacheStrategy::new(2048)),
            Arc::new(QueryHashCache::new(2048, 0.90)),
            Vec::new(),
            Vec::new(),
            engine_config.clone(),
        )
        .unwrap();

        for doc_id in 1..=1024u64 {
            let mut embedding = Vec::with_capacity(16);
            for i in 0..16usize {
                let base = (((doc_id as usize + 1) * (i + 3)) % 997) as f32 / 997.0;
                embedding.push(base + 0.001);
            }
            engine.insert(doc_id, embedding, HashMap::new()).unwrap();
        }

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(8);
        let state = Arc::new(ServerState {
            engine: Arc::new(RwLock::new(engine)),
            start_time: Instant::now(),
            app_config: KyroDbConfig::default(),
            engine_config,
            shutdown_tx,
            metrics: MetricsCollector::new(),
            auth: None,
            rate_limiter: None,
            tenant_id_mapper: None,
            tenant_vector_counts: None,
        });

        Arc::new(KyroDBServiceImpl { state })
    }

    fn build_test_service_with_tenant_quota(max_vectors: usize) -> Arc<KyroDBServiceImpl> {
        let engine_config = TieredEngineConfig {
            embedding_dimension: 16,
            ..TieredEngineConfig::default()
        };
        let engine = TieredEngine::new(
            Box::new(LruCacheStrategy::new(2048)),
            Arc::new(QueryHashCache::new(2048, 0.90)),
            Vec::new(),
            Vec::new(),
            engine_config.clone(),
        )
        .unwrap();

        let mut app_config = KyroDbConfig::default();
        app_config.auth.enabled = true;
        app_config.rate_limit.enabled = true;

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(8);
        let state = Arc::new(ServerState {
            engine: Arc::new(RwLock::new(engine)),
            start_time: Instant::now(),
            app_config,
            engine_config,
            shutdown_tx,
            metrics: MetricsCollector::new(),
            auth: None,
            rate_limiter: Some(RateLimiter::new()),
            tenant_id_mapper: None,
            tenant_vector_counts: Some(parking_lot::RwLock::new(HashMap::new())),
        });

        let service = Arc::new(KyroDBServiceImpl { state });
        let tenant_id = "tenant_quota".to_string();
        if let Some(counts) = &service.state.tenant_vector_counts {
            counts.write().insert(tenant_id, 0);
        }

        // Ensure the helper cannot silently ignore max_vectors=0.
        assert!(max_vectors > 0, "max_vectors must be > 0 in quota tests");
        service
    }

    fn tenant_ctx(max_vectors: usize) -> TenantContext {
        TenantContext {
            tenant_id: "tenant_quota".to_string(),
            tenant_index: 7,
            max_qps: 1_000_000,
            max_vectors,
        }
    }

    fn tenant_insert_request(
        local_doc_id: u64,
        embedding: Vec<f32>,
        tenant: TenantContext,
    ) -> Request<InsertRequest> {
        let mut req = Request::new(InsertRequest {
            doc_id: local_doc_id,
            embedding,
            metadata: HashMap::new(),
            namespace: String::new(),
        });
        req.extensions_mut().insert(tenant);
        req
    }

    #[test]
    fn classify_search_errors_maps_timeout() {
        assert_eq!(
            classify_search_error_message("Cold tier timed out after 1000ms"),
            SearchErrorKind::Timeout
        );
    }

    #[test]
    fn classify_search_errors_maps_resource_exhausted() {
        assert_eq!(
            classify_search_error_message("Query queue saturated: 1024 in-flight queries"),
            SearchErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn classify_search_errors_maps_validation() {
        assert_eq!(
            classify_search_error_message("query dimension mismatch: expected 768 found 384"),
            SearchErrorKind::Validation
        );
    }

    #[test]
    fn wal_inconsistency_forces_unhealthy() {
        let health = apply_wal_health_overrides(HealthStatus::Healthy, true, 0);
        assert!(matches!(health, HealthStatus::Unhealthy { .. }));
    }

    #[test]
    fn wal_failures_degrade_healthy_status() {
        let health = apply_wal_health_overrides(HealthStatus::Healthy, false, 3);
        assert!(matches!(health, HealthStatus::Degraded { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bulk_batch_groups_do_not_starve_writers() {
        let service = build_test_service_with_seed_data();

        // Build many 1-query groups (distinct namespaces => distinct BatchSearchKey values)
        // so the handler must repeatedly re-enter search execution per group.
        let requests: Vec<SearchRequest> = (0..512usize)
            .map(|i| {
                let mut query_embedding = Vec::with_capacity(16);
                for d in 0..16usize {
                    let base = (((i + 7) * (d + 11)) % 997) as f32 / 997.0;
                    query_embedding.push(base + 0.001);
                }
                SearchRequest {
                    query_embedding,
                    k: 10,
                    ef_search: 128,
                    include_embeddings: false,
                    filter: None,
                    metadata_filters: HashMap::new(),
                    namespace: format!("ns_{i}"),
                    min_score: 0.0,
                }
            })
            .collect();

        // Sanity check that request set spans many batch groups.
        let mut groups = std::collections::HashSet::new();
        for req in &requests {
            groups.insert(BatchSearchKey {
                search_k: req.k as usize,
                ef_search_override: Some(req.ef_search as usize),
                query_cache_scope: service.query_cache_scope(None, req),
            });
        }
        assert!(groups.len() > 100, "expected many distinct batch groups");

        let service_for_search = Arc::clone(&service);
        let search_task = tokio::spawn(async move {
            service_for_search
                .handle_search_requests_batch(None, requests)
                .await
        });

        // Let search acquire its first read-lock and start work.
        tokio::time::sleep(Duration::from_millis(5)).await;

        let writer_engine = Arc::clone(&service.state.engine);
        let writer_start = Instant::now();
        let writer_guard = tokio::time::timeout(Duration::from_secs(2), writer_engine.write())
            .await
            .expect("writer lock acquisition timed out during bulk search");
        writer_guard
            .insert(99_999, vec![0.1; 16], HashMap::new())
            .unwrap();
        drop(writer_guard);
        let writer_wait = writer_start.elapsed();

        let responses = search_task.await.unwrap();
        assert_eq!(responses.len(), 512);
        assert!(
            responses.iter().all(|r| r.is_ok()),
            "all grouped bulk-search responses should succeed"
        );
        assert!(
            writer_wait < Duration::from_secs(1),
            "writer should not be starved behind entire bulk batch; waited {:?}",
            writer_wait
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tenant_quota_enforced_atomically_under_concurrent_inserts() {
        let service = build_test_service_with_tenant_quota(1);
        let tenant = tenant_ctx(1);

        let s1 = Arc::clone(&service);
        let t1 = tenant.clone();
        let h1 = tokio::spawn(async move {
            let req = tenant_insert_request(1, vec![0.1; 16], t1);
            s1.insert(req).await
        });

        let s2 = Arc::clone(&service);
        let t2 = tenant.clone();
        let h2 = tokio::spawn(async move {
            let req = tenant_insert_request(2, vec![0.2; 16], t2);
            s2.insert(req).await
        });

        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();

        let success_count = usize::from(r1.is_ok()) + usize::from(r2.is_ok());
        assert_eq!(
            success_count, 1,
            "exactly one concurrent insert should pass max_vectors=1"
        );

        let status_codes: Vec<Code> = [r1, r2]
            .into_iter()
            .filter_map(|result| result.err().map(|status| status.code()))
            .collect();
        assert!(
            status_codes.contains(&Code::ResourceExhausted),
            "one insert must fail with RESOURCE_EXHAUSTED, got {:?}",
            status_codes
        );

        let quota_count = service
            .state
            .tenant_vector_counts
            .as_ref()
            .unwrap()
            .read()
            .get("tenant_quota")
            .copied()
            .unwrap_or(0);
        assert_eq!(
            quota_count, 1,
            "quota counter must remain consistent after concurrent inserts"
        );
    }

    #[tokio::test]
    async fn tenant_quota_reservation_rolls_back_after_failed_insert() {
        let service = build_test_service_with_tenant_quota(1);
        let tenant = tenant_ctx(1);

        // Wrong dimension causes engine.insert() to fail after quota reservation.
        let bad_req = tenant_insert_request(1, vec![0.1; 8], tenant.clone());
        let bad_status = service
            .insert(bad_req)
            .await
            .expect_err("insert with wrong dimension should fail");
        assert_eq!(bad_status.code(), Code::Internal);

        let after_failure_count = service
            .state
            .tenant_vector_counts
            .as_ref()
            .unwrap()
            .read()
            .get("tenant_quota")
            .copied()
            .unwrap_or(0);
        assert_eq!(
            after_failure_count, 0,
            "quota reservation must be rolled back when insert fails"
        );

        let good_req = tenant_insert_request(2, vec![0.2; 16], tenant);
        let good_res = service.insert(good_req).await;
        assert!(
            good_res.is_ok(),
            "quota rollback failed: subsequent valid insert was unexpectedly rejected: {:?}",
            good_res.err()
        );

        let final_count = service
            .state
            .tenant_vector_counts
            .as_ref()
            .unwrap()
            .read()
            .get("tenant_quota")
            .copied()
            .unwrap_or(0);
        assert_eq!(final_count, 1, "valid insert should consume one quota slot");
    }
}
