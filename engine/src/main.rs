//! CLI wrapper around PersistentEventLog.

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::stream::StreamExt;
use kyrodb_engine as engine_crate;
use std::sync::Arc;
use uuid::Uuid;
use warp::Filter;
#[cfg(feature = "grpc")]
mod grpc_svc;
mod sql;

// --- Rate limiting (simple token-bucket per client IP) -----------------------
use dashmap::DashMap;
use std::time::Instant;

#[derive(Clone)]
struct SimpleRateLimiter {
    capacity: f64,
    refill_per_sec: f64,
    buckets: Arc<DashMap<String, (f64, Instant)>>,
}

impl SimpleRateLimiter {
    fn new(rps: f64, burst: f64) -> Self {
        Self {
            capacity: burst.max(1.0),
            refill_per_sec: rps.max(0.1),
            buckets: Arc::new(DashMap::new()),
        }
    }
    fn allow(&self, key: &str, cost: f64) -> bool {
        let now = Instant::now();
        let mut entry = self
            .buckets
            .entry(key.to_string())
            .or_insert_with(|| (self.capacity, now));
        let (ref mut tokens, ref mut last) = *entry;
        let elapsed = now.duration_since(*last).as_secs_f64();
        if elapsed > 0.0 {
            let new_tokens = (*tokens + elapsed * self.refill_per_sec).min(self.capacity);
            *tokens = new_tokens;
            *last = now;
        }
        if *tokens >= cost {
            *tokens -= cost;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct AdminRateLimited;
impl warp::reject::Reject for AdminRateLimited {}
#[derive(Debug)]
struct DataRateLimited;
impl warp::reject::Reject for DataRateLimited {}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(default)
}

fn mk_rl_filter_admin(
    limiter: SimpleRateLimiter,
) -> impl Filter<Extract = ((),), Error = warp::Rejection> + Clone {
    warp::addr::remote().and_then(move |addr: Option<std::net::SocketAddr>| {
        let limiter = limiter.clone();
        async move {
            let key = addr
                .map(|a| a.ip().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            if limiter.allow(&key, 1.0) {
                Ok(())
            } else {
                Err(warp::reject::custom(AdminRateLimited))
            }
        }
    })
}
fn mk_rl_filter_data(
    limiter: SimpleRateLimiter,
) -> impl Filter<Extract = ((),), Error = warp::Rejection> + Clone {
    warp::addr::remote().and_then(move |addr: Option<std::net::SocketAddr>| {
        let limiter = limiter.clone();
        async move {
            let key = addr
                .map(|a| a.ip().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            if limiter.allow(&key, 1.0) {
                Ok(())
            } else {
                Err(warp::reject::custom(DataRateLimited))
            }
        }
    })
}
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "kyrodb-engine", about = "KyroDB Engine")]
struct Cli {
    /// Directory for data files (snapshots + WAL)
    #[arg(short, long, default_value = "./data")]
    data_dir: String,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Serve {
        host: String,
        port: u16,

        /// Automatically trigger snapshot every N seconds
        #[arg(long)]
        auto_snapshot_secs: Option<u64>,

        /// Bearer token required for protected HTTP endpoints (Authorization: Bearer <token>)
        #[arg(long)]
        auth_token: Option<String>,
        /// Trigger snapshot when N new events have been appended since last snapshot
        #[arg(long)]
        snapshot_every_n_appends: Option<u64>,
        /// Rotate/compact when WAL reaches this many bytes
        #[arg(long)]
        wal_max_bytes: Option<u64>,
        /// Rebuild RMI when N appends since last build
        #[arg(long)]
        rmi_rebuild_appends: Option<u64>,
        /// Rebuild RMI when delta/total ratio exceeds R (0.0-1.0)
        #[arg(long)]
        rmi_rebuild_ratio: Option<f64>,
        /// WAL rotation: per-segment max bytes
        #[arg(long)]
        wal_segment_bytes: Option<u64>,
        /// WAL retention: max segments to keep
        #[arg(long, default_value_t = 8)]
        wal_max_segments: usize,
        /// Background compaction every N seconds (0 to disable)
        #[arg(long, default_value_t = 0)]
        compact_interval_secs: u64,
        /// Compact when WAL bytes exceed this threshold (0 to disable)
        #[arg(long, default_value_t = 0)]
        compact_when_wal_bytes: u64,
        /// Optional gRPC bind address (e.g., 0.0.0.0:50051). If set, start gRPC data-plane.
        #[arg(long)]
        grpc_addr: Option<String>,
        /// TLS certificate file path (enables HTTPS)
        #[arg(long)]
        _tls_cert: Option<String>,
        /// TLS private key file path (enables HTTPS)
        #[arg(long)]
        _tls_key: Option<String>,
        /// Admin token for privileged operations (separate from auth_token for read/write)
        #[arg(long)]
        admin_token: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // init structured JSON logs with env-based filter (RUST_LOG)
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();

    // Build info stamp
    let build_commit: &str = option_env!("GIT_COMMIT_HASH").unwrap_or("unknown");
    let build_features: &str = option_env!("CARGO_FEATURES").unwrap_or("");
    tracing::info!(
        commit = build_commit,
        features = build_features,
        "build_info"
    );

    let cli = Cli::parse();
    let log = Arc::new(
        engine_crate::PersistentEventLog::open(std::path::Path::new(&cli.data_dir)).await?,
    );

    // --- Rate limiter configs (env tunables) --------------------------------
    let admin_rps = env_f64("KYRODB_RL_ADMIN_RPS", 2.0);
    let admin_burst = env_f64("KYRODB_RL_ADMIN_BURST", 5.0);
    let data_rps = env_f64("KYRODB_RL_DATA_RPS", 5000.0);
    let data_burst = env_f64("KYRODB_RL_DATA_BURST", 10000.0);
    let admin_limiter = SimpleRateLimiter::new(admin_rps, admin_burst);
    let data_limiter = SimpleRateLimiter::new(data_rps, data_burst);
    let admin_rl = mk_rl_filter_admin(admin_limiter.clone()).boxed();
    let data_rl = mk_rl_filter_data(data_limiter.clone()).boxed();
    // ------------------------------------------------------------------------

    match cli.cmd {
        Commands::Serve {
            host,
            port,
            auto_snapshot_secs,
            auth_token,
            snapshot_every_n_appends,
            wal_max_bytes,
            rmi_rebuild_appends,
            rmi_rebuild_ratio,
            wal_segment_bytes,
            wal_max_segments,
            compact_interval_secs,
            compact_when_wal_bytes,
            grpc_addr,
            _tls_cert,
            _tls_key,
            admin_token,
        } => {
            // Silence unused when learned-index feature is disabled
            #[cfg(not(feature = "learned-index"))]
            let _ = (rmi_rebuild_appends.as_ref(), rmi_rebuild_ratio.as_ref());

            // Optional warm-on-start: fault-in snapshot and RMI pages before serving
            if std::env::var("KYRODB_WARM_ON_START").ok().as_deref() == Some("1") {
                println!("🔥 Warm-on-start enabled; warming data and index pages...");
                log.warmup().await;
                println!("🔥 Warm-on-start complete.");
            }

            // Configure WAL rotation if requested
            if wal_segment_bytes.is_some() || wal_max_segments > 0 {
                log.configure_wal_rotation(wal_segment_bytes, wal_max_segments)
                    .await;
            }

            // Background, size-based compaction trigger
            if let Some(maxb) = wal_max_bytes {
                if maxb > 0 {
                    let log_for_size = log.clone();
                    tokio::spawn(async move {
                        let mut interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(2));
                        loop {
                            interval.tick().await;
                            let size = log_for_size.wal_size_bytes();
                            if size >= maxb {
                                println!(
                                    "📦 Size-based compaction: wal={} bytes >= {}",
                                    size, maxb
                                );
                                if let Err(e) =
                                    log_for_size.compact_keep_latest_and_snapshot().await
                                {
                                    eprintln!("❌ Size-based compaction failed: {}", e);
                                } else {
                                    println!("✅ Size-based compaction complete.");
                                }
                            }
                        }
                    });
                }
            }

            // Background interval compaction
            if compact_interval_secs > 0 {
                let log_for_compact = log.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                        compact_interval_secs,
                    ));
                    loop {
                        interval.tick().await;
                        if compact_when_wal_bytes == 0
                            || log_for_compact.wal_size_bytes() >= compact_when_wal_bytes
                        {
                            if let Err(e) = log_for_compact.compact_keep_latest_and_snapshot().await
                            {
                                eprintln!("❌ Interval compaction failed: {}", e);
                            } else {
                                println!("✅ Interval compaction complete.");
                            }
                        }
                    }
                });
            }

            if let Some(secs) = auto_snapshot_secs {
                if secs > 0 {
                    let snapshot_log = log.clone();
                    tokio::spawn(async move {
                        println!("📸 Auto-snapshot enabled every {} seconds.", secs);
                        let mut interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(secs));
                        loop {
                            interval.tick().await;
                            println!("📸 Kicking off automatic snapshot...");
                            if let Err(e) = snapshot_log.snapshot().await {
                                eprintln!("❌ Auto-snapshot failed: {}", e);
                            } else {
                                println!("✅ Snapshot complete.");
                            }
                        }
                    });
                }
            }
            if let Some(n) = snapshot_every_n_appends {
                if n > 0 {
                    let snap_log = log.clone();
                    tokio::spawn(async move {
                        let mut last = snap_log.get_offset().await;
                        let mut interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(1));
                        loop {
                            interval.tick().await;
                            let cur = snap_log.get_offset().await;
                            if cur.saturating_sub(last) >= n {
                                println!("📦 Compaction trigger: {} new events", cur - last);
                                if let Err(e) = snap_log.compact_keep_latest_and_snapshot().await {
                                    eprintln!("❌ Compaction failed: {}", e);
                                } else {
                                    last = cur;
                                    println!("✅ Compaction complete.");
                                }
                            }
                        }
                    });
                }
            }

            // Background RMI rebuild triggers (feature-gated)
            #[cfg(feature = "learned-index")]
            if rmi_rebuild_appends.unwrap_or(0) > 0 || rmi_rebuild_ratio.unwrap_or(0.0) > 0.0 {
                let data_dir = cli.data_dir.clone();
                let rebuild_log = log.clone();
                let app_thresh = rmi_rebuild_appends.unwrap_or(0);
                let ratio_thresh = rmi_rebuild_ratio.unwrap_or(0.0);
                tokio::spawn(async move {
                    let mut last_built = rebuild_log.get_offset().await;
                    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
                    loop {
                        interval.tick().await;
                        let cur = rebuild_log.get_offset().await;
                        let appended = cur.saturating_sub(last_built);
                        let pairs = rebuild_log.collect_key_offset_pairs().await;
                        let distinct = pairs.len() as u64;
                        let ratio = if distinct == 0 {
                            0.0
                        } else {
                            appended as f64 / distinct as f64
                        };
                        if (app_thresh > 0 && appended >= app_thresh)
                            || (ratio_thresh > 0.0 && ratio >= ratio_thresh)
                        {
                            let tmp = std::path::Path::new(&data_dir).join("index-rmi.tmp");
                            let dst = std::path::Path::new(&data_dir).join("index-rmi.bin");
                            let rebuild_timer =
                                engine_crate::metrics::RMI_REBUILD_DURATION_SECONDS.start_timer();
                            engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(1.0);
                            // Write index on blocking thread
                            let pairs_clone = pairs.clone();
                            let tmp_clone = tmp.clone();
                            let write_res = tokio::task::spawn_blocking(move || {
                                engine_crate::index::RmiIndex::write_from_pairs_auto(
                                    &tmp_clone,
                                    &pairs_clone,
                                )
                            })
                            .await;
                            if let Ok(Err(e)) = write_res {
                                eprintln!("❌ RMI rebuild write failed: {}", e);
                                rebuild_timer.observe_duration();
                                engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(0.0);
                                continue;
                            }
                            if write_res.is_err() {
                                eprintln!("❌ RMI rebuild task panicked");
                                rebuild_timer.observe_duration();
                                engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(0.0);
                                continue;
                            }
                            if let Ok(f) = std::fs::OpenOptions::new().read(true).open(&tmp) {
                                let _ = f.sync_all();
                            }
                            if let Err(e) = std::fs::rename(&tmp, &dst) {
                                eprintln!("❌ RMI rename failed: {}", e);
                                rebuild_timer.observe_duration();
                                engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(0.0);
                                continue;
                            }
                            // Ensure directory metadata is durable after rename
                            if let Err(e) = engine_crate::fsync_dir(std::path::Path::new(&data_dir))
                            {
                                eprintln!(
                                    "⚠️ fsync data dir after RMI rebuild rename failed: {}",
                                    e
                                );
                            }
                            if let Some(rmi) = engine_crate::index::RmiIndex::load_from_file(&dst) {
                                rebuild_log
                                    .swap_primary_index(engine_crate::index::PrimaryIndex::Rmi(rmi))
                                    .await;
                                last_built = cur;
                                engine_crate::metrics::RMI_REBUILDS_TOTAL.inc();
                                rebuild_timer.observe_duration();
                                let _ = rebuild_log.write_manifest().await;
                                println!("✅ RMI rebuilt, swapped, and manifest committed (appended={}, ratio={:.3})", appended, ratio);
                            } else {
                                eprintln!("❌ RMI reload failed after rebuild");
                                rebuild_timer.observe_duration();
                            }
                            engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(0.0);
                        }
                    }
                });
            }

            // Start gRPC data-plane server if requested (lookup/get fast paths)
            #[cfg(feature = "grpc")]
            if let Some(addr_str) = grpc_addr.clone() {
                let addr: std::net::SocketAddr = addr_str.parse().expect("invalid --grpc-addr");
                let svc = grpc_svc::GrpcService::new(log.clone());
                tokio::spawn(async move {
                    println!("📡 gRPC serving on {}", addr);
                    tonic::transport::Server::builder()
                        .add_service(svc.into_server())
                        .serve(addr)
                        .await
                        .expect("gRPC server failed");
                });
            }

            // --- Metrics to count RMI vs BTree reads ---
            #[cfg(not(feature = "bench-no-metrics"))]
            let (_rmi_reads_counter, _btree_reads_counter) = {
                use prometheus::register_counter;
                (
                    register_counter!("kyrodb_rmi_reads_total", "Total reads served by RMI").ok(),
                    register_counter!("kyrodb_btree_reads_total", "Total reads served by BTree")
                        .ok(),
                )
            };

            // --- Fast lookup endpoints (HTTP hot path) with versioned prefix -----------------
            let v1 = warp::path("v1");

            let raw_log = log.clone();
            let lookup_raw = v1
                .and(warp::path("lookup_raw"))
                .and(warp::get())
                .and(warp::query::<std::collections::HashMap<String, String>>())
                .and_then(move |q: std::collections::HashMap<String, String>| {
                    let log = raw_log.clone();
                    async move {
                        use warp::http::{Response, StatusCode};
                        if let Some(k) = q.get("key").and_then(|s| s.parse::<u64>().ok()) {
                            if log.lookup_key(k).await.is_some() {
                                let resp = Response::builder()
                                    .status(StatusCode::NO_CONTENT)
                                    .body("")
                                    .map_err(|_| warp::reject::reject())?;
                                return Ok::<_, warp::Rejection>(resp);
                            }
                        }
                        let resp = Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body("")
                            .map_err(|_| warp::reject::reject())?;
                        Ok::<_, warp::Rejection>(resp)
                    }
                });

            let fast_log = log.clone();
            let lookup_fast = v1
                .and(warp::path!("lookup_fast" / u64))
                .and(warp::get())
                .and_then(move |k: u64| {
                    let log = fast_log.clone();
                    async move {
                        use warp::http::{Response, StatusCode};
                        if let Some(off) = log.lookup_key(k).await {
                            let body = off.to_le_bytes().to_vec();
                            let resp = Response::builder()
                                .status(StatusCode::OK)
                                .header("Content-Type", "application/octet-stream")
                                .body(body)
                                .map_err(|_| warp::reject::reject())?;
                            return Ok::<_, warp::Rejection>(resp);
                        }
                        let resp = Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Vec::new())
                            .map_err(|_| warp::reject::reject())?;
                        Ok::<_, warp::Rejection>(resp)
                    }
                });

            let fast_val_log = log.clone();
            let get_fast = v1
                .and(warp::path!("get_fast" / u64))
                .and(warp::get())
                .and_then(move |k: u64| {
                    let log = fast_val_log.clone();
                    async move {
                        use warp::http::{Response, StatusCode};
                        if let Some(off) = log.lookup_key(k).await {
                            if let Some(bytes) = log.get(off).await {
                                if let Ok(rec) =
                                    bincode::deserialize::<kyrodb_engine::Record>(&bytes)
                                {
                                    let resp = Response::builder()
                                        .status(StatusCode::OK)
                                        .header("Content-Type", "application/octet-stream")
                                        .body(rec.value)
                                        .map_err(|_| warp::reject::reject())?;
                                    return Ok::<_, warp::Rejection>(resp);
                                }
                            }
                        }
                        let resp = Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Vec::new())
                            .map_err(|_| warp::reject::reject())?;
                        Ok::<_, warp::Rejection>(resp)
                    }
                });

            let append_log = log.clone();
            let append_route = v1
                .and(warp::path("append"))
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = append_log.clone();
                    async move {
                        use warp::http::StatusCode;
                        let payload = body["payload"].as_str().unwrap_or("").as_bytes().to_vec();
                        match log.append(Uuid::new_v4(), payload).await {
                            Ok(offset) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "offset": offset })),
                                StatusCode::OK,
                            )),
                            Err(e) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "error": e.to_string() })),
                                StatusCode::INTERNAL_SERVER_ERROR,
                            )),
                        }
                    }
                });

            // --- NEW: Health check endpoint ----------------------------------------------------
            let health_route = warp::path("health")
                .and(warp::get())
                .map(|| warp::reply::json(&serde_json::json!({ "status": "ok" })));

            // --- NEW: Snapshot trigger endpoint -----------------------------------------------
            let snapshot_log = log.clone();
            let snapshot_route =
                v1.and(warp::path("snapshot"))
                    .and(warp::post())
                    .and_then(move || {
                        let log = snapshot_log.clone();
                        async move {
                            if let Err(e) = log.snapshot().await {
                                eprintln!("❌ Snapshot failed: {}", e);
                                return Ok::<_, warp::Rejection>(warp::reply::with_status(
                                    warp::reply::json(
                                        &serde_json::json!({ "error": e.to_string() }),
                                    ),
                                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                                ));
                            }
                            Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "snapshot": "ok" })),
                                warp::http::StatusCode::OK,
                            ))
                        }
                    });

            // --- NEW: Compaction endpoint -----------------------------------------------------
            let compact_log = log.clone();
            let compact_route =
                v1.and(warp::path("compact"))
                    .and(warp::post())
                    .and_then(move || {
                        let log = compact_log.clone();
                        async move {
                            match log.compact_keep_latest_and_snapshot_stats().await {
                                Ok(stats) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                                    warp::reply::json(
                                        &serde_json::json!({ "compact": "ok", "stats": stats }),
                                    ),
                                    warp::http::StatusCode::OK,
                                )),
                                Err(e) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                                    warp::reply::json(
                                        &serde_json::json!({ "error": e.to_string() }),
                                    ),
                                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                                )),
                            }
                        }
                    });

            // --- NEW: Offset endpoint ---------------------------------------------------------
            let offset_log = log.clone();
            let offset_route = v1
                .and(warp::path("offset"))
                .and(warp::get())
                .and_then(move || {
                    let log = offset_log.clone();
                    async move {
                        let off = log.get_offset().await;
                        Ok::<_, warp::Rejection>(warp::reply::json(
                            &serde_json::json!({ "offset": off }),
                        ))
                    }
                });

            let replay_log = log.clone();
            let replay_route = v1
                .and(warp::path("replay"))
                .and(warp::get())
                .and(warp::query::<std::collections::HashMap<String, String>>())
                .and_then(move |q: std::collections::HashMap<String, String>| {
                    let log = replay_log.clone();
                    async move {
                        let start = q.get("start").and_then(|s| s.parse().ok()).unwrap_or(0);
                        let end = q.get("end").and_then(|s| s.parse().ok());
                        let evs = log.replay(start, end).await;
                        let resp: Vec<_> = evs
                            .into_iter()
                            .map(|e| {
                                serde_json::json!({
                                    "offset": e.offset,
                                    "payload": String::from_utf8_lossy(&e.payload)
                                })
                            })
                            .collect();
                        Ok::<_, warp::Rejection>(warp::reply::json(&resp))
                    }
                });

            // KV PUT: POST /v1/put { key: u64, value: string }
            let put_log = log.clone();
            let put_route = v1
                .and(warp::path("put"))
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = put_log.clone();
                    async move {
                        use warp::http::StatusCode;
                        let key = body["key"].as_u64().unwrap_or(0);
                        let value = body["value"].as_str().unwrap_or("").as_bytes().to_vec();
                        match log.append_kv(Uuid::new_v4(), key, value).await {
                            Ok(off) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "offset": off })),
                                StatusCode::OK,
                            )),
                            Err(e) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "error": e.to_string() })),
                                StatusCode::INTERNAL_SERVER_ERROR,
                            )),
                        }
                    }
                });

            // Simple lookup by key: GET /v1/lookup?key=123
            let lookup_log = log.clone();
            let lookup_route = v1
                .and(warp::path("lookup"))
                .and(warp::get())
                .and(warp::query::<std::collections::HashMap<String, String>>())
                .and_then(move |q: std::collections::HashMap<String, String>| {
                    let log = lookup_log.clone();
                    async move {
                        if let Some(k) = q.get("key").and_then(|s| s.parse::<u64>().ok()) {
                            if let Some(offset) = log.lookup_key(k).await {
                                // Fetch payload via mmap-backed snapshot (with WAL/memory fallback)
                                if let Some(bytes) = log.get(offset).await {
                                    if let Ok(rec) =
                                        bincode::deserialize::<kyrodb_engine::Record>(&bytes)
                                    {
                                        return Ok::<_, warp::Rejection>(warp::reply::json(
                                            &serde_json::json!({
                                                "key": rec.key,
                                                "value": String::from_utf8_lossy(&rec.value)
                                            }),
                                        ));
                                    }
                                }
                            } else if let Some((_, rec)) = log.find_key_scan(k).await {
                                return Ok::<_, warp::Rejection>(warp::reply::json(
                                    &serde_json::json!({
                                        "key": rec.key,
                                        "value": String::from_utf8_lossy(&rec.value)
                                    }),
                                ));
                            }
                        }
                        let not_found = serde_json::json!({"error":"not found"});
                        Ok::<_, warp::Rejection>(warp::reply::json(&not_found))
                    }
                });

            // POST /v1/sql  { sql: "INSERT ..." | "SELECT ..." }
            let sql_log = log.clone();
            let sql_route = v1
                .and(warp::path("sql"))
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = sql_log.clone();
                    async move {
                        let stmt = body["sql"].as_str().unwrap_or("");
                        match sql::execute_sql(&log, stmt).await {
                            Ok(sql::SqlResponse::Ack { offset }) => Ok::<_, warp::Rejection>(
                                warp::reply::json(&serde_json::json!({"ack": {"offset": offset}})),
                            ),
                            Ok(sql::SqlResponse::Rows(rows)) => {
                                let resp: Vec<_> = rows
                                    .into_iter()
                                    .map(|(k, v)| {
                                        serde_json::json!({
                                            "key": k,
                                            "value": String::from_utf8_lossy(&v)
                                        })
                                    })
                                    .collect();
                                Ok::<_, warp::Rejection>(warp::reply::json(&resp))
                            }
                            Ok(sql::SqlResponse::VecRows(rows)) => {
                                let resp: Vec<_> = rows
                                    .into_iter()
                                    .map(|(k, d)| {
                                        serde_json::json!({
                                            "key": k,
                                            "dist": d
                                        })
                                    })
                                    .collect();
                                Ok::<_, warp::Rejection>(warp::reply::json(&resp))
                            }
                            Err(e) => {
                                let err = serde_json::json!({"error": e.to_string()});
                                Ok::<_, warp::Rejection>(warp::reply::json(&err))
                            }
                        }
                    }
                });

            // Metrics endpoint stays unversioned for Prometheus convention
            let metrics_route = warp::path("metrics").and(warp::get()).map(|| {
                let text = engine_crate::metrics::render();
                warp::reply::with_header(text, "Content-Type", "text/plain; version=0.0.4")
            });

            // Authorization filter (optional)
            #[derive(Debug)]
            struct Unauthorized;
            impl warp::reject::Reject for Unauthorized {}

            // Role-based access control
            #[derive(Debug, Clone, PartialEq)]
            enum UserRole {
                Admin,
                ReadWrite,
                ReadOnly,
            }

            #[derive(Debug)]
            struct InsufficientPermissions;
            impl warp::reject::Reject for InsufficientPermissions {}

            let _auth_with_role = {
                let token_opt = auth_token.clone();
                let admin_token_opt = admin_token.clone();
                warp::any()
                    .and(warp::header::optional::<String>("authorization"))
                    .and_then(move |hdr: Option<String>| {
                        let token_opt = token_opt.clone();
                        let admin_token_opt = admin_token_opt.clone();
                        async move {
                            let role = if let Some(expected) = &admin_token_opt {
                                if hdr.as_deref() == Some(&format!("Bearer {}", expected)) {
                                    UserRole::Admin
                                } else if let Some(expected_rw) = &token_opt {
                                    if hdr.as_deref() == Some(&format!("Bearer {}", expected_rw)) {
                                        UserRole::ReadWrite
                                    } else {
                                        return Err(warp::reject::custom(Unauthorized));
                                    }
                                } else {
                                    UserRole::ReadOnly
                                }
                            } else if let Some(expected) = &token_opt {
                                if hdr.as_deref() == Some(&format!("Bearer {}", expected)) {
                                    UserRole::ReadWrite
                                } else {
                                    return Err(warp::reject::custom(Unauthorized));
                                }
                            } else {
                                UserRole::ReadOnly
                            };
                            Ok::<UserRole, warp::Rejection>(role)
                        }
                    })
            };

            let _auth = {
                let token_opt = auth_token.clone();
                warp::any()
                    .and(warp::header::optional::<String>("authorization"))
                    .and_then(move |hdr: Option<String>| {
                        let token_opt = token_opt.clone();
                        async move {
                            if let Some(expected) = &token_opt {
                                let ok = hdr.as_deref() == Some(&format!("Bearer {}", expected));
                                if !ok {
                                    return Err(warp::reject::custom(Unauthorized));
                                }
                            }
                            Ok::<(), warp::Rejection>(())
                        }
                    })
            };

            // RMI build: POST /v1/rmi/build  (feature-gated)
            #[cfg(feature = "learned-index")]
            let rmi_build = {
                let data_dir = cli.data_dir.clone();
                let build_log = log.clone();
                v1.and(warp::path!("rmi" / "build"))
                    .and(warp::post())
                    .and_then(move || {
                        let log = build_log.clone();
                        let data_dir = data_dir.clone();
                        async move {
                            let pairs = log.collect_key_offset_pairs().await;
                            let tmp = std::path::Path::new(&data_dir).join("index-rmi.tmp");
                            let dst = std::path::Path::new(&data_dir).join("index-rmi.bin");
                            let timer =
                                engine_crate::metrics::RMI_REBUILD_DURATION_SECONDS.start_timer();
                            engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(1.0);
                            // Write index on a blocking thread to avoid starving the reactor
                            let pairs_clone = pairs.clone();
                            let tmp_clone = tmp.clone();
                            let write_res = tokio::task::spawn_blocking(move || {
                                engine_crate::index::RmiIndex::write_from_pairs_auto(
                                    &tmp_clone,
                                    &pairs_clone,
                                )
                            })
                            .await;
                            let mut ok = matches!(write_res, Ok(Ok(())));
                            if ok {
                                if let Ok(f) = std::fs::OpenOptions::new().read(true).open(&tmp) {
                                    let _ = f.sync_all();
                                }
                                if let Err(e) = std::fs::rename(&tmp, &dst) {
                                    eprintln!("❌ RMI rename failed: {}", e);
                                    ok = false;
                                }
                                // Ensure directory metadata is durable after rename
                                if let Err(e) =
                                    engine_crate::fsync_dir(std::path::Path::new(&data_dir))
                                {
                                    eprintln!(
                                        "⚠️ fsync data dir after RMI build rename failed: {}",
                                        e
                                    );
                                }
                            }
                            timer.observe_duration();
                            engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(0.0);
                            Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
                                "ok": ok,
                                "count": pairs.len()
                            })))
                        }
                    })
            };

            #[cfg(not(feature = "learned-index"))]
            let rmi_build = {
                use warp::http::StatusCode;
                v1.and(warp::path!("rmi" / "build"))
                    .and(warp::post())
                    .map(|| {
                        warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({
                                "error": "learned-index feature not enabled"
                            })),
                            StatusCode::NOT_IMPLEMENTED,
                        )
                    })
            };

            let subscribe_log = log.clone();
            let subscribe_route = v1
                .and(warp::path("subscribe"))
                .and(warp::get())
                .and(warp::query::<std::collections::HashMap<String, String>>())
                .and_then(move |q: std::collections::HashMap<String, String>| {
                    let log = subscribe_log.clone();
                    async move {
                        let from = q.get("from").and_then(|s| s.parse().ok()).unwrap_or(0);
                        let (past, rx) = log.subscribe(from).await;

                        // Create an SSE stream of past + live events
                        let past_stream = futures::stream::iter(
                            past.into_iter().map(Ok::<_, std::convert::Infallible>),
                        );
                        let live_stream = tokio_stream::wrappers::BroadcastStream::new(rx)
                            .filter_map(|res| async move {
                                match res {
                                    Ok(v) => Some(v),
                                    Err(_) => {
                                        kyrodb_engine::metrics::inc_sse_lagged();
                                        None
                                    }
                                }
                            })
                            .map(Ok);
                        let combined = past_stream.chain(live_stream).map(|e| {
                            let event = e.unwrap(); // Should be infallible
                            Ok::<_, warp::Error>(warp::sse::Event::default()
                                .data(
                                    serde_json::json!({"offset": event.offset, "payload": String::from_utf8_lossy(&event.payload)})
                                        .to_string(),
                                ))
                        });
                        Ok::<_, warp::Rejection>(warp::sse::reply(
                            warp::sse::keep_alive().stream(combined),
                        ))
                    }
                });

            // Vector insert: POST /v1/vector/insert { key: u64, vector: [f32,...] }
            let vec_ins_log = log.clone();
            let vector_insert = v1
                .and(warp::path!("vector" / "insert"))
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = vec_ins_log.clone();
                    async move {
                        use warp::http::StatusCode;
                        let key = body["key"].as_u64().unwrap_or(0);
                        let vec: Vec<f32> = body["vector"]
                            .as_array()
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_f64().map(|x| x as f32))
                                    .collect()
                            })
                            .unwrap_or_default();
                        match log.append_vector(Uuid::new_v4(), key, vec).await {
                            Ok(off) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({"offset": off})),
                                StatusCode::OK,
                            )),
                            Err(e) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "error": e.to_string() })),
                                StatusCode::INTERNAL_SERVER_ERROR,
                            )),
                        }
                    }
                });

            // POST /v1/warmup: fault-in index and snapshot pages, best-effort
            let warmup = {
                let log2 = log.clone();
                v1.and(warp::path("warmup"))
                    .and(warp::post())
                    .and_then(move || {
                        let log = log2.clone();
                        async move {
                            log.warmup().await;
                            Ok::<_, warp::Rejection>(warp::reply::json(
                                &serde_json::json!({"status":"ok"}),
                            ))
                        }
                    })
            };

            // --- Attach rate limits ------------------------------------------------------
            // Admin RL for sensitive operations
            let snapshot_route = admin_rl.clone().and(snapshot_route).map(|(), r| r);
            let compact_route = admin_rl.clone().and(compact_route).map(|(), r| r);
            let rmi_build = admin_rl.clone().and(rmi_build).map(|(), r| r);
            let warmup = admin_rl.clone().and(warmup).map(|(), r| r);
            let replay_route = admin_rl.clone().and(replay_route).map(|(), r| r);
            // Data RL for common read/write paths
            let put_route = data_rl.clone().and(put_route).map(|(), r| r);
            let lookup_route = data_rl.clone().and(lookup_route).map(|(), r| r);
            let lookup_raw = data_rl.clone().and(lookup_raw).map(|(), r| r);
            let lookup_fast = data_rl.clone().and(lookup_fast).map(|(), r| r);
            let get_fast = data_rl.clone().and(get_fast).map(|(), r| r);
            let sql_route = data_rl.clone().and(sql_route).map(|(), r| r);
            let vector_insert = data_rl.clone().and(vector_insert).map(|(), r| r);
            // ---------------------------------------------------------------------------

            // Combine routes
            let routes = health_route
                .or(metrics_route)
                .or(append_route)
                .or(replay_route)
                .or(subscribe_route)
                .or(snapshot_route)
                .or(offset_route)
                .or(put_route)
                .or(lookup_route)
                .or(lookup_raw)
                .or(lookup_fast)
                .or(get_fast)
                .or(sql_route)
                .or(vector_insert)
                .or(rmi_build)
                .or(compact_route)
                .or(warmup)
                .or(warp::path("build_info").and(warp::get()).map({
                    let commit = build_commit;
                    let features = build_features;
                    let branch = option_env!("GIT_BRANCH").unwrap_or("unknown");
                    let build_time = option_env!("BUILD_TIME").unwrap_or("unknown");
                    let rust_version = option_env!("RUST_VERSION").unwrap_or("unknown");
                    let target_triple = option_env!("TARGET_TRIPLE").unwrap_or("unknown");
                    move || {
                        warp::reply::json(&serde_json::json!({
                            "commit": commit,
                            "branch": branch,
                            "build_time": build_time,
                            "rust_version": rust_version,
                            "target_triple": target_triple,
                            "features": features,
                            "version": env!("CARGO_PKG_VERSION"),
                            "name": env!("CARGO_PKG_NAME"),
                        }))
                    }
                }))
                .recover(|rej: warp::Rejection| async move {
                    use warp::http::StatusCode;
                    if rej.find::<Unauthorized>().is_some() {
                        Ok::<_, std::convert::Infallible>(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({"error":"unauthorized"})),
                            StatusCode::UNAUTHORIZED,
                        ))
                    } else if rej.find::<InsufficientPermissions>().is_some() {
                        Ok::<_, std::convert::Infallible>(warp::reply::with_status(
                            warp::reply::json(
                                &serde_json::json!({"error":"insufficient_permissions"}),
                            ),
                            StatusCode::FORBIDDEN,
                        ))
                    } else if rej.find::<AdminRateLimited>().is_some()
                        || rej.find::<DataRateLimited>().is_some()
                    {
                        Ok::<_, std::convert::Infallible>(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({"error":"rate_limited"})),
                            StatusCode::TOO_MANY_REQUESTS,
                        ))
                    } else {
                        Ok::<_, std::convert::Infallible>(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({"error":"not found"})),
                            StatusCode::NOT_FOUND,
                        ))
                    }
                });

            // Per-request logging with runtime disable via KYRODB_DISABLE_HTTP_LOG=1
            let disable_http_log =
                std::env::var("KYRODB_DISABLE_HTTP_LOG").ok().as_deref() == Some("1");
            let routes = routes.with(warp::log::custom({
                move |info: warp::log::Info| {
                    if disable_http_log {
                        return;
                    }
                    tracing::info!(
                        target: "kyrodb",
                        method = %info.method(),
                        path = info.path(),
                        status = info.status().as_u16(),
                        elapsed_ms = info.elapsed().as_millis()
                    );
                }
            }));

            // start server
            let addr = (host.parse::<std::net::IpAddr>()?, port);
            tracing::info!(
                "Starting kyrodb-engine on {}:{} (commit={}, features={})",
                host,
                port,
                build_commit,
                build_features
            );

            println!(
                "🚀 Starting server at http://{}:{} (commit={}, features={})",
                host, port, build_commit, build_features
            );

            warp::serve(routes).run(addr).await;
        }
    }

    Ok(())
}
