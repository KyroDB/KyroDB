//! KyroDB Engine - High-performance CLI and HTTP server
//!
//! Raw performance mode: No authentication, no rate limiting.
//! Pure database engine focused on maximum throughput and minimal latency.

use anyhow::Result;
use clap::{Parser, Subcommand};
use kyrodb_engine as engine_crate;
use std::sync::Arc;
use uuid::Uuid;
use warp::http::StatusCode;
use warp::Filter;

/// Adaptive system load detection for background task throttling
fn detect_system_load_multiplier() -> u64 {
    let mut load_signals = 0;

    // Signal 1: Check container CPU throttling (if available)
    if let Ok(cpu_stat) = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.stat") {
        for line in cpu_stat.lines() {
            if line.starts_with("nr_throttled") {
                if let Some(value_str) = line.split_whitespace().nth(1) {
                    if let Ok(throttled) = value_str.parse::<u64>() {
                        if throttled > 0 {
                            load_signals += 1; // Container CPU throttling detected
                        }
                    }
                }
            }
        }
    }

    // Signal 2: Check system load average
    if let Ok(loadavg) = std::fs::read_to_string("/proc/loadavg") {
        if let Some(load_str) = loadavg.split_whitespace().next() {
            if let Ok(load) = load_str.parse::<f64>() {
                let cpu_count = std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or(1) as f64;
                let load_ratio = load / cpu_count;

                if load_ratio > 2.0 {
                    load_signals += 2; // Very high load
                } else if load_ratio > 1.0 {
                    load_signals += 1; // High load
                }
            }
        }
    }

    // Signal 3: Check memory pressure
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        let mut available_kb = 0u64;
        let mut total_kb = 0u64;

        for line in meminfo.lines() {
            if line.starts_with("MemAvailable:") {
                if let Some(value) = line.split_whitespace().nth(1) {
                    available_kb = value.parse().unwrap_or(0);
                }
            } else if line.starts_with("MemTotal:") {
                if let Some(value) = line.split_whitespace().nth(1) {
                    total_kb = value.parse().unwrap_or(0);
                }
            }
        }

        if total_kb > 0 {
            let available_ratio = available_kb as f64 / total_kb as f64;
            if available_ratio < 0.1 {
                load_signals += 2; // Very low memory
            } else if available_ratio < 0.25 {
                load_signals += 1; // Low memory
            }
        }
    }

    // Calculate interval multiplier based on load signals
    match load_signals {
        0 => 1,     // No pressure - normal intervals
        1 => 2,     // Low pressure - 2x slower
        2..=3 => 4, // Medium pressure - 4x slower
        _ => 8,     // High pressure - 8x slower
    }
}

#[derive(Parser)]
#[command(
    name = "kyrodb-engine",
    about = "KyroDB Engine - High-performance KV Database"
)]
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

        /// Enable binary protocol server (default: enabled for maximum performance)
        #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
        enable_binary: bool,

        /// Binary protocol port offset from HTTP port (default: +1)
        #[arg(long, default_value = "1")]
        binary_port_offset: u16,

        /// Automatically trigger snapshot every N seconds
        #[arg(long)]
        auto_snapshot_secs: Option<u64>,

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
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured JSON logs with env-based filter (RUST_LOG)
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

    match cli.cmd {
        Commands::Serve {
            host,
            port,
            enable_binary,
            binary_port_offset,
            auto_snapshot_secs,
            snapshot_every_n_appends,
            wal_max_bytes,
            rmi_rebuild_appends,
            rmi_rebuild_ratio,
            wal_segment_bytes,
            wal_max_segments,
            compact_interval_secs,
            compact_when_wal_bytes,
        } => {
            // Silence unused when learned-index feature is disabled
            #[cfg(not(feature = "learned-index"))]
            let _ = (rmi_rebuild_appends.as_ref(), rmi_rebuild_ratio.as_ref());

            // Optional warm-on-start: fault-in snapshot and RMI pages before serving
            if std::env::var("KYRODB_WARM_ON_START").ok().as_deref() == Some("1") {
                println!("Warm-on-start enabled; warming data and index pages...");
                if let Err(err) = log.warmup().await {
                    eprintln!("Warmup failed: {err:?}");
                }
                println!("Warm-on-start complete.");
            }

            // Configure WAL rotation if requested
            if wal_segment_bytes.is_some() || wal_max_segments > 0 {
                log.configure_wal_rotation(wal_segment_bytes, wal_max_segments)
                    .await;
            }

            // Background, size-based compaction trigger with adaptive monitoring
            if let Some(maxb) = wal_max_bytes {
                if maxb > 0 {
                    let log_for_size = log.clone();
                    tokio::spawn(async move {
                        let base_check_interval = 2u64;
                        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                            base_check_interval,
                        ));
                        let mut last_load_check = std::time::Instant::now();
                        let mut consecutive_failures = 0;

                        loop {
                            interval.tick().await;

                            // Adapt check frequency based on system load every 60 seconds
                            let now = std::time::Instant::now();
                            if now.duration_since(last_load_check).as_secs() >= 60 {
                                last_load_check = now;
                                let load_multiplier = detect_system_load_multiplier();
                                let new_interval = base_check_interval * load_multiplier;

                                if new_interval != interval.period().as_secs() {
                                    println!(
                                        "🔧 Adapting WAL size check interval: {}s -> {}s",
                                        interval.period().as_secs(),
                                        new_interval
                                    );
                                    interval = tokio::time::interval(
                                        tokio::time::Duration::from_secs(new_interval),
                                    );
                                }
                            }

                            let size = log_for_size.wal_size_bytes().await;
                            if size >= maxb {
                                println!(
                                    "📦 Size-based compaction: wal={} bytes >= {}",
                                    size, maxb
                                );

                                // Yield to foreground operations before heavy compaction
                                tokio::task::yield_now().await;

                                match log_for_size.compact_keep_latest_and_snapshot().await {
                                    Ok(_) => {
                                        println!("Size-based compaction complete.");
                                        consecutive_failures = 0;
                                    }
                                    Err(e) => {
                                        consecutive_failures += 1;
                                        eprintln!(
                                            "Size-based compaction failed (attempt {}): {}",
                                            consecutive_failures, e
                                        );

                                        // Back off after repeated failures to prevent resource exhaustion
                                        if consecutive_failures > 2 {
                                            let backoff_duration =
                                                std::cmp::min(consecutive_failures * 10, 120); // Max 2 minutes
                                            println!(
                                                "Backing off size-based compaction for {} seconds",
                                                backoff_duration
                                            );
                                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                                backoff_duration,
                                            ))
                                            .await;
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            }

            // Background interval compaction with adaptive resource management
            if compact_interval_secs > 0 {
                let log_for_compact = log.clone();
                tokio::spawn(async move {
                    let base_interval = compact_interval_secs;
                    let mut interval =
                        tokio::time::interval(tokio::time::Duration::from_secs(base_interval));
                    let mut last_cpu_check = std::time::Instant::now();
                    let mut error_count = 0;

                    loop {
                        interval.tick().await;

                        // Adaptive interval adjustment based on system load every 30 seconds
                        let now = std::time::Instant::now();
                        if now.duration_since(last_cpu_check).as_secs() >= 30 {
                            last_cpu_check = now;

                            // Check system load and adapt interval
                            let load_multiplier = detect_system_load_multiplier();
                            let new_interval = base_interval * load_multiplier;

                            if new_interval != interval.period().as_secs() {
                                println!("🔧 Adapting compaction interval: {}s -> {}s (load factor: {}x)", 
                                    interval.period().as_secs(), new_interval, load_multiplier);
                                interval = tokio::time::interval(tokio::time::Duration::from_secs(
                                    new_interval,
                                ));
                            }
                        }

                        // Check if compaction should run based on conditions
                        let should_compact = if compact_when_wal_bytes == 0 {
                            true
                        } else {
                            log_for_compact.wal_size_bytes().await >= compact_when_wal_bytes
                        };

                        if should_compact {
                            // Yield to other tasks before heavy operation
                            tokio::task::yield_now().await;

                            match log_for_compact.compact_keep_latest_and_snapshot().await {
                                Ok(_) => {
                                    println!("✅ Interval compaction complete.");
                                    error_count = 0; // Reset error count on success
                                }
                                Err(e) => {
                                    error_count += 1;
                                    eprintln!(
                                        "❌ Interval compaction failed (attempt {}): {}",
                                        error_count, e
                                    );

                                    // Implement exponential backoff for repeated failures
                                    if error_count > 3 {
                                        let backoff_secs = std::cmp::min(error_count * 30, 300); // Max 5 minutes
                                        println!("🔄 Backing off compaction for {} seconds due to repeated failures", backoff_secs);
                                        tokio::time::sleep(tokio::time::Duration::from_secs(
                                            backoff_secs,
                                        ))
                                        .await;
                                        error_count = 0; // Reset after backoff
                                    }
                                }
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
                let _data_dir = cli.data_dir.clone();
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
                            let rebuild_timer =
                                engine_crate::metrics::RMI_REBUILD_DURATION_SECONDS.start_timer();
                            engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(1.0);
                            engine_crate::metrics::RMI_REBUILDS_TOTAL.inc();

                            // Build new AdaptiveRMI from current data
                            let new_index =
                                engine_crate::index::PrimaryIndex::new_adaptive_rmi_from_pairs(
                                    &pairs,
                                );

                            // Swap to the new index
                            if let Err(err) = rebuild_log.swap_primary_index(new_index).await {
                                eprintln!("Failed to swap primary index: {err:?}");
                            }

                            last_built = cur;
                            rebuild_timer.observe_duration();
                            engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(0.0);
                            let _ = rebuild_log.write_manifest().await;
                            println!(
                                "✅ AdaptiveRMI rebuilt successfully (appended={}, ratio={:.3})",
                                appended, ratio
                            );
                        }
                    }
                });
            }

            println!("🚀 KyroDB Engine - Raw Performance Mode");
            println!("📍 Data directory: {}", cli.data_dir);
            println!("💾 Full throttle - no rate limiting, no authentication");

            // Parse address for unified server
            let addr = (host.parse::<std::net::IpAddr>()?, port);
            let binary_port = port + binary_port_offset; // Configurable port offset

            tracing::info!(
                "Starting kyrodb-engine DUAL-PROTOCOL SERVERS: HTTP on {}:{}, Binary TCP on {}:{} (commit={}, features={})",
                host,
                port,
                host,
                binary_port,
                build_commit,
                build_features
            );

            if enable_binary {
                println!(
                    "🚀 DUAL-PROTOCOL KYRODB SERVERS STARTING:\n   🌐 HTTP API (Ultra-Fast + Legacy) at http://{}:{}\n   ⚡ Binary TCP Protocol at tcp://{}:{}\n   💾 Full throttle - no rate limiting, no authentication\n   📝 Commit: {}, Features: {}",
                    host, port, host, binary_port, build_commit, build_features
                );
            } else {
                println!(
                    "🚀 HTTP-ONLY KYRODB SERVER STARTING:\n   🌐 HTTP API (Ultra-Fast + Legacy) at http://{}:{}\n   💾 Full throttle - no rate limiting, no authentication\n   📝 Commit: {}, Features: {}",
                    host, port, build_commit, build_features
                );
            }

            // 🚀 CREATE UNIFIED HTTP ROUTES: Ultra-fast + legacy endpoints
            let unified_routes = create_unified_routes(log.clone());

            // 📡 START BINARY TCP SERVER (concurrent with HTTP) - OPTIONAL
            if enable_binary {
                let binary_log = log.clone();
                let binary_addr = format!("{}:{}", host, binary_port);
                tokio::spawn(async move {
                    if let Err(e) = kyrodb_engine::binary_protocol::binary_protocol_server(
                        binary_log,
                        binary_addr,
                    )
                    .await
                    {
                        eprintln!("❌ Binary TCP server error: {}", e);
                    }
                });
            }

            // 📡 START HTTP SERVER (primary server)
            warp::serve(unified_routes).run(addr).await;
        }
    }

    Ok(())
}

/// 🚀 CREATE UNIFIED ROUTES: Single server with ultra-fast + legacy endpoints
fn create_unified_routes(
    log: Arc<engine_crate::PersistentEventLog>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = std::convert::Infallible> + Clone {
    // 🚀 ULTRA-FAST WRITE ENDPOINTS: High-performance write paths
    let ultra_fast_write_routes = create_ultra_fast_write_routes(log.clone());

    // 🚀 ULTRA-FAST LOOKUP ENDPOINTS: Zero-allocation paths for maximum performance
    let ultra_fast_lookup_routes = create_ultra_fast_lookup_routes(log.clone());

    //  ADMIN ENDPOINTS: Management and monitoring
    let admin_routes = create_admin_endpoints(log.clone());

    // 💊 HEALTH ENDPOINTS: Health checks and diagnostics
    let health_routes = create_health_endpoints();

    // 🔗 COMBINE ALL ROUTES: Ultra-fast takes precedence for overlapping paths
    let all_routes = ultra_fast_write_routes
        .or(ultra_fast_lookup_routes)
        .or(admin_routes)
        .or(health_routes);

    // Apply middleware
    let routes_with_compression = all_routes.with(warp::compression::gzip());

    // Per-request logging (runtime disable via KYRODB_DISABLE_HTTP_LOG=1)
    let disable_http_log = std::env::var("KYRODB_DISABLE_HTTP_LOG").ok().as_deref() == Some("1");
    let routes_with_logging = routes_with_compression.with(warp::log::custom({
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

    // Error recovery - return proper error type
    let routes_with_recovery = routes_with_logging.recover(|_rej: warp::Rejection| async move {
        Ok::<_, std::convert::Infallible>(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"error":"not found"})),
            warp::http::StatusCode::NOT_FOUND,
        ))
    });

    routes_with_recovery
}

/// 🚀 ULTRA-FAST WRITE ROUTES: High-performance write paths
fn create_ultra_fast_write_routes(
    log: Arc<engine_crate::PersistentEventLog>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let v1 = warp::path("v1");

    // Binary PUT: POST /v1/put_fast/{key} with raw bytes body - maximum performance
    let put_fast_route = v1
        .and(warp::path!("put_fast" / u64))
        .and(warp::post())
        .and(warp::body::bytes())
        .and_then(move |key: u64, body: bytes::Bytes| {
            let log = log.clone();
            async move {
                match log.append_kv(Uuid::new_v4(), key, body.to_vec()).await {
                    Ok(off) => {
                        let response = off.to_le_bytes().to_vec();
                        Ok::<_, warp::Rejection>(warp::reply::with_status(
                            warp::reply::with_header(
                                response,
                                "Content-Type",
                                "application/octet-stream",
                            ),
                            StatusCode::OK,
                        ))
                    }
                    Err(_) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                        warp::reply::with_header(
                            "error".as_bytes().to_vec(),
                            "Content-Type",
                            "application/octet-stream",
                        ),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )),
                }
            }
        });

    put_fast_route
}

/// 🚀 ULTRA-FAST LOOKUP ROUTES: Zero-allocation paths for maximum performance
fn create_ultra_fast_lookup_routes(
    log: Arc<engine_crate::PersistentEventLog>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let v1 = warp::path("v1");

    // 🚀 GET_FAST PATH: /v1/get_fast/{key} - Returns raw bytes for put_fast compatibility
    let get_fast = {
        let log = log.clone();
        v1.and(warp::path("get_fast"))
            .and(warp::path::param::<u64>())
            .and(warp::get())
            .and_then(move |key: u64| {
                let log = log.clone();
                async move {
                    // Use ultra-fast lookup to check hot buffer first
                    if let Some(offset) = log.lookup_key_ultra_fast(key) {
                        if let Some(bytes) = log.get(offset).await {
                            if let Ok(rec) = bincode::deserialize::<kyrodb_engine::Record>(&bytes) {
                                let resp = warp::http::Response::builder()
                                    .status(StatusCode::OK)
                                    .header("Content-Type", "application/octet-stream")
                                    .body(rec.value)
                                    .map_err(|_| warp::reject::reject())?;
                                return Ok::<_, warp::Rejection>(resp);
                            }
                        }
                    }
                    let resp = warp::http::Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Vec::new())
                        .map_err(|_| warp::reject::reject())?;
                    Ok::<_, warp::Rejection>(resp)
                }
            })
    };

    // 🚀 ULTRA-FAST PATH: /v1/lookup_ultra/{key}
    let lookup_ultra = {
        let log = log.clone();
        v1.and(warp::path("lookup_ultra"))
            .and(warp::path::param::<u64>())
            .and(warp::get())
            .map(move |key: u64| {
                let value = log.lookup_key_ultra_fast(key);

                // 🚀 ZERO-ALLOCATION RESPONSE
                let pool = engine_crate::get_ultra_fast_pool();
                let mut response = pool.get_json_buffer();

                match value {
                    Some(v) => {
                        use std::fmt::Write;
                        write!(&mut response, "{{\"key\":{},\"value\":{}}}", key, v).unwrap();
                    }
                    None => {
                        use std::fmt::Write;
                        write!(&mut response, "{{\"key\":{},\"value\":null}}", key).unwrap();
                    }
                }

                let reply_data = response.clone();
                pool.return_json_buffer(response);

                warp::reply::with_header(reply_data, "content-type", "application/json")
            })
    };

    // 🚀 BATCH LOOKUP: /v1/lookup_batch - Support both array and object formats
    let lookup_batch = {
        let log = log.clone();
        v1.and(warp::path("lookup_batch"))
            .and(warp::post())
            .and(warp::body::json())
            .map(move |body: serde_json::Value| {
                // Support both formats: [1,2,3] and {"keys": [1,2,3]}
                let keys: Vec<u64> = if body.is_array() {
                    // Direct array format: [1,2,3]
                    body.as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .filter_map(|v| v.as_u64())
                        .collect()
                } else if let Some(keys_array) = body.get("keys").and_then(|k| k.as_array()) {
                    // Object format: {"keys": [1,2,3]}
                    keys_array.iter().filter_map(|v| v.as_u64()).collect()
                } else {
                    vec![]
                };

                let raw_results = log.lookup_keys_simd_batch(&keys);
                // Convert to format expected by ultra-fast client: [{"value": "123"}, {"value": null}, ...]
                let json_results: Vec<serde_json::Value> = raw_results
                    .into_iter()
                    .map(|(key, maybe_value)| match maybe_value {
                        Some(value) => serde_json::json!({"key": key, "value": value.to_string()}),
                        None => serde_json::json!({"key": key, "value": null}),
                    })
                    .collect();
                warp::reply::json(&json_results)
            })
    };

    get_fast.or(lookup_ultra).or(lookup_batch)
}

/// 🔧 ADMIN ENDPOINTS: Management and monitoring
fn create_admin_endpoints(
    log: Arc<engine_crate::PersistentEventLog>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let v1 = warp::path("v1");

    // Snapshot endpoint
    let snapshot_route = {
        let log = log.clone();
        v1.and(warp::path("snapshot"))
            .and(warp::post())
            .and_then(move || {
                let log = log.clone();
                async move {
                    log.snapshot().await.map_err(|_| warp::reject())?;
                    Ok::<_, warp::Rejection>(warp::reply::json(
                        &serde_json::json!({ "snapshot": "ok" }),
                    ))
                }
            })
    };

    // RMI build endpoint
    let rmi_build = {
        let log = log.clone();
        v1.and(warp::path("rmi"))
            .and(warp::path("build"))
            .and(warp::post())
            .and_then(move || {
                let log = log.clone();
                async move {
                    #[cfg(feature = "learned-index")]
                    {
                        log.build_rmi().await.map_err(|_| warp::reject())?;
                        Ok::<_, warp::Rejection>(warp::reply::json(
                            &serde_json::json!({ "rmi_build": "completed" }),
                        ))
                    }
                    #[cfg(not(feature = "learned-index"))]
                    {
                        Ok::<_, warp::Rejection>(warp::reply::json(
                            &serde_json::json!({ "rmi_build": "disabled" }),
                        ))
                    }
                }
            })
    };

    let warmup_route = {
        let log = log.clone();
        v1.and(warp::path("warmup"))
            .and(warp::post())
            .and_then(move || {
                let log = log.clone();
                async move {
                    log.warmup().await.map_err(|_| warp::reject())?;
                    Ok::<_, warp::Rejection>(warp::reply::json(
                        &serde_json::json!({ "warmup": "initiated" }),
                    ))
                }
            })
    };

    // Compaction endpoint (admin operation)
    let compact_route = {
        let log = log.clone();
        v1.and(warp::path("compact"))
            .and(warp::post())
            .and_then(move || {
                let log = log.clone();
                async move {
                    match log.compact_keep_latest_and_snapshot_stats().await {
                        Ok(stats) => Ok::<_, warp::Rejection>(warp::reply::json(
                            &serde_json::json!({ "compact": "ok", "stats": stats }),
                        )),
                        Err(e) => Ok::<_, warp::Rejection>(warp::reply::json(
                            &serde_json::json!({ "error": e.to_string() }),
                        )),
                    }
                }
            })
    };

    snapshot_route
        .or(rmi_build)
        .or(warmup_route)
        .or(compact_route)
}

/// 💊 HEALTH ENDPOINTS: Health checks and diagnostics
fn create_health_endpoints(
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let v1 = warp::path("v1");

    // Health check endpoint: GET /v1/health
    let health_route = v1
        .and(warp::path("health"))
        .and(warp::get())
        .map(|| warp::reply::json(&serde_json::json!({ "status": "ok" })));

    // Metrics endpoint: GET /metrics (no v1 prefix for Prometheus compatibility)
    let metrics_route = warp::path("metrics").and(warp::get()).map(|| {
        #[cfg(not(feature = "bench-no-metrics"))]
        {
            let encoder = prometheus::TextEncoder::new();
            let metric_families = prometheus::gather();
            match encoder.encode_to_string(&metric_families) {
                Ok(output) => {
                    warp::reply::with_header(output, "Content-Type", "text/plain; version=0.0.4")
                }
                Err(_) => warp::reply::with_header(
                    "# Metrics encoding failed".to_string(),
                    "Content-Type",
                    "text/plain",
                ),
            }
        }
        #[cfg(feature = "bench-no-metrics")]
        {
            warp::reply::with_header(
                "# Metrics disabled for benchmarking".to_string(),
                "Content-Type",
                "text/plain",
            )
        }
    });

    health_route.or(metrics_route)
}
