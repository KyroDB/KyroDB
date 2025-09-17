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
        0 => 1,      // No pressure - normal intervals
        1 => 2,      // Low pressure - 2x slower
        2..=3 => 4,  // Medium pressure - 4x slower  
        _ => 8,      // High pressure - 8x slower
    }
}

#[derive(Parser)]
#[command(name = "kyrodb-engine", about = "KyroDB Engine - High-performance KV Database")]
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
                println!("🔥 Warm-on-start enabled; warming data and index pages...");
                log.warmup().await;
                println!("🔥 Warm-on-start complete.");
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
                        let mut base_check_interval = 2u64; 
                        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(base_check_interval));
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
                                    println!("🔧 Adapting WAL size check interval: {}s -> {}s", 
                                        interval.period().as_secs(), new_interval);
                                    interval = tokio::time::interval(tokio::time::Duration::from_secs(new_interval));
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
                                        println!("✅ Size-based compaction complete.");
                                        consecutive_failures = 0;
                                    }
                                    Err(e) => {
                                        consecutive_failures += 1;
                                        eprintln!("❌ Size-based compaction failed (attempt {}): {}", consecutive_failures, e);
                                        
                                        // Back off after repeated failures to prevent resource exhaustion
                                        if consecutive_failures > 2 {
                                            let backoff_duration = std::cmp::min(consecutive_failures * 10, 120); // Max 2 minutes
                                            println!("🔄 Backing off size-based compaction for {} seconds", backoff_duration);
                                            tokio::time::sleep(tokio::time::Duration::from_secs(backoff_duration)).await;
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
                    let mut base_interval = compact_interval_secs;
                    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(base_interval));
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
                                interval = tokio::time::interval(tokio::time::Duration::from_secs(new_interval));
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
                                    eprintln!("❌ Interval compaction failed (attempt {}): {}", error_count, e);
                                    
                                    // Implement exponential backoff for repeated failures
                                    if error_count > 3 {
                                        let backoff_secs = std::cmp::min(error_count * 30, 300); // Max 5 minutes
                                        println!("🔄 Backing off compaction for {} seconds due to repeated failures", backoff_secs);
                                        tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
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
                            let new_index = engine_crate::index::PrimaryIndex::new_adaptive_rmi_from_pairs(&pairs);
                            
                            // Swap to the new index
                            rebuild_log.swap_primary_index(new_index).await;
                            
                            last_built = cur;
                            rebuild_timer.observe_duration();
                            engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(0.0);
                            let _ = rebuild_log.write_manifest().await;
                            println!("✅ AdaptiveRMI rebuilt successfully (appended={}, ratio={:.3})", appended, ratio);
                        }
                    }
                });
            }

            println!("🚀 KyroDB Engine - Raw Performance Mode");
            println!("📍 Data directory: {}", cli.data_dir);
            println!("💾 Full throttle - no rate limiting, no authentication");

            // Core HTTP API endpoints
            let v1 = warp::path("v1");

            // Health check endpoint
            let health_route = warp::path("health")
                .and(warp::get())
                .map(|| warp::reply::json(&serde_json::json!({ "status": "ok" })));

            // Build info endpoint
            let build_info_route = warp::path("build_info").and(warp::get()).map({
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
            });

            // Offset endpoint
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

            // KV PUT: POST /v1/put { key: u64, value: string }
            let put_log = log.clone();
            let put_route = v1
                .and(warp::path("put"))
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = put_log.clone();
                    async move {
                        let key = body["key"].as_u64().unwrap_or(0);
                        let value = body["value"].as_str().unwrap_or("").as_bytes().to_vec();
                        match log.append_kv(Uuid::new_v4(), key, value).await {
                            Ok(off) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "offset": off })),
                                StatusCode::OK,
                            )),
                            Err(e) => Ok::<_ ,warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "error": e.to_string() })),
                                StatusCode::INTERNAL_SERVER_ERROR,
                            )),
                        }
                    }
                });

            // Binary PUT: POST /v1/put_fast/{key} with raw bytes body - maximum performance
            let put_fast_log = log.clone();
            let put_fast_route = v1
                .and(warp::path!("put_fast" / u64))
                .and(warp::post())
                .and(warp::body::bytes())
                .and_then(move |key: u64, body: bytes::Bytes| {
                    let log = put_fast_log.clone();
                    async move {
                        match log.append_kv(Uuid::new_v4(), key, body.to_vec()).await {
                            Ok(off) => {
                                let response = off.to_le_bytes().to_vec();
                                Ok::<_, warp::Rejection>(warp::reply::with_status(
                                    warp::reply::with_header(
                                        response,
                                        "Content-Type", "application/octet-stream"
                                    ),
                                    StatusCode::OK,
                                ))
                            },
                            Err(_) => Ok::<_ ,warp::Rejection>(warp::reply::with_status(
                                warp::reply::with_header(
                                    "error".as_bytes().to_vec(),
                                    "Content-Type", "application/octet-stream"
                                ),
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

            // Fast lookup endpoints for maximum performance
            let lookup_fast_log = log.clone();
            let lookup_fast = v1
                .and(warp::path!("lookup_fast" / u64))
                .and(warp::get())
                .and_then(move |k: u64| {
                    let log = lookup_fast_log.clone();
                    async move {
                        if let Some(off) = log.lookup_key(k).await {
                            let body = off.to_le_bytes().to_vec();
                            let resp = warp::http::Response::builder()
                                .status(StatusCode::OK)
                                .header("Content-Type", "application/octet-stream")
                                .body(body)
                                .map_err(|_| warp::reject::reject())?;
                            return Ok::<_, warp::Rejection>(resp);
                        }
                        let resp = warp::http::Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Vec::new())
                            .map_err(|_| warp::reject::reject())?;
                        Ok::<_, warp::Rejection>(resp)
                    }
                });

            let get_fast_log = log.clone();
            let get_fast = v1
                .and(warp::path!("get_fast" / u64))
                .and(warp::get())
                .and_then(move |k: u64| {
                    let log = get_fast_log.clone();
                    async move {
                        if let Some(off) = log.lookup_key(k).await {
                            if let Some(bytes) = log.get(off).await {
                                if let Ok(rec) =
                                    bincode::deserialize::<kyrodb_engine::Record>(&bytes)
                                {
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
                        Ok::<_ ,warp::Rejection>(resp)
                    }
                });

            // Snapshot endpoint
            let snapshot_log = log.clone();
            let snapshot_route =
                v1.and(warp::path("snapshot"))
                    .and(warp::post())
                    .and_then(move || {
                        let log = snapshot_log.clone();
                        async move {
                            if let Err(e) = log.snapshot().await {
                                eprintln!("❌ Snapshot failed: {}", e);
                                return Ok::<_ ,warp::Rejection>(warp::reply::with_status(
                                    warp::reply::json(
                                        &serde_json::json!({ "error": e.to_string() }),
                                    ),
                                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                                ));
                            }
                            Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "snapshot": "ok" })),
                                StatusCode::OK,
                            ))
                        }
                    });

            // Compaction endpoint
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
                                    StatusCode::OK,
                                )),
                                Err(e) => Ok::<_ ,warp::Rejection>(warp::reply::with_status(
                                    warp::reply::json(
                                        &serde_json::json!({ "error": e.to_string() }),
                                    ),
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                )),
                            }
                        }
                    });

            // RMI build endpoint (feature-gated)
            #[cfg(feature = "learned-index")]
            let rmi_build = {
                let data_dir = cli.data_dir.clone();
                let build_log = log.clone();
                v1.and(warp::path!("rmi" / "build"))
                    .and(warp::post())
                    .and_then(move || {
                        let log = build_log.clone();
                        let _data_dir = data_dir.clone();
                        async move {
                            let pairs = log.collect_key_offset_pairs().await;
                            let timer =
                                engine_crate::metrics::RMI_REBUILD_DURATION_SECONDS.start_timer();
                            engine_crate::metrics::RMI_REBUILD_IN_PROGRESS.set(1.0);
                            engine_crate::metrics::RMI_REBUILDS_TOTAL.inc();
                            
                            // Build new AdaptiveRMI from current data
                            let new_index = engine_crate::index::PrimaryIndex::new_adaptive_rmi_from_pairs(&pairs);
                            
                            // Swap to the new index
                            log.swap_primary_index(new_index).await;
                            
                            let ok = true; // AdaptiveRMI build always succeeds
                            
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

            // Cache warmup endpoint
            let warmup_log = log.clone();
            let warmup = v1
                .and(warp::path("warmup"))
                .and(warp::post())
                .and_then(move || {
                    let log = warmup_log.clone();
                    async move {
                        log.warmup().await;
                        Ok::<_, warp::Rejection>(warp::reply::json(
                            &serde_json::json!({ "warmup": "ok" }),
                        ))
                    }
                });

            // Metrics endpoint
            let metrics_route = warp::path("metrics")
                .and(warp::get())
                .map(|| {
                    #[cfg(not(feature = "bench-no-metrics"))]
                    {
                        let encoder = prometheus::TextEncoder::new();
                        let metric_families = prometheus::gather();
                        match encoder.encode_to_string(&metric_families) {
                            Ok(output) => warp::reply::with_header(
                                output,
                                "Content-Type",
                                "text/plain; version=0.0.4",
                            ),
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

            // Clean routes without rate limiting
            let routes = put_route
                .or(put_fast_route)
                .or(lookup_route)
                .or(lookup_fast)
                .or(get_fast)
                .or(snapshot_route)
                .or(compact_route)
                .or(rmi_build)
                .or(warmup)
                .or(health_route)
                .or(build_info_route)
                .or(metrics_route)
                .or(offset_route);

            // Simple error recovery
            let routes = routes.recover(|_rej: warp::Rejection| async move {
                Ok::<_, std::convert::Infallible>(warp::reply::with_status(
                    warp::reply::json(&serde_json::json!({"error":"not found"})),
                    warp::http::StatusCode::NOT_FOUND,
                ))
            });

            // Apply compression middleware
            let routes = routes.with(warp::compression::gzip());

            // Per-request logging (runtime disable via KYRODB_DISABLE_HTTP_LOG=1)
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

            // Parse address for unified server
            let addr = (host.parse::<std::net::IpAddr>()?, port);
            
            tracing::info!(
                "Starting kyrodb-engine UNIFIED SERVER on {}:{} (commit={}, features={})",
                host,
                port,
                build_commit,
                build_features
            );

            println!(
                "🚀 UNIFIED KYRODB SERVER STARTING:\n   ⚡ Ultra-Fast Endpoints + Legacy Compatibility at http://{}:{}\n   💾 Full throttle - no rate limiting, no authentication\n   📝 Commit: {}, Features: {}",
                host, port, build_commit, build_features
            );

            // 🚀 CREATE UNIFIED ROUTES: Ultra-fast + legacy endpoints in single server
            let unified_routes = create_unified_routes(log.clone());

            // � START UNIFIED SERVER
            warp::serve(unified_routes).run(addr).await;
        }
    }

    Ok(())
}

/// 🚀 CREATE UNIFIED ROUTES: Single server with ultra-fast + legacy endpoints
fn create_unified_routes(
    log: Arc<engine_crate::PersistentEventLog>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = std::convert::Infallible> + Clone {
    let v1 = warp::path("v1");

    // 🚀 ULTRA-FAST LOOKUP ENDPOINTS: Zero-allocation paths for maximum performance
    let ultra_fast_routes = create_ultra_fast_lookup_routes(log.clone());
    
    // 📡 LEGACY ENDPOINTS: Existing functionality for compatibility
    let legacy_routes = create_legacy_endpoints(log.clone());
    
    // 🔧 ADMIN ENDPOINTS: Management and monitoring
    let admin_routes = create_admin_endpoints(log.clone());
    
    // 💊 HEALTH ENDPOINTS: Health checks and diagnostics
    let health_routes = create_health_endpoints();

    // 🔗 COMBINE ALL ROUTES: Ultra-fast takes precedence for overlapping paths
    let all_routes = ultra_fast_routes
        .or(legacy_routes)
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

/// 🚀 ULTRA-FAST LOOKUP ROUTES: Zero-allocation paths for maximum performance
fn create_ultra_fast_lookup_routes(
    log: Arc<engine_crate::PersistentEventLog>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let v1 = warp::path("v1");

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
                
                warp::reply::with_header(
                    reply_data,
                    "content-type", 
                    "application/json"
                )
            })
    };

    // 🚀 BATCH LOOKUP: /v1/lookup_batch
    let lookup_batch = {
        let log = log.clone();
        v1.and(warp::path("lookup_batch"))
            .and(warp::post())
            .and(warp::body::json())
            .map(move |keys: Vec<u64>| {
                let raw_results = log.lookup_keys_ultra_batch(&keys);
                // Convert to format expected by ultra-fast client: [{"value": "123"}, {"value": null}, ...]
                let json_results: Vec<serde_json::Value> = raw_results.into_iter().map(|(key, maybe_value)| {
                    match maybe_value {
                        Some(value) => serde_json::json!({"key": key, "value": value.to_string()}),
                        None => serde_json::json!({"key": key, "value": null}),
                    }
                }).collect();
                warp::reply::json(&json_results)
            })
    };

    lookup_ultra.or(lookup_batch)
}

///  LEGACY ENDPOINTS: Existing functionality for compatibility
fn create_legacy_endpoints(
    log: Arc<engine_crate::PersistentEventLog>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let v1 = warp::path("v1");

    // PUT endpoint for key-value pairs
    let put_route = {
        let log = log.clone();
        v1.and(warp::path("put"))
            .and(warp::path::param::<u64>())
            .and(warp::path::param::<u64>())
            .and(warp::put())
            .and_then(move |key: u64, value: u64| {
                let log = log.clone();
                async move {
                    let request_id = uuid::Uuid::new_v4();
                    let record = kyrodb_engine::Record { key, value: value.to_le_bytes().to_vec() };
                    let payload = bincode::serialize(&record)
                        .map_err(|_| warp::reject())?;
                    
                    let offset = log.append(request_id, payload).await
                        .map_err(|_| warp::reject())?;
                    
                    Ok::<_, warp::Rejection>(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({"offset": offset})),
                        warp::http::StatusCode::CREATED
                    ))
                }
            })
    };

    // Legacy lookup endpoint with async path  
    let lookup_route = {
        let log = log.clone();
        v1.and(warp::path("lookup"))
            .and(warp::path::param::<u64>())
            .and(warp::get())
            .and_then(move |key: u64| {
                let log = log.clone();
                async move {
                    match log.lookup_key(key).await {
                        Some(offset) => {
                            if let Some(payload) = log.get(offset).await {
                                Ok::<_, warp::Rejection>(warp::reply::with_status(
                                    payload,
                                    warp::http::StatusCode::OK
                                ))
                            } else {
                                Err(warp::reject::not_found())
                            }
                        }
                        None => Err(warp::reject::not_found())
                    }
                }
            })
    };

    // Fast offset lookup (now using ultra-fast implementation)
    let lookup_fast = {
        let log = log.clone();
        v1.and(warp::path("lookup_fast"))
            .and(warp::path::param::<u64>())
            .and(warp::get())
            .map(move |key: u64| {
                if let Some(offset) = log.lookup_key_ultra_fast(key) {
                    let response = offset.to_le_bytes().to_vec();
                    warp::reply::with_header(
                        response,
                        "content-type",
                        "application/octet-stream"
                    )
                } else {
                    warp::reply::with_header(
                        Vec::<u8>::new(),
                        "content-type",
                        "application/octet-stream"
                    )
                }
            })
    };

    put_route.or(lookup_route).or(lookup_fast)
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
                    log.snapshot().await
                        .map_err(|_| warp::reject())?;
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
                        log.compact_keep_latest_and_snapshot().await
                            .map_err(|_| warp::reject())?;
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

    snapshot_route.or(rmi_build)
}

/// 💊 HEALTH ENDPOINTS: Health checks and diagnostics
fn create_health_endpoints() -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    // Health check endpoint
    let health_route = warp::path("health")
        .and(warp::get())
        .map(|| {
            warp::reply::json(&serde_json::json!({ "status": "ok" }))
        });

    // Metrics endpoint
    let metrics_route = warp::path("metrics")
        .and(warp::get())
        .map(|| {
            #[cfg(not(feature = "bench-no-metrics"))]
            {
                let encoder = prometheus::TextEncoder::new();
                let metric_families = prometheus::gather();
                match encoder.encode_to_string(&metric_families) {
                    Ok(output) => warp::reply::with_header(
                        output,
                        "Content-Type",
                        "text/plain; version=0.0.4",
                    ),
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
