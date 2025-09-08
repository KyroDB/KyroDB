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
                        let data_dir = data_dir.clone();
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

            // Start optimized HTTP server
            let addr = (host.parse::<std::net::IpAddr>()?, port);
            tracing::info!(
                "Starting kyrodb-engine on {}:{} (commit={}, features={})",
                host,
                port,
                build_commit,
                build_features
            );

            println!(
                "🚀 Starting optimized HTTP server at http://{}:{} (commit={}, features={})",
                host, port, build_commit, build_features
            );

            warp::serve(routes).run(addr).await;
        }
    }

    Ok(())
}
