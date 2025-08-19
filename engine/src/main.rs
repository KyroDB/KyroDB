//! CLI wrapper around PersistentEventLog.

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::stream::StreamExt;
use kyrodb_engine as engine_crate;
use std::sync::Arc;
use uuid::Uuid;
use warp::Filter;
mod sql;

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
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let log = Arc::new(
        engine_crate::PersistentEventLog::open(std::path::Path::new(&cli.data_dir))
            .await
            .unwrap(),
    );

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
        } => {
            // Silence unused when learned-index feature is disabled
            #[cfg(not(feature = "learned-index"))]
            let _ = (rmi_rebuild_appends.as_ref(), rmi_rebuild_ratio.as_ref());

            // Configure WAL rotation if requested
            if wal_segment_bytes.is_some() || wal_max_segments > 0 {
                log.configure_wal_rotation(wal_segment_bytes, wal_max_segments)
                    .await;
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
                            // Write index on blocking thread
                            let pairs_clone = pairs.clone();
                            let tmp_clone = tmp.clone();
                            let write_res = tokio::task::spawn_blocking(move || {
                                engine_crate::index::RmiIndex::write_from_pairs(
                                    &tmp_clone,
                                    &pairs_clone,
                                )
                            })
                            .await;
                            if let Ok(Err(e)) = write_res {
                                eprintln!("❌ RMI rebuild write failed: {}", e);
                                rebuild_timer.observe_duration();
                                continue;
                            }
                            if write_res.is_err() {
                                eprintln!("❌ RMI rebuild task panicked");
                                rebuild_timer.observe_duration();
                                continue;
                            }
                            // fsync tmp then rename
                            if let Ok(f) = std::fs::OpenOptions::new().read(true).open(&tmp) {
                                let _ = f.sync_all();
                            }
                            if let Err(e) = std::fs::rename(&tmp, &dst) {
                                eprintln!("❌ RMI rename failed: {}", e);
                                rebuild_timer.observe_duration();
                                continue;
                            }
                            // Reload and swap
                            if let Some(rmi) = engine_crate::index::RmiIndex::load_from_file(&dst) {
                                rebuild_log
                                    .swap_primary_index(engine_crate::index::PrimaryIndex::Rmi(rmi))
                                    .await;
                                last_built = cur;
                                engine_crate::metrics::RMI_REBUILDS_TOTAL.inc();
                                rebuild_timer.observe_duration();
                                // Commit manifest LAST via library API
                                rebuild_log.write_manifest().await;
                                println!("✅ RMI rebuilt, swapped, and manifest committed (appended={}, ratio={:.3})", appended, ratio);
                            } else {
                                eprintln!("❌ RMI reload failed after rebuild");
                                rebuild_timer.observe_duration();
                            }
                        }
                    }
                });
            }

            // --- Fast lookup endpoints (HTTP hot path) ---------------------------------------
            let raw_log = log.clone();
            let lookup_raw = warp::path("lookup_raw")
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
            let lookup_fast =
                warp::path!("lookup_fast" / u64)
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

            let append_log = log.clone();
            let append_route = warp::path("append")
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = append_log.clone();
                    async move {
                        let payload = body["payload"].as_str().unwrap_or("").as_bytes().to_vec();
                        let offset = log.append(Uuid::new_v4(), payload).await.unwrap();
                        Ok::<_, warp::Rejection>(warp::reply::json(
                            &serde_json::json!({ "offset": offset }),
                        ))
                    }
                });

            // --- NEW: Health check endpoint ----------------------------------------------------
            let health_route = warp::path("health")
                .and(warp::get())
                .map(|| warp::reply::json(&serde_json::json!({ "status": "ok" })));

            // --- NEW: Snapshot trigger endpoint -----------------------------------------------
            let snapshot_log = log.clone();
            let snapshot_route = warp::path("snapshot").and(warp::post()).and_then(move || {
                let log = snapshot_log.clone();
                async move {
                    if let Err(e) = log.snapshot().await {
                        eprintln!("❌ Snapshot failed: {}", e);
                        return Ok::<_, warp::Rejection>(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({ "error": e.to_string() })),
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
            let compact_route = warp::path("compact").and(warp::post()).and_then(move || {
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
                            warp::reply::json(&serde_json::json!({ "error": e.to_string() })),
                            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                        )),
                    }
                }
            });

            // --- NEW: Offset endpoint ---------------------------------------------------------
            let offset_log = log.clone();
            let offset_route = warp::path("offset").and(warp::get()).and_then(move || {
                let log = offset_log.clone();
                async move {
                    let off = log.get_offset().await;
                    Ok::<_, warp::Rejection>(warp::reply::json(
                        &serde_json::json!({ "offset": off }),
                    ))
                }
            });

            let replay_log = log.clone();
            let replay_route = warp::path("replay")
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

            // KV PUT: POST /put { key: u64, value: string }
            let put_log = log.clone();
            let put_route = warp::path("put")
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = put_log.clone();
                    async move {
                        let key = body["key"].as_u64().unwrap_or(0);
                        let value = body["value"].as_str().unwrap_or("").as_bytes().to_vec();
                        let off = log.append_kv(Uuid::new_v4(), key, value).await.unwrap();
                        Ok::<_, warp::Rejection>(warp::reply::json(
                            &serde_json::json!({ "offset": off }),
                        ))
                    }
                });

            // Simple lookup by key: GET /lookup?key=123
            let lookup_log = log.clone();
            let lookup_route = warp::path("lookup")
                .and(warp::get())
                .and(warp::query::<std::collections::HashMap<String, String>>())
                .and_then(move |q: std::collections::HashMap<String, String>| {
                    let log = lookup_log.clone();
                    async move {
                        if let Some(k) = q.get("key").and_then(|s| s.parse::<u64>().ok()) {
                            if let Some(offset) = log.lookup_key(k).await {
                                // Fetch payload via mmap-backed snapshot (with WAL/memory fallback)
                                if let Some(bytes) = log.get(offset).await {
                                    if let Ok(rec) = bincode::deserialize::<kyrodb_engine::Record>(&bytes) {
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

            // POST /sql  { sql: "INSERT ..." | "SELECT ..." }
            let sql_log = log.clone();
            let sql_route = warp::path("sql")
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

            // Metrics endpoint
            let metrics_route = warp::path("metrics").and(warp::get()).map(|| {
                let text = engine_crate::metrics::render();
                warp::reply::with_header(text, "Content-Type", "text/plain; version=0.0.4")
            });

            // Authorization filter (optional)
            #[derive(Debug)]
            struct Unauthorized;
            impl warp::reject::Reject for Unauthorized {}
            let auth = {
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

            // RMI build: POST /rmi/build  (feature-gated)
            #[cfg(feature = "learned-index")]
            let rmi_build = {
                let data_dir = cli.data_dir.clone();
                let build_log = log.clone();
                warp::path!("rmi" / "build")
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

                            // Write index on a blocking thread to avoid starving the reactor
                            let pairs_clone = pairs.clone();
                            let tmp_clone = tmp.clone();
                            let write_res = tokio::task::spawn_blocking(move || {
                                engine_crate::index::RmiIndex::write_from_pairs(
                                    &tmp_clone,
                                    &pairs_clone,
                                )
                            })
                            .await;
                            let mut ok = matches!(write_res, Ok(Ok(())));

                            // fsync tmp then rename -> dst
                            if ok {
                                if let Ok(f) = std::fs::OpenOptions::new().read(true).open(&tmp) {
                                    let _ = f.sync_all();
                                }
                                if let Err(e) = std::fs::rename(&tmp, &dst) {
                                    eprintln!("❌ RMI rename failed: {}", e);
                                    ok = false;
                                }
                            }

                            // Reload, swap, and commit manifest LAST
                            if ok {
                                if let Some(rmi) =
                                    engine_crate::index::RmiIndex::load_from_file(&dst)
                                {
                                    log.swap_primary_index(engine_crate::index::PrimaryIndex::Rmi(
                                        rmi,
                                    ))
                                    .await;
                                    engine_crate::metrics::RMI_REBUILDS_TOTAL.inc();
                                    // Commit manifest as the external commit point
                                    log.write_manifest().await;
                                } else {
                                    eprintln!("❌ RMI reload failed after rebuild");
                                    ok = false;
                                }
                            }

                            timer.observe_duration();
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
                warp::path!("rmi" / "build").and(warp::post()).map(|| {
                    warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({
                            "error": "learned-index feature not enabled"
                        })),
                        StatusCode::NOT_IMPLEMENTED,
                    )
                })
            };

            let subscribe_log = log.clone();
            let subscribe_route = warp::path("subscribe")
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

            // Vector insert: POST /vector/insert { key: u64, vector: [f32,...] }
            let vec_ins_log = log.clone();
            let vector_insert = warp::path!("vector" / "insert")
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = vec_ins_log.clone();
                    async move {
                        let key = body["key"].as_u64().unwrap_or(0);
                        let vec: Vec<f32> = body["vector"]
                            .as_array()
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_f64().map(|x| x as f32))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let off = log.append_vector(Uuid::new_v4(), key, vec).await.unwrap();
                        Ok::<_, warp::Rejection>(warp::reply::json(
                            &serde_json::json!({"offset": off}),
                        ))
                    }
                });

            // Vector search: POST /vector/search { query: [f32,...], k: usize }
            let vec_search_log = log.clone();
            let vector_search = warp::path!("vector" / "search")
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = vec_search_log.clone();
                    async move {
                        let q: Vec<f32> = body["query"]
                            .as_array()
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_f64().map(|x| x as f32))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let k = body["k"].as_u64().unwrap_or(10) as usize;
                        let res = log.search_vector_l2(&q, k).await;
                        let out: Vec<_> = res
                            .into_iter()
                            .map(|(key, dist)| serde_json::json!({"key": key, "dist": dist}))
                            .collect();
                        Ok::<_, warp::Rejection>(warp::reply::json(&out))
                    }
                });

            // Apply auth to protected routes
            let append_route = auth.clone().and(append_route).map(|(), r| r);
            let replay_route = auth.clone().and(replay_route).map(|(), r| r);
            let subscribe_route = auth.clone().and(subscribe_route).map(|(), r| r);
            let snapshot_route = auth.clone().and(snapshot_route).map(|(), r| r);
            let offset_route = auth.clone().and(offset_route).map(|(), r| r);
            let put_route = auth.clone().and(put_route).map(|(), r| r);
            let lookup_route = auth.clone().and(lookup_route).map(|(), r| r);
            let lookup_raw = auth.clone().and(lookup_raw).map(|(), r| r);
            let lookup_fast = auth.clone().and(lookup_fast).map(|(), r| r);
            let sql_route = auth.clone().and(sql_route).map(|(), r| r);
            let vector_insert = auth.clone().and(vector_insert).map(|(), r| r);
            let vector_search = auth.clone().and(vector_search).map(|(), r| r);
            let rmi_build = auth.clone().and(rmi_build).map(|(), r| r);

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
                .or(sql_route)
                .or(vector_insert)
                .or(vector_search)
                .or(rmi_build)
                .or(compact_route)
                .recover(|rej: warp::Rejection| async move {
                    use warp::http::StatusCode;
                    if rej.find::<Unauthorized>().is_some() {
                        Ok::<_, std::convert::Infallible>(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({"error":"unauthorized"})),
                            StatusCode::UNAUTHORIZED,
                        ))
                    } else {
                        Ok::<_, std::convert::Infallible>(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({"error":"not found"})),
                            StatusCode::NOT_FOUND,
                        ))
                    }
                })
                .with(warp::log("kyrodb"));

            println!("🚀 Starting server at http://{}:{}", host, port);
            warp::serve(routes)
                .run((host.parse::<std::net::IpAddr>()?, port))
                .await;
        }
    }

    Ok(())
}
