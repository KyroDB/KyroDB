//! CLI wrapper around PersistentEventLog.

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::stream::StreamExt;
use ngdb_engine::PersistentEventLog;
use tokio::signal;
use uuid::Uuid;
use warp::Filter;
mod sql;

#[derive(Parser)]
#[command(name = "ngdb-engine", about = "NextGen-DB Engine")]
struct Cli {
    /// Directory for data files (snapshots + WAL)
    #[arg(short, long, default_value = "./data")]
    data_dir: String,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Append a UTF-8 string as a new event
    Append { payload: String },

    /// Replay events from `start` to `end` offsets
    Replay { start: u64, end: Option<u64> },

    /// Force a full snapshot to disk
    Snapshot,

    /// Tail live events from offset (Ctrl+C to exit)
    Subscribe { from: u64 },

    /// Execute a simple SQL statement (INSERT/SELECT)
    Sql { stmt: String },

    /// Lookup value by primary key
    Lookup { key: u64 },

    Serve {
        host: String,
        port: u16,

        /// Automatically trigger snapshot every N seconds
        #[arg(long)]
        auto_snapshot_secs: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let log = PersistentEventLog::open(&cli.data_dir).await?;

    match cli.cmd {
        Commands::Append { payload } => {
            let id = Uuid::new_v4();
            let offset = log.append(id, payload.into_bytes()).await?;
            println!("✅ Appended at offset {}", offset);
        }

        Commands::Replay { start, end } => {
            let events = log.replay(start, end).await;
            for e in events {
                println!(
                    "[{}] {} → {}",
                    e.offset,
                    e.request_id,
                    String::from_utf8_lossy(&e.payload)
                );
            }
        }

        Commands::Snapshot => {
            log.snapshot().await?;
            println!("📦 Snapshot written.");
        }

        Commands::Subscribe { from } => {
            let (past, mut rx) = log.subscribe(from).await;

            // Emit past events
            for e in past {
                println!("[{}] {}", e.offset, String::from_utf8_lossy(&e.payload));
            }
            println!("📡 Tailing live events—press Ctrl+C to exit");

            // Tail new events
            loop {
                tokio::select! {
                    Ok(evt) = rx.recv() => {
                        println!("[{}] {}", evt.offset, String::from_utf8_lossy(&evt.payload));
                    }
                    _ = signal::ctrl_c() => {
                        println!("\n👋 Goodbye.");
                        break;
                    }
                }
            }
        }
        Commands::Sql { stmt } => {
            match sql::execute_sql(&log, &stmt).await? {
                sql::SqlResponse::Ack { offset } => {
                    println!("ACK offset={}", offset);
                }
                sql::SqlResponse::Rows(rows) => {
                    for (k, v) in rows {
                        println!("key={} value={}", k, String::from_utf8_lossy(&v));
                    }
                }
            }
        }
        Commands::Lookup { key } => {
            if let Some(offset) = log.lookup_key(key).await {
                let evs = log.replay(offset, Some(offset + 1)).await;
                if let Some(ev) = evs.into_iter().next() {
                    if let Ok(rec) = bincode::deserialize::<ngdb_engine::Record>(&ev.payload) {
                        println!("key={} value={} (offset={})", rec.key, String::from_utf8_lossy(&rec.value), offset);
                    } else {
                        println!("not found");
                    }
                }
            } else {
                println!("not found");
            }
        }
        Commands::Serve {
            host,
            port,
            auto_snapshot_secs,
        } => {
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

            let append_log = log.clone();
            let append_route = warp::path("append")
                .and(warp::post())
                .and(warp::body::json())
                .and_then(move |body: serde_json::Value| {
                    let log = append_log.clone();
                    async move {
                        let payload = body["payload"]
                            .as_str()
                            .unwrap_or("")
                            .as_bytes()
                            .to_vec();
                        let offset = log.append(Uuid::new_v4(), payload).await.unwrap();
                        Ok::<_, warp::Rejection>(
                            warp::reply::json(&serde_json::json!({ "offset": offset })),
                        )
                    }
                });

            // --- NEW: Health check endpoint ----------------------------------------------------
            let health_route = warp::path("health")
                .and(warp::get())
                .map(|| warp::reply::json(&serde_json::json!({ "status": "ok" })));

            // --- NEW: Snapshot trigger endpoint -----------------------------------------------
            let snapshot_log = log.clone();
            let snapshot_route = warp::path("snapshot")
                .and(warp::post())
                .and_then(move || {
                    let log = snapshot_log.clone();
                    async move {
                        if let Err(e) = log.snapshot().await {
                            eprintln!("❌ Snapshot failed: {}", e);
                            return Ok::<_, warp::Rejection>(warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "error": e.to_string() })),
                                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                            ));
                        }
                        Ok::<_, warp::Rejection>(
                            warp::reply::with_status(
                                warp::reply::json(&serde_json::json!({ "snapshot": "ok" })),
                                warp::http::StatusCode::OK,
                            ),
                        )
                    }
                });

            // --- NEW: Offset endpoint ---------------------------------------------------------
            let offset_log = log.clone();
            let offset_route = warp::path("offset")
                .and(warp::get())
                .and_then(move || {
                    let log = offset_log.clone();
                    async move {
                        let off = log.get_offset().await;
                        Ok::<_, warp::Rejection>(
                            warp::reply::json(&serde_json::json!({ "offset": off })),
                        )
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
                        Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({ "offset": off })))
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
                                let evs = log.replay(offset, Some(offset + 1)).await;
                                if let Some(ev) = evs.into_iter().next() {
                                    if let Ok(rec) = bincode::deserialize::<ngdb_engine::Record>(&ev.payload) {
                                        return Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
                                            "key": rec.key,
                                            "value": String::from_utf8_lossy(&rec.value)
                                        })));
                                    }
                                }
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
                                let resp: Vec<_> = rows.into_iter().map(|(k, v)| serde_json::json!({
                                    "key": k,
                                    "value": String::from_utf8_lossy(&v)
                                })).collect();
                                Ok::<_, warp::Rejection>(warp::reply::json(&resp))
                            }
                            Err(e) => {
                                let err = serde_json::json!({"error": e.to_string()});
                                Ok::<_, warp::Rejection>(warp::reply::json(&err))
                            },
                        }
                    }
                });

            // Metrics endpoint
            let metrics_route = warp::path("metrics").and(warp::get()).map(|| {
                let text = ngdb_engine::metrics::render();
                warp::reply::with_header(text, "Content-Type", "text/plain; version=0.0.4")
            });

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
                            .filter_map(|res| async move { res.ok() })
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

            let routes = append_route
                .or(replay_route)
                .or(subscribe_route)
                .or(health_route)
                .or(snapshot_route)
                .or(offset_route)
                .or(put_route)
                .or(lookup_route)
                .or(sql_route)
                .or(metrics_route)
                .with(warp::log("ngdb"));

            println!("🚀 Starting server at http://{}:{}", host, port);
            warp::serve(routes)
                .run((host.parse::<std::net::IpAddr>()?, port))
                .await;
        }
    }

    Ok(())
}
