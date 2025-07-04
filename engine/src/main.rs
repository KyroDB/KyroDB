//! CLI wrapper around PersistentEventLog.

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::stream::StreamExt;
use ngdb_engine::PersistentEventLog;
use tokio::signal;
use uuid::Uuid;
use warp::Filter;

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

    Serve { host: String, port: u16 },
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
        Commands::Serve { host, port } => {
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
                .with(warp::log("ngdb"));

            println!("🚀 Starting server at http://{}:{}", host, port);
            warp::serve(routes)
                .run((host.parse::<std::net::IpAddr>()?, port))
                .await;
        }
    }

    Ok(())
}
