//! CLI wrapper around PersistentEventLog.

use clap::{Parser, Subcommand};
use ngdb_engine::PersistentEventLog;
use uuid::Uuid;
use anyhow::Result;
use tokio::signal;

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
    }

    Ok(())
}
