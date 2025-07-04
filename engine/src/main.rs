// engine/src/main.rs

use clap::{Parser, Subcommand};
use engine::{EventLog}; // adjust crate name if needed
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "ngdb-engine", about = "NextGen-DB Engine")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Append an event (as a UTF-8 string) to the log
    Append { payload: String },
    /// Replay events from `start` to `end` offsets
    Replay { start: u64, end: Option<u64> },
    /// Subscribe to live events from offset
    Subscribe { from: u64 },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let log = EventLog::new();

    match cli.cmd {
        Commands::Append { payload } => {
            let id = Uuid::new_v4();
            let result = log.append(id, payload.into_bytes()).await;
            println!("Appended at offset {}", result.offset);
        }
        Commands::Replay { start, end } => {
            let events = log.replay(start, end).await;
            for e in events {
                println!("[{}] {:?}: {}", e.offset, e.request_id, String::from_utf8_lossy(&e.payload));
            }
        }
        Commands::Subscribe { from } => {
            let events = log.subscribe(from).await;
            for e in events {
                println!("[{}] {:?}", e.offset, e.request_id);
            }
        }
    }

    Ok(())
}
