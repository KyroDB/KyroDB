use clap::{Parser, Subcommand};
use ngdb_engine::PersistentEventLog;
use uuid::Uuid;
use anyhow::Result;

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
    Append { payload: String },
    Replay { start: u64, end: Option<u64> },
    Snapshot,
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
    }

    Ok(())
}
