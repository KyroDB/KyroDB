use clap::{Parser, Subcommand};
use tokio::signal;

#[derive(Parser)]
#[command(name = "ngdb-engine", about = "NextGen-DB Engine")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Append an event (JSON) to the log
    Append { payload: String },
    /// Replay events from `start` to `end` offsets
    Replay { start: u64, end: Option<u64> },
    /// Subscribe to live events from offset
    Subscribe { from: u64 },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.cmd {
        Commands::Append { payload } => {
            println!("(stub) append: {}", payload);
            // TODO: call EventLog::append(...)
        }
        Commands::Replay { start, end } => {
            println!("(stub) replay from {} to {:?}", start, end);
            // TODO: call EventLog::replay(...)
        }
        Commands::Subscribe { from } => {
            println!("(stub) subscribe from {}", from);
            // TODO: call EventLog::subscribe(...)
            //       plus handle Ctrl-C via tokio::signal
            signal::ctrl_c().await?;
        }
    }

    Ok(())
}
