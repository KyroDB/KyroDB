use anyhow::Result;
use clap::{Parser, Subcommand};
use kyrodb_engine::backup::{BackupManager, RestoreManager, RetentionPolicy};
use std::path::PathBuf;
use uuid::Uuid;

#[cfg(feature = "cli-tools")]
use indicatif::{ProgressBar, ProgressStyle};
#[cfg(feature = "cli-tools")]
use tabled::{Table, Tabled};

#[cfg(not(feature = "cli-tools"))]
compile_error!("kyrodb_backup binary requires the 'cli-tools' feature. Build with: cargo build --bin kyrodb_backup --features cli-tools");

#[derive(Parser)]
#[command(name = "kyrodb-backup")]
#[command(about = "KyroDB backup and restore operations", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, default_value = "./data", help = "Database data directory")]
    data_dir: PathBuf,

    #[arg(long, default_value = "./backups", help = "Backup storage directory")]
    backup_dir: PathBuf,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Create a new backup")]
    Create {
        #[arg(long, help = "Create incremental backup instead of full")]
        incremental: bool,

        #[arg(long, help = "Backup description")]
        description: Option<String>,

        #[arg(
            long,
            help = "Reference backup ID for incremental backup",
            requires = "incremental"
        )]
        reference: Option<String>,
    },

    #[command(about = "List all available backups")]
    List {
        #[arg(long, default_value = "table", help = "Output format: table or json")]
        format: String,
    },

    #[command(about = "Restore database from a backup")]
    Restore {
        #[arg(long, help = "Backup ID to restore from", conflicts_with = "point_in_time", required_unless_present = "point_in_time")]
        backup_id: Option<String>,

        #[arg(
            long,
            help = "Point-in-time timestamp (Unix epoch seconds)",
            conflicts_with = "backup_id",
            required_unless_present = "backup_id"
        )]
        point_in_time: Option<u64>,
    },

    #[command(about = "Prune old backups based on retention policy")]
    Prune {
        #[arg(long, help = "Keep last N hourly backups")]
        keep_hourly: Option<usize>,

        #[arg(long, help = "Keep last N daily backups")]
        keep_daily: Option<usize>,

        #[arg(long, help = "Keep last N weekly backups")]
        keep_weekly: Option<usize>,

        #[arg(long, help = "Keep last N monthly backups")]
        keep_monthly: Option<usize>,

        #[arg(long, help = "Minimum age in days before pruning")]
        min_age_days: Option<u64>,
    },

    #[command(about = "Verify backup integrity")]
    Verify {
        #[arg(help = "Backup ID to verify")]
        backup_id: String,
    },
}

#[cfg(feature = "cli-tools")]
#[derive(Tabled)]
struct BackupRow {
    #[tabled(rename = "Backup ID")]
    id: String,
    #[tabled(rename = "Type")]
    backup_type: String,
    #[tabled(rename = "Created")]
    created_at: String,
    #[tabled(rename = "Size")]
    size: String,
    #[tabled(rename = "Vectors")]
    vectors: u64,
    #[tabled(rename = "Description")]
    description: String,
}

fn main() -> Result<()> {
    #[cfg(feature = "cli-tools")]
    env_logger::init();

    #[cfg(not(feature = "cli-tools"))]
    eprintln!("Warning: Running without cli-tools feature - no progress bars or formatted output");

    let cli = Cli::parse();

    match cli.command {
        Commands::Create {
            incremental,
            description,
            reference,
        } => {
            let backup_manager = BackupManager::new(&cli.backup_dir, &cli.data_dir)?;

            #[cfg(feature = "cli-tools")]
            let progress = {
                let p = ProgressBar::new_spinner();
                p.set_style(
                    ProgressStyle::default_spinner()
                        .template("{spinner:.green} {msg}")
                        .unwrap(),
                );
                p
            };

            if incremental {
                #[cfg(feature = "cli-tools")]
                progress.set_message("Creating incremental backup...");
                
                let reference_id = reference
                    .ok_or_else(|| anyhow::anyhow!("Reference backup ID required"))?
                    .parse::<Uuid>()?;

                let metadata =
                    backup_manager.create_incremental_backup(reference_id, description.unwrap_or_default())?;
                
                #[cfg(feature = "cli-tools")]
                progress.finish_with_message("Incremental backup created successfully");
                
                println!("\nBackup ID: {}", metadata.id);
                println!("Type: Incremental");
                println!("Size: {} bytes", metadata.size_bytes);
                println!("Vectors: {}", metadata.vector_count);
            } else {
                #[cfg(feature = "cli-tools")]
                progress.set_message("Creating full backup...");
                
                let metadata = backup_manager.create_full_backup(description.unwrap_or_default())?;
                
                #[cfg(feature = "cli-tools")]
                progress.finish_with_message("Full backup created successfully");
                
                println!("\nBackup ID: {}", metadata.id);
                println!("Type: Full");
                println!("Size: {} bytes", metadata.size_bytes);
                println!("Vectors: {}", metadata.vector_count);
            }
        }

        Commands::List { format } => {
            let backup_manager = BackupManager::new(&cli.backup_dir, &cli.data_dir)?;
            let backups = backup_manager.list_backups()?;

            if backups.is_empty() {
                println!("No backups found");
                return Ok(());
            }

            if format == "json" {
                println!("{}", serde_json::to_string_pretty(&backups)?);
            } else {
                #[cfg(feature = "cli-tools")]
                {
                    let rows: Vec<BackupRow> = backups
                        .into_iter()
                        .map(|b| BackupRow {
                            id: b.id.to_string(),
                            backup_type: match b.backup_type {
                                kyrodb_engine::backup::BackupType::Full => "Full".to_string(),
                                kyrodb_engine::backup::BackupType::Incremental => "Incremental".to_string(),
                            },
                            created_at: chrono::DateTime::from_timestamp(
                                b.timestamp.try_into().unwrap_or(i64::MAX), 0
                            )
                                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                                .unwrap_or_else(|| "Unknown".to_string()),
                            size: format_bytes(b.size_bytes),
                            vectors: b.vector_count,
                            description: b.description,
                        })
                        .collect();

                    let table = Table::new(rows).to_string();
                    println!("{}", table);
                }
                #[cfg(not(feature = "cli-tools"))]
                {
                    println!("Table format requires cli-tools feature. Use --format json instead.");
                }
            }
        }

        Commands::Restore {
            backup_id,
            point_in_time,
        } => {
            let restore_manager = RestoreManager::new(&cli.backup_dir, &cli.data_dir)?;

            #[cfg(feature = "cli-tools")]
            let progress = {
                let p = ProgressBar::new_spinner();
                p.set_style(
                    ProgressStyle::default_spinner()
                        .template("{spinner:.green} {msg}")
                        .unwrap(),
                );
                p
            };

            // Clap now enforces that exactly one of backup_id or point_in_time is present
            if let Some(backup_id_str) = backup_id {
                #[cfg(feature = "cli-tools")]
                progress.set_message(format!("Restoring from backup {}...", backup_id_str));
                
                let backup_id = backup_id_str.parse::<Uuid>()?;
                restore_manager.restore_from_backup(backup_id)?;
                
                #[cfg(feature = "cli-tools")]
                progress.finish_with_message("Restore completed successfully");
                
                println!("\nDatabase restored from backup {}", backup_id_str);
            } else if let Some(timestamp) = point_in_time {
                #[cfg(feature = "cli-tools")]
                progress.set_message(format!("Restoring to point-in-time {}...", timestamp));
                
                restore_manager.restore_point_in_time(timestamp)?;
                
                #[cfg(feature = "cli-tools")]
                progress.finish_with_message("Point-in-time restore completed successfully");
                
                let dt = chrono::DateTime::from_timestamp(
                    timestamp.try_into().unwrap_or(i64::MAX), 0
                )
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "Unknown".to_string());
                println!("\nDatabase restored to point-in-time: {}", dt);
            }
        }

        Commands::Prune {
            keep_hourly,
            keep_daily,
            keep_weekly,
            keep_monthly,
            min_age_days,
        } => {
            let backup_manager = BackupManager::new(&cli.backup_dir, &cli.data_dir)?;

            let policy = RetentionPolicy {
                hourly_hours: keep_hourly.unwrap_or(24),
                daily_days: keep_daily.unwrap_or(7),
                weekly_weeks: keep_weekly.unwrap_or(4),
                monthly_months: keep_monthly.unwrap_or(6),
                min_age_days: min_age_days.unwrap_or(0),
            };

            #[cfg(feature = "cli-tools")]
            let progress = {
                let p = ProgressBar::new_spinner();
                p.set_style(
                    ProgressStyle::default_spinner()
                        .template("{spinner:.green} {msg}")
                        .unwrap(),
                );
                p.set_message("Evaluating backups for pruning...");
                p
            };

            let pruned = backup_manager.prune_backups(&policy)?;
            
            #[cfg(feature = "cli-tools")]
            progress.finish();

            if pruned.is_empty() {
                println!("No backups pruned (all within retention policy)");
            } else {
                println!("Pruned {} backup(s):", pruned.len());
                for id in pruned {
                    println!("  - {}", id);
                }
            }
        }

        Commands::Verify { backup_id } => {
            let backup_manager = BackupManager::new(&cli.backup_dir, &cli.data_dir)?;
            let backup_id = backup_id.parse::<Uuid>()?;

            #[cfg(feature = "cli-tools")]
            let progress = {
                let p = ProgressBar::new_spinner();
                p.set_style(
                    ProgressStyle::default_spinner()
                        .template("{spinner:.green} {msg}")
                        .unwrap(),
                );
                p.set_message("Verifying backup integrity...");
                p
            };

            let backups = backup_manager.list_backups()?;
            let backup = backups
                .iter()
                .find(|b| b.id == backup_id)
                .ok_or_else(|| anyhow::anyhow!("Backup {} not found", backup_id))?;

            // Check for backup file with both possible extensions (.backup and .tar)
            let mut backup_path = cli.backup_dir.join(format!("{}.backup", backup_id));
            if !backup_path.exists() {
                backup_path = cli.backup_dir.join(format!("backup_{}.tar", backup_id));
            }
            if !backup_path.exists() {
                anyhow::bail!("Backup file not found at {} or backup_{}.tar", 
                    cli.backup_dir.join(format!("{}.backup", backup_id)).display(),
                    backup_id);
            }

            // Verify file size
            let file_size = std::fs::metadata(&backup_path)?.len();
            if file_size != backup.size_bytes {
                anyhow::bail!(
                    "Size mismatch: expected {} bytes, found {} bytes",
                    backup.size_bytes,
                    file_size
                );
            }

            // Compute and verify checksum
            #[cfg(feature = "cli-tools")]
            progress.set_message("Computing checksum...");
            
            let computed_checksum = kyrodb_engine::backup::compute_backup_checksum(&backup_path)?;
            
            if computed_checksum != backup.checksum {
                anyhow::bail!(
                    "Checksum mismatch: expected 0x{:08X}, computed 0x{:08X}",
                    backup.checksum,
                    computed_checksum
                );
            }

            #[cfg(feature = "cli-tools")]
            progress.finish_with_message("Backup verification successful");
            
            println!("\nBackup {} is valid", backup_id);
            println!("Size: {} bytes", file_size);
            println!("Checksum: 0x{:08X} (verified)", backup.checksum);
        }
    }

    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_idx])
}
