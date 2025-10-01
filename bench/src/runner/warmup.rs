use anyhow::Result;
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};
use crate::client::{BenchClient, HttpClient};

/// Warmup server by loading data and building indexes
pub async fn warmup_server(client: &HttpClient, warmup_secs: u64) -> Result<()> {
    println!("🔥 Warming up server ({} seconds)...", warmup_secs);
    
    let pb = ProgressBar::new(warmup_secs);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}s {msg}")
            .expect("Invalid progress bar template")
            .progress_chars("#>-"),
    );
    
    // Load some initial data
    pb.set_message("Loading initial data...");
    let mut tasks = Vec::new();
    for i in 0..10000 {
        let value = vec![0u8; 256];
        tasks.push(client.put(i, value));
    }
    
    // Execute puts concurrently (sample of them)
    for (idx, task) in tasks.into_iter().enumerate().take(100) {
        task.await.ok(); // Ignore errors during warmup
        if idx % 10 == 0 {
            pb.inc(1);
        }
    }
    
    // Create snapshot
    pb.set_message("Creating snapshot...");
    client.snapshot().await?;
    pb.inc(warmup_secs / 4);
    
    // Build RMI index
    pb.set_message("Building RMI index...");
    client.build_rmi().await?;
    pb.inc(warmup_secs / 4);
    
    // Warmup mmap pages
    pb.set_message("Warming up memory maps...");
    client.warmup().await?;
    pb.inc(warmup_secs / 4);
    
    // Wait remaining time
    pb.set_message("Finalizing warmup...");
    tokio::time::sleep(Duration::from_secs(warmup_secs / 4)).await;
    pb.finish_with_message("Warmup complete!");
    
    println!("✅ Server warmed up and ready");
    Ok(())
}
