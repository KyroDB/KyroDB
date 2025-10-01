use anyhow::Result;
use clap::Parser;
use kyrodb_bench::{
    BenchmarkConfig, ClientType, ResultsReporter, WorkloadType, WorkloadConfig,
    create_workload, runner::executor::WorkloadExecutor, ClientConfig,
    metrics::BenchmarkPhase,
};

#[derive(Parser, Debug)]
#[command(name = "kyrodb-bench")]
#[command(about = "KyroDB Comprehensive Benchmark Suite")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:3030")]
    server_url: String,
    
    #[arg(long, default_value = "http")]
    protocol: String,
    
    #[arg(long, default_value = "all")]
    phase: String,
    
    #[arg(long)]
    workload: Option<String>,
    
    #[arg(long, default_value = "64")]
    workers: usize,
    
    #[arg(long, default_value = "30")]
    duration: u64,
    
    #[arg(long, default_value = "10")]
    warmup: u64,
    
    #[arg(long, default_value = "1000000")]
    key_count: u64,
    
    #[arg(long, default_value = "256")]
    value_size: usize,
    
    #[arg(long, default_value = "0.5")]
    read_ratio: f64,
    
    #[arg(long, default_value = "uniform")]
    distribution: String,
    
    #[arg(long, default_value = "bench/results")]
    output_dir: String,
    
    #[arg(long, default_value = "summary")]
    format: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    println!("🔥 KyroDB Benchmark Suite");
    println!("Server: {}", args.server_url);
    println!("Phase: {}", args.phase);
    println!();
    
    let client_type = ClientType::from_str(&args.protocol)?;
    
    match args.phase.as_str() {
        "all" => {
            let config = BenchmarkConfig {
                server_url: args.server_url,
                client_type,
                duration_secs: args.duration,
                workers: args.workers,
                warmup_secs: args.warmup,
                output_dir: args.output_dir,
            };
            
            let results = kyrodb_bench::run_full_suite(config).await?;
            for result in &results {
                ResultsReporter::print_summary(result);
            }
        }
        "quick" => {
            let config = BenchmarkConfig {
                server_url: args.server_url,
                client_type,
                duration_secs: 10,
                workers: 8,
                warmup_secs: 5,
                output_dir: args.output_dir,
            };
            
            let result = kyrodb_bench::run_quick_test(config).await?;
            ResultsReporter::print_summary(&result);
        }
        _ => {
            let client_config = ClientConfig {
                base_url: args.server_url.clone(),
                timeout_secs: 30,
                pool_size: args.workers,
                auth_token: None,
            };
            
            // Create HTTP client directly
            let http_client = kyrodb_bench::client::HttpClient::new(client_config)?;
            
            let workload_type = if let Some(ref w) = args.workload {
                WorkloadType::from_str(w)?
            } else {
                WorkloadType::Mixed
            };
            
            let workload_config = WorkloadConfig {
                key_count: args.key_count,
                value_size: args.value_size,
                read_ratio: args.read_ratio,
                distribution: kyrodb_bench::workload::Distribution::from_str(&args.distribution)?,
                duration_secs: args.duration,
            };
            
            let executor = WorkloadExecutor::new_with_http(http_client, args.workers);
            let workload = create_workload(workload_type, workload_config)?;
            
            let phase = BenchmarkPhase::HttpLayer;
            let result = executor.execute(workload, phase).await?;
            
            match args.format.as_str() {
                "csv" => ResultsReporter::print_csv_row(&result),
                "json" => println!("{}", serde_json::to_string_pretty(&result)?),
                _ => ResultsReporter::print_summary(&result),
            }
        }
    }
    
    println!("\n🎉 Done!");
    Ok(())
}
