use anyhow::Result;
use clap::{Arg, Command};
use rand::prelude::*;
use rand_distr::Zipf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod grpc_client;
use grpc_client::{pb::kyrodb_client::KyrodbClient, GrpcBenchClient};
use tonic::transport::Channel;

#[derive(Clone, Debug)]
pub struct BenchmarkResults {
    pub total_reads: u64,
    pub duration_secs: f64,
    pub latencies_us: Vec<u64>,
}

impl BenchmarkResults {
    pub fn ops_per_sec(&self) -> f64 {
        self.total_reads as f64 / self.duration_secs
    }

    pub fn avg_latency_us(&self) -> f64 {
        if self.latencies_us.is_empty() {
            0.0
        } else {
            self.latencies_us.iter().sum::<u64>() as f64 / self.latencies_us.len() as f64
        }
    }

    pub fn percentile_latency_us(&self, percentile: f64) -> u64 {
        if self.latencies_us.is_empty() {
            return 0;
        }
        
        let mut sorted_latencies = self.latencies_us.clone();
        sorted_latencies.sort_unstable();
        
        let index = ((percentile / 100.0) * (sorted_latencies.len() - 1) as f64).round() as usize;
        sorted_latencies[index]
    }
}

async fn create_client(grpc_addr: &str, auth_token: Option<&str>) -> Result<KyrodbClient<Channel>> {
    let channel = tonic::transport::Channel::from_shared(grpc_addr.to_string())?
        .connect()
        .await?;
    Ok(KyrodbClient::new(channel))
}

async fn populate_data(
    grpc_addr: &str,
    auth_token: Option<&str>,
    num_keys: u64,
    value_size: usize,
    batch_size: usize,
) -> Result<()> {
    println!("🔄 Populating {} keys with {}B values in batches of {}", 
             num_keys, value_size, batch_size);

    let mut client = GrpcBenchClient::connect(
        grpc_addr.to_string(), 
        auth_token.map(|s| s.to_string())
    ).await?;

    let value = vec![0u8; value_size];
    let start_time = Instant::now();
    let mut keys_written = 0u64;

    for chunk_start in (1..=num_keys).step_by(batch_size) {
        let chunk_end = std::cmp::min(chunk_start + batch_size as u64 - 1, num_keys);
        let batch: Vec<(u64, Vec<u8>)> = (chunk_start..=chunk_end)
            .map(|key| (key, value.clone()))
            .collect();

        client.batch_put(batch).await?;
        keys_written += chunk_end - chunk_start + 1;

        if keys_written % 10000 == 0 {
            let elapsed = start_time.elapsed();
            let rate = keys_written as f64 / elapsed.as_secs_f64();
            println!("  ✓ Written {} keys ({:.0} keys/sec)", keys_written, rate);
        }
    }

    let total_time = start_time.elapsed();
    let final_rate = num_keys as f64 / total_time.as_secs_f64();
    println!("✅ Populated {} keys in {:.2}s ({:.0} keys/sec)", 
             num_keys, total_time.as_secs_f64(), final_rate);

    Ok(())
}

async fn benchmark_reads(
    grpc_addr: &str,
    auth_token: Option<&str>,
    duration_secs: u64,
    concurrency: u32,
    max_key: u64,
    distribution: String, // Changed from &str to String to avoid lifetime issues
    zipf_theta: f64,
    seed: u64,
) -> Result<BenchmarkResults> {
    println!("🔍 Running read benchmark: {}s, {} threads, {} distribution", 
             duration_secs, concurrency, distribution);

    let start_time = Instant::now();
    let duration = Duration::from_secs(duration_secs);
    let total_reads = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut handles = Vec::new();

    for worker_id in 0..concurrency {
        let grpc_addr = grpc_addr.to_string();
        let auth_token = auth_token.map(|s| s.to_string());
        let total_reads = total_reads.clone();
        let latencies = latencies.clone();
        let worker_seed = seed + worker_id as u64;
        let distribution = distribution.clone(); // Clone the string

        let handle = tokio::spawn(async move {
            let mut client = create_client(&grpc_addr, auth_token.as_deref()).await?;
            let mut rng = rand::rngs::StdRng::seed_from_u64(worker_seed);
            let mut worker_latencies = Vec::new();

            // Set up distribution
            let zipf_dist = if distribution == "zipf" {
                Some(Zipf::new(max_key, zipf_theta).unwrap())
            } else {
                None
            };

            let worker_start = Instant::now();
            while worker_start.elapsed() < duration {
                let key = if let Some(ref zipf) = zipf_dist {
                    zipf.sample(&mut rng) as u64
                } else {
                    rng.gen_range(1..=max_key)
                };

                let req_start = Instant::now();
                let request = tonic::Request::new(grpc_client::pb::GetReq { key });
                let _response = client.get(request).await?;
                let latency_us = req_start.elapsed().as_micros() as u64;

                worker_latencies.push(latency_us);
                total_reads.fetch_add(1, Ordering::Relaxed);
            }

            // Merge worker latencies
            let mut global_latencies = latencies.lock().await;
            global_latencies.extend(worker_latencies);

            Ok::<(), anyhow::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    let actual_duration = start_time.elapsed().as_secs_f64();
    let total_reads_final = total_reads.load(Ordering::Relaxed);
    let latencies_final = latencies.lock().await.clone();

    Ok(BenchmarkResults {
        total_reads: total_reads_final,
        duration_secs: actual_duration,
        latencies_us: latencies_final,
    })
}

fn print_results(results: &BenchmarkResults, test_name: &str) {
    println!("\n📊 {} Results:", test_name);
    println!("  Total operations: {}", results.total_reads);
    println!("  Duration: {:.2}s", results.duration_secs);
    println!("  Throughput: {:.0} ops/sec", results.ops_per_sec());
    println!("  Average latency: {:.2}ms", results.avg_latency_us() / 1000.0);
    println!("  P50 latency: {:.2}ms", results.percentile_latency_us(50.0) as f64 / 1000.0);
    println!("  P95 latency: {:.2}ms", results.percentile_latency_us(95.0) as f64 / 1000.0);
    println!("  P99 latency: {:.2}ms", results.percentile_latency_us(99.0) as f64 / 1000.0);
    println!("  P99.9 latency: {:.2}ms", results.percentile_latency_us(99.9) as f64 / 1000.0);
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Command::new("KyroDB gRPC Benchmark")
        .version("1.0")
        .about("High-performance gRPC benchmark for KyroDB")
        .arg(Arg::new("grpc-addr")
            .long("grpc-addr")
            .value_name("ADDR")
            .default_value("http://127.0.0.1:50051")
            .help("gRPC server address"))
        .arg(Arg::new("num-keys")
            .long("num-keys")
            .value_name("N")
            .default_value("100000")
            .help("Number of keys to populate and test"))
        .arg(Arg::new("value-size")
            .long("value-size")
            .value_name("BYTES")
            .default_value("1024")
            .help("Size of values in bytes"))
        .arg(Arg::new("concurrency")
            .long("concurrency")
            .value_name("N")
            .default_value("32")
            .help("Number of concurrent workers"))
        .arg(Arg::new("duration")
            .long("duration")
            .value_name("SECS")
            .default_value("30")
            .help("Duration of read benchmark in seconds"))
        .arg(Arg::new("distribution")
            .long("distribution")
            .value_name("TYPE")
            .default_value("uniform")
            .help("Access pattern: uniform, zipf"))
        .arg(Arg::new("zipf-theta")
            .long("zipf-theta")
            .value_name("THETA")
            .default_value("0.99")
            .help("Zipf distribution parameter (0.5-2.0)"))
        .arg(Arg::new("batch-size")
            .long("batch-size")
            .value_name("N")
            .default_value("1000")
            .help("Batch size for data population"))
        .arg(Arg::new("auth-token")
            .long("auth-token")
            .value_name("TOKEN")
            .help("Authentication token"))
        .arg(Arg::new("skip-populate")
            .long("skip-populate")
            .action(clap::ArgAction::SetTrue)
            .help("Skip data population phase"))
        .get_matches();

    let grpc_addr = matches.get_one::<String>("grpc-addr").unwrap();
    let num_keys: u64 = matches.get_one::<String>("num-keys").unwrap().parse()?;
    let value_size: usize = matches.get_one::<String>("value-size").unwrap().parse()?;
    let concurrency: u32 = matches.get_one::<String>("concurrency").unwrap().parse()?;
    let duration: u64 = matches.get_one::<String>("duration").unwrap().parse()?;
    let distribution = matches.get_one::<String>("distribution").unwrap().clone();
    let zipf_theta: f64 = matches.get_one::<String>("zipf-theta").unwrap().parse()?;
    let batch_size: usize = matches.get_one::<String>("batch-size").unwrap().parse()?;
    let auth_token = matches.get_one::<String>("auth-token");
    let skip_populate = matches.get_flag("skip-populate");

    println!("🚀 KyroDB gRPC Benchmark");
    println!("  Server: {}", grpc_addr);
    println!("  Keys: {}", num_keys);
    println!("  Value size: {}B", value_size);
    println!("  Concurrency: {}", concurrency);
    println!("  Duration: {}s", duration);
    println!("  Distribution: {}", distribution);
    if distribution == "zipf" {
        println!("  Zipf θ: {}", zipf_theta);
    }

    // Test connection
    println!("\n🔗 Testing connection...");
    let mut test_client = GrpcBenchClient::connect(
        grpc_addr.clone(), 
        auth_token.map(|s| s.to_string())
    ).await?;
    let health_status = test_client.health_check().await?;
    println!("✅ Server healthy: {}", health_status);

    // Population phase
    if !skip_populate {
        populate_data(grpc_addr, auth_token.map(|s| s.as_str()), num_keys, value_size, batch_size).await?;
    } else {
        println!("⏭️  Skipping data population");
    }

    // Benchmark phase
    println!("\n🏁 Starting benchmark...");
    let results = benchmark_reads(
        grpc_addr,
        auth_token.map(|s| s.as_str()),
        duration,
        concurrency,
        num_keys,
        distribution.clone(),
        zipf_theta,
        42, // Fixed seed for reproducibility
    ).await?;

    print_results(&results, "gRPC Read Benchmark");

    println!("\n🎯 Benchmark completed successfully!");
    Ok(())
}
