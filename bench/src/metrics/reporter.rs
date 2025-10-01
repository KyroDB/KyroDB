use super::BenchmarkResults;
use std::fmt;

/// Results reporter for formatting benchmark output
pub struct ResultsReporter;

impl ResultsReporter {
    pub fn print_summary(results: &BenchmarkResults) {
        println!("\n═══════════════════════════════════════════════════════");
        println!("  📊 Benchmark Results: {}", results.workload);
        println!("═══════════════════════════════════════════════════════");
        println!();

        println!("⏱️  Duration: {:.2}s", results.duration.as_secs_f64());
        println!("📈 Total Operations: {}", results.total_ops);
        println!(
            "✅ Successful: {} ({:.2}%)",
            results.successful_ops,
            (results.successful_ops as f64 / results.total_ops as f64) * 100.0
        );
        println!("❌ Failed: {}", results.failed_ops);
        println!();

        println!("🚀 Throughput: {:.0} ops/sec", results.throughput_ops_sec);
        println!();

        println!("📊 Latency Distribution:");
        println!("   Min:    {:>10} μs", results.latency.min_us);
        println!("   Mean:   {:>10.0} μs", results.latency.mean_us);
        println!("   P50:    {:>10} μs", results.latency.p50_us);
        println!("   P95:    {:>10} μs", results.latency.p95_us);
        println!("   P99:    {:>10} μs", results.latency.p99_us);
        println!("   P99.9:  {:>10} μs", results.latency.p999_us);
        println!("   Max:    {:>10} μs", results.latency.max_us);
        println!();

        if let Some(ref system) = results.system {
            println!("💻 System Metrics:");
            println!("   Memory:      {} MB", system.memory_mb);
            println!("   CPU:         {:.1}%", system.cpu_percent);
            println!("   Disk Reads:  {} MB", system.disk_reads_mb);
            println!("   Disk Writes: {} MB", system.disk_writes_mb);
            println!();
        }

        println!("═══════════════════════════════════════════════════════");
    }

    pub fn print_comparison(rmi_results: &BenchmarkResults, btree_results: &BenchmarkResults) {
        println!("\n═══════════════════════════════════════════════════════");
        println!("  ⚖️  RMI vs BTree Comparison");
        println!("═══════════════════════════════════════════════════════");
        println!();

        let rmi_throughput = rmi_results.throughput_ops_sec;
        let btree_throughput = btree_results.throughput_ops_sec;
        let throughput_advantage = (rmi_throughput / btree_throughput - 1.0) * 100.0;

        println!("🚀 Throughput:");
        println!("   RMI:     {:.0} ops/sec", rmi_throughput);
        println!("   BTree:   {:.0} ops/sec", btree_throughput);
        println!("   Advantage: {:.1}% faster", throughput_advantage);
        println!();

        let rmi_p99 = rmi_results.latency.p99_us;
        let btree_p99 = btree_results.latency.p99_us;
        let latency_advantage = (1.0 - rmi_p99 as f64 / btree_p99 as f64) * 100.0;

        println!("⚡ P99 Latency:");
        println!("   RMI:     {} μs", rmi_p99);
        println!("   BTree:   {} μs", btree_p99);
        println!("   Advantage: {:.1}% lower", latency_advantage);
        println!();

        println!("═══════════════════════════════════════════════════════");
    }

    pub fn print_csv_header() {
        println!("phase,workload,duration_secs,total_ops,successful_ops,failed_ops,throughput_ops_sec,min_us,mean_us,p50_us,p95_us,p99_us,p999_us,max_us");
    }

    pub fn print_csv_row(results: &BenchmarkResults) {
        println!(
            "{:?},{},{:.2},{},{},{},{:.0},{},{:.0},{},{},{},{},{}",
            results.phase,
            results.workload,
            results.duration.as_secs_f64(),
            results.total_ops,
            results.successful_ops,
            results.failed_ops,
            results.throughput_ops_sec,
            results.latency.min_us,
            results.latency.mean_us,
            results.latency.p50_us,
            results.latency.p95_us,
            results.latency.p99_us,
            results.latency.p999_us,
            results.latency.max_us,
        );
    }
}

impl fmt::Display for BenchmarkResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} - {:.0} ops/sec, P99: {} μs",
            self.workload, self.throughput_ops_sec, self.latency.p99_us
        )
    }
}
