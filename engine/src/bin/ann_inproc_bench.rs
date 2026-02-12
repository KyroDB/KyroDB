use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use kyrodb_engine::config::{AnnSearchMode, DistanceMetric};
use kyrodb_engine::hnsw_index::{HnswVectorIndex, SearchResult};
use serde::Serialize;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

const ANN_BIN_MAGIC: &[u8; 8] = b"KYROANN1";

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DistanceArg {
    Cosine,
    Euclidean,
    InnerProduct,
}

impl From<DistanceArg> for DistanceMetric {
    fn from(value: DistanceArg) -> Self {
        match value {
            DistanceArg::Cosine => DistanceMetric::Cosine,
            DistanceArg::Euclidean => DistanceMetric::Euclidean,
            DistanceArg::InnerProduct => DistanceMetric::InnerProduct,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum AnnSearchModeArg {
    Fp32Strict,
    Sq8Rerank,
}

impl From<AnnSearchModeArg> for AnnSearchMode {
    fn from(value: AnnSearchModeArg) -> Self {
        match value {
            AnnSearchModeArg::Fp32Strict => AnnSearchMode::Fp32Strict,
            AnnSearchModeArg::Sq8Rerank => AnnSearchMode::Sq8Rerank,
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "ann_inproc_bench",
    about = "True in-process ANN benchmark for KyroDB core index (no gRPC in measured path)."
)]
struct Args {
    #[arg(long, value_name = "FILE")]
    dataset_annbin: PathBuf,

    #[arg(long, value_name = "NAME")]
    dataset_name: Option<String>,

    #[arg(long, value_enum)]
    distance: Option<DistanceArg>,

    #[arg(long, default_value_t = 10)]
    k: usize,

    #[arg(long, default_value_t = 16)]
    m: usize,

    #[arg(long = "ef-construction", default_value_t = 200)]
    ef_construction: usize,

    #[arg(
        long = "ef-search",
        value_delimiter = ',',
        num_args = 1..,
        default_values_t = vec![16usize, 32, 64, 128, 256, 512, 768, 1024]
    )]
    ef_search: Vec<usize>,

    #[arg(long, default_value_t = 3)]
    repetitions: usize,

    #[arg(long, default_value_t = 200)]
    warmup_queries: usize,

    #[arg(long, default_value_t = 0)]
    max_train: usize,

    #[arg(long, default_value_t = 0)]
    max_queries: usize,

    #[arg(long, default_value_t = false)]
    disable_normalization_check: bool,

    #[arg(long, value_enum, default_value_t = AnnSearchModeArg::Fp32Strict)]
    ann_search_mode: AnnSearchModeArg,

    #[arg(long, default_value_t = 8)]
    quantized_rerank_multiplier: usize,

    #[arg(long, value_name = "FILE")]
    output_json: Option<PathBuf>,
}

#[derive(Debug)]
struct AnnDataset {
    train_rows: usize,
    test_rows: usize,
    dimension: usize,
    neighbors_cols: usize,
    train_flat: Vec<f32>,
    test_flat: Vec<f32>,
    neighbors_flat: Vec<u32>,
}

#[derive(Debug, Serialize)]
struct BenchmarkOutput {
    dataset: DatasetSummary,
    config: RunConfig,
    index_build: BuildMetrics,
    sweeps: Vec<SweepResult>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DatasetSummary {
    name: String,
    source_annbin: String,
    train_size: usize,
    query_size: usize,
    dimension: usize,
    truncated_train: bool,
    truncated_queries: bool,
}

#[derive(Debug, Serialize)]
struct RunConfig {
    ann_backend: String,
    ann_search_mode: String,
    quantized_rerank_multiplier: usize,
    distance: String,
    k: usize,
    m: usize,
    ef_construction: usize,
    ef_search: Vec<usize>,
    repetitions: usize,
    warmup_queries: usize,
    disable_normalization_check: bool,
}

#[derive(Debug, Serialize)]
struct BuildMetrics {
    elapsed_ms: f64,
    vectors_per_second: f64,
    indexed_vectors: usize,
    index_estimated_memory_bytes: usize,
}

#[derive(Debug, Serialize)]
struct SweepResult {
    ef_search: usize,
    repetitions: Vec<RepetitionResult>,
    aggregate: AggregateResult,
}

#[derive(Debug, Serialize)]
struct RepetitionResult {
    recall_at_k: f64,
    /// Search-only QPS (excludes recall/evaluation computation).
    qps_search: f64,
    /// End-to-end QPS for benchmark repetition (includes recall/evaluation).
    qps_end_to_end: f64,
    /// Backward-compatible alias for search-only QPS.
    qps: f64,
    p50_latency_ms: f64,
    p95_latency_ms: f64,
    p99_latency_ms: f64,
    measured_queries: usize,
    filtered_ground_truth_queries: usize,
    search_elapsed_ms: f64,
    evaluation_elapsed_ms: f64,
}

#[derive(Debug, Serialize)]
struct AggregateResult {
    recall_mean: f64,
    recall_std: f64,
    /// Search-only QPS aggregate (kept as canonical `qps_*` for compatibility).
    qps_mean: f64,
    qps_std: f64,
    end_to_end_qps_mean: f64,
    end_to_end_qps_std: f64,
    p50_latency_ms_mean: f64,
    p95_latency_ms_mean: f64,
    p99_latency_ms_mean: f64,
    search_elapsed_ms_mean: f64,
    evaluation_elapsed_ms_mean: f64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.k == 0 {
        anyhow::bail!("--k must be > 0");
    }
    if args.repetitions == 0 {
        anyhow::bail!("--repetitions must be > 0");
    }

    let dataset_name = resolve_dataset_name(&args);
    let distance = resolve_distance(&args, &dataset_name)?;

    let mut dataset = load_ann_dataset(&args.dataset_annbin)?;
    if dataset.train_rows == 0 {
        anyhow::bail!("dataset train split is empty");
    }
    if dataset.test_rows == 0 {
        anyhow::bail!("dataset test split is empty");
    }

    let original_train = dataset.train_rows;
    let original_queries = dataset.test_rows;

    let mut notes = Vec::new();

    if args.max_train > 0 && args.max_train < dataset.train_rows {
        dataset.truncate_train(args.max_train)?;
        notes.push(format!(
            "train set truncated from {} to {} rows",
            original_train, dataset.train_rows
        ));
    }

    if args.max_queries > 0 && args.max_queries < dataset.test_rows {
        dataset.truncate_queries(args.max_queries)?;
        notes.push(format!(
            "query set truncated from {} to {} rows",
            original_queries, dataset.test_rows
        ));
    }

    if matches!(
        distance,
        DistanceMetric::Cosine | DistanceMetric::InnerProduct
    ) {
        normalize_rows_in_place(
            &mut dataset.train_flat,
            dataset.train_rows,
            dataset.dimension,
        );
        normalize_rows_in_place(&mut dataset.test_flat, dataset.test_rows, dataset.dimension);
    }

    eprintln!(
        "inproc config: dataset={} distance={:?} mode={:?} rerank_mult={} train={} queries={} dim={} k={} m={} ef_construction={} ef_search={:?}",
        dataset_name,
        distance,
        args.ann_search_mode,
        args.quantized_rerank_multiplier,
        dataset.train_rows,
        dataset.test_rows,
        dataset.dimension,
        args.k,
        args.m,
        args.ef_construction,
        args.ef_search
    );

    let ann_search_mode: AnnSearchMode = args.ann_search_mode.into();
    let mut index = HnswVectorIndex::new_with_params_and_search_mode(
        dataset.dimension,
        dataset.train_rows,
        distance,
        args.m,
        args.ef_construction,
        args.disable_normalization_check,
        ann_search_mode,
        args.quantized_rerank_multiplier,
    )
    .context("failed to create HNSW index")?;

    let batch = dataset.build_insert_batch()?;

    let build_start = Instant::now();
    index
        .parallel_insert_batch(&batch)
        .context("parallel index build failed")?;
    let build_elapsed = build_start.elapsed().as_secs_f64();

    let build_metrics = BuildMetrics {
        elapsed_ms: build_elapsed * 1000.0,
        vectors_per_second: if build_elapsed > 0.0 {
            dataset.train_rows as f64 / build_elapsed
        } else {
            0.0
        },
        indexed_vectors: dataset.train_rows,
        index_estimated_memory_bytes: index.estimate_memory_bytes(),
    };

    println!("ef_search,recall_mean,recall_std,qps_mean,qps_std,p50_ms,p95_ms,p99_ms");

    let mut sweeps = Vec::with_capacity(args.ef_search.len());
    for &ef in &args.ef_search {
        let sweep = run_sweep(
            &index,
            &dataset,
            args.k,
            ef,
            args.repetitions,
            args.warmup_queries,
        )?;

        println!(
            "{},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{:.3}",
            ef,
            sweep.aggregate.recall_mean,
            sweep.aggregate.recall_std,
            sweep.aggregate.qps_mean,
            sweep.aggregate.qps_std,
            sweep.aggregate.p50_latency_ms_mean,
            sweep.aggregate.p95_latency_ms_mean,
            sweep.aggregate.p99_latency_ms_mean
        );

        sweeps.push(sweep);
    }

    let output = BenchmarkOutput {
        dataset: DatasetSummary {
            name: dataset_name,
            source_annbin: args.dataset_annbin.display().to_string(),
            train_size: dataset.train_rows,
            query_size: dataset.test_rows,
            dimension: dataset.dimension,
            truncated_train: dataset.train_rows != original_train,
            truncated_queries: dataset.test_rows != original_queries,
        },
        config: RunConfig {
            ann_backend: index.backend_name().to_string(),
            ann_search_mode: format!("{:?}", ann_search_mode).to_lowercase(),
            quantized_rerank_multiplier: args.quantized_rerank_multiplier,
            distance: format!("{:?}", distance).to_lowercase(),
            k: args.k,
            m: args.m,
            ef_construction: args.ef_construction,
            ef_search: args.ef_search,
            repetitions: args.repetitions,
            warmup_queries: args.warmup_queries,
            disable_normalization_check: args.disable_normalization_check,
        },
        index_build: build_metrics,
        sweeps,
        notes,
    };

    if let Some(path) = args.output_json {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create output dir {}", parent.display()))?;
        }
        let payload = serde_json::to_string_pretty(&output)?;
        std::fs::write(&path, payload)
            .with_context(|| format!("failed to write output json {}", path.display()))?;
        eprintln!("wrote output: {}", path.display());
    }

    Ok(())
}

impl AnnDataset {
    fn train_row(&self, idx: usize) -> &[f32] {
        let start = idx * self.dimension;
        &self.train_flat[start..start + self.dimension]
    }

    fn test_row(&self, idx: usize) -> &[f32] {
        let start = idx * self.dimension;
        &self.test_flat[start..start + self.dimension]
    }

    fn neighbors_row(&self, idx: usize) -> &[u32] {
        let start = idx * self.neighbors_cols;
        &self.neighbors_flat[start..start + self.neighbors_cols]
    }

    fn truncate_train(&mut self, new_rows: usize) -> Result<()> {
        if new_rows == 0 || new_rows > self.train_rows {
            anyhow::bail!(
                "invalid train truncation: new_rows={} train_rows={}",
                new_rows,
                self.train_rows
            );
        }
        let keep = new_rows
            .checked_mul(self.dimension)
            .ok_or_else(|| anyhow::anyhow!("train truncation overflow"))?;
        self.train_flat.truncate(keep);
        self.train_rows = new_rows;
        Ok(())
    }

    fn truncate_queries(&mut self, new_rows: usize) -> Result<()> {
        if new_rows == 0 || new_rows > self.test_rows {
            anyhow::bail!(
                "invalid query truncation: new_rows={} test_rows={}",
                new_rows,
                self.test_rows
            );
        }

        let keep_test = new_rows
            .checked_mul(self.dimension)
            .ok_or_else(|| anyhow::anyhow!("query truncation overflow"))?;
        self.test_flat.truncate(keep_test);

        let keep_neighbors = new_rows
            .checked_mul(self.neighbors_cols)
            .ok_or_else(|| anyhow::anyhow!("neighbors truncation overflow"))?;
        self.neighbors_flat.truncate(keep_neighbors);

        self.test_rows = new_rows;
        Ok(())
    }

    fn build_insert_batch(&self) -> Result<Vec<(&[f32], usize)>> {
        let mut batch = Vec::with_capacity(self.train_rows);
        for idx in 0..self.train_rows {
            batch.push((self.train_row(idx), idx));
        }
        Ok(batch)
    }
}

fn resolve_dataset_name(args: &Args) -> String {
    if let Some(name) = &args.dataset_name {
        return name.clone();
    }
    args.dataset_annbin
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown_dataset".to_string())
}

fn resolve_distance(args: &Args, dataset_name: &str) -> Result<DistanceMetric> {
    if let Some(distance) = args.distance {
        return Ok(distance.into());
    }

    let lower = dataset_name.to_ascii_lowercase();
    if lower.contains("angular") || lower.contains("cosine") {
        return Ok(DistanceMetric::Cosine);
    }
    if lower.contains("euclidean") || lower.contains("l2") {
        return Ok(DistanceMetric::Euclidean);
    }
    if lower.contains("inner") {
        return Ok(DistanceMetric::InnerProduct);
    }

    anyhow::bail!(
        "Unable to infer distance metric from dataset name '{}'. Pass --distance explicitly.",
        dataset_name
    )
}

fn load_ann_dataset(path: &Path) -> Result<AnnDataset> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);

    let mut magic = [0u8; 8];
    reader
        .read_exact(&mut magic)
        .context("failed to read annbin magic")?;
    if &magic != ANN_BIN_MAGIC {
        anyhow::bail!(
            "invalid annbin magic: expected {:?}, got {:?}",
            ANN_BIN_MAGIC,
            magic
        );
    }

    let train_rows = read_u64_as_usize(&mut reader, "train_rows")?;
    let test_rows = read_u64_as_usize(&mut reader, "test_rows")?;
    let dimension = read_u64_as_usize(&mut reader, "dimension")?;
    let neighbors_cols = read_u64_as_usize(&mut reader, "neighbors_cols")?;

    if train_rows == 0 || test_rows == 0 || dimension == 0 || neighbors_cols == 0 {
        anyhow::bail!(
            "invalid annbin header: train_rows={} test_rows={} dimension={} neighbors_cols={}",
            train_rows,
            test_rows,
            dimension,
            neighbors_cols
        );
    }

    let train_flat_count = checked_mul(train_rows, dimension, "train size")?;
    let test_flat_count = checked_mul(test_rows, dimension, "test size")?;
    let neighbors_flat_count = checked_mul(test_rows, neighbors_cols, "neighbors size")?;

    let train_flat = read_f32_array(&mut reader, train_flat_count)?;
    let test_flat = read_f32_array(&mut reader, test_flat_count)?;
    let neighbors_flat = read_u32_array(&mut reader, neighbors_flat_count)?;

    Ok(AnnDataset {
        train_rows,
        test_rows,
        dimension,
        neighbors_cols,
        train_flat,
        test_flat,
        neighbors_flat,
    })
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u64_as_usize(reader: &mut impl Read, field: &str) -> Result<usize> {
    let value = read_u64(reader)?;
    usize::try_from(value)
        .with_context(|| format!("annbin header field '{}' overflows usize: {}", field, value))
}

fn read_f32_array(reader: &mut impl Read, count: usize) -> Result<Vec<f32>> {
    let byte_len = checked_mul(count, 4, "f32 byte length")?;
    let mut bytes = vec![0u8; byte_len];
    reader
        .read_exact(&mut bytes)
        .context("failed to read f32 payload")?;

    let mut out = Vec::with_capacity(count);
    for chunk in bytes.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok(out)
}

fn read_u32_array(reader: &mut impl Read, count: usize) -> Result<Vec<u32>> {
    let byte_len = checked_mul(count, 4, "u32 byte length")?;
    let mut bytes = vec![0u8; byte_len];
    reader
        .read_exact(&mut bytes)
        .context("failed to read u32 payload")?;

    let mut out = Vec::with_capacity(count);
    for chunk in bytes.chunks_exact(4) {
        out.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok(out)
}

fn checked_mul(a: usize, b: usize, label: &str) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| anyhow::anyhow!("overflow computing {}: {} * {}", label, a, b))
}

fn normalize_rows_in_place(flat: &mut [f32], rows: usize, dim: usize) {
    for row in 0..rows {
        let start = row * dim;
        let end = start + dim;
        let slice = &mut flat[start..end];
        let norm_sq: f32 = slice.iter().map(|x| x * x).sum();
        let inv = 1.0f32 / norm_sq.sqrt().max(1e-12);
        for x in slice {
            *x *= inv;
        }
    }
}

fn run_sweep(
    index: &HnswVectorIndex,
    dataset: &AnnDataset,
    k: usize,
    ef_search: usize,
    repetitions: usize,
    warmup_queries: usize,
) -> Result<SweepResult> {
    let warmup_count = warmup_queries.min(dataset.test_rows);
    for q_idx in 0..warmup_count {
        let _ = index.knn_search_with_ef(dataset.test_row(q_idx), k, Some(ef_search))?;
    }

    let mut repetition_results = Vec::with_capacity(repetitions);

    for _ in 0..repetitions {
        let mut latencies_ns = Vec::with_capacity(dataset.test_rows);
        let mut query_results = Vec::with_capacity(dataset.test_rows);
        let search_start = Instant::now();

        for q_idx in 0..dataset.test_rows {
            let query = dataset.test_row(q_idx);
            let start = Instant::now();
            let results = index.knn_search_with_ef(query, k, Some(ef_search))?;
            latencies_ns.push(start.elapsed().as_nanos() as u64);
            query_results.push(results);
        }
        let search_elapsed = search_start.elapsed();

        let evaluation_start = Instant::now();
        let mut recall_sum = 0.0;
        let mut recall_count = 0usize;
        let mut filtered_ground_truth_queries = 0usize;

        for (q_idx, results) in query_results.iter().enumerate() {
            let gt_row = dataset.neighbors_row(q_idx);
            match recall_at_k(results, gt_row, k, dataset.train_rows) {
                Some(r) => {
                    recall_sum += r;
                    recall_count += 1;
                }
                None => {
                    filtered_ground_truth_queries += 1;
                }
            }
        }
        let evaluation_elapsed = evaluation_start.elapsed();

        let search_elapsed_secs = search_elapsed.as_secs_f64();
        let end_to_end_elapsed_secs = search_elapsed_secs + evaluation_elapsed.as_secs_f64();
        let measured = dataset.test_rows;
        let qps_search = if search_elapsed_secs > 0.0 {
            measured as f64 / search_elapsed_secs
        } else {
            0.0
        };
        let qps_end_to_end = if end_to_end_elapsed_secs > 0.0 {
            measured as f64 / end_to_end_elapsed_secs
        } else {
            0.0
        };

        let recall = if recall_count > 0 {
            recall_sum / recall_count as f64
        } else {
            0.0
        };

        let (p50, p95, p99) = latency_percentiles_ms(&latencies_ns);

        repetition_results.push(RepetitionResult {
            recall_at_k: recall,
            qps_search,
            qps_end_to_end,
            qps: qps_search,
            p50_latency_ms: p50,
            p95_latency_ms: p95,
            p99_latency_ms: p99,
            measured_queries: measured,
            filtered_ground_truth_queries,
            search_elapsed_ms: search_elapsed_secs * 1000.0,
            evaluation_elapsed_ms: evaluation_elapsed.as_secs_f64() * 1000.0,
        });
    }

    let recall_values: Vec<f64> = repetition_results.iter().map(|r| r.recall_at_k).collect();
    let qps_values: Vec<f64> = repetition_results.iter().map(|r| r.qps_search).collect();
    let end_to_end_qps_values: Vec<f64> = repetition_results
        .iter()
        .map(|r| r.qps_end_to_end)
        .collect();
    let p50_values: Vec<f64> = repetition_results
        .iter()
        .map(|r| r.p50_latency_ms)
        .collect();
    let p95_values: Vec<f64> = repetition_results
        .iter()
        .map(|r| r.p95_latency_ms)
        .collect();
    let p99_values: Vec<f64> = repetition_results
        .iter()
        .map(|r| r.p99_latency_ms)
        .collect();
    let search_elapsed_values: Vec<f64> = repetition_results
        .iter()
        .map(|r| r.search_elapsed_ms)
        .collect();
    let evaluation_elapsed_values: Vec<f64> = repetition_results
        .iter()
        .map(|r| r.evaluation_elapsed_ms)
        .collect();

    let aggregate = AggregateResult {
        recall_mean: mean(&recall_values),
        recall_std: stddev(&recall_values),
        qps_mean: mean(&qps_values),
        qps_std: stddev(&qps_values),
        end_to_end_qps_mean: mean(&end_to_end_qps_values),
        end_to_end_qps_std: stddev(&end_to_end_qps_values),
        p50_latency_ms_mean: mean(&p50_values),
        p95_latency_ms_mean: mean(&p95_values),
        p99_latency_ms_mean: mean(&p99_values),
        search_elapsed_ms_mean: mean(&search_elapsed_values),
        evaluation_elapsed_ms_mean: mean(&evaluation_elapsed_values),
    };

    Ok(SweepResult {
        ef_search,
        repetitions: repetition_results,
        aggregate,
    })
}

fn recall_at_k(
    results: &[SearchResult],
    ground_truth: &[u32],
    k: usize,
    max_train_rows: usize,
) -> Option<f64> {
    let mut gt: Vec<u32> = Vec::with_capacity(k);
    for &id in ground_truth {
        if (id as usize) < max_train_rows {
            gt.push(id);
            if gt.len() == k {
                break;
            }
        }
    }

    if gt.is_empty() {
        return None;
    }

    gt.sort_unstable();

    let mut hits = 0usize;
    for result in results.iter().take(k) {
        let id_u32 = match u32::try_from(result.doc_id) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if gt.binary_search(&id_u32).is_ok() {
            hits += 1;
        }
    }

    Some(hits as f64 / gt.len() as f64)
}

fn latency_percentiles_ms(samples_ns: &[u64]) -> (f64, f64, f64) {
    if samples_ns.is_empty() {
        return (0.0, 0.0, 0.0);
    }

    let mut sorted = samples_ns.to_vec();
    sorted.sort_unstable();

    (
        percentile_ns_to_ms(&sorted, 0.50),
        percentile_ns_to_ms(&sorted, 0.95),
        percentile_ns_to_ms(&sorted, 0.99),
    )
}

fn percentile_ns_to_ms(sorted_ns: &[u64], q: f64) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ns.len() - 1) as f64 * q).round() as usize;
    sorted_ns[idx] as f64 / 1_000_000.0
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn stddev(values: &[f64]) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }
    let m = mean(values);
    let variance = values
        .iter()
        .map(|v| {
            let d = *v - m;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny_dataset() -> AnnDataset {
        // 4 training vectors in 2D laid on axes/diagonals.
        let train_flat = vec![
            1.0, 0.0, // id 0
            0.0, 1.0, // id 1
            1.0, 1.0, // id 2
            -1.0, 0.0, // id 3
        ];

        // Two queries near id 0 and id 1.
        let test_flat = vec![
            0.9, 0.1, // q0
            0.1, 0.9, // q1
        ];

        // Ground truth top-2 IDs for each query.
        let neighbors_flat = vec![
            0, 2, // q0
            1, 2, // q1
        ];

        AnnDataset {
            train_rows: 4,
            test_rows: 2,
            dimension: 2,
            neighbors_cols: 2,
            train_flat,
            test_flat,
            neighbors_flat,
        }
    }

    #[test]
    fn run_sweep_reports_search_and_end_to_end_qps() {
        let dataset = tiny_dataset();
        let mut index =
            HnswVectorIndex::new_with_params(2, 4, DistanceMetric::Euclidean, 8, 32, false)
                .expect("index init");
        let batch = dataset.build_insert_batch().expect("batch build");
        index
            .parallel_insert_batch(&batch)
            .expect("parallel insert should succeed");

        let sweep = run_sweep(&index, &dataset, 2, 32, 1, 0).expect("sweep");
        assert_eq!(sweep.repetitions.len(), 1);
        let rep = &sweep.repetitions[0];

        assert!(rep.qps_search > 0.0, "search qps should be > 0");
        assert!(rep.qps_end_to_end > 0.0, "end-to-end qps should be > 0");
        assert!(
            rep.qps_search + 1e-9 >= rep.qps_end_to_end,
            "search-only qps should be >= end-to-end qps"
        );
        assert!(
            rep.evaluation_elapsed_ms >= 0.0,
            "evaluation timing should be non-negative"
        );
        assert_eq!(rep.qps, rep.qps_search, "compatibility qps alias mismatch");

        assert_eq!(sweep.aggregate.qps_mean, rep.qps_search);
        assert_eq!(sweep.aggregate.end_to_end_qps_mean, rep.qps_end_to_end);
    }
}
