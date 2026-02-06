use anyhow::Result;
use clap::{Parser, ValueEnum};
use kyrodb_engine::config::DistanceMetric;
use kyrodb_engine::hnsw_index::HnswVectorIndex;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, ValueEnum)]
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

#[derive(Parser, Debug)]
#[command(name = "hnsw_experiment")]
#[command(about = "Local HNSW experiment: sweep params and report QPS/latency/recall", long_about = None)]
struct Args {
    #[arg(long, value_enum, default_value = "euclidean")]
    distance: DistanceArg,

    #[arg(long, default_value_t = 128)]
    dim: usize,

    #[arg(long, default_value_t = 100_000)]
    n: usize,

    #[arg(long, default_value_t = 500)]
    queries: usize,

    #[arg(long, default_value_t = 10)]
    k: usize,

    #[arg(long, default_value_t = 16)]
    m: usize,

    #[arg(long = "ef-construction", default_value_t = 200)]
    ef_construction: usize,

    #[arg(long = "ef-search")]
    ef_search: Vec<usize>,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    #[arg(long, default_value_t = false)]
    disable_normalization_check: bool,

    #[arg(long, default_value_t = false)]
    exact_recall: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.dim == 0 {
        anyhow::bail!("--dim must be > 0");
    }
    if args.n == 0 {
        anyhow::bail!("--n must be > 0");
    }
    if args.queries == 0 {
        anyhow::bail!("--queries must be > 0");
    }
    if args.k == 0 {
        anyhow::bail!("--k must be > 0");
    }
    if args.k > args.n {
        anyhow::bail!("--k must be <= --n (got k={}, n={})", args.k, args.n);
    }

    let distance_metric: DistanceMetric = args.distance.into();
    let ef_search_list = if args.ef_search.is_empty() {
        vec![10, 50, 100, 200, 400, 800]
    } else {
        args.ef_search.clone()
    };

    eprintln!(
        "config: distance={:?} dim={} n={} queries={} k={} m={} ef_construction={} disable_norm_check={} exact_recall={} ef_search={:?}",
        distance_metric,
        args.dim,
        args.n,
        args.queries,
        args.k,
        args.m,
        args.ef_construction,
        args.disable_normalization_check,
        args.exact_recall,
        ef_search_list
    );

    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    // Generate dataset
    let embeddings = generate_vectors(&mut rng, args.n, args.dim, distance_metric);
    let queries = generate_vectors(&mut rng, args.queries, args.dim, distance_metric);

    // Build index
    let mut index = HnswVectorIndex::new_with_params(
        args.dim,
        args.n,
        distance_metric,
        args.m,
        args.ef_construction,
        args.disable_normalization_check,
    )?;

    let build_start = Instant::now();
    for (id, v) in embeddings.iter().enumerate() {
        index.add_vector(id as u64, v)?;
    }
    let build_elapsed = build_start.elapsed();

    eprintln!(
        "build: total_ms={} per_vec_ns={}",
        build_elapsed.as_millis(),
        (build_elapsed.as_nanos() / args.n as u128)
    );

    // Warm
    for q in queries.iter().take(100.min(queries.len())) {
        let _ = index.knn_search_with_ef(q, args.k, Some(args.k.max(50)))?;
    }

    // Compute exact results once (optional)
    let exact_topk = if args.exact_recall {
        Some(compute_exact_topk(
            &embeddings,
            &queries,
            args.k,
            distance_metric,
        ))
    } else {
        None
    };

    println!(
        "distance,dim,n,queries,k,m,ef_construction,disable_norm_check,ef_search,qps,p50_ms,p99_ms,recall"
    );

    for ef in ef_search_list {
        let mut durations: Vec<Duration> = Vec::with_capacity(queries.len());
        let mut total_hits: usize = 0;
        let mut total: usize = 0;

        let batch_start = Instant::now();
        for (q_idx, q) in queries.iter().enumerate() {
            let start = Instant::now();
            let results = index.knn_search_with_ef(q, args.k, Some(ef))?;
            durations.push(start.elapsed());

            if let (Some(ref exact), true) = (&exact_topk, args.exact_recall) {
                total += args.k;
                let exact_ids = &exact[q_idx];
                for id in results.iter().take(args.k).map(|r| r.doc_id) {
                    if exact_ids.contains(&id) {
                        total_hits += 1;
                    }
                }
            }
        }
        let batch_elapsed = batch_start.elapsed();
        let elapsed_secs = batch_elapsed.as_secs_f64();
        let qps = if queries.is_empty() || elapsed_secs == 0.0 {
            0.0
        } else {
            (queries.len() as f64) / elapsed_secs
        };
        let (p50_ms, p99_ms) = percentiles_ms(&mut durations);

        let recall = if args.exact_recall {
            (total_hits as f64) / (total as f64).max(1.0)
        } else {
            f64::NAN
        };

        println!(
            "{:?},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.6}",
            distance_metric,
            args.dim,
            args.n,
            args.queries,
            args.k,
            args.m,
            args.ef_construction,
            args.disable_normalization_check,
            ef,
            qps,
            p50_ms,
            p99_ms,
            recall
        );
    }

    Ok(())
}

fn generate_vectors(
    rng: &mut impl Rng,
    count: usize,
    dimension: usize,
    distance: DistanceMetric,
) -> Vec<Vec<f32>> {
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let mut v: Vec<f32> = (0..dimension)
            .map(|_| rng.gen_range(-1.0f32..1.0f32))
            .collect();
        if matches!(
            distance,
            DistanceMetric::Cosine | DistanceMetric::InnerProduct
        ) {
            l2_normalize_in_place(&mut v);
        }
        out.push(v);
    }
    out
}

fn l2_normalize_in_place(v: &mut [f32]) {
    let norm_sq: f32 = v.iter().map(|x| x * x).sum();
    let inv_norm = 1.0f32 / norm_sq.sqrt().max(1e-12);
    for x in v {
        *x *= inv_norm;
    }
}

fn compute_exact_topk(
    embeddings: &[Vec<f32>],
    queries: &[Vec<f32>],
    k: usize,
    distance: DistanceMetric,
) -> Vec<Vec<u64>> {
    let mut out: Vec<Vec<u64>> = Vec::with_capacity(queries.len());

    // Reuse a buffer to avoid reallocations per query.
    let mut scored: Vec<(u64, f32)> = Vec::with_capacity(embeddings.len());

    for q in queries {
        scored.clear();
        for (id, v) in embeddings.iter().enumerate() {
            let dist = match distance {
                DistanceMetric::Euclidean => l2_distance_sq(q, v),
                DistanceMetric::Cosine => cosine_distance(q, v),
                DistanceMetric::InnerProduct => inner_product_distance(q, v),
            };
            scored.push((id as u64, dist));
        }

        let k = k.min(scored.len());
        if k == 0 {
            out.push(Vec::new());
            continue;
        }

        // Avoid full sort: select the k-th element, then only sort the top-k slice.
        scored.select_nth_unstable_by(k - 1, |a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        let (topk, _rest) = scored.split_at_mut(k);
        topk.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        out.push(topk.iter().map(|(id, _)| *id).collect());
    }

    out
}

fn l2_distance_sq(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum()
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    // Assumes both vectors are normalized.
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    1.0 - dot
}

fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    // DistDot is implemented as a distance in hnsw_rs; for recall comparison here we use
    // -dot so that "smaller is better".
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    -dot
}

fn percentiles_ms(durations: &mut [Duration]) -> (f64, f64) {
    durations.sort();
    let n = durations.len();
    if n == 0 {
        return (f64::NAN, f64::NAN);
    }

    let p50 = durations[(n * 50) / 100];
    let p99 = durations[((n * 99) / 100).min(n - 1)];

    (p50.as_secs_f64() * 1e3, p99.as_secs_f64() * 1e3)
}

// Recall is computed incrementally in the main loop to avoid extra allocations.
