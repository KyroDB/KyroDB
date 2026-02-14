# Benchmark Evidence Package

This document defines the minimum artifact set required to make a performance claim for KyroDB ANN runs.

## Scope

Use this package for **ANN harness claims** (recall/throughput/latency/build/memory trade-offs).  
Do not mix these numbers with service-level end-to-end SLO claims.

## Required Artifacts

For each benchmark campaign, include:

1. Raw outputs
   - `target/ann_inproc/<run_id>/raw/*.json` (or `target/ann_inproc_parallel/<run_id>/shards/*/raw/*.json`)
2. Run manifest
   - `target/ann_inproc/<run_id>/manifest.txt` (or parallel run manifest)
3. Aggregated summary
   - `.../summary/inproc_summary.json`
   - `.../summary/inproc_summary.md`
   - `.../summary/inproc_candidates.csv`
4. Plots
   - `.../plots/*.png`
   - `.../plots/index.md`
5. Logs
   - harness logs and shard logs used to diagnose failures/timeouts

## Reproducibility Checklist

Before publishing any result:

1. Pin code revision (`git rev-parse HEAD`) and include it in the package.
2. Record machine type, CPU topology, memory, OS/kernel, and Rust toolchain.
3. Record dataset names, metric, `k`, `m`, `ef_construction`, `ef_search`, repetitions, and query caps.
4. Keep harness assumptions aligned with the intended evaluation setup.
5. Re-run at least once and include variance (mean/std or repeated run comparison).

## Suggested Packaging

```bash
tar -czf kyrodb_bench_<run_id>.tar.gz \
  target/ann_inproc/<run_id>/manifest.txt \
  target/ann_inproc/<run_id>/raw \
  target/ann_inproc/<run_id>/summary \
  target/ann_inproc/<run_id>/plots
sha256sum kyrodb_bench_<run_id>.tar.gz > kyrodb_bench_<run_id>.tar.gz.sha256
```

## Publication Rule

Any external claim (docs, PR notes, presentations) must reference a specific evidence package and run id.
