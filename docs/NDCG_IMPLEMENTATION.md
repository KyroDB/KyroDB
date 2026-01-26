# NDCG Quality Metrics Implementation

## Overview

NDCG (Normalized Discounted Cumulative Gain) measures ranking quality in KyroDB validation runs. It is currently used in the `validation_enterprise` binary, not on the production server hot path.

---

## Core Metrics

| Metric | Purpose | Range |
|--------|---------|-------|
| **NDCG@10** | Ranking quality of top-10 results | 0.0 to 1.0 |
| **MRR** | Mean Reciprocal Rank of first relevant result | 0.0 to 1.0 |
| **Recall@10** | Fraction of relevant docs in top-10 | 0.0 to 1.0 |

---

## API Reference

```rust
use kyrodb_engine::ndcg::{
  calculate_ndcg, calculate_mrr, calculate_recall_at_k,
  calculate_mean_ndcg, RankingResult
};

// Create ranking results
let results = vec![
  RankingResult { doc_id: 1, position: 0, relevance: 1.0 },
  RankingResult { doc_id: 2, position: 1, relevance: 0.0 },
];

// Calculate metrics
let ndcg = calculate_ndcg(&results, 10);
let mrr = calculate_mrr(&results);
let recall = calculate_recall_at_k(&results, 10, 5);

// Batch calculate for multiple queries
let mean_ndcg = calculate_mean_ndcg(&query_results_map, 10);
```

---

## Cache Quality Measurement (validation binary)

In `validation_enterprise`, relevance is derived from access counts during the workload. Higher access counts are treated as higher relevance, and NDCG is computed on the ranked access-frequency list.

This provides a coarse signal of whether frequently accessed documents rise to the top of the ranking. It does not directly measure cache admissions in `kyrodb_server`.

---

## Expected Performance

Performance targets are workload-dependent. Use the validation run to establish baselines for your dataset and compare different cache strategies.

---

## Validation

### Running Validation

```bash
./target/release/validation_enterprise
```

### Unit Tests

```bash
cargo test --lib ndcg
```

All 10 tests should pass:
- `test_dcg_calculation`
- `test_idcg_calculation`
- `test_ndcg_perfect_ranking`
- `test_ndcg_imperfect_ranking`
- `test_ndcg_no_relevant_results`
- `test_mrr_first_result_relevant`
- `test_mrr_second_result_relevant`
- `test_recall_at_k`
- `test_mean_ndcg_multiple_queries`
- `test_ndcg_at_10_with_binary_relevance`

---

## Performance Overhead

NDCG calculations are performed after the workload completes in `validation_enterprise` and are not on the production hot path.

---

## Integration Points

| Component | Status | Description |
|-----------|--------|-------------|
| Validation workload | Implemented | Computes NDCG/MRR/Recall from access frequency ranking |
| Production metrics | Not wired | No NDCG metrics exported by the server |

---

## Files

| File | Description |
|------|-------------|
| `engine/src/ndcg.rs` | Core NDCG calculation module |
| `engine/src/lib.rs` | NDCG API exports |
| `engine/src/bin/validation_enterprise.rs` | NDCG tracking and display integration |

---

## Scaling Considerations

For workloads exceeding millions of queries, consider:
1. **Sampling**: Retain every Nth query result
2. **Streaming calculation**: Use approximate NDCG
3. **Time-window aggregation**: Calculate hourly or daily NDCG

---
