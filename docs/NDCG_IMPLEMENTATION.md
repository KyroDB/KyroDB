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

In `validation_enterprise`, the goal is to validate that the learned predictor ranks the *right* documents as hot.

- **Ground truth (relevance vector)**: observed access counts per document during the workload. Documents accessed more frequently receive higher relevance scores.
- **Evaluated ranking**: documents ordered by the learned predictor's admission score (descending). This is the ranking the predictor *actually produces*.
- **NDCG@10**: measures how closely the predictor's top-10 ranking matches the ideal ranking obtained by sorting documents by their access counts. An NDCG of 1.0 means the predictor places the most-accessed documents in exactly the same order as the ideal ranking; lower values indicate the predictor mis-ranks frequently accessed documents further from the top.

The evaluated ranking is produced by the predictor, not by sorting access counts, which avoids the trivial case where both orderings are identical (which would yield NDCG=1.0 by construction and test nothing).

**MRR and Recall@10** use **binary relevance**: the ground-truth top-10 most-accessed documents are treated as the relevant set (relevant=1), and all other documents are irrelevant (relevant=0). The predictor's ranking is evaluated against this binary set.

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
| Validation workload | Implemented | Computes NDCG/MRR/Recall from predictor ranking vs access-frequency ground truth |
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
