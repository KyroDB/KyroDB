# NDCG Quality Metrics Implementation

## Overview

NDCG (Normalized Discounted Cumulative Gain) measures ranking quality in KyroDB. It validates cache admission quality by determining whether the system caches documents that will actually be queried again.

**Purpose**: Distinguish between a cache with high hit rate from lucky popularity and one with genuine predictive power.

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

## Cache Quality Measurement

**Problem**: Hit rate alone does not indicate whether the correct documents are being cached.

**Example**:
- Cache A: 80% hit rate, caches random popular documents
- Cache B: 80% hit rate, caches documents that will be queried soon

Both have identical hit rates, but Cache B has superior **predictive quality**.

**Measurement Approach**:
1. **Track cache admissions**: Record when document X is admitted to cache
2. **Record future accesses**: Determine if document X is queried again
   - If queried: relevance = 1.0 (correct prediction)
   - If not queried: relevance = 0.0 (incorrect prediction)
3. **Calculate NDCG@10**: Measures ranking quality of admission decisions

**Interpretation**:
| NDCG@10 Score | Meaning |
|---------------|---------|
| 1.0 | Perfect cache admission (always caches documents that will be queried) |
| 0.5-0.7 | Good predictive quality |
| 0.3-0.5 | Moderate quality |
| < 0.3 | Poor predictive power |

---

## Expected Performance

**LRU Baseline**:
- NDCG@10: 0.3-0.5
- MRR: 0.4-0.6
- Recall@10: 0.2-0.4

**Hybrid Semantic Cache**:
- NDCG@10: 0.6-0.8
- MRR: 0.7-0.9
- Recall@10: 0.5-0.7

**Target**: 1.5-2x NDCG improvement over LRU baseline.

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

| Metric | Cost |
|--------|------|
| Per-query tracking | O(1) |
| End-of-test calculation | O(n log n) |
| Memory usage | O(queries) |

**For 70K queries**:
- Memory: ~1.7 MB
- Calculation time: <10ms
- Impact: <0.01% of test duration

---

## Integration Points

| Component | Status | Description |
|-----------|--------|-------------|
| Cache admission validation | Implemented | Measures if cached documents are queried again |
| HNSW search result ranking | Planned | Measures semantic relevance of k-NN results |
| RMI prediction quality | Planned | Validates RMI prediction accuracy |
| Semantic adapter quality | Planned | Measures cluster grouping quality |

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
