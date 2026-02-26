# NDCG Metrics

## Purpose

Document ranking-quality metrics used in validation tooling.

## Scope

- `NDCG@k`, `MRR`, `Recall@k`
- validation binary integration
- unit-test coverage

## Commands

```bash
# Build validation binary
cargo build --release -p kyrodb-engine --features cli-tools --bin validation_enterprise

# Inspect validation binary options
./target/release/validation_enterprise --help

# Run ndcg unit tests
cargo test --lib ndcg
```

## Key Contracts

- NDCG module is not part of production request hot path
- production server does not expose NDCG metrics by default
- quality calculations are used in validation and analysis workflows

## API Surface

```rust
use kyrodb_engine::ndcg::{
  calculate_ndcg,
  calculate_mrr,
  calculate_recall_at_k,
  calculate_mean_ndcg,
  RankingResult,
};
```

## Files

- `engine/src/ndcg.rs`
- `engine/src/bin/validation_enterprise.rs`
- inline unit tests in `engine/src/ndcg.rs` (`#[cfg(test)] mod tests`)

The inline `ndcg.rs` tests exercise core ranking routines such as:
- `calculate_ndcg`
- `calculate_mrr`
- `calculate_recall_at_k`
- `calculate_mean_ndcg`

## Related Docs

- [ENGINEERING_STATUS.md](ENGINEERING_STATUS.md)
