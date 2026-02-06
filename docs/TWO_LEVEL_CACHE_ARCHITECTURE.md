# Two-Level L1 Cache Architecture

KyroDB uses a two-level cache at Layer 1 to maximize cache hit rates for RAG workloads. This document explains how the cache works and why it achieves significantly better hit rates than traditional LRU caching.

## Overview

The two-level L1 cache combines two complementary caching strategies:

| Layer | Name | Strategy | Hit Rate |
|-------|------|----------|----------|
| L1a | Document Cache | Learned hotness prediction (freq+recency) | ~63% |
| L1b | Semantic search‑result cache | Semantic similarity | ~10% |
| **Combined** | | | **~73%** |

Traditional LRU caches achieve 25-35% hit rates on typical RAG workloads. KyroDB's two-level cache delivers 2-3x improvement.

## How It Works

### Search Query Flow (k‑NN)

```
Incoming Query
      │
      ▼
┌─────────────────────────────────────────┐
│  L1b: Semantic search‑result cache      │
│  "Have we seen a similar query before?" │
│  • Matches paraphrased queries          │
│  • Cosine similarity >= 0.52 threshold  │
│  • Hit rate: ~10% additional            │
└─────────────────────────────────────────┘
      │ (miss)
      ▼
┌─────────────────────────────────────────┐
│  L2: Hot Tier (recent writes)           │
│  BTree keyed by doc_id; holds recently  │
│  inserted/updated vectors. Entries age  │
│  out after hot_tier_max_age_secs or are │
│  flushed when size limits are reached.  │
│  Queries hit L2 on L1 miss before       │
│  falling through to L3.                 │
└─────────────────────────────────────────┘
      │ (miss)
      ▼
┌─────────────────────────────────────────┐
│  L3: Cold Tier (HNSW index)             │
│  Long-term vector storage backed by the │
│  HNSW index. Data is compacted here     │
│  from L2 during background flushes.     │
│  Handles k-NN search for the full       │
│  indexed dataset.                       │
└─────────────────────────────────────────┘
```

### Point Lookup Flow (doc_id)

```
Incoming Get (doc_id)
      │
      ▼
┌─────────────────────────────────────────┐
│  L1a: Document Cache                    │
│  "Is this document frequently accessed?"│
│  • Predictor estimates document hotness │
│  • O(1) lookup by doc_id                │
│  • Hit rate: ~63%                       │
└─────────────────────────────────────────┘
      │ (miss)
      ▼
┌─────────────────────────────────────────┐
│  L2: Hot Tier (recent writes)           │
│  BTree lookup by doc_id for recent data │
└─────────────────────────────────────────┘
      │ (miss)
      ▼
┌─────────────────────────────────────────┐
│  L3: Cold Tier (HNSW index)             │
│  Full indexed dataset (persistent)      │
└─────────────────────────────────────────┘
```

### L1a: Document Cache (Learned hotness prediction)

The document cache predicts which documents are "hot" based on access frequency patterns.

**How it works:**
1. The engine can log accesses (doc_id + timestamp) via the access logger.
2. Cache hits are served from the L1a document cache (an in-memory vector cache keyed by `doc_id`).
3. On misses, the engine fetches from lower tiers and then makes an admission decision:
      - **LRU strategy**: always admits.
      - **Learned strategy**: admits based on the predictor if it has been trained; otherwise it operates in a bootstrap mode and admits.
4. When the cache is full, eviction is LRU.

Note on retraining:
- The background retraining loop (`training_task::spawn_training_task`) periodically rebuilds the predictor from access logs and swaps it into the learned strategy.
- In `kyrodb_server`, this is controlled by `cache.enable_training_task`.

**Why it works for RAG:**
RAG workloads follow Zipfian distributions - a small subset of documents (FAQs, popular topics) receive most queries. The predictor learns this pattern and pre-caches hot documents.

### L1b: Semantic search‑result cache (Semantic similarity)

The query cache handles a different problem: paraphrased queries asking the same thing.

**How it works:**
1. Store query embeddings with their top‑k search results (k = number of neighbors requested per search, e.g., `k=10` in benchmarks; controlled by the `SearchRequest.k` field)
2. For new queries, compute cosine similarity against cached queries
3. If similarity >= 0.52, return the cached top‑k results
4. On cache full, LRU eviction removes oldest entries

Changing `k` affects cache memory and reuse: larger `k` stores more results per cached query (higher memory, potentially better downstream recall), while smaller `k` reduces memory footprint but may under‑serve rerankers that expect a deeper candidate set.

**Why it works for RAG:**
Users ask the same question in different ways:
- "How do I reset my password?"
- "Password reset instructions"
- "Can't log in, need to change password"

All these queries should return the same evidence set (top‑k). The query cache catches these paraphrases.

## Why Two Levels?

Neither cache alone is sufficient:

| Scenario | L1a (Frequency) | L1b (Similarity) |
|----------|-----------------|------------------|
| Same user asks same question repeatedly | Catches after first access | Catches immediately |
| Popular FAQ document | Catches well | May miss (different phrasings) |
| Paraphrased question | Misses (different doc access pattern) | Catches well |
| Cold traffic (first-time query) | Misses | Misses |

Combined, they cover more scenarios than either alone.

## Configuration

### Default Settings

The current `kyrodb_server` behavior is:

- L1a capacity is driven by `cache.capacity`.
- Background retraining is controlled by:
      - `cache.enable_training_task` (default: true)
      - `cache.training_interval_secs`, `cache.training_window_secs`, `cache.recency_halflife_secs`
      - `cache.logger_window_size` (access log ring buffer size)
      - `cache.predictor_capacity_multiplier` (predictor tracks more candidates than cache)
- L1b (query cache) is configurable:
      - `cache.query_cache_capacity` (default: 100)
      - `cache.query_cache_similarity_threshold` (default: 0.52)

The `0.52` default comes from a quick empirical check using the bundled
MS MARCO query embeddings (see `scripts/evaluate_query_cache_threshold.py --pairs 20000`).
On that dataset:

- `t=0.52` yields **TPR=0.396**, **FPR≈0.0000**, **FNR=0.604** (balanced sample).
- Thresholds in the **0.8+** range yield near‑zero similarity hits.

This reflects the tradeoff: lower thresholds increase hit rate but risk semantic drift; higher thresholds are stricter but often miss paraphrases. As a rule of thumb, use **~0.5–0.65** for higher recall/hit rate and **~0.7–0.8** for stricter reuse. Values **>0.8** are extremely strict and often produce minimal hits (including on the MS MARCO sample); only use them if your workload has very consistent paraphrases and you can tolerate low cache hit rate.

### Tuning Guidelines

If you are tuning hit rate, the biggest knobs are working-set size vs L1a capacity and the amount of query repetition/paraphrasing (which drives L1b effectiveness). If you want the learned admission behavior (as opposed to always-admit bootstrap behavior), the retraining task needs to be wired into the production server.

## Performance Characteristics

### Latency by Path

| Path | Typical Latency |
|------|-----------------|
| L1 hit (cache) | < 100 microseconds |
| L2 hit (hot tier) | < 500 microseconds |
| L3 search (HNSW) | 1-10 milliseconds |

### Overall Latency (12-Hour Validation)

The latency distribution is **bimodal** due to the high cache hit rate:

| Metric | Cache Hits (73.5%) | Cache Misses (26.5%) | Overall |
|--------|-------------------|---------------------|---------|
| P50 | 4 μs | 4.3 ms | 12 μs |
| P90 | 1 ms | 8.8 ms | 6.8 ms |
| P99 | 9 ms | 9.9 ms | 9.7 ms |

**Why P50 is 12μs but P99 is 9.7ms (750x difference):**

The distribution has two distinct populations:
- **Cache hits (73.5%)**: Complete in microseconds (0-1ms range)
- **Cache misses (26.5%)**: Require HNSW search (1-10ms range)

Since 73.5% of queries hit the cache, the median (P50) falls in the fast cache-hit bucket. But P99 samples from the slowest 1% of queries, which are all cache misses requiring full HNSW search.

This bimodal distribution is expected and shows the cache is working correctly.

### Memory Usage

| Component | Memory |
|-----------|--------|
| L1a (180 docs, 384-dim) | ~280 KB |
| L1b (100 queries, 384-dim, k=10) | ~170 KB |
| Predictor | ~50 KB |
| Access logger (ring buffer, 1,000,000 events) | ~24 MB |
| **Total L1 overhead** | **~25 MB** |

### Validated Results (12-Hour Test)

These figures are from the validation harness, not a production guarantee.

| Metric | Value |
|--------|-------|
| Total queries | 8,640,000 |
| L1a hit rate | 63.5% |
| L1b hit rate | 10.1% |
| **Combined L1 hit rate** | **73.5%** |
| LRU baseline comparison | 25-35% |
| **Improvement over LRU** | **2-3x** |
| Training cycles | 72/72 successful |
| Memory growth | 26 MB (12%) over 12 hours |

## Metrics

KyroDB exposes metrics via the HTTP `/metrics` endpoint. In the current server implementation, cache metrics are exported at an aggregate level (not split into L1a vs L1b):

```
# Cache
kyrodb_cache_hits_total
kyrodb_cache_misses_total
kyrodb_cache_hit_rate

# Training task (only increments if a training loop is running)
kyrodb_training_cycles_completed_total
```

The L1a/L1b hit rates shown in **Validated Results** come from the validation harness instrumentation (not from the aggregate `/metrics` counters).

## Troubleshooting

### Low Hit Rate (< 60%)

**Possible causes:**
1. **High cold traffic ratio**: If >40% of queries are for never-seen documents, hit rate will be lower
2. **Cache too small**: Increase `cache.capacity` and `cache.query_cache_capacity`
3. **Workload not Zipfian**: Uniform access patterns don't benefit from frequency prediction
4. **Short training window**: predictor needs time to learn patterns

**Diagnosis:**
```bash
# Check overall cache metrics
curl localhost:51051/metrics | grep kyrodb_cache
```

### High Memory Usage

**Possible causes:**
1. **Cache capacity too large**: Reduce `cache.capacity`
2. **Access logger too large**: Reduce `cache.logger_window_size`

**Diagnosis:**
```bash
# Check memory breakdown
curl localhost:51051/metrics | grep kyrodb_memory
```

### Slow Training

**Possible causes:**
1. **Large access log**: predictor training time scales with log size
2. **Too frequent training**: Increase `training_interval_secs`

**Diagnosis:**
```bash
# Check training duration
curl localhost:51051/metrics | grep kyrodb_rmi_training_duration
```

## Related Documentation

- [Architecture Overview](ARCHITECTURE.md) - Full system architecture
- [Configuration Guide](CONFIGURATION_MANAGEMENT.md) - All config options
- [Observability Guide](OBSERVABILITY.md) - Metrics and monitoring
