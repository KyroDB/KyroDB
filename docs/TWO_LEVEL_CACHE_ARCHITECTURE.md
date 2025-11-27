# Two-Level L1 Cache Architecture

KyroDB uses a two-level cache at Layer 1 to maximize cache hit rates for RAG workloads. This document explains how the cache works and why it achieves significantly better hit rates than traditional LRU caching.

## Overview

The two-level L1 cache combines two complementary caching strategies:

| Layer | Name | Strategy | Hit Rate |
|-------|------|----------|----------|
| L1a | Document Cache | Frequency prediction (RMI) | ~63% |
| L1b | Query Cache | Semantic similarity | ~10% |
| **Combined** | | | **~73%** |

Traditional LRU caches achieve 25-35% hit rates on typical RAG workloads. KyroDB's two-level cache delivers 2-3x improvement.

## How It Works

### Query Flow

```
Incoming Query
      │
      ▼
┌─────────────────────────────────────────┐
│  L1a: Document Cache                    │
│  "Is this document frequently accessed?"│
│  • RMI predicts document hotness        │
│  • O(1) lookup by doc_id                │
│  • Hit rate: ~63%                       │
└─────────────────────────────────────────┘
      │ (miss)
      ▼
┌─────────────────────────────────────────┐
│  L1b: Query Cache                       │
│  "Have we seen a similar query before?" │
│  • Matches paraphrased queries          │
│  • Cosine similarity > 0.25 threshold   │
│  • Hit rate: ~10% additional            │
└─────────────────────────────────────────┘
      │ (miss)
      ▼
┌─────────────────────────────────────────┐
│  L2: Hot Tier (recent writes)           │
└─────────────────────────────────────────┘
      │ (miss)
      ▼
┌─────────────────────────────────────────┐
│  L3: Cold Tier (HNSW index)             │
└─────────────────────────────────────────┘
```

### L1a: Document Cache (RMI Frequency Prediction)

The document cache predicts which documents are "hot" based on access frequency patterns.

**How it works:**
1. Access logger tracks every document access with timestamps
2. RMI (Recursive Model Index) trains on access patterns every 10 minutes
3. Each document gets a "hotness score" (0.0 to 1.0)
4. Documents above the threshold are cached
5. On cache full, LRU eviction removes oldest entries

**Why it works for RAG:**
RAG workloads follow Zipfian distributions - a small subset of documents (FAQs, popular topics) receive most queries. The RMI learns this pattern and pre-caches hot documents.

### L1b: Query Cache (Semantic Similarity)

The query cache handles a different problem: paraphrased queries asking the same thing.

**How it works:**
1. Store query embeddings with their results
2. For new queries, compute cosine similarity against cached queries
3. If similarity > 0.25, return the cached result
4. On cache full, LRU eviction removes oldest entries

**Why it works for RAG:**
Users ask the same question in different ways:
- "How do I reset my password?"
- "Password reset instructions"
- "Can't log in, need to change password"

All these queries should return the same document. The query cache catches these paraphrases.

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

```yaml
cache:
  # L1a: Document cache
  document_cache_capacity: 180
  training_interval_secs: 600
  
  # L1b: Query cache  
  query_cache_capacity: 300
  similarity_threshold: 0.25
```

### Tuning Guidelines

**Document cache capacity:**
- Set to ~2-3% of your corpus size
- Larger capacity = more memory, diminishing returns on hit rate
- Default: 180 documents

**Query cache capacity:**
- Set based on query variety
- High paraphrase rate = larger cache helps
- Default: 300 queries

**Similarity threshold:**
- Lower = more hits, risk of false matches
- Higher = fewer hits, more accurate
- Default: 0.25 (tuned on MS MARCO dataset)

**Training interval:**
- More frequent = faster adaptation to pattern changes
- Less frequent = lower CPU overhead
- Default: 600 seconds (10 minutes)

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
| L1b (300 queries, 384-dim) | ~460 KB |
| RMI predictor | ~50 KB |
| Access logger (ring buffer) | ~3 MB |
| **Total L1 overhead** | **~4 MB** |

### Validated Results (12-Hour Test)

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

KyroDB exposes cache metrics via the `/metrics` endpoint:

```
# L1a (Document Cache)
kyrodb_l1a_cache_hits_total
kyrodb_l1a_cache_misses_total
kyrodb_l1a_hit_rate

# L1b (Query Cache)
kyrodb_l1b_cache_hits_total
kyrodb_l1b_cache_misses_total
kyrodb_l1b_exact_hits_total
kyrodb_l1b_similarity_hits_total
kyrodb_l1b_hit_rate

# Combined
kyrodb_l1_combined_hit_rate

# Training
kyrodb_rmi_training_duration_seconds
kyrodb_rmi_training_cycles_total
```

## Troubleshooting

### Low Hit Rate (< 60%)

**Possible causes:**
1. **High cold traffic ratio**: If >40% of queries are for never-seen documents, hit rate will be lower
2. **Cache too small**: Increase `document_cache_capacity` and `query_cache_capacity`
3. **Workload not Zipfian**: Uniform access patterns don't benefit from frequency prediction
4. **Short training window**: RMI needs time to learn patterns

**Diagnosis:**
```bash
# Check hit rate breakdown
curl localhost:51051/metrics | grep kyrodb_l1
```

### High Memory Usage

**Possible causes:**
1. **Cache capacity too large**: Reduce `document_cache_capacity`
2. **Access logger too large**: Reduce `logger_window_size`

**Diagnosis:**
```bash
# Check memory breakdown
curl localhost:51051/metrics | grep kyrodb_memory
```

### Slow Training

**Possible causes:**
1. **Large access log**: RMI training time scales with log size
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
