# KyroDB Architecture

## Current State (October 2025)

### What We Built

KyroDB currently implements a **hybrid semantic cache layer** that combines learned frequency prediction with semantic similarity scoring. This cache layer has been validated independently but is not yet integrated with the HNSW vector search backend.

### System Components

```
┌────────────────────────────────────────────────────┐
│              Query (embedding, doc_id)             │
└───────────────────────┬────────────────────────────┘
                        │
                        ▼
            ┌───────────────────────┐
            │  A/B Test Splitter    │
            │  (50/50 traffic)      │
            └─────┬─────────────┬───┘
                  │             │
      ┌───────────▼──┐      ┌──▼──────────────────┐
      │ LRU Baseline │      │ Learned Strategy    │
      │ Strategy     │      │ (Hybrid Semantic)   │
      └───────────┬──┘      └──┬──────────────────┘
                  │             │
                  │   ┌─────────▼─────────┐
                  │   │ RMI Core          │
                  │   │ Frequency Model   │
                  │   └─────────┬─────────┘
                  │             │
                  │   ┌─────────▼─────────┐
                  │   │ Semantic Adapter  │
                  │   │ Cosine Similarity │
                  │   └─────────┬─────────┘
                  │             │
                  └─────────────▼─────────────┐
                                │              │
                        ┌───────▼──────┐  ┌───▼──────┐
                        │  Cache Hit   │  │Cache Miss│
                        │  (44% rate)  │  │ (logged) │
                        └──────────────┘  └──────────┘
```

### Core Files

**Cache Layer**:
- `engine/src/cache_strategy.rs` - A/B testing framework and strategy interface
- `engine/src/learned_cache.rs` - Hybrid semantic cache predictor
- `engine/src/vector_cache.rs` - Unified cache storage
- `engine/src/ab_stats.rs` - Statistics persistence

**Learned Models**:
- `engine/src/rmi_core.rs` - Recursive Model Index for frequency prediction
- `engine/src/semantic_adapter.rs` - Semantic similarity computation

**Infrastructure**:
- `engine/src/access_logger.rs` - Ring buffer for access pattern logging
- `engine/src/training_task.rs` - Background model retraining
- `engine/src/memory_profiler.rs` - Memory leak detection

**Vector Search** (not integrated):
- `engine/src/hnsw_index.rs` - HNSW wrapper with recall validation

**Quality Metrics** (validation only):
- `engine/src/ndcg.rs` - NDCG@k, MRR, Recall@k for cache quality

### Validation Results

**6-Minute Test** (validation_enterprise):
- LRU baseline: 20.7% hit rate
- Learned cache: 44.0% hit rate
- **Improvement: 2.1x**
- Memory growth: 2.0% (excellent stability)
- Training cycles: 6 successful (no crashes)

**Quality Metrics**:
- Both strategies: NDCG@10 = 1.0 (perfect ranking)
- LRU: Recall@10 = 0.71 (concentrated on frequent docs)
- Learned: Recall@10 = 0.13 (distributed across workload)

### What Makes This Unique

**Hybrid Decision Making**:
```rust
// Combine frequency and semantic signals
fn should_cache(&self, doc_id: u64, embedding: &[f32]) -> bool {
    let frequency_score = self.rmi.predict_hotness(doc_id);
    let semantic_score = self.semantic.similarity(embedding, cached_embeddings);
    
    let combined = frequency_score * semantic_score;
    combined > self.threshold
}
```

**Memory Efficiency**:
- Original AccessEvent: 1568 bytes (stored full embeddings)
- Optimized AccessEvent: 32 bytes (hash only)
- **49x reduction** enabling 100K event capacity

**Training Pipeline**:
- Automatic retraining every 60 seconds
- ArcSwap for atomic model updates (zero read disruption)
- Bounded memory with fixed-capacity ring buffer

## Target State (Vision)

### Three-Tier Architecture

```
┌─────────────────────────────────────────┐
│      Client Query (embedding, k)        │
└──────────────────┬──────────────────────┘
                   │
                   ▼
      ┌────────────────────────┐
      │  Layer 1: Cache        │
      │  (Hybrid Semantic)     │
      │  70-90% hit rate       │
      └────┬───────────────┬───┘
           │ HIT           │ MISS
           ▼               ▼
      ┌────────┐    ┌─────────────────┐
      │ Return │    │ Layer 2: Hot    │
      │ Cached │    │ (Recent Writes) │
      └────────┘    └──────┬──────────┘
                           │ Not Found
                           ▼
                  ┌────────────────────┐
                  │ Layer 3: Cold      │
                  │ (HNSW Full Index)  │
                  │ >95% recall        │
                  └──────┬─────────────┘
                         │
                         ▼
                  ┌─────────────┐
                  │ Log Access  │
                  │ Train Model │
                  └─────────────┘
```

### Planned Query Path

```rust
async fn search(&self, embedding: &[f32], k: usize) -> Result<Vec<SearchResult>> {
    // Layer 1: Check hybrid semantic cache
    if let Some(cached) = self.cache.get(embedding, k).await {
        return Ok(cached); // 70-90% of queries end here
    }
    
    // Layer 2: Search hot tier (recent writes, BTree)
    let hot_results = self.hot_tier.search(embedding, k).await?;
    if hot_results.len() >= k {
        self.cache.train_from_pattern(embedding, &hot_results).await;
        return Ok(hot_results);
    }
    
    // Layer 3: Full HNSW search on cold tier
    let cold_results = self.cold_tier.knn_search(embedding, k).await?;
    
    // Update cache and access log
    self.access_logger.log_pattern(embedding, &cold_results).await;
    self.cache.maybe_cache(embedding, &cold_results).await;
    
    Ok(cold_results)
}
```

### Integration Gap

**Current**: Cache layer operates independently with mock document store
**Target**: Cache checks HNSW index on miss, logs real access patterns

**Why the gap?** Cache validation was prioritized to prove the hybrid semantic approach works before building the full integration.

## Next Steps

### 1. Connect Cache to HNSW (1-2 weeks)

Replace mock document store with HNSW backend:

```rust
// Current (validation binary)
struct MockDocumentStore {
    documents: HashMap<u64, CachedVector>
}

// Target (production)
impl LearnedCacheStrategy {
    async fn check_cache(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        if let Some(cached) = self.cache.get(query, k) {
            return Ok(cached);
        }
        
        // Call HNSW on miss
        let results = self.hnsw_index.knn_search(query, k)?;
        
        // Log for training
        self.access_logger.log_pattern(query, &results);
        
        Ok(results)
    }
}
```

### 2. Add Persistence (2-3 weeks)

Implement WAL and snapshots:
- Write-ahead log for vector inserts/deletes
- Periodic HNSW index snapshots
- Crash recovery with WAL replay
- Cache state persistence (optional)

### 3. Scale Validation (1 week)

Extend validation tests:
- 6 minutes → 1 hour (60 training cycles)
- Tune cache parameters to reach 60%+ hit rate
- Validate memory stability over longer duration

### 4. Three-Tier Architecture (3-4 weeks)

Add hot tier for recent writes:
- BTree index for documents not yet flushed to HNSW
- Async background flush to cold tier
- Query path checks cache → hot → cold in order

## Design Decisions

### Why Hybrid (Frequency + Semantic)?

Pure frequency prediction failed (27% hit rate) because RAG workloads have semantic variance:
- "dog training tips" and "puppy obedience guide" are semantically similar
- But have different doc_ids, so frequency-only RMI can't predict correlation

Semantic adapter solves this by comparing query embeddings to cached document embeddings.

### Why Ring Buffer for Access Log?

Fixed-capacity ring buffer (100K events) provides:
- **Bounded memory**: No growth regardless of query volume
- **Predictable behavior**: Old events automatically evicted
- **Fast logging**: Lock-free SPSC queue (17.6ns overhead)

Alternative (VecDeque with periodic drain) caused memory leaks and required explicit cleanup.

### Why A/B Testing Framework?

Need to prove learned cache beats baseline objectively:
- 50/50 traffic split ensures fair comparison
- Same workload, same hardware, different strategies
- Statistical validation of improvement claims

Without A/B testing, we couldn't prove the 2.1x improvement is real.

### Why Separate Cache and Index?

Cache layer is:
- **High-level**: Prediction, admission policy, training
- **Portable**: Can work with any backend (HNSW, FAISS, etc.)

HNSW index is:
- **Low-level**: k-NN graph traversal, distance computation
- **Specialized**: Battle-tested hnswlib-rs library

Separation allows independent validation and future backend flexibility.

## Performance Characteristics

### Current Cache Layer

| Metric | Value |
|--------|-------|
| Hit rate (learned) | 44% (2.1x vs baseline) |
| Memory per event | 32 bytes |
| Ring buffer capacity | 100K events (3.2 MB) |
| Training frequency | Every 60 seconds |
| Training duration | <500ms |
| Memory growth | 2% over 6 minutes |

### Target Full System

| Metric | Goal |
|--------|------|
| Cache hit rate | 60-90% |
| Cache hit latency | <100µs P99 |
| HNSW search latency | <1ms P99 |
| HNSW recall@10 | >95% |
| Memory growth | <10% over 24 hours |

## References

**Implementation Details**:
- See `Implementation.md` for week-by-week plan
- See `other_docs/IMPLEMENTATION_UPDATE_ANALYSIS.md` for current status
- See `other_docs/PHASE0_WEEK5-8_RESULTS.md` for memory leak fixes

**Vision**:
- See `docs/visiondocument.md` for long-term roadmap
- See `README.md` for project overview

**Validation**:
- See `engine/src/bin/validation_enterprise.rs` for full test harness
- See `engine/src/ndcg.rs` for quality metrics
