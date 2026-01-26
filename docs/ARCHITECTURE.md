# Architecture Guide

Understand how KyroDB works internally.

## System overview

KyroDB is a gRPC-first vector database with a two-level L1 cache, a hot tier, and an HNSW cold tier. HTTP endpoints are used only for observability.

```
┌─────────────────────────────────────────────────────────┐
│                    Client Applications                   │
└────────────────────────┬────────────────────────────────┘
                         │ gRPC (data plane)
                         ▼
┌─────────────────────────────────────────────────────────┐
│                     KyroDB Server                        │
│  ┌────────────────────────────────────────────────────┐ │
│  │   Layer 1a: Document Cache                          │ │
│  └────────────────────────────────────────────────────┘ │
│                         │ miss                           │
│                         ▼                                │
│  ┌────────────────────────────────────────────────────┐ │
│  │   Layer 1b: Query Cache                             │ │
│  └────────────────────────────────────────────────────┘ │
│                         │ miss                           │
│                         ▼                                │
│  ┌────────────────────────────────────────────────────┐ │
│  │   Layer 2: Hot Tier (recent writes)                 │ │
│  └────────────────────────────────────────────────────┘ │
│                         │ miss                           │
│                         ▼                                │
│  ┌────────────────────────────────────────────────────┐ │
│  │   Layer 3: Cold Tier (HNSW index)                   │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │   Persistence (WAL + Snapshots)                    │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
                   ┌──────────┐
                   │   Disk   │
                   └──────────┘
```

HTTP observability endpoints:

- `GET /metrics`
- `GET /health`
- `GET /ready`
- `GET /slo`

## Read path (search)

1. Client sends a gRPC `Search` request.
2. The server validates the request and passes it to `TieredEngine`.
3. `TieredEngine` checks L1a (document cache), then L1b (query cache), then the hot tier, then HNSW.
4. Results are filtered by tenant metadata, namespace, and filters when applicable.
5. The server returns a `SearchResponse`.

## Write path (insert)

1. Client sends a gRPC `Insert` or `BulkInsert` request.
2. The server validates and writes to the WAL and hot tier.
3. Background flushes move hot-tier entries into the HNSW cold tier.
4. Snapshots are created at the interval set by `persistence.snapshot_interval_secs`.

## Persistence

- WAL and snapshots live under `persistence.data_dir`.
- Recovery on startup uses the MANIFEST + snapshot + WAL tail.

## Backups and recovery

- Use the `kyrodb_backup` CLI for create/list/restore/verify/prune.
- Restores require confirmation via `BACKUP_ALLOW_CLEAR=true` or a custom restore workflow.

## Related documentation

- [API Reference](API_REFERENCE.md)
- [Configuration Guide](CONFIGURATION_MANAGEMENT.md)
- [Backup and Recovery](BACKUP_AND_RECOVERY.md)
   │
   ├─► Copy HNSW index files
   ├─► Verify checksums
   └─► Load index into memory
   │
4. Replay WAL (if exists)
   │
   ├─► Read WAL entries since snapshot
   ├─► Apply inserts/updates
   └─► Rebuild hot tier
   │
5. Start server
   │
   └─► Server ready, data restored

Recovery time:
• Snapshot load: ~10s for 10M vectors
• WAL replay: ~1s per 10K ops
• Total: < 1 minute for typical workload
```

---

## Training Flow (L1a Document Cache)

**How the RMI cache predictor learns:**

```
Access Logger (continuous):
│
├─► Every query logs: (doc_id, timestamp)
└─► Ring buffer (bounded memory, 17.6ns overhead)

Training Task (every 15 seconds in validation, 10 min in production):
│
1. Collect access logs from last window
   │
2. Build training dataset
   │
   ├─► Label "hot" if accessed in last 5 min
   └─► Label "cold" if not accessed
   │
3. Train RMI model
   │
   ├─► Two-stage linear regression
   ├─► Predicts cache hotness score (0-1)
   └─► Training time: ~100ms for 100K samples
   │
4. Deploy new model
   │
   ├─► Atomic swap (no query interruption)
   └─► Old model garbage collected
   │
5. Measure accuracy
   │
   ├─► Track L1a hit rate over next training cycle
   └─► Export metrics (L1a, L1b, combined)

Validated L1a accuracy: 63.5% hit rate (12-hour validation)
Validated L1b performance: 10.1% additional hit rate
Combined L1 hit rate: 73.5%
```

---

## Component Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                       kyrodb_server                           │
│                                                               │
│  ┌────────────────┐        ┌────────────────┐               │
│  │  HTTP Server   │        │  gRPC Server   │               │
│  │  (port 51051)  │        │  (port 50051)  │               │
│  └───────┬────────┘        └───────┬────────┘               │
│          │                         │                          │
│          └──────────┬──────────────┘                         │
│                     │                                         │
│                     ▼                                         │
│          ┌─────────────────────┐                             │
│          │   Request Router    │                             │
│          └─────────┬───────────┘                             │
│                    │                                          │
│      ┌─────────────┼─────────────┐                           │
│      │             │             │                            │
│      ▼             ▼             ▼                            │
│  ┌────────┐  ┌─────────┐  ┌──────────┐                      │
│  │ Insert │  │  Query  │  │  Search  │                       │
│  │Handler │  │ Handler │  │ Handler  │                       │
│  └───┬────┘  └────┬────┘  └────┬─────┘                      │
│      │            │             │                             │
│      ▼            ▼             ▼                             │
│  ┌──────────────────────────────────────┐                    │
│  │         TieredEngine                  │                   │
│  │  ┌──────────────────────────────┐    │                   │
│  │  │     LearnedCache (RMI)       │    │                   │
│  │  └──────────────────────────────┘    │                   │
│  │  ┌──────────────────────────────┐    │                   │
│  │  │     HotTier (BTree)          │    │                   │
│  │  └──────────────────────────────┘    │                   │
│  │  ┌──────────────────────────────┐    │                   │
│  │  │     ColdTier (HNSW)          │    │                   │
│  │  └──────────────────────────────┘    │                   │
│  └──────────────────────────────────────┘                    │
│                    │                                          │
│                    ▼                                          │
│  ┌──────────────────────────────────────┐                    │
│  │      Persistence Layer                │                   │
│  │  • WAL (append-only log)              │                   │
│  │  • Snapshots (HNSW index)             │                   │
│  │  • Manifest (metadata)                │                   │
│  └──────────────────────────────────────┘                    │
│                    │                                          │
│                    ▼                                          │
│  ┌──────────────────────────────────────┐                    │
│  │      Background Tasks                 │                   │
│  │  • Hot tier flush (10 min)            │                   │
│  │  • RMI training (10 min)              │                   │
│  │  • WAL compaction (hourly)            │                   │
│  │  • Snapshot creation (daily)          │                   │
│  └──────────────────────────────────────┘                    │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

---

## Data Structures

### Hybrid Semantic Cache (RMI)

```
RMI (Recursive Model Index):
├─► Stage 1: Root model (linear regression)
│   • Maps doc_id → bucket (0-255)
│
└─► Stage 2: Leaf models (256 linear regressions)
    • Each bucket has its own model
    • Predicts hotness score (0.0-1.0)
    • Threshold: > 0.5 = hot, else cold

Memory: ~4KB (256 models × 16 bytes)
Prediction: < 5ns (2 linear regressions)
```

### HNSW Index

```
HNSW (Hierarchical Navigable Small World):
├─► Layer 0: All vectors (base layer)
│   • Complete graph of neighbors
│
├─► Layer 1: 1/M vectors (skip layer)
│   • Faster navigation
│
└─► Layer N: 1/M^N vectors (top layer)
    • Entry point for search

Parameters:
• M = 16 (neighbors per node)
• ef_construction = 200 (build quality)
• ef_search = 50 (search quality)

Memory: ~100 bytes per vector
Search: < 1ms P99 @ 10M vectors
```

### Hot Tier (BTree)

```
BTree:
├─► Key: doc_id (u64)
└─► Value: (embedding: Vec<f32>, timestamp: u64)

Flush criteria:
• Age > 10 minutes, OR
• Size > 100MB, OR
• Manual flush via API

Memory: ~4KB per vector
Lookup: < 100ns (O(log n))
```

---

## Configuration

**Key config parameters:**

```yaml
# config.yaml
server:
  grpc_port: 50051
  http_port: 51051
  
cache:
  strategy: "learned"  # or "lru"
  max_size_mb: 1024
  training_interval_secs: 600  # 10 minutes
  
hot_tier:
  flush_interval_secs: 600  # 10 minutes
  max_size_mb: 100
  
hnsw:
  dimension: 768
  m: 16  # neighbors per node
  ef_construction: 200
  ef_search: 50
  
persistence:
  wal_path: "/var/lib/kyrodb/wal"
  snapshot_path: "/var/lib/kyrodb/snapshots"
  fsync_policy: "data"  # or "always", "never"
```

---

## Performance Characteristics

**Latency by operation (12-hour validation on MS MARCO):**

| Operation | P50 | P99 | Notes |
|-----------|-----|-----|-------|
| Query (L1 cache hit) | 4μs | 9ms | 73% of queries |
| Query (cache miss) | 4.3ms | 9.9ms | Falls through to HNSW |
| Query (overall) | 12μs | 9.7ms | Blended with 73% hit rate |
| Insert | 50μs | 100μs | To hot tier |
| Search (k=10) | 500μs | 5ms | HNSW approximate search |
| Flush | 5s | 10s | Hot tier to cold tier |
| Backup (full) | 30s | 60s | Depends on data size |

**Throughput:**

| Operation | Target QPS |
|-----------|-----------|
| Insert | 10,000 |
| Query | 100,000+ (with cache) |
| Search | 10,000 |

**Scalability:**

| Vectors | Memory | Disk | Search P99 |
|---------|--------|------|------------|
| 10K | 100MB | 200MB | 5ms |
| 100K | 500MB | 1GB | 5ms |
| 1M | 2GB | 5GB | 10ms |
| 10M | 15GB | 40GB | 20ms |

---

## Failure Modes

### What happens when...

**Server crashes?**
- WAL ensures no data loss (up to last fsync)
- Recovery replays WAL on restart
- Downtime: < 1 minute for 10M vectors

**Disk full?**
- Circuit breaker opens (stops WAL writes)
- Queries still work (read-only mode)
- Inserts fail with error
- Alert fires: "WAL write failed"

**HNSW index corrupted?**
- Restore from snapshot
- Replay WAL to rebuild
- Or restore from backup

**Cache predictor broken?**
- Fallback to LRU cache automatically
- Performance degrades but still works
- Retrain predictor to fix

**Out of memory?**
- OS kills process (OOM killer)
- Restart and recover from WAL
- Prevention: set cache size limits in config

---

## Security

**Authentication:**
- API key-based (optional)
- Configured in `api_keys.yaml`
- Per-tenant isolation

**Encryption:**
- TLS for client connections (optional)
- Data at rest NOT encrypted (use disk encryption)

**Rate Limiting:**
- Per-connection QPS limits
- Global QPS limits
- Configured in `config.yaml`

---

## Future Enhancements

**Phase 1 :**
- Distributed architecture (multi-node)
- Replication (primary + replicas)
- Sharding (horizontal scaling)

**Phase 2 :**
- Hybrid queries (vector + metadata filters)
- Multi-vector per document
- Batch insert API

**Phase 3 :**
- Auto-scaling (cloud deployment)
- Cost optimization (tiered storage)
- Advanced analytics (query patterns)
