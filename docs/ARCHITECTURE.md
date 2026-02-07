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
│  │   Layer 1a: Document Cache (Get)                    │ │
│  └────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────┐ │
│  │   Layer 1b: Semantic search‑result cache (k-NN)     │ │
│  └────────────────────────────────────────────────────┘ │
│                         │ miss (per path)                │
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
3. `TieredEngine` checks L1b (semantic search‑result cache), then the hot tier, then HNSW.
4. Results are filtered by tenant metadata, namespace, and filters when applicable.
5. The server returns a `SearchResponse`.

## Read path (point lookup)

1. Client sends a gRPC `Get` (by `doc_id`) request.
2. The server validates the request and passes it to `TieredEngine`.
3. `TieredEngine` checks L1a (document cache), then the hot tier, then HNSW.
4. The server returns the embedding (and metadata if requested).

## Write path (insert)

1. Client sends a gRPC `Insert` or `BulkInsert` request.
2. The server validates and writes to the WAL and hot tier.
3. Background flushes move hot-tier entries into the HNSW cold tier.
4. Snapshots are created when the operation count reaches `persistence.snapshot_interval_mutations` (set to `0` to disable).

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

## Recovery flow (WAL + snapshots)

On startup, KyroDB recovers state from disk using the MANIFEST, the newest valid snapshot, and WAL segments:

```
1. Load MANIFEST (snapshot + WAL segment list)
2. Load the newest valid snapshot (fallback to older snapshots if the newest is corrupt)
3. Replay WAL entries newer than the snapshot
4. Rebuild in-memory structures (HNSW + doc store + metadata index)
```

Recovery time depends on snapshot size, WAL tail length, and disk performance. Measure in staging on production-like hardware before relying on a restart-time SLO.

---

## Training Flow (L1a Document Cache)

**How the learned cache predictor learns:**

```
Access Logger (continuous):
│
├─► Every query logs: (doc_id, timestamp)
└─► Ring buffer (bounded memory)

Training Task (periodic; controlled by `cache.training_interval_secs` and `cache.enable_training_task`):
(see Configuration section below for schema: `cache.enable_training_task` is a boolean, default true)
│
1. Collect access logs from last window
   │
2. Build training dataset
   │
   ├─► Label "hot" if accessed in last 5 min
   └─► Label "cold" if not accessed
   │
3. Train predictor
   │
   ├─► Two-stage linear regression
   ├─► Predicts cache hotness score (0-1)
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

Example harness result: combined L1 hit rate ~73.5% (L1a ~63.5%, L1b ~10.1%)
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
│  │  │     Learned predictor        │    │                   │
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
│  │  • Hot tier flush (periodic)          │                   │
│  │  • Predictor retraining (optional)    │                   │
│  │  • WAL rotation (size-based)          │                   │
│  │  • Snapshotting (op-count based)      │                   │
│  │  • WAL cleanup (post-snapshot)        │                   │
│  └──────────────────────────────────────┘                    │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

---

## Data Structures

### Learned cache predictor (L1a)

```
Learned cache predictor:
├─► Input: access events (doc_id, timestamp)
├─► Score: hotness = frequency × recency_weight
│   • Recency weight = exp(-age / half_life)
├─► Feedback: penalize/boost based on observed false +/- outcomes
├─► Selection: keep top-N hot docs with diversity guardrails
└─► Admission: admit if score >= threshold (optionally auto-tuned)
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

Memory usage and search latency depend on dimension, `m`, `ef_search`, and dataset characteristics. Use `benchmarks/` to measure recall/QPS/latency for your workload.
```

### Hot Tier (BTree)

```
BTree:
├─► Key: doc_id (u64)
└─► Value: (embedding: Vec<f32>, timestamp: u64)

Flush criteria:
• Age > `hot_tier_max_age_secs`, OR
• Size > configured soft/hard limits, OR
• Manual flush via API
```

---

## Configuration

**Key config parameters (current schema):**

```yaml
# config.yaml
server:
    host: "127.0.0.1"
    port: 50051
    http_port: 51051

cache:
    capacity: 10000
    strategy: learned # lru | learned | abtest
    training_interval_secs: 600
    enable_training_task: true # boolean; enables periodic learned predictor retraining
    hot_tier_max_age_secs: 600
    query_cache_capacity: 100
    query_cache_similarity_threshold: 0.52

hnsw:
    dimension: 768
    m: 16 # neighbors per node
    ef_construction: 200
    ef_search: 50

persistence:
    data_dir: "/var/lib/kyrodb"
    wal_flush_interval_ms: 100
    fsync_policy: data_only # data_only | full | none
    snapshot_interval_mutations: 10000
```

---

## Performance

Performance is workload-dependent. Use the included validation and ANN benchmark kit:

- Cache validation + hit-rate/latency breakdown: [Two-Level Cache Architecture](TWO_LEVEL_CACHE_ARCHITECTURE.md)
- ANN recall/QPS/latency workflow: `benchmarks/README.md`

---

## Failure Modes

### What happens when...

**Server crashes?**

- Durability depends on `persistence.fsync_policy` (e.g., `none` may lose recent writes on crash).
- Recovery replays WAL on restart and may load a snapshot to speed up recovery.
- Restart/recovery time depends on snapshot size, WAL tail length, and disk performance.

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

- If the training task is disabled or crashes, the learned predictor stops updating and hit rate may degrade.
- The server continues serving queries; re-enable/fix training and restart if needed.

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
