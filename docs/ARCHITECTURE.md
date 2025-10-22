# Architecture Guide

Understand how KyroDB works internally.

## System Overview

KyroDB is a three-tier vector database optimized for read speed:

```
┌─────────────────────────────────────────────────────────┐
│                    Client Application                    │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP/gRPC
                         ▼
┌─────────────────────────────────────────────────────────┐
│                     KyroDB Server                        │
│  ┌────────────────────────────────────────────────────┐ │
│  │     Layer 1: Hybrid Semantic Cache (RMI)           │ │
│  │  • Predicts hot documents                          │ │
│  │  • 70-90% hit rate target                          │ │
│  │  • < 5ns prediction latency                        │ │
│  └────────────────────────────────────────────────────┘ │
│                         │                                │
│                         │ Cache miss                     │
│                         ▼                                │
│  ┌────────────────────────────────────────────────────┐ │
│  │          Layer 2: Hot Tier (BTree)                 │ │
│  │  • Recent writes (last 10 minutes)                 │ │
│  │  • Fast exact lookups                              │ │
│  │  • Flushes to cold tier periodically               │ │
│  └────────────────────────────────────────────────────┘ │
│                         │                                │
│                         │ Not in hot tier                │
│                         ▼                                │
│  ┌────────────────────────────────────────────────────┐ │
│  │          Layer 3: Cold Tier (HNSW)                 │ │
│  │  • Bulk of data (millions of vectors)              │ │
│  │  • k-NN approximate search                         │ │
│  │  • < 1ms P99 latency @ 10M vectors                 │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │          Persistence Layer (WAL + Snapshots)       │ │
│  │  • Write-Ahead Log (durability)                    │ │
│  │  • Periodic snapshots (fast recovery)              │ │
│  │  • Checksummed for integrity                       │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
                   ┌──────────┐
                   │   Disk   │
                   └──────────┘
```

---

## Query Flow (Read Path)

**Step-by-step when you search for a vector:**

```
1. Client sends search request
   │
   ├─► HTTP POST /v1/search
   │   Body: {"query_embedding": [...], "k": 10}
   │
2. Server receives request
   │
   ├─► Parse JSON
   ├─► Validate embedding dimension
   ├─► Extract query vector
   │
3. Layer 1: Check Hybrid Semantic Cache
   │
   ├─► RMI predicts if vector is "hot"
   ├─► If predicted hot: check cache
   │   │
   │   ├─► Cache hit (70-90% of queries)
   │   │   └─► Return cached vector (< 10ns)
   │   │
   │   └─► Cache miss (false positive)
   │       └─► Continue to Layer 2
   │
4. Layer 2: Check Hot Tier
   │
   ├─► BTree lookup for recent writes
   │   │
   │   ├─► Found in hot tier
   │   │   └─► Return vector (< 100ns)
   │   │
   │   └─► Not in hot tier
   │       └─► Continue to Layer 3
   │
5. Layer 3: HNSW Search
   │
   ├─► k-NN approximate search
   ├─► Returns top-k nearest neighbors
   ├─► Sorted by cosine similarity
   │   └─► Return results (< 1ms P99)
   │
6. Log access pattern
   │
   └─► Feed to RMI trainer (every 10 min)

Total latency:
• Cache hit: < 10ns
• Hot tier: < 100ns
• Cold tier: < 1ms (P99)
```

---

## Insert Flow (Write Path)

**Step-by-step when you insert a vector:**

```
1. Client sends insert request
   │
   ├─► HTTP POST /v1/insert
   │   Body: {"doc_id": "doc_123", "embedding": [...]}
   │
2. Server receives request
   │
   ├─► Parse JSON
   ├─► Validate embedding dimension
   │
3. Write to WAL (durability)
   │
   ├─► Append insert operation to log
   ├─► fsync() to disk (configurable)
   └─► WAL write complete (crash-safe)
   │
4. Write to Hot Tier
   │
   ├─► Insert into in-memory BTree
   └─► Immediately queryable
   │
5. Return success to client
   │
   └─► Response: {"status": "ok", "doc_id": "doc_123"}
   │
6. Background flush (every 10 minutes)
   │
   ├─► Move hot tier vectors to HNSW index
   ├─► Rebuild HNSW with new vectors
   ├─► Atomic swap (no downtime)
   └─► Clear hot tier

Insert latency:
• WAL write: ~100μs (with fsync)
• Hot tier insert: ~1μs
• Total: ~100μs P99
```

---

## Backup Flow

**How backups work:**

```
Full Backup:
1. Flush hot tier to cold tier
2. Create snapshot of HNSW index
3. Copy WAL + snapshot to backup directory
4. Generate checksum
5. Store metadata (timestamp, doc count, size)

Incremental Backup:
1. Track last backup timestamp
2. Copy only new WAL segments since last backup
3. Generate checksum
4. Store metadata

Retention Policy:
• Keep all backups from last 24 hours
• Keep daily backups from last 7 days
• Keep weekly backups from last 30 days
• Keep monthly backups from last 365 days
```

---

## Recovery Flow

**How recovery from backup works:**

```
1. Stop server (if running)
   │
2. Clear target directory
   │
   ├─► Safety check: require BACKUP_ALLOW_CLEAR=1
   └─► Remove old data
   │
3. Restore snapshot
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

## Training Flow (Hybrid Semantic Cache)

**How the RMI cache predictor learns:**

```
Access Logger (continuous):
│
├─► Every query logs: (doc_id, timestamp)
└─► Ring buffer (bounded memory, 17.6ns overhead)

Training Task (every 10 minutes):
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
   ├─► Track hit rate over next 10 min
   └─► Export metrics to Prometheus

Expected accuracy: 80-95%
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
```yaml
# config.yaml
server:
  grpc_port: 50051
  http_port: 51051
```
  
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

**Latency by operation:**

| Operation | P50 | P99 | P99.9 |
|-----------|-----|-----|-------|
| Insert | 50μs | 100μs | 200μs |
| Query (cache hit) | 5ns | 10ns | 20ns |
| Query (hot tier) | 50ns | 100ns | 200ns |
| Query (cold tier) | 500μs | 1ms | 2ms |
| Search (k=10) | 500μs | 1ms | 2ms |
| Flush | 5s | 10s | 20s |
| Backup (full) | 30s | 60s | 120s |

**Throughput:**

| Operation | Target QPS |
|-----------|-----------|
| Insert | 10,000 |
| Query | 1,000,000 (with cache) |
| Search | 10,000 |

**Scalability:**

| Vectors | Memory | Disk | Search P99 |
|---------|--------|------|------------|
| 1M | 1GB | 2GB | 500μs |
| 10M | 10GB | 20GB | 1ms |
| 100M | 100GB | 200GB | 5ms |

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

**Phase 1 (Months 13-16):**
- Distributed architecture (multi-node)
- Replication (primary + replicas)
- Sharding (horizontal scaling)

**Phase 2 (Months 17-20):**
- Hybrid queries (vector + metadata filters)
- Multi-vector per document
- Batch insert API

**Phase 3 (Months 21-24):**
- Auto-scaling (cloud deployment)
- Cost optimization (tiered storage)
- Advanced analytics (query patterns)
