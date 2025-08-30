# KyroDB — Vision and Architecture 

Status: living document. Grounded in today’s single‑node KV + RMI engine; points the way to a broader data platform.

---

## The arc of KyroDB (why and where we’re going)

Modern AI applications bolt together many systems: streams, OLTP, vector stores, feature stores, provenance tools, and model infra. It works — until it doesn’t. Incidents cascade, glue code multiplies, and nobody can answer simple questions like “what decision did the model make and why?”

KyroDB starts from a simple, durable core and grows into an AI‑native database. We fuse learned systems with classical database correctness. The end goal is a platform that learns from data, optimizes itself, and offers built‑in primitives for provenance and governance — without sacrificing durability or developer ergonomics.

A story in three acts:
- Act I — Make the core undeniable: a production‑grade single‑node KV engine with a learned primary index (RMI), honest latency tails, and a clear durability story.
- Act II — Programmable intelligence: adaptive indexing, self‑tuning knobs, and optional vector primitives behind stable traits, with a clean API/SDK surface.
- Act III — Federated and autonomous: replication, sharding, and policy‑driven optimization that keeps correctness and auditability front and center.

---

## Where we are now (today)

KyroDB is a single‑node, durable key–value engine with a learned primary index.

- Interface: HTTP/JSON v1. Control and data share one surface for now.
- Durability: append‑only WAL, atomic snapshots (manifested), clean recovery.
- Reads: RMI is the default read path; values fetched via a mmap’d payload index.
- Writes: fast appends into WAL; a small in‑memory delta makes new keys visible immediately.
- Swap: RMI is rebuilt off a snapshot and swapped atomically; reads stay linearizable.
- Ops: Prometheus metrics (/metrics), build info (/build_info), health (/health), rate limiting and bearer auth. Warm‑on‑start and HTTP logging toggle.

Key endpoints (v1): /v1/put, /v1/get_fast/{key}, /v1/snapshot, /v1/rmi/build, /v1/warmup.

---

## Why KyroDB (the problem and our bet)

- Today’s AI stacks are a zoo of services. They’re powerful but operationally heavy. Data and semantics scatter across systems.
- Learned indexes show real promise, but many demos ignore durability, rebuild behavior, and hot read paths under load.
- Our bet: start with a correct, observable kernel that productionizes learned indexing; then open carefully chosen extension points for vectors and model‑aware features. No magic. Evidence over hype.

---

## System overview

```mermaid
graph TB
  subgraph "Client Layer"
    C1[HTTP/JSON Clients]
    C2[CLI Tools]
    C3[SDK Libraries]
  end

  subgraph "API Gateway"
    API[HTTP v1 API Server]
    AUTH[Bearer Authentication]
    RL[Rate Limiting<br/>Per-IP]
    CORS[CORS Handling]
  end

  subgraph "Core Engine"
    subgraph "Write Path"
      WAL[WAL Manager<br/>Append-Only Log]
      DELTA[In-Memory Delta<br/>Recent Writes]
      SEQ[Sequence Generator<br/>Monotonic Offsets]
    end

    subgraph "Read Path"
      RMI[RMI Index<br/>Learned Index]
      PCACHE[Payload Cache<br/>LRU Hot Values]
      MMAP[Memory-Mapped<br/>Snapshot Data]
    end

    subgraph "Storage Layer"
      SNAP[Snapshot Manager<br/>Atomic State]
      MANIFEST[Manifest Manager<br/>Metadata Store]
      FS[Filesystem<br/>Local Disk]
    end
  end

  subgraph "Background Services"
    COMPACT[WAL Compaction<br/>Retention Policy]
    REBUILD[RMI Rebuild<br/>Adaptive Thresholds]
    WARMUP[Page Warming<br/>Prefetching]
    METRICS[Metrics Collection<br/>Prometheus]
  end

  subgraph "Observability"
    PROM[Prometheus<br/>Metrics Export]
    HEALTH[Health Checks<br/>Readiness Probes]
    LOGS[Structured Logging<br/>Tracing]
    BUILD[Build Info<br/>Version Metadata]
  end

  %% Client connections
  C1 --> API
  C2 --> API
  C3 --> API

  %% API flow
  API --> RL
  RL --> AUTH
  AUTH --> WAL
  AUTH --> RMI

  %% Write flow
  WAL --> SEQ
  SEQ --> DELTA
  DELTA -.-> RMI

  %% Read flow
  RMI --> PCACHE
  PCACHE --> MMAP
  MMAP --> FS

  %% Storage connections
  WAL --> FS
  SNAP --> FS
  MANIFEST --> FS

  %% Background services
  COMPACT --> WAL
  COMPACT --> SNAP
  REBUILD --> RMI
  REBUILD --> MANIFEST
  WARMUP --> MMAP
  WARMUP --> RMI

  %% Observability
  METRICS --> PROM
  HEALTH --> API
  LOGS --> API
  BUILD --> API

  %% Styling
  classDef clientClass fill:#e1f5fe,stroke:#01579b,stroke-width:2px
  classDef apiClass fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
  classDef coreClass fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
  classDef storageClass fill:#fff3e0,stroke:#e65100,stroke-width:2px
  classDef backgroundClass fill:#fce4ec,stroke:#880e4f,stroke-width:2px
  classDef observabilityClass fill:#f3e5f5,stroke:#311b92,stroke-width:2px

  class C1,C2,C3 clientClass
  class API,AUTH,RL,CORS apiClass
  class WAL,DELTA,SEQ,RMI,PCACHE,MMAP coreClass
  class SNAP,MANIFEST,FS storageClass
  class COMPACT,REBUILD,WARMUP,METRICS backgroundClass
  class PROM,HEALTH,LOGS,BUILD observabilityClass
```

**Key Architecture Principles:**
- **Single Binary**: Everything runs in one process for simplicity and performance
- **Memory-Mapped Reads**: Direct OS-level access to hot data without copying
- **Atomic Operations**: Snapshots and index swaps never leave the system in inconsistent state
- **Background Isolation**: Maintenance tasks never block user operations
- **Observable by Default**: Rich metrics and health checks for operational visibility

---

## Read, write, and background flows

```mermaid
flowchart TD
  subgraph Write_Path
    P1[PUT key,value] --> P2[Validate]
    P2 --> P3[Append event to WAL]
    P3 --> P4[Update in-memory delta]
    P4 --> P5[Return offset]
  end

  subgraph Read_Path
    G1[GET key] --> G2[RMI predict and probe]
    G2 --> G3[Resolve offset]
    G3 --> G4[Fetch via mmap payload index]
    G4 --> G5{Cache hit?}
    G5 -- Yes --> G7[Return value]
    G5 -- No --> G6[Insert into payload cache]
    G6 --> G7[Return value]
    G2 -. miss in snapshot .-> G8[Read from delta]
    G8 --> G7
  end

  subgraph Background
    B0[Heuristics error probe len volume] --> B10[Rebuild RMI]
    B10 --> B11[Validate and checksum]
    B11 --> B12[Atomic swap and manifest]

    B20[Size or age triggers] --> B21[Compact WAL]
    B21 --> B22[Create snapshot bin plus data]
    B22 --> B23[Prune old segments]
  end
```

Notes:
- Reads prefer the snapshot‑backed path; the delta overlays recent writes.
- Snapshots produce two files: structured state (bin) and mmap‑friendly payload (data).

---

## RMI Architecture and Lookup Flow

```mermaid
graph TB
  subgraph "RMI Index Structure (On-Disk)"
    subgraph "Header (24 bytes)"
      MAGIC[RMI Magic<br/>8 bytes]
      VER[Version<br/>1 byte]
      FLAGS[Flags<br/>3 bytes]
      LEAF_COUNT[Leaf Count<br/>4 bytes]
      KEY_COUNT[Key Count<br/>8 bytes]
    end

    subgraph "Router Table (Dynamic Size)"
      ROUTER_BITS[Router Bits<br/>Configurable 8-24]
      ROUTER_ENTRIES["Router Entries<br/>2^bits × 4 bytes"]
    end

    subgraph "Leaf Models"
      LEAF_META[Leaf Metadata<br/>key_min, key_max,<br/>slope, intercept,<br/>epsilon, start, len]
      MODEL_PARAMS[Model Parameters<br/>Fixed-point arithmetic]
    end

    subgraph "Key Data (AoS v5 Format)"
      KEY_ARRAY[Sorted Keys<br/>u64 array]
      OFFSET_ARRAY[Offsets<br/>u32/u64 array<br/>16-byte stride]
    end

    subgraph "Integrity"
      CHECKSUM[xxHash3<br/>8 bytes]
    end
  end

  MAGIC --> VER
  VER --> FLAGS
  FLAGS --> LEAF_COUNT
  LEAF_COUNT --> KEY_COUNT
  KEY_COUNT --> ROUTER_BITS
  ROUTER_BITS --> ROUTER_ENTRIES
  ROUTER_ENTRIES --> LEAF_META
  LEAF_META --> MODEL_PARAMS
  MODEL_PARAMS --> KEY_ARRAY
  KEY_ARRAY --> OFFSET_ARRAY
  OFFSET_ARRAY --> CHECKSUM

  subgraph "Lookup Algorithm Flow"
    INPUT[Input Key<br/>u64] --> ROUTE["Router Lookup<br/>key shifted right by (64 - bits)"]
    ROUTE --> LEAF_SELECT[Select Leaf<br/>From router table]
    LEAF_SELECT --> PREDICT[Predict Position<br/>slope × key + intercept]
    PREDICT --> CLAMP["Clamp to Bounds<br/>max(0, min(pred ± ε, leaf_end))"]
    CLAMP --> PREFETCH[Prefetch Window<br/>OS page hints]
    PREFETCH --> PROBE{SIMD Probe<br/>AVX2/AVX512/NEON}
    
    PROBE --> HIT[Found Key<br/>Return offset]
    PROBE --> MISS[Not in window]
    MISS --> BINARY_SEARCH[Binary Search<br/>Bounded fallback]
    BINARY_SEARCH --> FOUND[Found in search<br/>Return offset]
    BINARY_SEARCH --> NOT_FOUND[Key not present<br/>Mispredict +1]
    
    HIT --> METRICS[Record probe len = 1]
    FOUND --> METRICS
    NOT_FOUND --> METRICS
  end

  subgraph "SIMD Probe Details"
    AVX512["AVX512 Path<br/>8-way gather + compare"]
    AVX2["AVX2 Path<br/>4-way gather + compare"]
    NEON["NEON Path<br/>2-way pairwise compare"]
    SCALAR["Scalar Fallback<br/>4-way unrolled loop"]
  end

  PROBE -.-> AVX512
  PROBE -.-> AVX2
  PROBE -.-> NEON
  PROBE -.-> SCALAR

  subgraph "Performance Optimizations"
    FIXED_POINT[Fixed-Point Math<br/>32-bit precision]
    PREFETCH_AHEAD[Prefetch Ahead<br/>64-byte strides]
    CACHE_ALIGNED[Cache Alignment<br/>Avoid false sharing]
    BRANCHLESS[Branchless Code<br/>Predictable execution]
  end

  PREDICT -.-> FIXED_POINT
  PREFETCH -.-> PREFETCH_AHEAD
  PROBE -.-> CACHE_ALIGNED
  BINARY_SEARCH -.-> BRANCHLESS

  %% Styling
  classDef headerClass fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
  classDef routerClass fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
  classDef leafClass fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
  classDef dataClass fill:#fff3e0,stroke:#e65100,stroke-width:2px
  classDef lookupClass fill:#fce4ec,stroke:#880e4f,stroke-width:2px
  classDef simdClass fill:#e1f5fe,stroke:#01579b,stroke-width:2px
  classDef perfClass fill:#f3e5f5,stroke:#311b92,stroke-width:2px

  class MAGIC,VER,FLAGS,LEAF_COUNT,KEY_COUNT headerClass
  class ROUTER_BITS,ROUTER_ENTRIES routerClass
  class LEAF_META,MODEL_PARAMS leafClass
  class KEY_ARRAY,OFFSET_ARRAY,CHECKSUM dataClass
  class INPUT,ROUTE,LEAF_SELECT,PREDICT,CLAMP lookupClass
  class PREFETCH,PROBE,HIT,MISS,BINARY_SEARCH,FOUND,NOT_FOUND,METRICS lookupClass
  class AVX512,AVX2,NEON,SCALAR simdClass
  class FIXED_POINT,PREFETCH_AHEAD,CACHE_ALIGNED,BRANCHLESS perfClass
```

**RMI Design Principles:**
- **Learned Index**: Uses machine learning models to predict key positions
- **Bounded Search**: Epsilon bounds limit search space to O(1) expected time
- **SIMD Acceleration**: Runtime CPU feature detection for optimal performance
- **Memory Efficiency**: AoS layout with 16-byte stride for gather operations
- **Crash Safety**: Checksummed files with atomic replacement

---

## Durability and recovery

```mermaid
sequenceDiagram
  participant Client
  participant Engine
  participant WAL
  participant Snapshot
  participant Index
  participant Manifest

  Note over Client,Manifest: Normal write
  Client->>Engine: PUT(key,value)
  Engine->>WAL: append
  WAL-->>Engine: fsync ok
  Engine-->>Client: offset

  Note over Client,Manifest: Snapshot
  Engine->>Snapshot: build snapshot.bin + snapshot.data
  Snapshot->>Manifest: write + fsync
  Snapshot-->>Engine: ready

  Note over Client,Manifest: RMI rebuild
  Engine->>Index: build from snapshot
  Index->>Index: validate + checksum
  Index->>Manifest: atomic rename + update
  Index-->>Engine: swap pointer

  Note over Client,Manifest: Recovery
  Engine->>Manifest: read
  Engine->>Snapshot: mmap payload
  Engine->>WAL: replay tail
  Engine->>Index: load if present
  Engine-->>Engine: serve traffic
```

---

## API surface (v1) and ops knobs

- Data/control endpoints:
  - POST /v1/put, GET /v1/get_fast/{key}
  - POST /v1/snapshot, POST /v1/rmi/build, POST /v1/warmup
  - GET /health, GET /build_info, GET /metrics
- Security/ops:
  - Bearer auth header (optional)
  - Per‑IP rate limiting (env‑controlled)
  - KYRODB_WARM_ON_START, KYRODB_DISABLE_HTTP_LOG; RUST_LOG for logging

---

## Performance and benchmarking philosophy

- Bench warm vs cold explicitly; snapshot → rmi/build → warmup before measuring.
- Measure both overall and "during rebuild" lookup latency; watch fallback scan counters.
- Prefer full‑throttle tests with per‑request logging off and generous rate limits.
- Check mmap fast path is active (snapshot.data present) to avoid O(n) scans.

---

## Performance Characteristics

```mermaid
graph LR
  subgraph "Latency Distribution"
    P99[P99 Latency<br/>Tail Performance]
    P50[P50 Latency<br/>Median Performance]
    AVG[Average Latency<br/>Mean Performance]
  end

  subgraph "Optimization Layers"
    SIMD[SIMD Acceleration<br/>AVX2/AVX512/NEON]
    MMAP[Memory Mapping<br/>Zero-copy reads]
    PREFETCH[Prefetching<br/>OS page hints]
    CACHE[Caching<br/>LRU payload cache]
  end

  subgraph "Workload Adaptation"
    ADAPTIVE[Adaptive Rebuild<br/>Mispredict thresholds]
    WARMING[Page Warming<br/>On startup]
    TUNING[Runtime Tuning<br/>Environment knobs]
  end

  subgraph "Metrics & Monitoring"
    PROBE_LEN[Probe Length<br/>Histogram]
    MISPREDICT[Mispredict Rate<br/>Counter]
    CACHE_HIT[Cache Hit Rate<br/>Gauge]
    REBUILD_TIME[Rebuild Duration<br/>Histogram]
  end

  P99 --> SIMD
  P50 --> MMAP
  AVG --> PREFETCH
  SIMD --> CACHE
  MMAP --> CACHE
  PREFETCH --> CACHE

  CACHE --> ADAPTIVE
  ADAPTIVE --> WARMING
  WARMING --> TUNING

  TUNING --> PROBE_LEN
  TUNING --> MISPREDICT
  TUNING --> CACHE_HIT
  TUNING --> REBUILD_TIME

  %% Performance targets
  subgraph "Target Performance"
    TARGET_P50["P50 < 10μs<br/>Point lookups"]
    TARGET_P99["P99 < 100μs<br/>Tail latency"]
    TARGET_QPS["QPS > 100K<br/>Sustained throughput"]
  end

  PROBE_LEN --> TARGET_P50
  MISPREDICT --> TARGET_P99
  CACHE_HIT --> TARGET_QPS
```
```mermaid
graph TB
  subgraph "Benchmark Results (Example)"
    COLD_START[Cold Start<br/>First request latency]
    WARM_START[Warm Start<br/>After warmup]
    DURING_REBUILD[During Rebuild<br/>Background interference]
    STEADY_STATE[Steady State<br/>Normal operation]
  end

  subgraph "Latency Breakdown"
    ROUTER_LOOKUP[Router Lookup<br/>O(1) hash]
    MODEL_PREDICT[Model Prediction<br/>Fixed-point math]
    SIMD_PROBE[SIMD Probe<br/>Vectorized search]
    PAYLOAD_FETCH[Payload Fetch<br/>mmap access]
    CACHE_LOOKUP[Cache Lookup<br/>Hash table]
  end

  COLD_START --> ROUTER_LOOKUP
  WARM_START --> MODEL_PREDICT
  DURING_REBUILD --> SIMD_PROBE
  STEADY_STATE --> PAYLOAD_FETCH

  ROUTER_LOOKUP --> MODEL_PREDICT
  MODEL_PREDICT --> SIMD_PROBE
  SIMD_PROBE --> CACHE_LOOKUP
  CACHE_LOOKUP --> PAYLOAD_FETCH

  subgraph "Optimization Opportunities"
    OP1[Vectorize router<br/>SIMD hash]
    OP2[Prefetch leaf models<br/>Cache-aware layout]
    OP3[Batch predictions<br/>SIMD math]
    OP4[Parallel probes<br/>Multi-threaded]
    OP5[Compressed payloads<br/>Bandwidth optimization]
  end

  SIMD_PROBE -.-> OP1
  MODEL_PREDICT -.-> OP2
  ROUTER_LOOKUP -.-> OP3
  PAYLOAD_FETCH -.-> OP4
  CACHE_LOOKUP -.-> OP5

  %% Styling
  classDef perfClass fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
  classDef optClass fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
  classDef benchClass fill:#fff3e0,stroke:#e65100,stroke-width:2px
  classDef targetClass fill:#fce4ec,stroke:#880e4f,stroke-width:2px

  class P99,P50,AVG,SIMD,MMAP,PREFETCH,CACHE perfClass
  class ADAPTIVE,WARMING,TUNING optClass
  class PROBE_LEN,MISPREDICT,CACHE_HIT,REBUILD_TIME benchClass
  class TARGET_P50,TARGET_P99,TARGET_QPS targetClass
  class COLD_START,WARM_START,DURING_REBUILD,STEADY_STATE benchClass
  class ROUTER_LOOKUP,MODEL_PREDICT,SIMD_PROBE,PAYLOAD_FETCH,CACHE_LOOKUP perfClass
  class OP1,OP2,OP3,OP4,OP5 optClass
```
**Performance Philosophy:**
- **Measure Everything**: Rich metrics for latency, throughput, and cache efficiency
- **Optimize Tails**: P99 latency matters more than averages for user experience
- **Adaptive Behavior**: System learns and adapts to workload patterns
- **Memory Efficiency**: Zero-copy reads and intelligent caching reduce GC pressure

---

## End‑state vision (the “final point”)

KyroDB becomes an AI‑native database kernel with:
- Unified, immutable history (WAL) plus snapshot epochs for time travel and audits.
- Learned indexing as a first‑class primitive that adapts to workload and data drift.
- Optional vector storage/ANN behind a trait; filters and hybrid lookups in one place.
- Built‑in provenance: lineage of data, models, and inferences; reproducible pipelines.
- Self‑tuning policies with human‑visible safety rails and clear explainability hooks.
- A path to distribution (replication, sharding) that preserves the same invariants.

Success looks like: reproducible research artifacts; stable 1.0 used in production; a community that trusts the numbers and the durability story.

---

## How we’ll get there (strategy and milestones)

Pillars:
- Correctness first: learning augments performance, never weakens guarantees.
- Evidence over claims: benchmarks, CI, fuzzing, failpoints; publish scripts and CSVs.
- Small, strong kernel: clear traits for storage/indexes; feature plugins on top.
- Operational clarity: simple defaults, strong observability, safe rebuild/compaction.



## Risks and mitigations

- Rebuild interference with latency tails → segment metrics; throttle; delta‑first reads; atomic swap.
- Learned index drift → mispredict/probe metrics; rebuild thresholds; checksummed artifacts.
- mmap portability and OS quirks → clear tuning docs; fallbacks; checks on load.
- Durability regressions → failpoints for snapshot/rename/fsync; recovery tests; fuzzing.

---

## Who is it for (use cases)

- AI startups building RAG systems that want provenance and fewer moving parts.
- Services with heavy point lookups needing compact indexes and tight p99.
- Researchers exploring learned indexes with reproducible, honest results.
- Regulated workloads needing immutable logs plus search/audit hooks.

---

## Success signals

- Engineering: green CI with fuzz/failpoints; recovery matrix stable.
- Performance: tail latency improvements on realistic workloads; transparent plots.
- Research: public preprint + artifact; external reproductions of benchmarks.
- Adoption: external users running benches and pilots; constructive community.

---

## Appendix

- Metrics: /metrics (Prometheus). Includes RMI lookup latency and probe length, fallback counters, rebuild progress, WAL rotation/retention stats.
- Operational endpoints: /health, /build_info, /v1/snapshot, /v1/rmi/build, /v1/warmup.
- Helpful env toggles: warm on start, rate limit knobs, per‑request logging toggle.
- Benchmarks: in‑process (cargo bench -p bench --bench kv_index) and end‑to‑end (bench/README.bench.md).



---

## Data Flow and Consistency Model

```mermaid
stateDiagram-v2
  [*] --> Ready: Engine starts
  Ready --> AcceptingWrites: WAL ready
  AcceptingWrites --> WriteInProgress: PUT request
  WriteInProgress --> AcceptingWrites: WAL append + fsync
  AcceptingWrites --> SnapshotInProgress: Size/time threshold
  SnapshotInProgress --> AcceptingWrites: Atomic snapshot complete
  AcceptingWrites --> RebuildInProgress: Mispredict threshold
  RebuildInProgress --> AcceptingWrites: RMI swap complete
  
  note right of WriteInProgress
    - Append to WAL
    - Update delta map
    - Return offset
  end note
  
  note right of SnapshotInProgress
    - Create snapshot.bin
    - Create snapshot.data
    - Update manifest.json
    - Prune old WAL segments
  end note
  
  note right of RebuildInProgress
    - Build RMI from snapshot
    - Validate checksums
    - Atomic pointer swap
    - Update manifest
  end note
```

```mermaid
flowchart TD
  subgraph "Write Path (Fast Path)"
    W1[PUT /v1/put] --> W2[Validate Request]
    W2 --> W3[Generate Offset]
    W3 --> W4[Append to WAL<br/>+ fsync]
    W4 --> W5[Insert to Delta<br/>In-memory]
    W5 --> W6[Return Offset<br/>HTTP 200]
  end

  subgraph "Read Path (Multi-Level)"
    R1["GET /v1/get_fast/{key}"] --> R2[Check Delta<br/>Recent writes]
    R2 --> R3{Found in<br/>Delta?}
    R3 -->|Yes| R4[Return Value<br/>From memory]
    R3 -->|No| R5[RMI Lookup<br/>Learned index]
    R5 --> R6[Predict Position<br/>Model inference]
    R6 --> R7[Bounded Probe<br/>SIMD search]
    R7 --> R8{Found in<br/>Window?}
    R8 -->|Yes| R9[Get Offset<br/>From index]
    R8 -->|No| R10[Binary Search<br/>Fallback]
    R10 --> R11{Found in<br/>Search?}
    R11 -->|Yes| R9
    R11 -->|No| R12[Not Found<br/>Return 404]
    R9 --> R13[Fetch Payload<br/>mmap snapshot.data]
    R13 --> R14{Cache Hit?}
    R14 -->|Yes| R15[Return Cached<br/>Value]
    R14 -->|No| R16[Insert to Cache<br/>LRU eviction]
    R16 --> R15
  end

  subgraph "Background Operations"
    B1[Compaction Trigger<br/>Size/Age] --> B2[Create Snapshot<br/>snapshot.bin + .data]
    B2 --> B3[Update Manifest<br/>Atomic rename]
    B3 --> B4[Prune WAL<br/>Retention policy]
    
    B5[RMI Trigger<br/>Mispredict volume] --> B6[Build New Index<br/>From snapshot]
    B6 --> B7[Validate Index<br/>Checksum + bounds]
    B7 --> B8[Atomic Swap<br/>Update manifest]
    B8 --> B9[Cleanup Old<br/>Index files]
  end

  subgraph "Consistency Guarantees"
    C1[Linearizable Reads<br/>Delta overlays snapshot]
    C2[Atomic Snapshots<br/>All-or-nothing state]
    C3[Durable WAL<br/>fsync before ack]
    C4[Monotonic Offsets<br/>Strictly increasing]
  end

  %% Styling
  classDef writeClass fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
  classDef readClass fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
  classDef backgroundClass fill:#fff3e0,stroke:#e65100,stroke-width:2px
  classDef consistencyClass fill:#fce4ec,stroke:#880e4f,stroke-width:2px

  class W1,W2,W3,W4,W5,W6 writeClass
  class R1,R2,R3,R4,R5,R6,R7,R8,R9,R10,R11,R12,R13,R14,R15,R16 readClass
  class B1,B2,B3,B4,B5,B6,B7,B8,B9 backgroundClass
  class C1,C2,C3,C4 consistencyClass
```

```mermaid
graph TB
  subgraph "Phase A: Foundation"
    A1[Single Node KV<br/>WAL + Snapshots]
    A2[Learned Index RMI<br/>Bounded search]
    A3[HTTP API<br/>RESTful interface]
    A4[Observability<br/>Metrics + health]
  end

  subgraph "Phase B: Polish"
    B1[SIMD Optimization<br/>AVX2/AVX512/NEON]
    B2[Adaptive Tuning<br/>Runtime knobs]
    B3[Benchmark Suite<br/>Performance validation]
    B4[Documentation<br/>Operational guides]
  end

  subgraph "Phase C: Primitives"
    C1[Vector Storage<br/>ANN/HNSW backend]
    C2[Hybrid Queries<br/>Multi-modal search]
    C3[Provenance<br/>Data lineage]
    C4[Plugin System<br/>Extensible backends]
  end

  subgraph "Phase D: Autonomy"
    D1[Self-Tuning<br/>Policy engine]
    D2[Governance<br/>Compliance framework]
    D3[Audit Trails<br/>Immutable logs]
    D4[Multi-Tenant<br/>Namespace isolation]
  end

  subgraph "Phase E: Scale"
    E1[Replication<br/>Consensus protocol]
    E2[Sharding<br/>Horizontal scaling]
    E3[Distributed RMI<br/>Global indexes]
    E4[Federation<br/>Multi-cluster]
  end

  A1 --> B1
  A2 --> B2
  A3 --> B3
  A4 --> B4

  B1 --> C1
  B2 --> C2
  B3 --> C3
  B4 --> C4

  C1 --> D1
  C2 --> D2
  C3 --> D3
  C4 --> D4

  D1 --> E1
  D2 --> E2
  D3 --> E3
  D4 --> E4

  subgraph "Success Metrics"
    M1[Production Deployments<br/>External users]
    M2[Performance Benchmarks<br/>Published results]
    M3[Research Citations<br/>Academic papers]
    M4[Community Ecosystem<br/>Extensions + tools]
  end

  E1 --> M1
  E2 --> M2
  E3 --> M3
  E4 --> M4

  %% Styling
  classDef phaseAClass fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
  classDef phaseBClass fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
  classDef phaseCClass fill:#fff3e0,stroke:#e65100,stroke-width:2px
  classDef phaseDClass fill:#fce4ec,stroke:#880e4f,stroke-width:2px
  classDef phaseEClass fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
  classDef successClass fill:#e1f5fe,stroke:#01579b,stroke-width:2px

  class A1,A2,A3,A4 phaseAClass
  class B1,B2,B3,B4 phaseBClass
  class C1,C2,C3,C4 phaseCClass
  class D1,D2,D3,D4 phaseDClass
  class E1,E2,E3,E4 phaseEClass
  class M1,M2,M3,M4 successClass
```