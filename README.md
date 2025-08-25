# KyroDB — Durable KV with a Production Recursive Model Index (RMI)

Status: Alpha (focused scope: KV + RMI)

KyroDB is a durable, append-only key-value engine with a production-grade learned index (RMI) for ultra-fast point lookups and predictable tail latency.

- Default read path: RMI (learned-index) with SIMD-accelerated probing
- WAL + snapshot durability, fast recovery, compaction controls
- Simple HTTP API under /v1, Prometheus metrics at /metrics, build info at /build_info

For an even shorter guide, see README-SHORT.md.

---

## Quickstart

Prereqs: Rust toolchain. Optional: Go for the CLI in `orchestrator/`.

- Build: `cargo build -p engine --release` (binary: `target/release/kyrodb-engine`)
- Run: `target/release/kyrodb-engine serve 127.0.0.1 8080`

Basic checks

- Health: `curl -s http://127.0.0.1:8080/health`
- Offset: `curl -s http://127.0.0.1:8080/v1/offset`
- Put: `curl -sX POST http://127.0.0.1:8080/v1/put -H 'content-type: application/json' -d '{"key":123,"value":"hello"}'`
- Get (fast): `curl -i http://127.0.0.1:8080/v1/get_fast/123`

Warm start (avoids cold-page tails)

- Auto: set env `KYRODB_WARM_ON_START=1` before starting
- Manual: `POST /v1/rmi/build` (if needed) then `POST /v1/warmup`

---

## API (selected)

Data plane (JSON unless noted)

- POST `/v1/put` → `{ "offset": <u64> }` on success
- GET `/v1/lookup_fast/{key}` → 200 octet-stream (8-byte offset) or 404
- GET `/v1/get_fast/{key}` → value bytes or 404
- GET `/v1/lookup?key=...` → JSON object on hit, `{ "error": "not found" }` on miss
- GET `/v1/lookup_raw?key=...` → 204 on hit, 404 on miss

Admin/ops

- POST `/v1/rmi/build` → `{ ok: bool, count: <usize> }`
- POST `/v1/warmup` → `{ status: "ok" }`
- POST `/v1/snapshot`, POST `/v1/compact`, GET `/v1/offset`, `POST /v1/replay`
- Metrics: GET `/metrics` (Prometheus)
- Build info: GET `/build_info` → `{ commit, features }`

Auth: start the server with `--auth-token <TOKEN>` and send `Authorization: Bearer <TOKEN>`.

---

## Performance notes

- Warm vs cold: first queries pay OS page-fault costs. Prefer warm runs (env `KYRODB_WARM_ON_START=1` or call `/v1/warmup`).
- SIMD: runtime-detected (AVX2/AVX-512 on x86_64; NEON/scalar fallback). Use `--release`.
- Index-only vs full read: use `/v1/lookup_fast/{k}` for index-only; `/v1/get_fast/{k}` includes value read.

---

## Benchmarks

Reproducible HTTP workload bench and plots live under `bench/`. See `bench/README.bench.md`.

Typical flow

1) Start engine (release, learned-index default)

2) Load and read with bench client, export CSV:

```
COMMIT=$(git rev-parse --short HEAD)
cargo run -p bench --release -- \
  --base http://127.0.0.1:8080 \
  --load-n 10000000 \
  --val-bytes 16 \
  --load-concurrency 64 \
  --read-concurrency 64 \
  --read-seconds 30 \
  --dist uniform \
  --out-csv bench/results/${COMMIT}/http_uniform_10m.csv
```

3) Generate plots:

```
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt || pip install numpy matplotlib
python comp.py
```

---

## Operations

- Metrics: scrape `/metrics`
- Rate limiting (per-IP):
  - Admin routes default 2 rps, burst 5
  - Data routes default 5000 rps, burst 10000
  - Tune via env: `KYRODB_RL_ADMIN_RPS`, `KYRODB_RL_ADMIN_BURST`, `KYRODB_RL_DATA_RPS`, `KYRODB_RL_DATA_BURST`
- TLS: run behind a reverse proxy. Examples:

Caddy

```
# Caddyfile
kyro.example.com {
  reverse_proxy 127.0.0.1:8080
  header {
    Strict-Transport-Security "max-age=31536000; includeSubDomains; preload"
  }
}
```

Nginx

```
server {
  listen 443 ssl http2;
  server_name kyro.example.com;
  ssl_certificate /etc/letsencrypt/live/kyro.example.com/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/kyro.example.com/privkey.pem;
  location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;
  }
}
```

Additional operational notes live in `docs/`.

---

## Architecture (brief)

- WAL (append-only) + snapshot for durability and fast recovery
- In-memory recent-write delta; single-node read path
- RMI builder/loader with bounded probe; on-disk formats versioned
- Compaction triggers for WAL space management

---

## Contributing and License

- Contributions welcome. Include tests for WAL/snapshot/RMI changes.
- License: Apache-2.0

Contact: open an issue; for quicker feedback ping @vatskishan03.
