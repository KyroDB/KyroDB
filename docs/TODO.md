# KyroDB TODO (from review)

This TODO captures mismatches and issues and tracks fixes.

- [x] API parity between engine and CLI:
  - [x] Implement /health in engine
  - [x] Implement /offset in engine (JSON {"offset": u64})
  - [x] Implement /snapshot in engine
  - [x] Ensure /sql, /lookup, /vector/insert, /vector/search exist (verified)
  - [x] Feature-gated /rmi/build exists under `learned-index` (returns 501 when disabled)
  - [x] Expose /metrics and document it (now added to routes)

- [x] CLI vector-search `k` handling
  - [x] Parse `k` as integer and send numeric JSON (engine expects number). Already handled in CLI via `strconv.Atoi` and sending number.

- [x] Offset command handling in CLI
  - [x] Check HTTP status before decoding JSON (present in code)

- [x] Repo hygiene: ignore data artifacts
  - [x] .gitignore includes data/*.bin entries under root/engine/orchestrator
  - [x] Remove committed binaries from git index (git rm --cached done)

- [x] Naming/branding consistency
  - [x] Engine about string: "KyroDB Engine"
  - [x] CLI short description: "KyroDB orchestrator CLI"
  - [x] Warp log target updated from "ngdb" to "kyrodb"
  - [x] Repository-wide rename: ngdb → kyrodb (crate, imports, metrics names, docs)

- [x] Docs cleanup
  - [x] README and visiondocument cross-reference clearly

- [x] Operational gaps
  - [x] Optional bearer auth for protected endpoints (flag: --auth-token)
  - [x] Basic SSE resilience metric (increments on lagged/failed sends)
  - [x] Snapshot policies: add --auto-snapshot-secs and --snapshot-every-n-appends

- [x] Feature flags vs code
  - [x] Confirmed features in engine/Cargo.toml (`learned-index`, `ann-hnsw`)
  - [x] Clear 501 error when feature route disabled for /rmi/build

