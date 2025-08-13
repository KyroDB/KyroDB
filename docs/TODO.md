# KyroDB TODO (from review)

This TODO captures mismatches and issues and tracks fixes.

- [x] API parity between engine and CLI:
  - [x] Implement /health in engine
  - [x] Implement /offset in engine (JSON {"offset": u64})
  - [x] Implement /snapshot in engine
  - [x] Ensure /sql, /lookup, /vector/insert, /vector/search exist (verified)
  - [x] Feature-gated /rmi/build exists under `learned-index`
  - [x] Expose /metrics and document it (now added to routes)

- [x] CLI vector-search `k` handling
  - [x] Parse `k` as integer and send numeric JSON (engine expects number). Already handled in CLI via `strconv.Atoi` and sending number.

- [x] Offset command handling in CLI
  - [x] Check HTTP status before decoding JSON (present in code)

- [x] Repo hygiene: ignore data artifacts
  - [x] .gitignore includes data/*.bin entries under root/engine/orchestrator
  - [ ] Remove any committed binaries from git history/index (manual `git rm --cached` needed)

- [x] Naming/branding consistency
  - [x] Engine about string: "KyroDB Engine"
  - [x] CLI short description: "KyroDB orchestrator CLI"
  - [x] Warp log target updated from "ngdb" to "kyrodb"
  - [ ] Review README and binaries for remaining "ngdb" mentions (manual sweep)

- [ ] Docs cleanup
  - [ ] Ensure README and visiondocument refer to each other correctly; reword if confusing

- [ ] Operational gaps (future tasks)
  - [ ] Auth/token for HTTP endpoints (dev only now)
  - [ ] Backpressure for SSE /subscribe
  - [ ] Snapshot/compaction policy configuration

- [ ] Feature flags vs code
  - [x] Confirmed features in engine/Cargo.toml (`learned-index`, `ann-hnsw`)
  - [ ] Return clear error/404 when feature routes disabled (currently route absent)

