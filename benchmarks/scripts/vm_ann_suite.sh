#!/usr/bin/env bash
# VM-ready ANN dataset suite (end-to-end KyroDB server benchmark).
#
# Produces a timestamped results directory containing:
# - env.json (system + git + pip freeze)
# - one JSON result per run (dataset x ef_search x concurrency)
#
# Assumes Linux. Uses taskset if available.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

RESULTS_ROOT="benchmarks/results/vm_suite_$(date -u +%Y%m%d_%H%M%S)"
# Optional cleanup: set CLEAN_RESULTS=true to remove prior results.
if [[ "${CLEAN_RESULTS:-}" == "true" ]]; then
  rm -rf benchmarks/results/*
fi
mkdir -p "$RESULTS_ROOT"

echo "Results: $RESULTS_ROOT"

python3 - <<'PY'
import sys

missing = []
for mod in ("grpc", "grpc_tools", "h5py", "numpy"):
  try:
    __import__(mod)
  except Exception:
    missing.append(mod)

if missing:
  sys.stderr.write(
    "Missing Python deps: %s\n" % ", ".join(missing)
    + "Install with: python3 -m venv .venv && source .venv/bin/activate && pip install -r benchmarks/requirements.txt\n"
  )
  raise SystemExit(2)
PY

# Build server
cargo build --release --bin kyrodb_server

# Generate gRPC stubs (required by benchmarks/run_benchmark.py)
python3 benchmarks/scripts/gen_grpc_stubs.py

# Collect environment info
python3 benchmarks/scripts/collect_env.py --out "$RESULTS_ROOT/env.json" || true

pin_cmd=()
if command -v taskset >/dev/null 2>&1; then
  max_cpu=$(($(nproc) - 1))
  if (( max_cpu > 15 )); then max_cpu=15; fi
  pin_cmd=(taskset -c "0-$max_cpu")
fi

start_server() {
  local config_path="$1"
  echo "Starting server with config: $config_path"
  pkill -9 kyrodb_server 2>/dev/null || true
  sleep 1

  ${pin_cmd[@]+${pin_cmd[@]}} ./target/release/kyrodb_server --config "$config_path" &
  SERVER_PID=$!

  # 30s best-effort health wait
  for _ in $(seq 1 60); do
    if command -v curl >/dev/null 2>&1; then
      if curl -sf "http://127.0.0.1:51051/health" >/dev/null 2>&1; then
        return 0
      fi
    else
      if python3 - <<'PY' >/dev/null 2>&1
import urllib.request

with urllib.request.urlopen("http://127.0.0.1:51051/health", timeout=1.0) as r:
    r.read(1)
PY
      then
        return 0
      fi
    fi
    sleep 0.5
  done

  echo "Server failed to become healthy" >&2
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  return 1
}

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    unset SERVER_PID
  fi
  pkill -9 kyrodb_server 2>/dev/null || true
}

make_config() {
  local out="$1"
  local data_dir="$2"
  local dim="$3"
  local distance="$4"
  local m="$5"
  local ef_c="$6"
  local ef_s="$7"
  local max_elements="$8"

  cat >"$out" <<EOF
[server]
host = "0.0.0.0"
port = 50051
http_port = 51051

[cache]
capacity = $((max_elements * 2))
strategy = "lru"
auto_tune_threshold = false
enable_training_task = false

[hnsw]
max_elements = $max_elements
m = $m
ef_construction = $ef_c
ef_search = $ef_s
dimension = $dim
distance = "$distance"

[persistence]
data_dir = "$data_dir"
fsync_policy = "none"
wal_flush_interval_ms = 100
snapshot_interval_mutations = 0
max_wal_size_bytes = 1073741824
enable_recovery = true
allow_fresh_start_on_recovery_failure = true

[logging]
level = "warn"
EOF
}

run_dataset_suite() {
  local dataset="$1"
  local dim="$2"
  local distance="$3"
  local m="$4"
  local ef_c="$5"

  local max_elements=2000000
  local data_dir="$RESULTS_ROOT/data_${dataset//[^a-zA-Z0-9]/_}"
  local cfg="$RESULTS_ROOT/server_${dataset//[^a-zA-Z0-9]/_}.toml"

  make_config "$cfg" "$data_dir" "$dim" "$distance" "$m" "$ef_c" 50 "$max_elements"
  start_server "$cfg"

  local max_q=10000
  if [[ "$dataset" == "gist-960-euclidean" ]]; then
    max_q=1000
  fi

  # First run does indexing.
  python3 benchmarks/run_benchmark.py \
    --dataset "$dataset" \
    --k 10 \
    --host 127.0.0.1 \
    --ef-search 50 \
    --max-queries "$max_q" \
    --warmup-queries 200 \
    --repetitions 3 \
    --concurrency 1 \
    --output-dir "$RESULTS_ROOT" \
    || { stop_server; return 1; }

  # Skip ef_s=50/conc=1 since it was already run during indexing
  for ef_s in 10 20 50 100 200 400 800; do
    for conc in 1 4 8 16; do
      if [[ "$ef_s" -eq 50 && "$conc" -eq 1 ]]; then
        continue
      fi
      python3 benchmarks/run_benchmark.py \
        --dataset "$dataset" \
        --k 10 \
        --host 127.0.0.1 \
        --ef-search "$ef_s" \
        --max-queries "$max_q" \
        --warmup-queries 200 \
        --repetitions 3 \
        --concurrency "$conc" \
        --skip-index \
        --output-dir "$RESULTS_ROOT" \
        || { stop_server; return 1; }
    done
  done

  stop_server
}

trap stop_server EXIT

# Dimension-aware HNSW parameters for optimal recall/QPS trade-off:
# - SIFT-128:  M=16, ef_c=200 (standard, works well for low-dim)
# - GloVe-100: M=16, ef_c=200 (standard)
# - GIST-960:  M=32, ef_c=400 (high-dim needs more connectivity)
run_dataset_suite "sift-128-euclidean" 128 "euclidean" 16 200
run_dataset_suite "glove-100-angular" 100 "cosine" 16 200
run_dataset_suite "gist-960-euclidean" 960 "euclidean" 32 400

echo "Done. Results in: $RESULTS_ROOT"
