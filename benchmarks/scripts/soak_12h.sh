#!/usr/bin/env bash
# 12-hour soak test harness for KyroDB ANN search.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

DATASET="${SOAK_DATASET:-glove-100-angular}"
K="${SOAK_K:-10}"
EF_SEARCH="${SOAK_EF_SEARCH:-200}"
CONCURRENCY="${SOAK_CONCURRENCY:-16}"
MAX_QUERIES="${SOAK_MAX_QUERIES:-5000}"
WARMUP_QUERIES="${SOAK_WARMUP_QUERIES:-500}"
MAX_TRAIN="${SOAK_MAX_TRAIN:-}"
DURATION_HOURS="${SOAK_DURATION_HOURS:-12}"
OUTPUT_ROOT="${SOAK_OUTPUT_DIR:-benchmarks/results/soak_$(date -u +%Y%m%d_%H%M%S)}"
CONFIG_PATH="${SOAK_CONFIG:-}"

mkdir -p "$OUTPUT_ROOT"
echo "Soak results: $OUTPUT_ROOT"

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

cargo build --release --bin kyrodb_server
python3 benchmarks/scripts/gen_grpc_stubs.py
python3 benchmarks/scripts/collect_env.py --out "$OUTPUT_ROOT/env.json" || true

pin_cmd=()
if command -v taskset >/dev/null 2>&1; then
  max_cpu=$(($(nproc) - 1))
  if (( max_cpu > 15 )); then max_cpu=15; fi
  pin_cmd=(taskset -c "0-$max_cpu")
fi

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

if [[ -z "$CONFIG_PATH" ]]; then
  case "$DATASET" in
    sift-128-euclidean)
      DIM=128; DIST="euclidean"; M=16; EF_C=200;;
    glove-100-angular)
      DIM=100; DIST="cosine"; M=16; EF_C=200;;
    gist-960-euclidean)
      DIM=960; DIST="euclidean"; M=32; EF_C=400;;
    *)
      echo "Unknown dataset '$DATASET'. Provide SOAK_CONFIG=/path/to/config.toml" >&2
      exit 2
      ;;
  esac
  DATA_DIR="$OUTPUT_ROOT/data_${DATASET//[^a-zA-Z0-9]/_}"
  CONFIG_PATH="$OUTPUT_ROOT/server_${DATASET//[^a-zA-Z0-9]/_}.toml"
  make_config "$CONFIG_PATH" "$DATA_DIR" "$DIM" "$DIST" "$M" "$EF_C" "$EF_SEARCH" 2000000
fi

start_server() {
  local config_path="$1"
  echo "Starting server with config: $config_path"
  pkill -9 kyrodb_server 2>/dev/null || true
  sleep 1

  ${pin_cmd[@]+${pin_cmd[@]}} ./target/release/kyrodb_server --config "$config_path" \
    >"$OUTPUT_ROOT/server.log" 2>&1 &
  SERVER_PID=$!

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

trap stop_server EXIT

run_benchmark() {
  local skip_index="$1"
  local extra_args=()
  if [[ -n "$MAX_TRAIN" ]]; then
    extra_args+=(--max-train "$MAX_TRAIN" --recompute-ground-truth)
  fi
  if [[ "$skip_index" == "true" ]]; then
    extra_args+=(--skip-index)
  fi
  python3 benchmarks/run_benchmark.py \
    --dataset "$DATASET" \
    --k "$K" \
    --host 127.0.0.1 \
    --ef-search "$EF_SEARCH" \
    --max-queries "$MAX_QUERIES" \
    --warmup-queries "$WARMUP_QUERIES" \
    --repetitions 1 \
    --concurrency "$CONCURRENCY" \
    --output-dir "$OUTPUT_ROOT" \
    ${extra_args[@]:+"${extra_args[@]}"}
}

start_server "$CONFIG_PATH"

echo "Indexing for soak dataset..."
run_benchmark "false"

start_ts=$(date +%s)
end_ts=$((start_ts + DURATION_HOURS * 3600))
iteration=1

while (( $(date +%s) < end_ts )); do
  echo "Soak iteration $iteration..."
  run_benchmark "true"
  iteration=$((iteration + 1))
done

echo "Soak complete. Results in: $OUTPUT_ROOT"
