#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

require_cmd cargo
require_cmd awk
require_cmd date
require_cmd tee
require_cmd sed
require_cmd grep
require_cmd seq
require_cmd lsof

BUILD_BINARIES="${BUILD_BINARIES:-1}"
SERVER_BIN="${SERVER_BIN:-$ROOT_DIR/target/release/kyrodb_server}"
LOAD_TESTER_BIN="${LOAD_TESTER_BIN:-$ROOT_DIR/target/release/kyrodb_load_tester}"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config.example.toml}"

SOAK_CYCLES="${SOAK_CYCLES:-6}"
CYCLE_DURATION_SECS="${CYCLE_DURATION_SECS:-120}"
KILL_EVERY_N_CYCLES="${KILL_EVERY_N_CYCLES:-3}"
QPS="${QPS:-1200}"
CONCURRENCY="${CONCURRENCY:-32}"
DATASET_SIZE="${DATASET_SIZE:-4096}"
LOAD_TESTER_REQUEST_TIMEOUT_MS="${LOAD_TESTER_REQUEST_TIMEOUT_MS:-5000}"
LOAD_TESTER_MAX_FAILURE_RATIO="${LOAD_TESTER_MAX_FAILURE_RATIO:-0.5}"
PORT="${PORT:-}"
STARTUP_TIMEOUT_SECS="${STARTUP_TIMEOUT_SECS:-30}"
GRACEFUL_SHUTDOWN_TIMEOUT_SECS="${GRACEFUL_SHUTDOWN_TIMEOUT_SECS:-20}"
LOAD_PHASE_TIMEOUT_SECS="${LOAD_PHASE_TIMEOUT_SECS:-$((CYCLE_DURATION_SECS + 120))}"

port_in_use() {
  local port="$1"
  lsof -n -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

resolve_ports() {
  if [[ -n "${PORT}" ]]; then
    local http_port_fixed=$((PORT + 1000))
    if port_in_use "$PORT" || port_in_use "$http_port_fixed"; then
      echo "error: requested PORT=$PORT (or HTTP $http_port_fixed) is already in use" >&2
      exit 1
    fi
    return
  fi

  local candidate=56051
  local max_candidate=$((65535 - 1000))
  while true; do
    local candidate_http=$((candidate + 1000))
    if ! port_in_use "$candidate" && ! port_in_use "$candidate_http"; then
      PORT="$candidate"
      return
    fi
    candidate=$((candidate + 1))
    if (( candidate > max_candidate )); then
      echo "error: unable to find free gRPC/HTTP port pair" >&2
      exit 1
    fi
  done
}

infer_dimension_from_config() {
  local config_path="$1"
  if [[ ! -f "$config_path" ]]; then
    return 1
  fi

  case "$config_path" in
    *.toml)
      awk -F'=' '
        /^[[:space:]]*dimension[[:space:]]*=[[:space:]]*[0-9]+/ {
          gsub(/[^0-9]/, "", $2);
          print $2;
          exit
        }
      ' "$config_path"
      ;;
    *.yaml|*.yml)
      awk -F':' '
        /^[[:space:]]*dimension[[:space:]]*:[[:space:]]*[0-9]+/ {
          gsub(/[^0-9]/, "", $2);
          print $2;
          exit
        }
      ' "$config_path"
      ;;
    *)
      return 1
      ;;
  esac
}

CONFIG_DIMENSION="$(infer_dimension_from_config "$CONFIG_PATH" || true)"
DIMENSION="${DIMENSION:-${CONFIG_DIMENSION:-768}}"
resolve_ports

RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT_DIR/target/qa/soak_${RUN_STAMP}}"
DATA_DIR="${DATA_DIR:-$ARTIFACT_DIR/data}"
LOG_DIR="$ARTIFACT_DIR/logs"
mkdir -p "$ARTIFACT_DIR" "$DATA_DIR" "$LOG_DIR"

SUMMARY_CSV="$ARTIFACT_DIR/summary.csv"
echo "cycle,mode,status,start_utc,end_utc,duration_sec,server_log,load_log" >"$SUMMARY_CSV"

SERVER_PID=""
SERVER_LOG=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]]; then
    if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      kill "$SERVER_PID" >/dev/null 2>&1 || true
      sleep 1
      if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
        kill -9 "$SERVER_PID" >/dev/null 2>&1 || true
      fi
    fi
  fi
  # Best-effort cleanup for orphan listeners when script exits unexpectedly.
  if [[ -n "${PORT:-}" ]]; then
    lsof -n -t -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | xargs -I{} kill -9 {} >/dev/null 2>&1 || true
    lsof -n -t -iTCP:"$((PORT + 1000))" -sTCP:LISTEN 2>/dev/null | xargs -I{} kill -9 {} >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

build_binaries() {
  if [[ "$BUILD_BINARIES" != "1" ]]; then
    return
  fi
  echo "[build] compiling release binaries..."
  cargo build --release -p kyrodb-engine --bin kyrodb_server --bin kyrodb_load_tester
}

start_server() {
  local cycle="$1"
  local phase="$2"
  SERVER_LOG="$LOG_DIR/server_cycle_${cycle}_${phase}.log"
  echo "[cycle $cycle] starting server ($phase) (port=$PORT data_dir=$DATA_DIR)"
  "$SERVER_BIN" --config "$CONFIG_PATH" --port "$PORT" --data-dir "$DATA_DIR" >"$SERVER_LOG" 2>&1 &
  SERVER_PID="$!"

  local start_epoch now
  start_epoch="$(date +%s)"
  while true; do
    if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      echo "[cycle $cycle] server exited early during ${phase}; log: $SERVER_LOG" >&2
      tail -n 80 "$SERVER_LOG" >&2 || true
      return 1
    fi
    if grep -q "Server ready to accept connections" "$SERVER_LOG"; then
      echo "[cycle $cycle] server ready for ${phase} (pid=$SERVER_PID)"
      return 0
    fi
    now="$(date +%s)"
    if (( now - start_epoch >= STARTUP_TIMEOUT_SECS )); then
      echo "[cycle $cycle] server startup timeout during ${phase} after ${STARTUP_TIMEOUT_SECS}s" >&2
      tail -n 80 "$SERVER_LOG" >&2 || true
      return 1
    fi
    sleep 1
  done
}

stop_server_graceful() {
  if [[ -z "${SERVER_PID}" ]]; then
    return 0
  fi
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    SERVER_PID=""
    return 0
  fi

  kill "$SERVER_PID" >/dev/null 2>&1 || true
  local start_epoch now
  start_epoch="$(date +%s)"
  while kill -0 "$SERVER_PID" >/dev/null 2>&1; do
    now="$(date +%s)"
    if (( now - start_epoch >= GRACEFUL_SHUTDOWN_TIMEOUT_SECS )); then
      kill -9 "$SERVER_PID" >/dev/null 2>&1 || true
      break
    fi
    sleep 1
  done
  SERVER_PID=""
}

stop_server_hard() {
  if [[ -z "${SERVER_PID}" ]]; then
    return 0
  fi
  if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill -9 "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  SERVER_PID=""
}

kill_stale_kyrodb_listener_pids() {
  local grpc_port="$1"
  local http_port="$2"
  local stale_pids=()
  local pid cmd

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    if [[ "$cmd" == *"kyrodb_server"* ]]; then
      stale_pids+=("$pid")
    fi
  done < <(
    {
      lsof -n -t -iTCP:"$grpc_port" -sTCP:LISTEN 2>/dev/null || true
      lsof -n -t -iTCP:"$http_port" -sTCP:LISTEN 2>/dev/null || true
    } | awk '!seen[$0]++'
  )

  if (( ${#stale_pids[@]} == 0 )); then
    return 0
  fi

  echo "[cleanup] stale kyrodb_server listeners detected on ports $grpc_port/$http_port: ${stale_pids[*]}"
  kill "${stale_pids[@]}" >/dev/null 2>&1 || true
  sleep 1

  local survivors=()
  for pid in "${stale_pids[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      survivors+=("$pid")
    fi
  done

  if (( ${#survivors[@]} > 0 )); then
    echo "[cleanup] forcing kill for stale listeners: ${survivors[*]}"
    kill -9 "${survivors[@]}" >/dev/null 2>&1 || true
  fi
}

run_load_phase() {
  local cycle="$1"
  local mode="$2"
  local load_log="$LOG_DIR/load_cycle_${cycle}_${mode}.log"
  local phase_start phase_end phase_duration status start_utc end_utc
  local load_pid=""
  phase_start="$(date +%s)"
  start_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  echo "[cycle $cycle] running load phase ($mode) qps=$QPS concurrency=$CONCURRENCY duration=${CYCLE_DURATION_SECS}s"
  "$LOAD_TESTER_BIN" \
    --server "http://127.0.0.1:$PORT" \
    --qps "$QPS" \
    --duration "$CYCLE_DURATION_SECS" \
    --concurrency "$CONCURRENCY" \
    --dataset "$DATASET_SIZE" \
    --dimension "$DIMENSION" \
    --request-timeout-ms "$LOAD_TESTER_REQUEST_TIMEOUT_MS" \
    --max-failure-ratio "$LOAD_TESTER_MAX_FAILURE_RATIO" >"$load_log" 2>&1 &
  load_pid="$!"

  while kill -0 "$load_pid" >/dev/null 2>&1; do
    local now
    now="$(date +%s)"
    if (( now - phase_start >= LOAD_PHASE_TIMEOUT_SECS )); then
      status="failed_timeout"
      echo "[cycle $cycle] load phase timeout after ${LOAD_PHASE_TIMEOUT_SECS}s; killing load tester pid=$load_pid" >&2
      kill -9 "$load_pid" >/dev/null 2>&1 || true
      wait "$load_pid" >/dev/null 2>&1 || true
      break
    fi
    sleep 1
  done

  if [[ -z "${status:-}" ]]; then
    if wait "$load_pid"; then
      status="ok"
    else
      status="failed"
    fi
  fi

  phase_end="$(date +%s)"
  end_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  phase_duration="$((phase_end - phase_start))"
  echo "${cycle},${mode},${status},${start_utc},${end_utc},${phase_duration},${SERVER_LOG},${load_log}" >>"$SUMMARY_CSV"

  if [[ "$status" != "ok" ]]; then
    echo "[cycle $cycle] load phase failed; last log lines:" >&2
    tail -n 60 "$load_log" >&2 || true
    return 1
  fi
}

build_binaries

echo "[setup] artifact_dir=$ARTIFACT_DIR"
echo "[setup] cycles=$SOAK_CYCLES cycle_duration_secs=$CYCLE_DURATION_SECS kill_every_n_cycles=$KILL_EVERY_N_CYCLES"
echo "[setup] qps=$QPS concurrency=$CONCURRENCY dataset=$DATASET_SIZE dimension=$DIMENSION request_timeout_ms=$LOAD_TESTER_REQUEST_TIMEOUT_MS max_failure_ratio=$LOAD_TESTER_MAX_FAILURE_RATIO load_phase_timeout_secs=$LOAD_PHASE_TIMEOUT_SECS port=$PORT http_port=$((PORT + 1000))"

for cycle in $(seq 1 "$SOAK_CYCLES"); do
  kill_stale_kyrodb_listener_pids "$PORT" "$((PORT + 1000))"
  start_server "$cycle" "steady"
  run_load_phase "$cycle" "steady"

  if (( cycle % KILL_EVERY_N_CYCLES == 0 )); then
    echo "[cycle $cycle] chaos action: hard kill"
    stop_server_hard
  else
    echo "[cycle $cycle] chaos action: graceful restart"
    stop_server_graceful
  fi

  kill_stale_kyrodb_listener_pids "$PORT" "$((PORT + 1000))"
  start_server "$cycle" "recovery"
  run_load_phase "$cycle" "recovery_probe"
  # Ensure each cycle ends with clean listener teardown before the next cycle.
  stop_server_graceful
done

stop_server_graceful
echo "[done] soak+chaos run complete"
echo "[done] summary: $SUMMARY_CSV"
