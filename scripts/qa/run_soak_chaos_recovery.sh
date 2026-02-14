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
DIMENSION="${DIMENSION:-384}"
PORT="${PORT:-56051}"
STARTUP_TIMEOUT_SECS="${STARTUP_TIMEOUT_SECS:-30}"
GRACEFUL_SHUTDOWN_TIMEOUT_SECS="${GRACEFUL_SHUTDOWN_TIMEOUT_SECS:-20}"

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

run_load_phase() {
  local cycle="$1"
  local mode="$2"
  local load_log="$LOG_DIR/load_cycle_${cycle}_${mode}.log"
  local phase_start phase_end phase_duration status start_utc end_utc
  phase_start="$(date +%s)"
  start_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  echo "[cycle $cycle] running load phase ($mode) qps=$QPS concurrency=$CONCURRENCY duration=${CYCLE_DURATION_SECS}s"
  if "$LOAD_TESTER_BIN" \
    --server "http://127.0.0.1:$PORT" \
    --qps "$QPS" \
    --duration "$CYCLE_DURATION_SECS" \
    --concurrency "$CONCURRENCY" \
    --dataset "$DATASET_SIZE" \
    --dimension "$DIMENSION" >"$load_log" 2>&1; then
    status="ok"
  else
    status="failed"
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
echo "[setup] qps=$QPS concurrency=$CONCURRENCY dataset=$DATASET_SIZE dimension=$DIMENSION"

for cycle in $(seq 1 "$SOAK_CYCLES"); do
  start_server "$cycle" "steady"
  run_load_phase "$cycle" "steady"

  if (( cycle % KILL_EVERY_N_CYCLES == 0 )); then
    echo "[cycle $cycle] chaos action: hard kill"
    stop_server_hard
  else
    echo "[cycle $cycle] chaos action: graceful restart"
    stop_server_graceful
  fi

  start_server "$cycle" "recovery"
  run_load_phase "$cycle" "recovery_probe"
done

stop_server_graceful
echo "[done] soak+chaos run complete"
echo "[done] summary: $SUMMARY_CSV"
