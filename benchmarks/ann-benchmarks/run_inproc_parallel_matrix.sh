#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

SUITE_SCRIPT="${REPO_ROOT}/benchmarks/ann-benchmarks/run_inproc_azure_suite.sh"
CONVERTER_SCRIPT="${REPO_ROOT}/benchmarks/ann-benchmarks/export_ann_hdf5_to_annbin.py"
SUMMARIZER_SCRIPT="${REPO_ROOT}/benchmarks/ann-benchmarks/summarize_inproc_results.py"
GATE_CHECKER_SCRIPT="${REPO_ROOT}/benchmarks/ann-benchmarks/check_frontier_gates.py"
PLOTTER_SCRIPT="${REPO_ROOT}/benchmarks/ann-benchmarks/render_inproc_plots.py"

DEFAULT_DATASETS="sift-128-euclidean,glove-100-angular,gist-960-euclidean,mnist-784-euclidean"
DEFAULT_M_VALUES="24,32,40,48,56,64,72,80"
DEFAULT_EF_CONSTRUCTION_VALUES="200,400,600,800,1000,1200,1600"
DEFAULT_EF_SEARCH_VALUES="64,128,192,256,384,512,768,1024,1536,2048,2560,3072"
DEFAULT_RECALL_TARGETS="0.90,0.95,0.99"

DATASETS="${DEFAULT_DATASETS}"
DATA_DIR="${REPO_ROOT}/benchmarks/data"
OUT_ROOT="${REPO_ROOT}/target/ann_inproc_parallel"
RUN_LABEL="fx_parallel"
RUN_ID_OVERRIDE=""

M_VALUES="${DEFAULT_M_VALUES}"
EF_CONSTRUCTION_VALUES="${DEFAULT_EF_CONSTRUCTION_VALUES}"
EF_SEARCH_VALUES="${DEFAULT_EF_SEARCH_VALUES}"
RECALL_TARGETS="${DEFAULT_RECALL_TARGETS}"

K=10
REPETITIONS=3
WARMUP_QUERIES=200
MAX_TRAIN=0
MAX_QUERIES=0
PARALLEL_CONFIGS=8
THREADS_PER_JOB=2

PYTHON_BIN="${PYTHON_BIN:-python3}"
SKIP_BUILD=0
SKIP_DOWNLOAD=0
FORCE_RECONVERT=0
CHECK_GATES=0
ALLOW_MISSING_GATE_DATASETS=0
RESUME=0
JOB_TIMEOUT_SECONDS=0
LOCK_FILE=""
NO_PLOTS=0

usage() {
  cat <<'USAGE'
Run full in-process ANN config matrix in parallel shards and aggregate outputs.

Usage:
  benchmarks/ann-benchmarks/run_inproc_parallel_matrix.sh [options]

Core options:
  --datasets CSV
  --data-dir PATH
  --out-root PATH
  --run-label STRING
  --run-id STRING
  --m-values CSV
  --ef-construction-values CSV
  --ef-search-values CSV
  --recall-targets CSV
  --k INT
  --repetitions INT
  --warmup-queries INT
  --max-train INT
  --max-queries INT

Parallelization options:
  --parallel-configs INT              Number of shard jobs to run concurrently
  --threads-per-job INT               RAYON_NUM_THREADS passed to each shard

Execution options:
  --python BIN
  --resume
  --job-timeout-seconds INT
  --lock-file PATH
  --skip-build
  --skip-download
  --force-reconvert
  --check-gates
  --allow-missing-gate-datasets
  --no-plots
  -h, --help
USAGE
}

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "${s}"
}

split_csv() {
  local csv="$1"
  local part
  SPLIT_CSV_RESULT=()
  IFS=',' read -r -a __parts <<<"${csv}"
  for part in "${__parts[@]}"; do
    part="$(trim "${part}")"
    if [[ -n "${part}" ]]; then
      SPLIT_CSV_RESULT+=("${part}")
    fi
  done
  if [[ "${#SPLIT_CSV_RESULT[@]}" -eq 0 ]]; then
    echo "empty CSV list: '${csv}'" >&2
    exit 1
  fi
}

sanitize_label() {
  echo "$1" | tr -cs '[:alnum:]_.-' '_'
}

dataset_url() {
  case "$1" in
    sift-128-euclidean) echo "https://ann-benchmarks.com/sift-128-euclidean.hdf5" ;;
    glove-100-angular) echo "https://ann-benchmarks.com/glove-100-angular.hdf5" ;;
    gist-960-euclidean) echo "https://ann-benchmarks.com/gist-960-euclidean.hdf5" ;;
    mnist-784-euclidean) echo "https://ann-benchmarks.com/mnist-784-euclidean.hdf5" ;;
    *)
      echo "unsupported dataset '${1}'" >&2
      exit 1
      ;;
  esac
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

require_arg() {
  local flag="$1"
  local value="${2-}"
  if [[ -z "${value}" ]] || [[ "${value}" == --* ]]; then
    echo "missing value for ${flag}" >&2
    usage
    exit 1
  fi
}

is_valid_json_file() {
  local path="$1"
  "${PYTHON_BIN}" -c 'import json,sys; json.load(open(sys.argv[1], "r", encoding="utf-8"))' \
    "$path" >/dev/null 2>&1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --datasets) require_arg "$1" "${2-}"; DATASETS="$2"; shift 2 ;;
    --data-dir) require_arg "$1" "${2-}"; DATA_DIR="$2"; shift 2 ;;
    --out-root) require_arg "$1" "${2-}"; OUT_ROOT="$2"; shift 2 ;;
    --run-label) require_arg "$1" "${2-}"; RUN_LABEL="$2"; shift 2 ;;
    --run-id) require_arg "$1" "${2-}"; RUN_ID_OVERRIDE="$2"; shift 2 ;;
    --m-values) require_arg "$1" "${2-}"; M_VALUES="$2"; shift 2 ;;
    --ef-construction-values) require_arg "$1" "${2-}"; EF_CONSTRUCTION_VALUES="$2"; shift 2 ;;
    --ef-search-values) require_arg "$1" "${2-}"; EF_SEARCH_VALUES="$2"; shift 2 ;;
    --recall-targets) require_arg "$1" "${2-}"; RECALL_TARGETS="$2"; shift 2 ;;
    --k) require_arg "$1" "${2-}"; K="$2"; shift 2 ;;
    --repetitions) require_arg "$1" "${2-}"; REPETITIONS="$2"; shift 2 ;;
    --warmup-queries) require_arg "$1" "${2-}"; WARMUP_QUERIES="$2"; shift 2 ;;
    --max-train) require_arg "$1" "${2-}"; MAX_TRAIN="$2"; shift 2 ;;
    --max-queries) require_arg "$1" "${2-}"; MAX_QUERIES="$2"; shift 2 ;;
    --parallel-configs) require_arg "$1" "${2-}"; PARALLEL_CONFIGS="$2"; shift 2 ;;
    --threads-per-job) require_arg "$1" "${2-}"; THREADS_PER_JOB="$2"; shift 2 ;;
    --python) require_arg "$1" "${2-}"; PYTHON_BIN="$2"; shift 2 ;;
    --resume) RESUME=1; shift ;;
    --job-timeout-seconds) require_arg "$1" "${2-}"; JOB_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --lock-file) require_arg "$1" "${2-}"; LOCK_FILE="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --skip-download) SKIP_DOWNLOAD=1; shift ;;
    --force-reconvert) FORCE_RECONVERT=1; shift ;;
    --check-gates) CHECK_GATES=1; shift ;;
    --allow-missing-gate-datasets) ALLOW_MISSING_GATE_DATASETS=1; shift ;;
    --no-plots) NO_PLOTS=1; shift ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd bash
require_cmd cargo
require_cmd curl
require_cmd find
require_cmd xargs
require_cmd flock
require_cmd "${PYTHON_BIN}"
if [[ "${JOB_TIMEOUT_SECONDS}" -gt 0 ]]; then
  require_cmd timeout
fi

if [[ ! -f "${SUITE_SCRIPT}" ]]; then
  echo "missing suite script: ${SUITE_SCRIPT}" >&2
  exit 1
fi
if [[ ! -f "${CONVERTER_SCRIPT}" ]]; then
  echo "missing converter script: ${CONVERTER_SCRIPT}" >&2
  exit 1
fi
if [[ ! -f "${SUMMARIZER_SCRIPT}" ]]; then
  echo "missing summarizer script: ${SUMMARIZER_SCRIPT}" >&2
  exit 1
fi

split_csv "${DATASETS}"
DATASET_LIST=("${SPLIT_CSV_RESULT[@]}")
split_csv "${M_VALUES}"
M_LIST=("${SPLIT_CSV_RESULT[@]}")
split_csv "${EF_CONSTRUCTION_VALUES}"
EFC_LIST=("${SPLIT_CSV_RESULT[@]}")

for value in "${M_LIST[@]}" "${EFC_LIST[@]}"; do
  if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
    echo "expected integer in M/ef_construction sweep, got '${value}'" >&2
    exit 1
  fi
done

for value in "${K}" "${REPETITIONS}" "${WARMUP_QUERIES}" "${MAX_TRAIN}" "${MAX_QUERIES}" "${PARALLEL_CONFIGS}" "${THREADS_PER_JOB}" "${JOB_TIMEOUT_SECONDS}"; do
  if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
    echo "expected integer argument, got '${value}'" >&2
    exit 1
  fi
done

if [[ "${K}" -lt 1 ]] || [[ "${REPETITIONS}" -lt 1 ]]; then
  echo "k and repetitions must be >= 1" >&2
  exit 1
fi
if [[ "${WARMUP_QUERIES}" -lt 0 ]] || [[ "${MAX_TRAIN}" -lt 0 ]] || [[ "${MAX_QUERIES}" -lt 0 ]]; then
  echo "warmup-queries/max-train/max-queries must be >= 0" >&2
  exit 1
fi

if [[ "${PARALLEL_CONFIGS}" -lt 1 ]]; then
  echo "parallel-configs must be >= 1" >&2
  exit 1
fi
if [[ "${THREADS_PER_JOB}" -lt 1 ]]; then
  echo "threads-per-job must be >= 1" >&2
  exit 1
fi
if [[ "${RESUME}" -eq 1 ]] && [[ -z "${RUN_ID_OVERRIDE}" ]]; then
  echo "--resume requires --run-id to target an existing matrix run directory" >&2
  exit 1
fi

RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
SAFE_LABEL="$(echo "${RUN_LABEL}" | tr -cs '[:alnum:]_.-' '_')"
if [[ -n "${RUN_ID_OVERRIDE}" ]]; then
  RUN_ID="$(echo "${RUN_ID_OVERRIDE}" | tr -cs '[:alnum:]_.-' '_')"
else
  RUN_ID="${SAFE_LABEL}_${RUN_TS}"
fi
RUN_DIR="${OUT_ROOT}/${RUN_ID}"
SHARD_OUT_ROOT="${RUN_DIR}/shards"
LOG_DIR="${RUN_DIR}/logs"
TMP_DIR="${RUN_DIR}/tmp"
AGG_RAW_DIR="${RUN_DIR}/aggregate/raw"
AGG_SUMMARY_DIR="${RUN_DIR}/aggregate/summary"

if [[ -z "${LOCK_FILE}" ]]; then
  LOCK_FILE="${OUT_ROOT}/.run_inproc_parallel_matrix.lock"
fi

mkdir -p "${OUT_ROOT}" "${DATA_DIR}"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "another matrix run is active (lock: ${LOCK_FILE})" >&2
  exit 1
fi

if [[ -d "${RUN_DIR}" ]] && [[ "${RESUME}" -eq 0 ]]; then
  echo "run directory already exists (use --resume to continue): ${RUN_DIR}" >&2
  exit 1
fi

mkdir -p "${SHARD_OUT_ROOT}" "${LOG_DIR}" "${TMP_DIR}" "${AGG_RAW_DIR}" "${AGG_SUMMARY_DIR}"

MANIFEST="${RUN_DIR}/manifest.txt"
if [[ "${RESUME}" -eq 1 ]] && [[ -f "${MANIFEST}" ]]; then
  {
    echo "resumed_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "resumed_by=run_inproc_parallel_matrix.sh"
  } >> "${MANIFEST}"
else
  {
    echo "run_id=${RUN_ID}"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "repo_root=${REPO_ROOT}"
    echo "datasets=${DATASETS}"
    echo "m_values=${M_VALUES}"
    echo "ef_construction_values=${EF_CONSTRUCTION_VALUES}"
    echo "ef_search_values=${EF_SEARCH_VALUES}"
    echo "recall_targets=${RECALL_TARGETS}"
    echo "k=${K}"
    echo "repetitions=${REPETITIONS}"
    echo "warmup_queries=${WARMUP_QUERIES}"
    echo "max_train=${MAX_TRAIN}"
    echo "max_queries=${MAX_QUERIES}"
    echo "parallel_configs=${PARALLEL_CONFIGS}"
    echo "threads_per_job=${THREADS_PER_JOB}"
    echo "python_bin=${PYTHON_BIN}"
    echo "skip_build=${SKIP_BUILD}"
    echo "skip_download=${SKIP_DOWNLOAD}"
    echo "force_reconvert=${FORCE_RECONVERT}"
    echo "no_plots=${NO_PLOTS}"
    echo "resume=${RESUME}"
    echo "job_timeout_seconds=${JOB_TIMEOUT_SECONDS}"
    echo "run_id_override=${RUN_ID_OVERRIDE:-none}"
    echo "lock_file=${LOCK_FILE}"
    echo "host=$(hostname)"
    echo "kernel=$(uname -a)"
    echo "rustc=$(rustc -V 2>/dev/null || true)"
    echo "cargo=$(cargo -V 2>/dev/null || true)"
    echo "python=$(${PYTHON_BIN} --version 2>&1 || true)"
    echo "git_commit=$(git rev-parse HEAD 2>/dev/null || true)"
    echo "git_short=$(git rev-parse --short HEAD 2>/dev/null || true)"
  } > "${MANIFEST}"
fi

echo "[run] output root: ${RUN_DIR}"
echo "[run] manifest: ${MANIFEST}"

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  echo "[build] cargo build --release -p kyrodb-engine --bin ann_inproc_bench"
  cargo build --release -p kyrodb-engine --bin ann_inproc_bench
fi

for dataset in "${DATASET_LIST[@]}"; do
  hdf5_path="${DATA_DIR}/${dataset}.hdf5"
  annbin_path="${DATA_DIR}/${dataset}.annbin"

  if [[ "${SKIP_DOWNLOAD}" -eq 1 ]]; then
    if [[ ! -f "${hdf5_path}" ]]; then
      echo "missing dataset (and --skip-download set): ${hdf5_path}" >&2
      exit 1
    fi
  elif [[ ! -f "${hdf5_path}" ]]; then
    url="$(dataset_url "${dataset}")"
    echo "[data] downloading ${dataset} -> ${hdf5_path}"
    hdf5_tmp="${hdf5_path}.tmp"
    curl -fL --retry 5 --retry-delay 2 --retry-all-errors "${url}" -o "${hdf5_tmp}"
    mv "${hdf5_tmp}" "${hdf5_path}"
  else
    echo "[data] reusing ${hdf5_path}"
  fi

  if [[ "${FORCE_RECONVERT}" -eq 1 || ! -f "${annbin_path}" ]]; then
    echo "[data] converting ${hdf5_path} -> ${annbin_path}"
    "${PYTHON_BIN}" "${CONVERTER_SCRIPT}" --input "${hdf5_path}" --output "${annbin_path}"
  else
    echo "[data] reusing ${annbin_path}"
  fi
done

JOBS_FILE="${TMP_DIR}/jobs.txt"
: > "${JOBS_FILE}"
STATUS_FILE="${RUN_DIR}/shard_status.tsv"
if [[ ! -s "${STATUS_FILE}" ]]; then
  echo -e "dataset\tm\tef_construction\trun_id\texit_code\telapsed_seconds" > "${STATUS_FILE}"
elif [[ "${RESUME}" -eq 1 ]]; then
  echo -e "# resume_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)\trun_id=${RUN_ID}" >> "${STATUS_FILE}"
fi
SKIPPED_EXISTING=0
PENDING_JOBS=0
for dataset in "${DATASET_LIST[@]}"; do
  for m in "${M_LIST[@]}"; do
    for efc in "${EFC_LIST[@]}"; do
      shard_run_id="$(sanitize_label "${RUN_LABEL}.${dataset}.m${m}.efc${efc}")"
      expected_json="${SHARD_OUT_ROOT}/${shard_run_id}/raw/${dataset}.m${m}.efc${efc}.json"
      if [[ "${RESUME}" -eq 1 ]] && [[ -s "${expected_json}" ]]; then
        if is_valid_json_file "${expected_json}"; then
          SKIPPED_EXISTING=$((SKIPPED_EXISTING + 1))
          continue
        fi
        echo "[resume] removing invalid shard JSON and rerunning: ${expected_json}" >&2
        rm -f "${expected_json}"
      fi
      echo "${dataset} ${m} ${efc} ${shard_run_id}" >> "${JOBS_FILE}"
      PENDING_JOBS=$((PENDING_JOBS + 1))
    done
  done
done

TOTAL_JOBS=$(( ${#DATASET_LIST[@]} * ${#M_LIST[@]} * ${#EFC_LIST[@]} ))
echo "[run] shard jobs total=${TOTAL_JOBS} pending=${PENDING_JOBS} skipped_existing=${SKIPPED_EXISTING}"
echo "[run] parallel-configs=${PARALLEL_CONFIGS}, threads-per-job=${THREADS_PER_JOB}"
if [[ "${PENDING_JOBS}" -eq 0 ]]; then
  echo "[run] no pending shard jobs; using existing shard outputs"
fi

export SUITE_SCRIPT DATA_DIR SHARD_OUT_ROOT RUN_LABEL M_VALUES EF_CONSTRUCTION_VALUES EF_SEARCH_VALUES RECALL_TARGETS
export K REPETITIONS WARMUP_QUERIES MAX_TRAIN MAX_QUERIES
export THREADS_PER_JOB LOG_DIR STATUS_FILE JOB_TIMEOUT_SECONDS RESUME

if [[ "${PENDING_JOBS}" -gt 0 ]]; then
  terminate_parallel_jobs() {
    local sig="$1"
    echo "[interrupt] received ${sig}, stopping shard workers..." >&2
    if [[ -n "${XARGS_PID:-}" ]] && kill -0 "${XARGS_PID}" 2>/dev/null; then
      kill "${XARGS_PID}" 2>/dev/null || true
      pkill -P "${XARGS_PID}" 2>/dev/null || true
    fi
    exit 130
  }
  trap 'terminate_parallel_jobs INT' INT
  trap 'terminate_parallel_jobs TERM' TERM
  trap 'terminate_parallel_jobs TSTP' TSTP

  xargs -a "${JOBS_FILE}" -P "${PARALLEL_CONFIGS}" -n 4 bash -lc '
  set -euo pipefail
  dataset="$1"
  m="$2"
  efc="$3"
  shard_run_id="$4"
  stdout_log="${LOG_DIR}/${dataset}.m${m}.efc${efc}.stdout.log"
  stderr_log="${LOG_DIR}/${dataset}.m${m}.efc${efc}.stderr.log"
  start_s="$(date +%s)"

  suite_args=(
    --run-label "${RUN_LABEL}.${dataset}.m${m}.efc${efc}"
    --run-id "${shard_run_id}"
    --data-dir "${DATA_DIR}"
    --out-root "${SHARD_OUT_ROOT}"
    --datasets "${dataset}"
    --m-values "${m}"
    --ef-construction-values "${efc}"
    --ef-search-values "${EF_SEARCH_VALUES}"
    --recall-targets "${RECALL_TARGETS}"
    --k "${K}"
    --repetitions "${REPETITIONS}"
    --warmup-queries "${WARMUP_QUERIES}"
    --max-train "${MAX_TRAIN}"
    --max-queries "${MAX_QUERIES}"
    --threads "${THREADS_PER_JOB}"
    --skip-build
    --skip-download
    --no-summary
  )
  if [[ "${RESUME}" -eq 1 ]]; then
    suite_args+=(--resume)
  fi

  rc=0
  if [[ "${JOB_TIMEOUT_SECONDS}" -gt 0 ]]; then
    timeout --signal=TERM --kill-after=30s "${JOB_TIMEOUT_SECONDS}" \
      bash "${SUITE_SCRIPT}" "${suite_args[@]}" \
      > "${stdout_log}" \
      2> "${stderr_log}" || rc=$?
  else
    bash "${SUITE_SCRIPT}" "${suite_args[@]}" \
      > "${stdout_log}" \
      2> "${stderr_log}" || rc=$?
  fi

  end_s="$(date +%s)"
  elapsed=$((end_s - start_s))
  printf "%s\t%s\t%s\t%s\t%s\t%s\n" "${dataset}" "${m}" "${efc}" "${shard_run_id}" "${rc}" "${elapsed}" >> "${STATUS_FILE}"
  exit "${rc}"
' _ &
  XARGS_PID=$!
  wait "${XARGS_PID}"
  XARGS_EXIT=$?
  trap - INT TERM TSTP
  if [[ "${XARGS_EXIT}" -ne 0 ]]; then
    echo "one or more shard jobs failed (status=${XARGS_EXIT}); see ${STATUS_FILE} and shard logs in ${LOG_DIR}" >&2
    exit "${XARGS_EXIT}"
  fi
fi

JSON_COUNT=0
while IFS= read -r -d '' json_file; do
  if ! is_valid_json_file "${json_file}"; then
    echo "invalid shard JSON detected: ${json_file}" >&2
    exit 1
  fi
  cp "${json_file}" "${AGG_RAW_DIR}/"
  JSON_COUNT=$((JSON_COUNT + 1))
done < <(find "${SHARD_OUT_ROOT}" -type f -path "*/raw/*.json" -print0)

if [[ "${JSON_COUNT}" -eq 0 ]]; then
  echo "no shard outputs found under ${SHARD_OUT_ROOT}" >&2
  exit 1
fi

SUMMARY_JSON="${AGG_SUMMARY_DIR}/inproc_summary.json"
SUMMARY_MD="${AGG_SUMMARY_DIR}/inproc_summary.md"
SUMMARY_CSV="${AGG_SUMMARY_DIR}/inproc_candidates.csv"

echo "[summary] aggregating ${JSON_COUNT} shard JSON files"
"${PYTHON_BIN}" "${SUMMARIZER_SCRIPT}" \
  --input-glob "${AGG_RAW_DIR}/*.json" \
  --recall-targets "${RECALL_TARGETS}" \
  --output-json "${SUMMARY_JSON}" \
  --output-md "${SUMMARY_MD}" \
  --output-csv "${SUMMARY_CSV}"

if [[ "${CHECK_GATES}" -eq 1 ]]; then
  if [[ ! -f "${GATE_CHECKER_SCRIPT}" ]]; then
    echo "missing gate checker script: ${GATE_CHECKER_SCRIPT}" >&2
    exit 1
  fi
  echo "[gates] validating frontier acceptance gates"
  gate_args=(
    --summary-json "${SUMMARY_JSON}"
    --candidates-csv "${SUMMARY_CSV}"
    --log-glob "${LOG_DIR}/*.stderr.log"
    --log-glob "${LOG_DIR}/*.stdout.log"
  )
  if [[ "${ALLOW_MISSING_GATE_DATASETS}" -eq 1 ]]; then
    gate_args+=(--allow-missing-datasets)
  fi
  "${PYTHON_BIN}" "${GATE_CHECKER_SCRIPT}" "${gate_args[@]}"
fi

if [[ "${NO_PLOTS}" -eq 0 ]]; then
  if [[ -f "${PLOTTER_SCRIPT}" ]]; then
    PLOTS_DIR="${RUN_DIR}/aggregate/plots"
    if ! "${PYTHON_BIN}" "${PLOTTER_SCRIPT}" \
      --summary-json "${SUMMARY_JSON}" \
      --candidates-csv "${SUMMARY_CSV}" \
      --output-dir "${PLOTS_DIR}" \
      --recall-targets "${RECALL_TARGETS}"; then
      echo "warning: plot generation failed for ${RUN_DIR}" >&2
    fi
  else
    echo "warning: plot script missing (${PLOTTER_SCRIPT}); skipping plot generation" >&2
  fi
fi

{
  echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "total_jobs=${TOTAL_JOBS}"
  echo "pending_jobs=${PENDING_JOBS}"
  echo "skipped_existing_jobs=${SKIPPED_EXISTING}"
  echo "aggregated_json_files=${JSON_COUNT}"
  echo "status_file=${STATUS_FILE}"
  if [[ -n "${PLOTS_DIR:-}" ]]; then
    echo "plots_dir=${PLOTS_DIR}"
  fi
  echo "run_dir=${RUN_DIR}"
} >> "${MANIFEST}"

echo "[done] run directory: ${RUN_DIR}"
echo "[done] summary: ${SUMMARY_MD}"
