#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

SUITE_SCRIPT="${REPO_ROOT}/benchmarks/ann-benchmarks/run_inproc_azure_suite.sh"
CONVERTER_SCRIPT="${REPO_ROOT}/benchmarks/ann-benchmarks/export_ann_hdf5_to_annbin.py"
SUMMARIZER_SCRIPT="${REPO_ROOT}/benchmarks/ann-benchmarks/summarize_inproc_results.py"
GATE_CHECKER_SCRIPT="${REPO_ROOT}/benchmarks/ann-benchmarks/check_frontier_gates.py"

DEFAULT_DATASETS="sift-128-euclidean,glove-100-angular,gist-960-euclidean,mnist-784-euclidean"
DEFAULT_M_VALUES="24,32,40,48,56,64,72,80"
DEFAULT_EF_CONSTRUCTION_VALUES="200,400,600,800,1000,1200,1600"
DEFAULT_EF_SEARCH_VALUES="64,128,192,256,384,512,768,1024,1536,2048,2560,3072"
DEFAULT_RECALL_TARGETS="0.90,0.95,0.99"

DATASETS="${DEFAULT_DATASETS}"
DATA_DIR="${REPO_ROOT}/benchmarks/data"
OUT_ROOT="${REPO_ROOT}/target/ann_inproc_parallel"
RUN_LABEL="fx_parallel"

M_VALUES="${DEFAULT_M_VALUES}"
EF_CONSTRUCTION_VALUES="${DEFAULT_EF_CONSTRUCTION_VALUES}"
EF_SEARCH_VALUES="${DEFAULT_EF_SEARCH_VALUES}"
RECALL_TARGETS="${DEFAULT_RECALL_TARGETS}"

K=10
REPETITIONS=3
WARMUP_QUERIES=200
MAX_TRAIN=0
MAX_QUERIES=0
ANN_SEARCH_MODE="fp32-strict"
QUANTIZED_RERANK_MULTIPLIER=8
PARALLEL_CONFIGS=8
THREADS_PER_JOB=2

PYTHON_BIN="${PYTHON_BIN:-python3}"
SKIP_BUILD=0
SKIP_DOWNLOAD=0
FORCE_RECONVERT=0
CHECK_GATES=0

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
  --m-values CSV
  --ef-construction-values CSV
  --ef-search-values CSV
  --recall-targets CSV
  --k INT
  --repetitions INT
  --warmup-queries INT
  --max-train INT
  --max-queries INT
  --ann-search-mode MODE              fp32-strict|sq8-rerank|sq4-rerank
  --quantized-rerank-multiplier INT  [1..64]

Parallelization options:
  --parallel-configs INT              Number of shard jobs to run concurrently
  --threads-per-job INT               RAYON_NUM_THREADS passed to each shard

Execution options:
  --python BIN
  --skip-build
  --skip-download
  --force-reconvert
  --check-gates
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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --datasets) require_arg "$1" "${2-}"; DATASETS="$2"; shift 2 ;;
    --data-dir) require_arg "$1" "${2-}"; DATA_DIR="$2"; shift 2 ;;
    --out-root) require_arg "$1" "${2-}"; OUT_ROOT="$2"; shift 2 ;;
    --run-label) require_arg "$1" "${2-}"; RUN_LABEL="$2"; shift 2 ;;
    --m-values) require_arg "$1" "${2-}"; M_VALUES="$2"; shift 2 ;;
    --ef-construction-values) require_arg "$1" "${2-}"; EF_CONSTRUCTION_VALUES="$2"; shift 2 ;;
    --ef-search-values) require_arg "$1" "${2-}"; EF_SEARCH_VALUES="$2"; shift 2 ;;
    --recall-targets) require_arg "$1" "${2-}"; RECALL_TARGETS="$2"; shift 2 ;;
    --k) require_arg "$1" "${2-}"; K="$2"; shift 2 ;;
    --repetitions) require_arg "$1" "${2-}"; REPETITIONS="$2"; shift 2 ;;
    --warmup-queries) require_arg "$1" "${2-}"; WARMUP_QUERIES="$2"; shift 2 ;;
    --max-train) require_arg "$1" "${2-}"; MAX_TRAIN="$2"; shift 2 ;;
    --max-queries) require_arg "$1" "${2-}"; MAX_QUERIES="$2"; shift 2 ;;
    --ann-search-mode) require_arg "$1" "${2-}"; ANN_SEARCH_MODE="$2"; shift 2 ;;
    --quantized-rerank-multiplier) require_arg "$1" "${2-}"; QUANTIZED_RERANK_MULTIPLIER="$2"; shift 2 ;;
    --parallel-configs) require_arg "$1" "${2-}"; PARALLEL_CONFIGS="$2"; shift 2 ;;
    --threads-per-job) require_arg "$1" "${2-}"; THREADS_PER_JOB="$2"; shift 2 ;;
    --python) require_arg "$1" "${2-}"; PYTHON_BIN="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --skip-download) SKIP_DOWNLOAD=1; shift ;;
    --force-reconvert) FORCE_RECONVERT=1; shift ;;
    --check-gates) CHECK_GATES=1; shift ;;
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
require_cmd "${PYTHON_BIN}"

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

for value in "${K}" "${REPETITIONS}" "${WARMUP_QUERIES}" "${MAX_TRAIN}" "${MAX_QUERIES}" "${PARALLEL_CONFIGS}" "${THREADS_PER_JOB}" "${QUANTIZED_RERANK_MULTIPLIER}"; do
  if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
    echo "expected integer argument, got '${value}'" >&2
    exit 1
  fi
done

if [[ "${PARALLEL_CONFIGS}" -lt 1 ]]; then
  echo "parallel-configs must be >= 1" >&2
  exit 1
fi
if [[ "${THREADS_PER_JOB}" -lt 1 ]]; then
  echo "threads-per-job must be >= 1" >&2
  exit 1
fi
if [[ "${ANN_SEARCH_MODE}" != "fp32-strict" && "${ANN_SEARCH_MODE}" != "sq8-rerank" && "${ANN_SEARCH_MODE}" != "sq4-rerank" ]]; then
  echo "ann-search-mode must be one of: fp32-strict, sq8-rerank, sq4-rerank" >&2
  exit 1
fi
if [[ "${QUANTIZED_RERANK_MULTIPLIER}" -lt 1 || "${QUANTIZED_RERANK_MULTIPLIER}" -gt 64 ]]; then
  echo "quantized-rerank-multiplier must be in range [1..64]" >&2
  exit 1
fi

RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
SAFE_LABEL="$(echo "${RUN_LABEL}" | tr -cs '[:alnum:]_.-' '_')"
RUN_ID="${SAFE_LABEL}_${RUN_TS}"
RUN_DIR="${OUT_ROOT}/${RUN_ID}"
SHARD_OUT_ROOT="${RUN_DIR}/shards"
LOG_DIR="${RUN_DIR}/logs"
TMP_DIR="${RUN_DIR}/tmp"
AGG_RAW_DIR="${RUN_DIR}/aggregate/raw"
AGG_SUMMARY_DIR="${RUN_DIR}/aggregate/summary"

mkdir -p "${DATA_DIR}" "${SHARD_OUT_ROOT}" "${LOG_DIR}" "${TMP_DIR}" "${AGG_RAW_DIR}" "${AGG_SUMMARY_DIR}"

MANIFEST="${RUN_DIR}/manifest.txt"
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
  echo "ann_search_mode=${ANN_SEARCH_MODE}"
  echo "quantized_rerank_multiplier=${QUANTIZED_RERANK_MULTIPLIER}"
  echo "parallel_configs=${PARALLEL_CONFIGS}"
  echo "threads_per_job=${THREADS_PER_JOB}"
  echo "python_bin=${PYTHON_BIN}"
  echo "skip_build=${SKIP_BUILD}"
  echo "skip_download=${SKIP_DOWNLOAD}"
  echo "force_reconvert=${FORCE_RECONVERT}"
  echo "host=$(hostname)"
  echo "kernel=$(uname -a)"
  echo "rustc=$(rustc -V 2>/dev/null || true)"
  echo "cargo=$(cargo -V 2>/dev/null || true)"
  echo "python=$(${PYTHON_BIN} --version 2>&1 || true)"
  echo "git_commit=$(git rev-parse HEAD 2>/dev/null || true)"
  echo "git_short=$(git rev-parse --short HEAD 2>/dev/null || true)"
} > "${MANIFEST}"

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
for dataset in "${DATASET_LIST[@]}"; do
  for m in "${M_LIST[@]}"; do
    for efc in "${EFC_LIST[@]}"; do
      echo "${dataset} ${m} ${efc}" >> "${JOBS_FILE}"
    done
  done
done

TOTAL_JOBS="$(wc -l < "${JOBS_FILE}" | tr -d ' ')"
echo "[run] shard jobs: ${TOTAL_JOBS}"
echo "[run] parallel-configs=${PARALLEL_CONFIGS}, threads-per-job=${THREADS_PER_JOB}"

export SUITE_SCRIPT DATA_DIR SHARD_OUT_ROOT RUN_LABEL M_VALUES EF_CONSTRUCTION_VALUES EF_SEARCH_VALUES RECALL_TARGETS
export K REPETITIONS WARMUP_QUERIES MAX_TRAIN MAX_QUERIES ANN_SEARCH_MODE QUANTIZED_RERANK_MULTIPLIER
export THREADS_PER_JOB LOG_DIR

cat "${JOBS_FILE}" | xargs -P "${PARALLEL_CONFIGS}" -n 3 bash -lc '
  set -euo pipefail
  dataset="$1"
  m="$2"
  efc="$3"
  run_label="${RUN_LABEL}.${dataset}.m${m}.efc${efc}"
  stdout_log="${LOG_DIR}/${dataset}.m${m}.efc${efc}.stdout.log"
  stderr_log="${LOG_DIR}/${dataset}.m${m}.efc${efc}.stderr.log"

  bash "${SUITE_SCRIPT}" \
    --run-label "${run_label}" \
    --data-dir "${DATA_DIR}" \
    --out-root "${SHARD_OUT_ROOT}" \
    --datasets "${dataset}" \
    --m-values "${m}" \
    --ef-construction-values "${efc}" \
    --ef-search-values "${EF_SEARCH_VALUES}" \
    --recall-targets "${RECALL_TARGETS}" \
    --k "${K}" \
    --repetitions "${REPETITIONS}" \
    --warmup-queries "${WARMUP_QUERIES}" \
    --max-train "${MAX_TRAIN}" \
    --max-queries "${MAX_QUERIES}" \
    --ann-search-mode "${ANN_SEARCH_MODE}" \
    --quantized-rerank-multiplier "${QUANTIZED_RERANK_MULTIPLIER}" \
    --threads "${THREADS_PER_JOB}" \
    --skip-build \
    --skip-download \
    --no-summary \
    > "${stdout_log}" \
    2> "${stderr_log}"
' _

JSON_COUNT=0
while IFS= read -r -d '' json_file; do
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
  "${PYTHON_BIN}" "${GATE_CHECKER_SCRIPT}" \
    --summary-json "${SUMMARY_JSON}" \
    --candidates-csv "${SUMMARY_CSV}" \
    --log-glob "${LOG_DIR}/*.stderr.log" \
    --log-glob "${LOG_DIR}/*.stdout.log"
fi

{
  echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "total_jobs=${TOTAL_JOBS}"
  echo "aggregated_json_files=${JSON_COUNT}"
  echo "run_dir=${RUN_DIR}"
} >> "${MANIFEST}"

echo "[done] run directory: ${RUN_DIR}"
echo "[done] summary: ${SUMMARY_MD}"
