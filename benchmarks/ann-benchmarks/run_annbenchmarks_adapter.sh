#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Prepare and run KyroDB ANN-Benchmarks adapter against upstream ann-benchmarks.

Usage:
  benchmarks/ann-benchmarks/run_annbenchmarks_adapter.sh --ann-root /path/to/ann-benchmarks [options]

Required:
  --ann-root PATH                      Path to upstream ann-benchmarks checkout

Options:
  --datasets CSV                       Datasets to run (default: sift-128-euclidean)
  --kyrodb-git URL                     KyroDB git URL used for Docker build
  --kyrodb-ref REF                     KyroDB git ref/commit (default: benchmark)
  --sdk-version VERSION                kyrodb pip version for adapter image (default: 0.1.0)
  --algorithm NAME                     Algorithm name in ann-benchmarks (default: kyrodb)
  --python BIN                         Python executable (default: python3)
  --skip-install                       Skip `install.py` image build step
  --plot                               Run `plot.py` for each dataset after benchmark run
  -h, --help                           Show help
USAGE
}

require_arg() {
  local flag="$1"
  local value="${2-}"
  if [[ -z "${value}" ]] || [[ "${value}" == --* ]]; then
    echo "missing value for ${flag}" >&2
    exit 1
  fi
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
  SPLIT_RESULT=()
  IFS=',' read -r -a parts <<<"${csv}"
  for part in "${parts[@]}"; do
    part="$(trim "${part}")"
    [[ -n "${part}" ]] && SPLIT_RESULT+=("${part}")
  done
  if [[ "${#SPLIT_RESULT[@]}" -eq 0 ]]; then
    echo "empty CSV list: '${csv}'" >&2
    exit 1
  fi
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

ANN_ROOT=""
DATASETS="sift-128-euclidean"
KYRODB_GIT="https://github.com/KyroDB/KyroDB.git"
KYRODB_REF="benchmark"
SDK_VERSION="0.1.0"
ALGORITHM="kyrodb"
PYTHON_BIN="python3"
SKIP_INSTALL=0
RUN_PLOTS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ann-root) require_arg "$1" "${2-}"; ANN_ROOT="$2"; shift 2 ;;
    --datasets) require_arg "$1" "${2-}"; DATASETS="$2"; shift 2 ;;
    --kyrodb-git) require_arg "$1" "${2-}"; KYRODB_GIT="$2"; shift 2 ;;
    --kyrodb-ref) require_arg "$1" "${2-}"; KYRODB_REF="$2"; shift 2 ;;
    --sdk-version) require_arg "$1" "${2-}"; SDK_VERSION="$2"; shift 2 ;;
    --algorithm) require_arg "$1" "${2-}"; ALGORITHM="$2"; shift 2 ;;
    --python) require_arg "$1" "${2-}"; PYTHON_BIN="$2"; shift 2 ;;
    --skip-install) SKIP_INSTALL=1; shift ;;
    --plot) RUN_PLOTS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${ANN_ROOT}" ]]; then
  echo "--ann-root is required" >&2
  usage
  exit 1
fi

if [[ ! -d "${ANN_ROOT}" ]]; then
  echo "ann-benchmarks root does not exist: ${ANN_ROOT}" >&2
  exit 1
fi

require_cmd "${PYTHON_BIN}"
require_cmd cp
require_cmd mkdir

ANN_ROOT="$(cd "${ANN_ROOT}" && pwd)"
if [[ ! -f "${ANN_ROOT}/install.py" ]] || [[ ! -f "${ANN_ROOT}/run.py" ]]; then
  echo "invalid ann-benchmarks root: ${ANN_ROOT}" >&2
  echo "expected install.py and run.py" >&2
  exit 1
fi

split_csv "${DATASETS}"
DATASET_LIST=("${SPLIT_RESULT[@]}")

ALG_DIR="${ANN_ROOT}/ann_benchmarks/algorithms/${ALGORITHM}"
mkdir -p "${ALG_DIR}"

cp "${REPO_ROOT}/benchmarks/ann-benchmarks/Dockerfile" "${ALG_DIR}/Dockerfile"
cp "${REPO_ROOT}/benchmarks/ann-benchmarks/module.py" "${ALG_DIR}/module.py"
cp "${REPO_ROOT}/benchmarks/ann-benchmarks/__init__.py" "${ALG_DIR}/__init__.py"
cp "${REPO_ROOT}/benchmarks/ann-benchmarks/config.yml" "${ALG_DIR}/config.yml"

echo "[adapter] staged files into ${ALG_DIR}"

echo "[adapter] config: algorithm=${ALGORITHM} kyrodb_ref=${KYRODB_REF} sdk=${SDK_VERSION}"

cd "${ANN_ROOT}"

if [[ "${SKIP_INSTALL}" -eq 0 ]]; then
  echo "[adapter] building algorithm image"
  "${PYTHON_BIN}" install.py --algorithm "${ALGORITHM}" \
    --build-arg KYRODB_GIT="${KYRODB_GIT}" \
    --build-arg KYRODB_REF="${KYRODB_REF}" \
    --build-arg KYRODB_SDK_VERSION="${SDK_VERSION}"
else
  echo "[adapter] skipping image build (--skip-install)"
fi

for dataset in "${DATASET_LIST[@]}"; do
  echo "[adapter] running dataset ${dataset}"
  "${PYTHON_BIN}" run.py --algorithm "${ALGORITHM}" --dataset "${dataset}"
  if [[ "${RUN_PLOTS}" -eq 1 ]]; then
    echo "[adapter] plotting dataset ${dataset}"
    "${PYTHON_BIN}" plot.py --dataset "${dataset}"
  fi
done

echo "[adapter] completed datasets: ${DATASET_LIST[*]}"
