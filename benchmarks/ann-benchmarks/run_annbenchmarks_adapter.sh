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

validate_query_args_shape() {
  local config_path="$1"
  "${PYTHON_BIN}" - "${config_path}" <<'PY'
import pathlib
import sys
from typing import List

import yaml

path = pathlib.Path(sys.argv[1])
data = yaml.safe_load(path.read_text())

algorithms = data.get("float", {}).get("any", [])
if len(algorithms) != 1:
    raise SystemExit(f"expected exactly one algorithm entry in config, found {len(algorithms)}")

run_groups = algorithms[0].get("run_groups", {})
if not run_groups:
    raise SystemExit("run_groups is empty in config.yml")

group_counts: List[int] = []
for group_name, group in run_groups.items():
    query_args = group.get("query_args")
    if not isinstance(query_args, list) or not query_args:
        raise SystemExit(f"{group_name}: query_args must be a non-empty list")
    if len(query_args) != 1:
        raise SystemExit(
            f"{group_name}: query_args must contain exactly one argument axis (ef_search), got {len(query_args)}"
        )

    ef_search_values = query_args[0]
    if not isinstance(ef_search_values, list) or not ef_search_values:
        raise SystemExit(f"{group_name}: query_args[0] must be a non-empty list of ef_search values")

    for idx, value in enumerate(ef_search_values):
        if not isinstance(value, int) or value <= 0:
            raise SystemExit(
                f"{group_name}: query_args[0][{idx}] must be positive integer, got {value!r}"
            )
        if idx > 0 and value <= ef_search_values[idx - 1]:
            raise SystemExit(
                f"{group_name}: query_args[0] must be strictly increasing; "
                f"got {ef_search_values[idx - 1]} then {value}"
            )
    group_counts.append(len(ef_search_values))

total_groups = sum(group_counts)
min_groups = min(group_counts)
max_groups = max(group_counts)
print(f"{total_groups} {len(run_groups)} {min_groups} {max_groups}")
PY
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
read -r EXPECTED_TOTAL_GROUPS RUN_GROUP_COUNT MIN_GROUP_COUNT MAX_GROUP_COUNT <<<"$(validate_query_args_shape "${ALG_DIR}/config.yml")"
echo "[adapter] query sweep validated (${RUN_GROUP_COUNT} run-groups; per-group=${MIN_GROUP_COUNT}..${MAX_GROUP_COUNT}; total expected groups=${EXPECTED_TOTAL_GROUPS})"

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
  RESULT_DATASET_DIR="${ANN_ROOT}/results/${dataset}"
  if [[ -d "${RESULT_DATASET_DIR}" ]]; then
    echo "[adapter] removing stale results for dataset ${dataset}: ${RESULT_DATASET_DIR}"
    rm -rf "${RESULT_DATASET_DIR}"
  fi

  RUN_LOG="${ANN_ROOT}/results/_kyrodb_logs/${ALGORITHM}_${dataset}_$(date -u +%Y%m%dT%H%M%SZ).log"
  mkdir -p "$(dirname "${RUN_LOG}")"
  echo "[adapter] running dataset ${dataset}"
  if ! "${PYTHON_BIN}" run.py --force --algorithm "${ALGORITHM}" --dataset "${dataset}" 2>&1 | tee "${RUN_LOG}"; then
    echo "[adapter] run failed for dataset ${dataset}; see ${RUN_LOG}" >&2
    exit 1
  fi

  RESULT_DIR="${ANN_ROOT}/results/${dataset}/10/${ALGORITHM}"
  if [[ ! -d "${RESULT_DIR}" ]]; then
    echo "[adapter] missing results directory for dataset ${dataset}: ${RESULT_DIR}" >&2
    echo "[adapter] see ${RUN_LOG}" >&2
    exit 1
  fi

  RESULT_COUNT="$(find "${RESULT_DIR}" -type f -name '*.hdf5' | wc -l | tr -d '[:space:]')"
  EXPECTED_MIN_RESULTS="${EXPECTED_TOTAL_GROUPS}"
  if [[ "${RESULT_COUNT}" -lt "${EXPECTED_MIN_RESULTS}" ]]; then
    echo "[adapter] incomplete sweep detected for dataset ${dataset}; expected at least ${EXPECTED_MIN_RESULTS} result files, found ${RESULT_COUNT}" >&2
    echo "[adapter] see ${RUN_LOG}" >&2
    exit 1
  fi

  # Docker writes result files as root; fix permissions so we can read and plot them
  if [ -d "results/" ]; then
    chown -R "$(id -u):$(id -g)" results/ 2>/dev/null || {
      if command -v sudo >/dev/null 2>&1; then
        sudo chown -R "$(id -u):$(id -g)" results/ || true
      fi
    }
  fi

  if [[ "${RUN_PLOTS}" -eq 1 ]]; then
    echo "[adapter] plotting dataset ${dataset}"
    "${PYTHON_BIN}" plot.py --dataset "${dataset}"
  fi
done

echo "[adapter] completed datasets: ${DATASET_LIST[*]}"
