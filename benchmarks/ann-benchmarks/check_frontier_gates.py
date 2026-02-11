#!/usr/bin/env python3
"""Validate ANN frontier acceptance gates from summarized benchmark artifacts."""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def load_summary(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict) or "datasets" not in payload:
        raise ValueError(f"invalid summary json: {path}")
    return payload


def load_candidates(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def candidate_field_float(candidate: Dict[str, object], field: str) -> float:
    value = candidate.get(field)
    if value is None:
        raise ValueError(f"candidate missing field '{field}'")
    return float(value)


def bytes_per_vector(candidate: Dict[str, object]) -> float:
    mem = candidate_field_float(candidate, "index_estimated_memory_bytes")
    vectors = candidate_field_float(candidate, "indexed_vectors")
    if vectors <= 0:
        raise ValueError("indexed_vectors must be > 0")
    return mem / vectors


def pick_best_by_target(summary: dict, dataset: str, target: str) -> Dict[str, object]:
    datasets = summary.get("datasets", {})
    dataset_summary = datasets.get(dataset)
    if not isinstance(dataset_summary, dict):
        raise ValueError(f"dataset '{dataset}' missing from summary")
    best_by_target = dataset_summary.get("best_by_target", {})
    if not isinstance(best_by_target, dict):
        raise ValueError(f"dataset '{dataset}' missing best_by_target")
    best = best_by_target.get(target)
    if not isinstance(best, dict):
        raise ValueError(f"dataset '{dataset}' missing target {target}")
    candidate = best.get("candidate")
    if not isinstance(candidate, dict):
        raise ValueError(f"dataset '{dataset}' target {target} missing candidate")
    return candidate


def try_pick_best_by_target(summary: dict, dataset: str, target: str) -> Dict[str, object] | None:
    try:
        return pick_best_by_target(summary, dataset, target)
    except ValueError:
        return None


def verify_monotonic_recall(candidates: Iterable[dict], tolerance: float) -> List[str]:
    grouped: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)
    for row in candidates:
        key = (row["dataset"], row["m"], row["ef_construction"])
        grouped[key].append(row)

    violations: List[str] = []
    for (dataset, m, efc), rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda r: int(r["ef_search"]))
        prev_recall = -math.inf
        prev_efs = None
        for row in ordered:
            recall = float(row["recall"])
            efs = int(row["ef_search"])
            if recall + tolerance < prev_recall:
                violations.append(
                    f"{dataset} m={m} efc={efc}: recall dropped from {prev_recall:.6f}"
                    f" at ef_search={prev_efs} to {recall:.6f} at ef_search={efs}"
                )
            prev_recall = max(prev_recall, recall)
            prev_efs = efs
    return violations


def scan_logs_for_failures(log_globs: List[str], allow_regex: str | None) -> List[str]:
    if not log_globs:
        return []

    deny = re.compile(r"(panic|thread '.*' panicked|fatal|segmentation fault)", re.IGNORECASE)
    allow = re.compile(allow_regex, re.IGNORECASE) if allow_regex else None
    hits: List[str] = []
    for pattern in log_globs:
        for path in sorted(Path(p) for p in glob.glob(pattern)):
            if not path.is_file():
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for line_no, line in enumerate(text.splitlines(), start=1):
                if not deny.search(line):
                    continue
                if allow and allow.search(line):
                    continue
                hits.append(f"{path}:{line_no}: {line.strip()}")
    return hits


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary-json", required=True, type=Path)
    parser.add_argument("--candidates-csv", required=True, type=Path)
    parser.add_argument("--log-glob", action="append", default=[])
    parser.add_argument("--log-allow-regex", default="")
    parser.add_argument("--tolerance", type=float, default=1e-9)

    parser.add_argument("--min-gist-recall", type=float, default=0.99)
    parser.add_argument("--baseline-gist-qps-095", type=float, default=299.7)
    parser.add_argument("--baseline-glove-qps-099", type=float, default=87.7)
    parser.add_argument("--baseline-sift-qps-099", type=float, default=1577.8)
    parser.add_argument("--min-gist-qps-improvement", type=float, default=1.15)
    parser.add_argument("--min-glove-qps-improvement", type=float, default=1.15)
    parser.add_argument("--min-sift-retention", type=float, default=0.90)
    parser.add_argument("--baseline-gist-bytes-per-vector", type=float, default=9630.0)
    parser.add_argument("--baseline-mnist-bytes-per-vector", type=float, default=8155.0)
    parser.add_argument("--min-memory-reduction", type=float, default=0.20)
    parser.add_argument(
        "--allow-missing-datasets",
        action="store_true",
        help="Skip gates for datasets that are absent in summary (useful for smoke/partial runs).",
    )
    args = parser.parse_args()

    summary = load_summary(args.summary_json)
    candidates = load_candidates(args.candidates_csv)
    failures: List[str] = []

    missing: List[str] = []

    gist_max_recall = try_pick_best_by_target(summary, "gist-960-euclidean", "0.99")
    if gist_max_recall is None:
        missing.append("gist-960-euclidean@0.99")
    elif candidate_field_float(gist_max_recall, "recall") < args.min_gist_recall:
        failures.append(
            "GIST recall gate failed: "
            f"got {candidate_field_float(gist_max_recall, 'recall'):.4f}, "
            f"expected >= {args.min_gist_recall:.4f}"
        )

    gist_095 = try_pick_best_by_target(summary, "gist-960-euclidean", "0.95")
    if gist_095 is None:
        missing.append("gist-960-euclidean@0.95")
    else:
        gist_qps_target = args.baseline_gist_qps_095 * args.min_gist_qps_improvement
        if candidate_field_float(gist_095, "qps") < gist_qps_target:
            failures.append(
                "GIST QPS@0.95 gate failed: "
                f"got {candidate_field_float(gist_095, 'qps'):.2f}, expected >= {gist_qps_target:.2f}"
            )
        gist_mem_target = args.baseline_gist_bytes_per_vector * (1.0 - args.min_memory_reduction)
        if bytes_per_vector(gist_095) > gist_mem_target:
            failures.append(
                "GIST bytes/vector reduction failed: "
                f"got {bytes_per_vector(gist_095):.1f}, expected <= {gist_mem_target:.1f}"
            )

    glove_099 = try_pick_best_by_target(summary, "glove-100-angular", "0.99")
    if glove_099 is None:
        missing.append("glove-100-angular@0.99")
    else:
        glove_qps_target = args.baseline_glove_qps_099 * args.min_glove_qps_improvement
        if candidate_field_float(glove_099, "qps") < glove_qps_target:
            failures.append(
                "GloVe QPS@0.99 gate failed: "
                f"got {candidate_field_float(glove_099, 'qps'):.2f}, expected >= {glove_qps_target:.2f}"
            )

    sift_099 = try_pick_best_by_target(summary, "sift-128-euclidean", "0.99")
    if sift_099 is None:
        missing.append("sift-128-euclidean@0.99")
    else:
        sift_qps_floor = args.baseline_sift_qps_099 * args.min_sift_retention
        if candidate_field_float(sift_099, "qps") < sift_qps_floor:
            failures.append(
                "SIFT QPS@0.99 retention failed: "
                f"got {candidate_field_float(sift_099, 'qps'):.2f}, expected >= {sift_qps_floor:.2f}"
            )

    mnist_095 = try_pick_best_by_target(summary, "mnist-784-euclidean", "0.95")
    if mnist_095 is None:
        missing.append("mnist-784-euclidean@0.95")
    else:
        mnist_mem_target = args.baseline_mnist_bytes_per_vector * (1.0 - args.min_memory_reduction)
        if bytes_per_vector(mnist_095) > mnist_mem_target:
            failures.append(
                "MNIST bytes/vector reduction failed: "
                f"got {bytes_per_vector(mnist_095):.1f}, expected <= {mnist_mem_target:.1f}"
            )

    if missing and not args.allow_missing_datasets:
        failures.append(
            "Missing required datasets/targets in summary: " + ", ".join(sorted(set(missing)))
        )

    monotonic_violations = verify_monotonic_recall(candidates, args.tolerance)
    if monotonic_violations:
        failures.extend(
            ["Recall monotonicity violations detected:"] + monotonic_violations[:20]
        )

    log_hits = scan_logs_for_failures(args.log_glob, args.log_allow_regex or None)
    if log_hits:
        failures.extend(["Fatal log patterns detected:"] + log_hits[:20])

    if failures:
        print("Frontier acceptance gates: FAILED")
        for item in failures:
            print(f"- {item}")
        return 1

    print("Frontier acceptance gates: PASSED")
    print(
        "All checks passed: recall/QPS gates, memory reduction gates, monotonic recall, "
        "and fatal-log scan."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
