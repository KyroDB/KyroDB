#!/usr/bin/env python3
"""Summarize KyroDB in-process ANN benchmark results.

Input files are JSON outputs produced by `ann_inproc_bench`.
This script generates:
- strict machine-readable aggregate JSON
- optional markdown report
- optional flattened candidate CSV
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


@dataclass(frozen=True)
class Candidate:
    dataset: str
    source_json: str
    distance: str
    k: int
    m: int
    ef_construction: int
    ef_search: int
    repetitions: int
    warmup_queries: int
    recall: float
    recall_std: float
    qps: float
    qps_std: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    build_elapsed_ms: float
    build_vectors_per_second: float
    indexed_vectors: int
    index_estimated_memory_bytes: int


def parse_float_list(csv_value: str) -> List[float]:
    out: List[float] = []
    for token in csv_value.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(float(token))
    if not out:
        raise ValueError("expected at least one float value")
    for value in out:
        if not (0.0 < value <= 1.0):
            raise ValueError(f"recall target must be in (0,1], got {value}")
    return sorted(set(out))


def expand_inputs(globs: Sequence[str]) -> List[Path]:
    files: List[Path] = []
    for pattern in globs:
        matches = [Path(p) for p in glob.glob(pattern)]
        files.extend(matches)
    unique = sorted({p.resolve() for p in files})
    return unique


def load_candidates(paths: Iterable[Path]) -> tuple[List[Candidate], List[Dict[str, str]]]:
    candidates: List[Candidate] = []
    skipped: List[Dict[str, str]] = []
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        if not isinstance(payload, dict):
            skipped.append({"path": str(path), "reason": "top-level JSON is not an object"})
            continue
        if "dataset" not in payload or "config" not in payload or "index_build" not in payload or "sweeps" not in payload:
            skipped.append(
                {
                    "path": str(path),
                    "reason": "missing one of required keys: dataset, config, index_build, sweeps",
                }
            )
            continue

        if not isinstance(payload["dataset"], dict) or "name" not in payload["dataset"]:
            skipped.append({"path": str(path), "reason": "invalid dataset block"})
            continue

        dataset = payload["dataset"]["name"]
        config = payload["config"]
        build = payload["index_build"]
        sweeps = payload["sweeps"]

        if not isinstance(sweeps, list):
            skipped.append({"path": str(path), "reason": "sweeps is not a list"})
            continue

        for sweep in sweeps:
            if not isinstance(sweep, dict) or "aggregate" not in sweep or "ef_search" not in sweep:
                skipped.append({"path": str(path), "reason": "invalid sweep entry"})
                candidates = [c for c in candidates if c.source_json != str(path)]
                break
            aggregate = sweep["aggregate"]
            try:
                candidates.append(
                    Candidate(
                        dataset=str(dataset),
                        source_json=str(path),
                        distance=str(config["distance"]),
                        k=int(config["k"]),
                        m=int(config["m"]),
                        ef_construction=int(config["ef_construction"]),
                        ef_search=int(sweep["ef_search"]),
                        repetitions=int(config["repetitions"]),
                        warmup_queries=int(config["warmup_queries"]),
                        recall=float(aggregate["recall_mean"]),
                        recall_std=float(aggregate["recall_std"]),
                        qps=float(aggregate["qps_mean"]),
                        qps_std=float(aggregate["qps_std"]),
                        p50_ms=float(aggregate["p50_latency_ms_mean"]),
                        p95_ms=float(aggregate["p95_latency_ms_mean"]),
                        p99_ms=float(aggregate["p99_latency_ms_mean"]),
                        build_elapsed_ms=float(build["elapsed_ms"]),
                        build_vectors_per_second=float(build["vectors_per_second"]),
                        indexed_vectors=int(build["indexed_vectors"]),
                        index_estimated_memory_bytes=int(build["index_estimated_memory_bytes"]),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                skipped.append(
                    {"path": str(path), "reason": f"invalid numeric/content fields: {exc}"}
                )
                candidates = [c for c in candidates if c.source_json != str(path)]
                break
    return candidates, skipped


def is_dominated(candidate: Candidate, others: Sequence[Candidate]) -> bool:
    for other in others:
        if other is candidate:
            continue
        if other.dataset != candidate.dataset:
            continue
        if (
            other.recall >= candidate.recall
            and other.qps >= candidate.qps
            and (other.recall > candidate.recall or other.qps > candidate.qps)
        ):
            return True
    return False


def pareto_frontier(rows: Sequence[Candidate]) -> List[Candidate]:
    frontier = [row for row in rows if not is_dominated(row, rows)]
    return sorted(frontier, key=lambda r: (r.recall, r.qps), reverse=True)


def pick_best_qps(rows: Sequence[Candidate], target: float) -> Dict[str, object]:
    eligible = [row for row in rows if row.recall >= target]
    if eligible:
        winner = sorted(
            eligible,
            key=lambda r: (-r.qps, -r.recall, r.p99_ms, r.ef_search, r.m, r.ef_construction),
        )[0]
        return {"met_target": True, "candidate": asdict(winner)}

    # Fallback: no point satisfies target, return the max-recall point.
    fallback = sorted(rows, key=lambda r: (-r.recall, -r.qps, r.p99_ms))[0]
    return {"met_target": False, "candidate": asdict(fallback)}


def pick_max_recall(rows: Sequence[Candidate]) -> Candidate:
    return sorted(rows, key=lambda r: (-r.recall, -r.qps, r.p99_ms))[0]


def pick_max_qps(rows: Sequence[Candidate]) -> Candidate:
    return sorted(rows, key=lambda r: (-r.qps, -r.recall, r.p99_ms))[0]


def ranking_sort_key(row: Candidate, recall_target: float) -> tuple:
    met_target = row.recall >= recall_target
    if met_target:
        return (0, -row.qps, -row.recall, row.p99_ms, row.ef_search, row.m, row.ef_construction)
    return (1, -row.recall, -row.qps, row.p99_ms, row.ef_search, row.m, row.ef_construction)


def format_float(v: float, digits: int = 4) -> str:
    if math.isfinite(v):
        return f"{v:.{digits}f}"
    return "nan"


def render_markdown(
    *,
    generated_utc: str,
    recall_targets: Sequence[float],
    inputs: Sequence[Path],
    grouped: Dict[str, List[Candidate]],
    dataset_summaries: Dict[str, Dict[str, object]],
    top_frontier: int,
) -> str:
    lines: List[str] = []
    lines.append("# KyroDB In-Process ANN Summary")
    lines.append("")
    lines.append(f"- generated_utc: `{generated_utc}`")
    lines.append(f"- input_files: `{len(inputs)}`")
    lines.append(f"- datasets: `{len(grouped)}`")
    lines.append(f"- recall_targets: `{','.join(format_float(x, 2) for x in recall_targets)}`")
    lines.append("")

    lines.append("## Best QPS At Recall Targets")
    lines.append("")
    lines.append(
        "| dataset | target_recall | met_target | recall | qps | p95_ms | p99_ms | m | ef_construction | ef_search | source_json |"
    )
    lines.append(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    for dataset in sorted(grouped):
        best = dataset_summaries[dataset]["best_by_target"]
        assert isinstance(best, dict)
        for target in recall_targets:
            key = format_float(target, 2)
            entry = best[key]
            assert isinstance(entry, dict)
            cand = entry["candidate"]
            met = "yes" if entry["met_target"] else "no"
            lines.append(
                "| {dataset} | {target} | {met} | {recall} | {qps} | {p95} | {p99} | {m} | {efc} | {efs} | `{src}` |".format(
                    dataset=dataset,
                    target=format_float(target, 2),
                    met=met,
                    recall=format_float(float(cand["recall"]), 4),
                    qps=format_float(float(cand["qps"]), 2),
                    p95=format_float(float(cand["p95_ms"]), 3),
                    p99=format_float(float(cand["p99_ms"]), 3),
                    m=int(cand["m"]),
                    efc=int(cand["ef_construction"]),
                    efs=int(cand["ef_search"]),
                    src=Path(str(cand["source_json"])).name,
                )
            )
    lines.append("")

    lines.append("## Dataset Extremes")
    lines.append("")
    lines.append(
        "| dataset | max_recall | qps_at_max_recall | max_qps | recall_at_max_qps | build_vec_per_sec | index_mem_gb | candidates |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for dataset in sorted(grouped):
        summary = dataset_summaries[dataset]
        max_recall = summary["max_recall"]
        max_qps = summary["max_qps"]
        assert isinstance(max_recall, dict)
        assert isinstance(max_qps, dict)
        lines.append(
            "| {dataset} | {mr} | {mr_qps} | {mq} | {mq_rec} | {build_vps} | {mem_gb} | {n} |".format(
                dataset=dataset,
                mr=format_float(float(max_recall["recall"]), 4),
                mr_qps=format_float(float(max_recall["qps"]), 2),
                mq=format_float(float(max_qps["qps"]), 2),
                mq_rec=format_float(float(max_qps["recall"]), 4),
                build_vps=format_float(float(max_qps["build_vectors_per_second"]), 2),
                mem_gb=format_float(
                    float(max_qps["index_estimated_memory_bytes"]) / (1024.0 ** 3),
                    3,
                ),
                n=int(summary["candidate_count"]),
            )
        )
    lines.append("")

    lines.append("## Global Ranking")
    lines.append("")
    for target in recall_targets:
        lines.append(f"### Recall >= {format_float(target, 2)}")
        lines.append("")
        lines.append(
            "| rank | dataset | met_target | recall | qps | p95_ms | p99_ms | m | ef_construction | ef_search |"
        )
        lines.append("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        ranking: List[Candidate] = []
        for dataset in sorted(grouped):
            ranking.extend(grouped[dataset])
        ranking = sorted(ranking, key=lambda row: ranking_sort_key(row, target))
        for idx, row in enumerate(ranking[: min(20, len(ranking))], start=1):
            lines.append(
                "| {rank} | {dataset} | {met} | {recall} | {qps} | {p95} | {p99} | {m} | {efc} | {efs} |".format(
                    rank=idx,
                    dataset=row.dataset,
                    met="yes" if row.recall >= target else "no",
                    recall=format_float(row.recall, 4),
                    qps=format_float(row.qps, 2),
                    p95=format_float(row.p95_ms, 3),
                    p99=format_float(row.p99_ms, 3),
                    m=row.m,
                    efc=row.ef_construction,
                    efs=row.ef_search,
                )
            )
        lines.append("")

    lines.append("## Pareto Frontier")
    lines.append("")
    for dataset in sorted(grouped):
        lines.append(f"### {dataset}")
        lines.append("")
        lines.append("| recall | qps | p95_ms | p99_ms | m | ef_construction | ef_search |")
        lines.append("|---:|---:|---:|---:|---:|---:|---:|")
        frontier = dataset_summaries[dataset]["pareto_frontier"]
        assert isinstance(frontier, list)
        for row in frontier[:top_frontier]:
            lines.append(
                "| {recall} | {qps} | {p95} | {p99} | {m} | {efc} | {efs} |".format(
                    recall=format_float(float(row["recall"]), 4),
                    qps=format_float(float(row["qps"]), 2),
                    p95=format_float(float(row["p95_ms"]), 3),
                    p99=format_float(float(row["p99_ms"]), 3),
                    m=int(row["m"]),
                    efc=int(row["ef_construction"]),
                    efs=int(row["ef_search"]),
                )
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_csv(path: Path, rows: Sequence[Candidate]) -> None:
    fieldnames = list(asdict(rows[0]).keys()) if rows else []
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            for row in rows:
                writer.writerow(asdict(row))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize ann_inproc_bench JSON outputs into ranked report artifacts."
    )
    parser.add_argument(
        "--input-glob",
        action="append",
        required=True,
        help="Input glob pattern for ann_inproc_bench json outputs (repeatable).",
    )
    parser.add_argument(
        "--recall-targets",
        default="0.90,0.95,0.99",
        help="Comma-separated recall thresholds for QPS@R reporting.",
    )
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument("--output-md", type=Path)
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument(
        "--frontier-top",
        type=int,
        default=10,
        help="Max frontier points per dataset in markdown output.",
    )
    args = parser.parse_args()

    recall_targets = parse_float_list(args.recall_targets)
    input_files = expand_inputs(args.input_glob)
    if not input_files:
        raise SystemExit("no input files matched")

    candidates, skipped_files = load_candidates(input_files)
    if not candidates:
        raise SystemExit("no sweep candidates found in input files")

    for skipped in skipped_files:
        print(
            f"warning: skipping {skipped['path']}: {skipped['reason']}",
            file=sys.stderr,
        )

    grouped: Dict[str, List[Candidate]] = {}
    for row in candidates:
        grouped.setdefault(row.dataset, []).append(row)

    dataset_summaries: Dict[str, Dict[str, object]] = {}
    for dataset, rows in sorted(grouped.items()):
        best_by_target = {}
        for target in recall_targets:
            best_by_target[format_float(target, 2)] = pick_best_qps(rows, target)
        frontier = pareto_frontier(rows)
        dataset_summaries[dataset] = {
            "candidate_count": len(rows),
            "best_by_target": best_by_target,
            "max_recall": asdict(pick_max_recall(rows)),
            "max_qps": asdict(pick_max_qps(rows)),
            "pareto_frontier": [asdict(row) for row in frontier],
        }

    global_rankings = {}
    for target in recall_targets:
        all_rows = sorted(candidates, key=lambda row: ranking_sort_key(row, target))
        ranked = []
        for rank, row in enumerate(all_rows, start=1):
            item = asdict(row)
            item["rank"] = rank
            item["met_target"] = row.recall >= target
            ranked.append(item)
        global_rankings[format_float(target, 2)] = ranked

    generated_utc = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    output = {
        "generated_utc": generated_utc,
        "input_files": [str(p) for p in input_files],
        "input_file_count": len(input_files),
        "skipped_files": skipped_files,
        "candidate_count": len(candidates),
        "recall_targets": recall_targets,
        "datasets": dataset_summaries,
        "global_rankings": global_rankings,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")

    if args.output_md is not None:
        args.output_md.parent.mkdir(parents=True, exist_ok=True)
        args.output_md.write_text(
            render_markdown(
                generated_utc=generated_utc,
                recall_targets=recall_targets,
                inputs=input_files,
                grouped=grouped,
                dataset_summaries=dataset_summaries,
                top_frontier=max(1, args.frontier_top),
            ),
            encoding="utf-8",
        )

    if args.output_csv is not None:
        write_csv(args.output_csv, sorted(candidates, key=lambda r: (r.dataset, r.m, r.ef_construction, r.ef_search)))

    print(f"Wrote {args.output_json}")
    if args.output_md is not None:
        print(f"Wrote {args.output_md}")
    if args.output_csv is not None:
        print(f"Wrote {args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
