#!/usr/bin/env python3
"""Render ANN-style benchmark plots from in-process summary artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

if "MPLCONFIGDIR" not in os.environ:
    os.environ["MPLCONFIGDIR"] = tempfile.mkdtemp(prefix="kyro_mpl_")


try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "matplotlib is required for plot rendering. Install it with `pip install matplotlib`."
    ) from exc

EPS = 1e-9


@dataclass(frozen=True)
class CandidateRow:
    dataset: str
    recall: float
    qps_search: float
    qps_end_to_end: float
    p95_ms: float
    p99_ms: float
    build_elapsed_ms: float
    index_estimated_memory_bytes: float
    indexed_vectors: float
    m: int
    ef_construction: int
    ef_search: int

    @property
    def bytes_per_vector(self) -> float:
        if self.indexed_vectors <= 0:
            return 0.0
        return self.index_estimated_memory_bytes / self.indexed_vectors


def parse_float_list(csv_value: str) -> List[float]:
    out: List[float] = []
    for token in csv_value.split(","):
        token = token.strip()
        if token:
            out.append(float(token))
    if not out:
        raise ValueError("expected at least one recall target")
    return sorted(set(out))


def load_summary(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict) or "datasets" not in payload:
        raise ValueError(f"invalid summary JSON: {path}")
    return payload


def load_candidates(path: Path) -> List[CandidateRow]:
    rows: List[CandidateRow] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            rows.append(
                CandidateRow(
                    dataset=str(raw["dataset"]),
                    recall=float(raw["recall"]),
                    qps_search=float(raw.get("qps_search", raw.get("qps", 0.0))),
                    qps_end_to_end=float(raw.get("qps_end_to_end", raw.get("qps", 0.0))),
                    p95_ms=float(raw["p95_ms"]),
                    p99_ms=float(raw["p99_ms"]),
                    build_elapsed_ms=float(raw["build_elapsed_ms"]),
                    index_estimated_memory_bytes=float(raw["index_estimated_memory_bytes"]),
                    indexed_vectors=float(raw["indexed_vectors"]),
                    m=int(raw["m"]),
                    ef_construction=int(raw["ef_construction"]),
                    ef_search=int(raw["ef_search"]),
                )
            )
    if not rows:
        raise ValueError(f"no candidate rows found in {path}")
    return rows


def dataset_rows(rows: Iterable[CandidateRow]) -> Dict[str, List[CandidateRow]]:
    grouped: Dict[str, List[CandidateRow]] = defaultdict(list)
    for row in rows:
        grouped[row.dataset].append(row)
    for ds in grouped:
        grouped[ds] = sorted(grouped[ds], key=lambda r: (r.recall, r.qps_search))
    return dict(grouped)


def _save_plot(path: Path, dpi: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=dpi)
    plt.close()


def render_dataset_plots(
    *,
    dataset: str,
    rows: Sequence[CandidateRow],
    frontier: Sequence[dict],
    out_dir: Path,
    dpi: int,
) -> List[str]:
    files: List[str] = []
    safe = dataset.replace("/", "_")

    x_recall = [r.recall for r in rows]
    y_qps = [max(r.qps_search, EPS) for r in rows]
    y_build_s = [r.build_elapsed_ms / 1000.0 for r in rows]
    y_p95 = [r.p95_ms for r in rows]
    x_bpv = [r.bytes_per_vector for r in rows]

    frontier_rows = sorted(
        [
            (
                float(row.get("recall", 0.0)),
                max(float(row.get("qps", row.get("qps_search", 0.0))), 1e-9),
            )
            for row in frontier
        ],
        key=lambda t: t[0],
    )
    fx = [v[0] for v in frontier_rows]
    fy = [v[1] for v in frontier_rows]

    plt.figure(figsize=(8.5, 5.2))
    plt.scatter(x_recall, y_qps, s=28, alpha=0.55, color="#1f77b4", label="Configs")
    if fx and fy:
        plt.plot(fx, fy, color="#d62728", linewidth=2.0, label="Pareto Frontier")
    plt.yscale("log")
    plt.xlabel("Recall@k")
    plt.ylabel("QPS (search-only, log scale)")
    plt.title(f"{dataset}: Recall vs QPS")
    plt.grid(alpha=0.25, linestyle="--")
    plt.legend(loc="best")
    path = out_dir / f"{safe}_recall_vs_qps.png"
    _save_plot(path, dpi)
    files.append(path.name)

    plt.figure(figsize=(8.5, 5.2))
    plt.scatter(x_recall, y_build_s, s=28, alpha=0.6, color="#2ca02c")
    plt.xlabel("Recall@k")
    plt.ylabel("Build Time (seconds)")
    plt.title(f"{dataset}: Recall vs Build Time")
    plt.grid(alpha=0.25, linestyle="--")
    path = out_dir / f"{safe}_recall_vs_build_seconds.png"
    _save_plot(path, dpi)
    files.append(path.name)

    plt.figure(figsize=(8.5, 5.2))
    plt.scatter(x_recall, y_p95, s=28, alpha=0.6, color="#9467bd")
    plt.xlabel("Recall@k")
    plt.ylabel("p95 Latency (ms)")
    plt.title(f"{dataset}: Recall vs p95 Latency")
    plt.grid(alpha=0.25, linestyle="--")
    path = out_dir / f"{safe}_recall_vs_p95_ms.png"
    _save_plot(path, dpi)
    files.append(path.name)

    plt.figure(figsize=(8.5, 5.2))
    plt.scatter(x_bpv, y_qps, s=28, alpha=0.6, color="#ff7f0e")
    plt.yscale("log")
    plt.xlabel("Estimated Bytes per Vector")
    plt.ylabel("QPS (search-only, log scale)")
    plt.title(f"{dataset}: Memory Density vs QPS")
    plt.grid(alpha=0.25, linestyle="--")
    path = out_dir / f"{safe}_memory_vs_qps.png"
    _save_plot(path, dpi)
    files.append(path.name)

    return files


def render_global_target_plot(
    *,
    summary: dict,
    recall_targets: Sequence[float],
    out_dir: Path,
    dpi: int,
) -> str:
    datasets = sorted(summary["datasets"].keys())
    plt.figure(figsize=(10.5, 5.8))

    width = 0.8 / max(len(recall_targets), 1)
    x_positions = list(range(len(datasets)))
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]

    for idx, target in enumerate(recall_targets):
        key = f"{target:.2f}"
        values = []
        for dataset in datasets:
            ds_summary = summary["datasets"][dataset]
            best = ds_summary["best_by_target"].get(key)
            qps = 0.0
            if isinstance(best, dict):
                cand = best.get("candidate")
                if isinstance(cand, dict):
                    qps = float(cand.get("qps", cand.get("qps_search", 0.0)))
            values.append(max(qps, EPS))
        offset = (idx - (len(recall_targets) - 1) / 2.0) * width
        bars_x = [x + offset for x in x_positions]
        plt.bar(
            bars_x,
            values,
            width=width,
            color=colors[idx % len(colors)],
            alpha=0.85,
            label=f"Recall >= {target:.2f}",
        )

    plt.yscale("log")
    plt.xticks(x_positions, datasets, rotation=22, ha="right")
    plt.ylabel("QPS (search-only, log scale)")
    plt.title("Best QPS at Target Recall")
    plt.grid(axis="y", alpha=0.25, linestyle="--")
    plt.legend(loc="best")

    path = out_dir / "global_qps_at_recall_targets.png"
    _save_plot(path, dpi)
    return path.name


def write_index_md(
    *,
    path: Path,
    generated_utc: str,
    summary_json: Path,
    candidates_csv: Path,
    per_dataset_files: Dict[str, List[str]],
    global_file: str,
) -> None:
    lines: List[str] = []
    lines.append("# In-Process Benchmark Plots")
    lines.append("")
    lines.append(f"- generated_utc: `{generated_utc}`")
    lines.append(f"- summary_json: `{summary_json}`")
    lines.append(f"- candidates_csv: `{candidates_csv}`")
    lines.append("")
    lines.append("## Global")
    lines.append("")
    lines.append(f"![global_qps_at_recall_targets]({global_file})")
    lines.append("")
    lines.append("## Per Dataset")
    lines.append("")
    for dataset in sorted(per_dataset_files):
        lines.append(f"### {dataset}")
        lines.append("")
        for file_name in per_dataset_files[dataset]:
            lines.append(f"![{file_name}]({file_name})")
            lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary-json", required=True, type=Path)
    parser.add_argument("--candidates-csv", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--recall-targets", default="0.95,0.99")
    parser.add_argument("--dpi", type=int, default=140)
    args = parser.parse_args()

    summary = load_summary(args.summary_json)
    rows = load_candidates(args.candidates_csv)
    grouped = dataset_rows(rows)
    recall_targets = parse_float_list(args.recall_targets)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    per_dataset_files: Dict[str, List[str]] = {}
    for dataset, ds_rows in sorted(grouped.items()):
        ds_summary = summary["datasets"].get(dataset, {})
        frontier = ds_summary.get("pareto_frontier", []) if isinstance(ds_summary, dict) else []
        per_dataset_files[dataset] = render_dataset_plots(
            dataset=dataset,
            rows=ds_rows,
            frontier=frontier if isinstance(frontier, list) else [],
            out_dir=args.output_dir,
            dpi=args.dpi,
        )

    global_file = render_global_target_plot(
        summary=summary,
        recall_targets=recall_targets,
        out_dir=args.output_dir,
        dpi=args.dpi,
    )

    manifest = {
        "generated_utc": summary.get("generated_utc"),
        "summary_json": str(args.summary_json),
        "candidates_csv": str(args.candidates_csv),
        "output_dir": str(args.output_dir),
        "global_plot": global_file,
        "per_dataset_plots": per_dataset_files,
    }
    manifest_path = args.output_dir / "plot_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    index_md = args.output_dir / "index.md"
    write_index_md(
        path=index_md,
        generated_utc=str(summary.get("generated_utc", "unknown")),
        summary_json=args.summary_json,
        candidates_csv=args.candidates_csv,
        per_dataset_files=per_dataset_files,
        global_file=global_file,
    )

    print(f"Wrote {manifest_path}")
    print(f"Wrote {index_md}")
    print(f"Wrote plots under {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
