#!/usr/bin/env python3
"""
KyroDB performance plots from real benchmark data
- RMI vs B-Tree lookup latency: uses bench/rmi_vs_btree_data.csv
- HTTP uniform workload (64c, ~30s, 64B values): uses latest bench/results/*/summary.csv

This script overwrites the images referenced in README:
- bench/rmi_vs_btree.(png|svg)
- bench/http_uniform_64c_30s.(png|svg)
"""

import os
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

plt.style.use('seaborn-v0_8-whitegrid')

# Ensure we operate relative to repo's bench/ directory, not scripts/
# scripts/ is one level below bench/
BENCH_DIR = Path(__file__).resolve().parent.parent

# -------------------------------
# Plot 1: RMI vs B-Tree (engine)
# -------------------------------

def plot_rmi_vs_btree():
    csv_path = BENCH_DIR / "rmi_vs_btree_data.csv"
    if not csv_path.exists():
        print(f"Missing data file: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    # Ensure expected columns
    required = {
        "keys","btree_low_ns","btree_med_ns","btree_high_ns",
        "rmi_low_ns","rmi_med_ns","rmi_high_ns"
    }
    if not required.issubset(df.columns):
        print(f"Unexpected columns in {csv_path}; got {df.columns.tolist()}")
        return

    df = df.sort_values("keys")
    x_labels = [f"{int(k)//1_000_000}M" for k in df["keys"].values]
    positions = np.arange(len(df))

    btree_med = df["btree_med_ns"].values
    btree_low = df["btree_low_ns"].values
    btree_high = df["btree_high_ns"].values
    btree_err = np.vstack([btree_med - btree_low, btree_high - btree_med])

    rmi_med = df["rmi_med_ns"].values
    rmi_low = df["rmi_low_ns"].values
    rmi_high = df["rmi_high_ns"].values
    rmi_err = np.vstack([rmi_med - rmi_low, rmi_high - rmi_med])

    speedup = btree_med / rmi_med

    fig, ax = plt.subplots(figsize=(9.5, 5.5), dpi=160)
    ax.set_xticks(positions)
    ax.set_xticklabels(x_labels)

    c_btree = "#1f77b4"
    c_rmi = "#d62728"

    ax.errorbar(positions, btree_med, yerr=btree_err, fmt='-o', color=c_btree, capsize=4, lw=2,
                markersize=6, label='B-Tree (median ± range)')
    ax.errorbar(positions, rmi_med, yerr=rmi_err, fmt='-s', color=c_rmi, capsize=4, lw=2,
                markersize=6, label='RMI (median ± range)')

    ax.set_ylabel("Lookup latency (ns, lower is better)")
    ax.set_xlabel("Dataset size (unique keys)")
    ax.set_title("RMI vs B-Tree: Raw key lookup latency by dataset size")

    ax2 = ax.twinx()
    ax2.plot(positions, speedup, '--^', color="#444444", lw=1.5, markersize=6, label='Speedup (B-Tree / RMI)')
    ax2.set_ylabel("Speedup (×)")

    for x, y in zip(positions, btree_med):
        ax.annotate(f"{y:.0f} ns", (x, y), textcoords="offset points", xytext=(0, 10), ha='center',
                    fontsize=9, color="#333333", bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))
    for x, y in zip(positions, rmi_med):
        ax.annotate(f"{y:.0f} ns", (x, y), textcoords="offset points", xytext=(0, 10), ha='center',
                    fontsize=9, color="#333333", bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))
    for x, s in zip(positions, speedup):
        ax2.annotate(f"×{s:.2f}", (x, s), textcoords="offset points", xytext=(0, -18), ha='center',
                     fontsize=9, color="#333333", bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))

    ymax = max(btree_high.max(), rmi_high.max()) * 1.15
    ax.set_ylim(0, ymax)

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True)

    fig.tight_layout()
    out_png = BENCH_DIR / "rmi_vs_btree.png"
    out_svg = BENCH_DIR / "rmi_vs_btree.svg"
    plt.savefig(out_png, bbox_inches='tight')
    plt.savefig(out_svg, bbox_inches='tight')
    plt.close()
    print(f"Saved {out_png}")
    print(f"Saved {out_svg}")

# ------------------------------------------------------------
# Plot 2: HTTP uniform throughput/latency from summary results
# ------------------------------------------------------------

def find_latest_summary():
    results_base = BENCH_DIR / "results"
    if not results_base.exists():
        return None
    candidates = []
    for d in results_base.iterdir():
        if d.is_dir():
            f = d / "summary.csv"
            if f.exists():
                candidates.append((d.stat().st_mtime, f))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def plot_http_uniform():
    summary_csv = find_latest_summary()
    if summary_csv is None:
        print("No bench/results/*/summary.csv found; skipping HTTP uniform plot")
        return
    df = pd.read_csv(summary_csv)
    # Expect columns: scale, distribution, rps, p50_us, p95_us, p99_us, zipf_theta (optional)
    if "distribution" not in df.columns or "scale" not in df.columns:
        print(f"Unexpected summary format in {summary_csv}")
        return

    uni = df[df["distribution"] == "uniform"].copy()
    if uni.empty:
        print("No uniform distribution rows in summary; skipping HTTP plot")
        return
    uni = uni.sort_values("scale")

    N = uni["scale"].values.astype(float)
    x_labels = [f"{int(n)//1_000_000}M" for n in N]
    rps = uni["rps"].values.astype(float)
    p50_us = uni["p50_us"].values.astype(float)
    p95_us = uni["p95_us"].values.astype(float)
    p99_us = uni["p99_us"].values.astype(float)

    fig, ax = plt.subplots(figsize=(9.5, 5.5), dpi=160)
    ax.set_xscale('log', base=10)
    ax.set_xticks(N)
    ax.set_xticklabels(x_labels)

    c_rps = "#1f77b4"
    ax.plot(N, rps, '-o', color=c_rps, lw=2, markersize=6, label='Throughput (RPS)')
    ax.set_ylabel("Throughput (requests/sec)", color=c_rps)
    ax.tick_params(axis='y', labelcolor=c_rps)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))

    ax2 = ax.twinx()
    c_p50, c_p95, c_p99 = "#2ca02c", "#ff7f0e", "#d62728"
    ax2.plot(N, p50_us, '-o', color=c_p50, lw=2, markersize=6, label='p50 latency (µs)')
    ax2.plot(N, p95_us, '-s', color=c_p95, lw=2, markersize=6, label='p95 latency (µs)')
    ax2.plot(N, p99_us, '-^', color=c_p99, lw=2, markersize=6, label='p99 latency (µs)')
    ax2.set_ylabel("Latency (µs, lower is better)")
    ax2.grid(False)

    for x, y in zip(N, rps):
        ax.annotate(f"{y:,.0f}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center',
                    fontsize=9, color=c_rps, bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))
    for x, y in zip(N, p50_us):
        ax2.annotate(f"{y:.0f}µs", (x, y), textcoords="offset points", xytext=(0, -16), ha='center',
                     fontsize=9, color=c_p50, bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))
    for x, y in zip(N, p99_us):
        ax2.annotate(f"p99 {y:.0f}µs", (x, y), textcoords="offset points", xytext=(0, 10), ha='center',
                     fontsize=9, color=c_p99, bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))

    ax.set_title("HTTP read benchmark: throughput and latency vs dataset size\nuniform (latest results)")
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True)

    ax.set_ylim(0, max(rps) * 1.25)
    ax2.set_ylim(0, max(p99_us) * 1.25)

    fig.tight_layout()
    out_png = BENCH_DIR / "http_uniform_64c_30s.png"
    out_svg = BENCH_DIR / "http_uniform_64c_30s.svg"
    plt.savefig(out_png, bbox_inches='tight')
    plt.savefig(out_svg, bbox_inches='tight')
    plt.close()
    print(f"Saved {out_png}")
    print(f"Saved {out_svg}")


def main():
    plot_rmi_vs_btree()
    plot_http_uniform()

if __name__ == "__main__":
    main()