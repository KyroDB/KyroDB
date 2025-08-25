# RMI vs B-Tree: Raw key lookup latency by dataset size (engine, in-process)

import numpy as np
import matplotlib.pyplot as plt

plt.style.use('seaborn-v0_8-whitegrid')

# Dataset sizes
N = np.array([1_000_000, 10_000_000, 50_000_000, 100_000_000, 300_000_000], dtype=float)
x_labels = ["1M", "10M", "50M", "100M", "300M"]

# Latencies in ns: [low, median, high]
btree = np.array([
    [262.19, 264.70, 267.01],
    [549.69, 556.46, 563.92],
    [857.66, 874.82, 892.34],
    [898.32, 913.88, 927.44],
    [1011.3, 1021.7, 1031.5],  # 1.0113–1.0315 µs → ns
])

rmi = np.array([
    [104.50, 107.02, 109.15],
    [221.22, 221.50, 221.81],
    [266.43, 267.51, 268.66],
    [283.76, 286.09, 288.62],
    [391.10, 395.58, 400.07],
])

# Extract medians and bounds
btree_med = btree[:, 1]
btree_low = btree[:, 0]
btree_high = btree[:, 2]
btree_err = np.vstack([btree_med - btree_low, btree_high - btree_med])

rmi_med = rmi[:, 1]
rmi_low = rmi[:, 0]
rmi_high = rmi[:, 2]
rmi_err = np.array([rmi_med - rmi_low, rmi_high - rmi_med])

# Speedup (btree / rmi) using medians
speedup = btree_med / rmi_med

fig, ax = plt.subplots(figsize=(9.5, 5.5), dpi=160)

# Use evenly spaced positions instead of a log scale for visual separation
positions = np.arange(len(N))
ax.set_xticks(positions)
ax.set_xticklabels(x_labels)

# Plot with error bars using positions
c_btree = "#1f77b4"
c_rmi = "#d62728"

btree_plot = ax.errorbar(
    positions, btree_med, yerr=btree_err, fmt='-o', color=c_btree, capsize=4, lw=2,
    markersize=6, label='B-Tree (median ± range)'
)
rmi_plot = ax.errorbar(
    positions, rmi_med, yerr=rmi_err, fmt='-s', color=c_rmi, capsize=4, lw=2,
    markersize=6, label='RMI (median ± range)'
)

ax.set_ylabel("Lookup latency (ns, lower is better)")
ax.set_xlabel("Dataset size (unique keys)")
ax.set_title("RMI vs B-Tree: Raw key lookup latency by dataset size")

# Secondary axis for speedup
ax2 = ax.twinx()
ax2.plot(positions, speedup, '--^', color="#444444", lw=1.5, markersize=6, label='Speedup (B-Tree / RMI)')
ax2.set_ylabel("Speedup (×)")

# Annotate latency near points
for x, y in zip(positions, btree_med):
    ax.annotate(f"{y:.0f} ns", (x, y), textcoords="offset points", xytext=(0, 10), ha='center',
                fontsize=9, color="#333333", bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))
for x, y in zip(positions, rmi_med):
    ax.annotate(f"{y:.0f} ns", (x, y), textcoords="offset points", xytext=(0, 10), ha='center',
                fontsize=9, color="#333333", bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))

# Annotate speedup
for x, s in zip(positions, speedup):
    ax2.annotate(f"×{s:.2f}", (x, s), textcoords="offset points", xytext=(0, -18), ha='center',
                 fontsize=9, color="#333333", bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))

# Limits
ymin = 0
ymax = max(rmi_high.max(), btree_high.max()) * 1.15
ax.set_ylim(ymin, ymax)

# Legend across axes
lines1, labels1 = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True)

fig.tight_layout()

# Save outputs
out_png = "/Users/kishan/Desktop/Codes/Project/ProjectKyro/bench/rmi_vs_btree.png"
out_svg = "/Users/kishan/Desktop/Codes/Project/ProjectKyro/bench/rmi_vs_btree.svg"
plt.savefig(out_png, bbox_inches='tight')
plt.savefig(out_svg, bbox_inches='tight')
print(f"Saved {out_png}")
print(f"Saved {out_svg}")





# HTTP read benchmark: throughput and latency vs dataset size (uniform, 64 concurrency, 30s, 64B values)    

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

plt.style.use('seaborn-v0_8-whitegrid')

# Dataset sizes
N = np.array([1_000_000, 10_000_000, 50_000_000], dtype=float)
x_labels = ["1M", "10M", "50M"]

# Metrics (uniform dist, 64 concurrency, 30s, 64B values)
rps = np.array([47871.17, 50101.00, 49511.37])
p50_us = np.array([483.3, 672.8, 591.4])
p95_us = np.array([1657.9, 1688.6, 1631.2])
p99_us = np.array([2021.4, 2107.4, 1997.8])

fig, ax = plt.subplots(figsize=(9.5, 5.5), dpi=160)

# X on log scale to reflect 1M→50M spacing
ax.set_xscale('log', base=10)
ax.set_xticks(N)
ax.set_xticklabels(x_labels)

# Left axis: throughput (RPS)
c_rps = "#1f77b4"
ax.plot(N, rps, '-o', color=c_rps, lw=2, markersize=6, label='Throughput (RPS)')
ax.set_ylabel("Throughput (requests/sec)", color=c_rps)
ax.tick_params(axis='y', labelcolor=c_rps)
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))

# Right axis: tail latencies (µs)
ax2 = ax.twinx()
c_p50, c_p95, c_p99 = "#2ca02c", "#ff7f0e", "#d62728"
ax2.plot(N, p50_us, '-o', color=c_p50, lw=2, markersize=6, label='p50 latency (µs)')
ax2.plot(N, p95_us, '-s', color=c_p95, lw=2, markersize=6, label='p95 latency (µs)')
ax2.plot(N, p99_us, '-^', color=c_p99, lw=2, markersize=6, label='p99 latency (µs)')
ax2.set_ylabel("Latency (µs, lower is better)")
ax2.grid(False)

# Annotate values for quick read
for x, y in zip(N, rps):
    ax.annotate(f"{y:,.0f}", (x, y), textcoords="offset points", xytext=(0, 8), ha='center',
                fontsize=9, color=c_rps, bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))
for x, y, lbl, col in [(N[0], p50_us[0], f"{p50_us[0]:.0f}µs", c_p50),
                       (N[1], p50_us[1], f"{p50_us[1]:.0f}µs", c_p50),
                       (N[2], p50_us[2], f"{p50_us[2]:.0f}µs", c_p50)]:
    ax2.annotate(lbl, (x, y), textcoords="offset points", xytext=(0, -16), ha='center',
                 fontsize=9, color=col, bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))
for x, y, lbl, col in [(N[0], p99_us[0], f"p99 {p99_us[0]:.0f}µs", c_p99),
                       (N[1], p99_us[1], f"p99 {p99_us[1]:.0f}µs", c_p99),
                       (N[2], p99_us[2], f"p99 {p99_us[2]:.0f}µs", c_p99)]:
    ax2.annotate(lbl, (x, y), textcoords="offset points", xytext=(0, 10), ha='center',
                 fontsize=9, color=col, bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#dddddd", alpha=0.8))

# Titles and legend
ax.set_title("HTTP read benchmark: throughput and latency vs dataset size\n"
             "uniform, 64 concurrency, 30s, value=64B")
lines1, labels1 = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True)

# Bounds
ax.set_ylim(0, max(rps)*1.25)
ax2.set_ylim(0, max(p99_us)*1.25)

fig.tight_layout()

# Save outputs
out_png = "/Users/kishan/Desktop/Codes/Project/ProjectKyro/bench/http_uniform_64c_30s.png"
out_svg = "/Users/kishan/Desktop/Codes/Project/ProjectKyro/bench/http_uniform_64c_30s.svg"
plt.savefig(out_png, bbox_inches='tight')
plt.savefig(out_svg, bbox_inches='tight')
print(f"Saved {out_png}")
print(f"Saved {out_svg}")

# Printed summary
print("\nDataset   RPS      p50(µs)  p95(µs)  p99(µs)")
for lbl, tr, a, b, c in zip(x_labels, rps, p50_us, p95_us, p99_us):
    print(f"{lbl:<6}  {tr:7.0f}   {a:7.1f}  {b:7.1f}  {c:7.1f}")