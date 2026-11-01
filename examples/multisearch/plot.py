"""Standalone publication/export chart from the recorded raw latency samples."""

import json
from pathlib import Path
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

root = Path("artifacts/multisearch-v2")
rows = json.loads((root / "performance-summary.json").read_text())["runs"]
variants = [
    ("stock", "search", "Stock Search"),
    ("modified", "search", "Modified Search"),
    ("modified", "multi", "MultiSearch"),
]
fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), layout="constrained")
colors = ["#2c5276", "#5697ba", "#087f80"]
for j, (kind, mode, label) in enumerate(variants):
    qps = []
    latency = []
    for cpus in (4, 2):
        rs = [
            r
            for r in rows
            if r["cpu_limit"] == cpus
            and r["logical_inflight_cap"] == 128
            and r["kind"] == kind
            and r["mode"] == mode
            and not r["offered_logical_qps"]
        ]
        assert len(rs) == 2, (cpus, kind, mode, len(rs))
        qps.append(np.mean([r["logical_qps"] for r in rs]))
        raw = []
        for r in rs:
            raw += json.loads((root / (r["name"] + ".json")).read_text())["groups"]["latencies_ms"]
        raw.sort()
        latency.append(raw[int((len(raw) - 1) * 0.99)])
    x = np.arange(2) + (j - 1) * 0.23
    for ax, values in zip(axes, (qps, latency)):
        bars = ax.bar(x, values, width=0.22, color=colors[j], label=label)
        ax.bar_label(bars, fmt="%.0f", padding=3, fontsize=9)
for ax in axes:
    ax.set_xticks([0, 1], ["4 CPU budget", "2 CPU contention"])
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.18)
    ax.set_axisbelow(True)
    ax.margins(y=0.18)
axes[0].set_title("Logical throughput — higher is better")
axes[0].set_ylabel("Successful searches / second")
axes[1].set_title("Group p99 latency — lower is better")
axes[1].set_ylabel("Milliseconds for all 8 searches")
axes[0].legend(frameon=False, fontsize=8, loc="upper right")
fig.suptitle("Native MultiSearch vs Milvus v3.0.5 • 128 logical requests in flight", fontsize=14)
fig.savefig(root / "performance-chart.svg")
fig.savefig(root / "performance-chart.png", dpi=160)
print(root / "performance-chart.svg")

svg = root / "performance-chart.svg"
svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")
