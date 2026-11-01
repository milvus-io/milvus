"""Aggregate mixed-load evidence; keep resident and injected latencies separate."""

import json
import statistics
from pathlib import Path

root = Path("artifacts/multisearch-mixed")
rows = []
for p in sorted(root.glob("r[0-9]-*.json")):
    if p.name.endswith(".resources.json"):
        continue
    resource = p.with_suffix(".resources.json")
    if not resource.exists():
        continue
    d = json.loads(p.read_text())
    rs = json.loads(resource.read_text())
    resident = d["resident"]
    secondary = d["secondary"]["mixed"]
    row = {
        "name": p.stem,
        "mode": d["mode"],
        "offered_secondary_logical_qps": d["offered_secondary_logical_qps"],
        "resident": {
            k: {kk: vv for kk, vv in v.items() if kk != "latencies_ms"} for k, v in resident.items()
        },
        "secondary": {k: v for k, v in secondary.items() if k != "latencies_ms"},
        "resources": rs["phases"],
        "resident_retention": resident["mixed"]["success_qps"]
        / resident["baseline"]["success_qps"],
        "resident_recovery": resident["recovery"]["success_qps"]
        / resident["baseline"]["success_qps"],
        "errors_all": d["resident_errors_all"] + d["secondary_errors_all"],
        "completed_all": d["resident_total"] + d["secondary_total"],
        "scheduled_groups": d["scheduled_groups"],
        "admitted_groups": d["admitted_groups"],
        "dropped_groups": d["dropped_groups"],
        "generator_late_groups": d["generator_late_groups"],
    }
    ss = [
        s
        for s in d["seconds"]
        if resident["mixed"]["window_start_s"] <= s["second"] < resident["mixed"]["window_end_s"]
    ]
    row["minimum_resident_successes_per_second"] = min(s["resident_successes"] for s in ss)
    ins = [
        s
        for s in d["inflight_samples"]
        if resident["mixed"]["window_start_s"] <= s["elapsed_s"] < resident["mixed"]["window_end_s"]
    ]
    row["mean_resident_inflight"] = statistics.mean(s["resident_calls"] for s in ins)
    row["mean_secondary_groups_inflight"] = statistics.mean(s["secondary_groups"] for s in ins)
    rows.append(row)


def pooled_quantile(selected, traffic, phase):
    values = []
    for r in selected:
        d = json.loads((root / (r["name"] + ".json")).read_text())
        values += d[traffic][phase]["latencies_ms"] or []
    values.sort()
    return values[int((len(values) - 1) * 0.99)] if values else 0


aggregates = []
for mode, rate in [
    ("none", 0),
    ("search", 160),
    ("multi", 160),
    ("search", 480),
    ("multi", 480),
    ("search", 0),
    ("multi", 0),
]:
    selected = [r for r in rows if r["mode"] == mode and r["offered_secondary_logical_qps"] == rate]
    if not selected:
        continue
    avg = lambda fn: statistics.mean(fn(r) for r in selected)
    aggregates.append(
        {
            "mode": mode,
            "offered_secondary_logical_qps": rate,
            "repetitions": len(selected),
            "resident_qps": {
                k: avg(lambda r: r["resident"][k]["success_qps"])
                for k in ("baseline", "mixed", "recovery")
            },
            "resident_p99_ms": {
                k: pooled_quantile(selected, "resident", k)
                for k in ("baseline", "mixed", "recovery")
            },
            "resident_retention": avg(lambda r: r["resident_retention"]),
            "resident_retention_range": [
                min(r["resident_retention"] for r in selected),
                max(r["resident_retention"] for r in selected),
            ],
            "resident_recovery": avg(lambda r: r["resident_recovery"]),
            "secondary_qps": avg(lambda r: r["secondary"]["success_qps"]),
            "secondary_group_p99_ms": pooled_quantile(selected, "secondary", "mixed"),
            "cpu_utilization": {
                k: avg(lambda r: r["resources"][k]["mean_cpu_utilization"])
                for k in ("baseline", "mixed", "recovery")
            },
            "min_resident_successes_per_second": min(
                r["minimum_resident_successes_per_second"] for r in selected
            ),
            "errors_all": sum(r["errors_all"] for r in selected),
            "drops": sum(r["dropped_groups"] + r["generator_late_groups"] for r in selected),
            "mean_resident_inflight": avg(lambda r: r["mean_resident_inflight"]),
            "mean_secondary_groups_inflight": avg(lambda r: r["mean_secondary_groups_inflight"]),
        }
    )
summary = {
    "runs": rows,
    "aggregates": aggregates,
    "total_completed_all_phases_including_warmup": sum(r["completed_all"] for r in rows),
    "errors_all": sum(r["errors_all"] for r in rows),
    "drops": sum(r["dropped_groups"] for r in rows),
    "generator_late_groups": sum(r["generator_late_groups"] for r in rows),
    "server_binary_changed": False,
}
(root / "summary.json").write_text(json.dumps(summary, indent=2))
for r in aggregates:
    print(
        f"{r['mode']:6} rate={r['offered_secondary_logical_qps']:3} n={r['repetitions']} A={r['resident_qps']['baseline']:.1f}->{r['resident_qps']['mixed']:.1f}->{r['resident_qps']['recovery']:.1f} retain={r['resident_retention']:.1%} A-p99={r['resident_p99_ms']['mixed']:.1f} B={r['secondary_qps']:.1f} B-p99={r['secondary_group_p99_ms']:.1f} cpu={r['cpu_utilization']['mixed']:.1%}"
    )
