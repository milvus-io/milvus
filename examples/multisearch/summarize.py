import json, re, statistics
from pathlib import Path

root = Path("artifacts/multisearch-v2")
rows = []
for p in sorted(root.glob("*.json")):
    if not re.match(r"(2cpu-)?(stock|multi)-(search|multi)-(c\d+-r\d+|rate\d+)\.json$", p.name):
        continue
    rp = p.with_suffix(".resources.json")
    if not rp.exists():
        continue
    d = json.loads(p.read_text())
    r = json.loads(rp.read_text())
    samples = r["samples"]
    warmup = 5
    m = [x for x in samples if warmup <= x["elapsed_s"] < warmup + d["measurement_seconds"]]
    throttle = None
    if len(m) > 1:
        a, b = m[0]["throttling"], m[-1]["throttling"]
        periods = b.get("periods", 0) - a.get("periods", 0)
        if periods:
            throttle = (b.get("throttled_periods", 0) - a.get("throttled_periods", 0)) / periods
    rows.append(
        {
            "name": p.stem,
            "cpu_limit": r["cpu_limit"],
            "mode": d["mode"],
            "kind": "stock" if d["target"].endswith("29560") else "modified",
            "logical_inflight_cap": d["group_workers"] * 8,
            "offered_logical_qps": d["offered_groups_per_second"] * 8,
            "logical_qps": d["logical_success_qps"],
            "group_p50_ms": d["groups"]["p50_ms"],
            "group_p95_ms": d["groups"]["p95_ms"],
            "group_p99_ms": d["groups"]["p99_ms"],
            "probe_p99_ms": d["ordinary_search_probe"]["p99_ms"],
            "probe_samples": d["ordinary_search_probe"]["samples"],
            "cpu_utilization": r["mean_cpu_utilization"],
            "throttled_period_fraction": throttle,
            "peak_memory_mib": r["peak_memory_bytes"] / 2**20,
            "peak_goroutines": r["peak_goroutines"],
            "errors": d["groups"]["logical_errors"] + d["ordinary_search_probe"]["logical_errors"],
            "dropped_groups": d["dropped_groups"],
            "logical_completed": d["groups"]["logical_completed"],
            "duration_seconds": d["measurement_seconds"],
        }
    )
summary = {
    "runs": rows,
    "total_logical_searches": sum(x["logical_completed"] for x in rows),
    "total_errors": sum(x["errors"] for x in rows),
    "dropped_groups": sum(x["dropped_groups"] for x in rows),
    "measurement_seconds": sum(x["duration_seconds"] for x in rows),
}
(root / "performance-summary.json").write_text(json.dumps(summary, indent=2))
for r in rows:
    print(
        f"{r['name']:33} qps={r['logical_qps']:7.1f} cpu={r['cpu_utilization']:.1%} p99={r['group_p99_ms']:7.2f} probe={r['probe_p99_ms']:7.2f} errors={r['errors']}"
    )
