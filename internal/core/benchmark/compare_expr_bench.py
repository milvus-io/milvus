#!/usr/bin/env python3
"""Compare Google Benchmark JSON runs (base = master-equivalent, head = branch).

Takes the per-benchmark median over all repetitions in all given files and
fails when any head median is slower than base by more than --threshold.
The "matched" counter must be identical on both sides.
Exit codes: 0 ok, 1 regression, 2 benchmark sets / time units / matched counts
differ, a run errored, or no benchmark was comparable.
"""
import argparse
import json
import statistics
import sys
from collections import defaultdict


def load(paths, metric):
    samples = defaultdict(list)
    units = {}
    matched = defaultdict(set)
    for path in paths:
        with open(path) as f:
            data = json.load(f)
        for bench in data.get("benchmarks", []):
            if bench.get("run_type", "iteration") != "iteration":
                continue
            name = bench["name"]
            if bench.get("error_occurred"):
                print(f"ERROR {path}: {name}: {bench.get('error_message')}")
                sys.exit(2)
            unit = bench["time_unit"]
            if units.setdefault(name, unit) != unit:
                print(f"ERROR {name}: mixed time units {units[name]} / {unit}")
                sys.exit(2)
            samples[name].append(float(bench[metric]))
            matched[name].add(bench.get("matched"))
    return samples, units, matched


def relative_mad(values):
    med = statistics.median(values)
    if med <= 0:
        return 0.0
    return statistics.median([abs(v - med) for v in values]) / med


def fmt_matched(values):
    return ",".join(str(v) for v in sorted(values, key=str))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", nargs="+", required=True)
    parser.add_argument("--head", nargs="+", required=True)
    parser.add_argument("--metric", default="cpu_time",
                        choices=["cpu_time", "real_time"])
    parser.add_argument("--threshold", type=float, default=0.05)
    args = parser.parse_args()

    base, base_units, base_matched = load(args.base, args.metric)
    head, head_units, head_matched = load(args.head, args.metric)
    status = 0

    missing = sorted(set(base) ^ set(head))
    for name in missing:
        print(f"MISSING {name} (only in {'base' if name in base else 'head'})")
    if missing:
        status = 2

    rows = []
    for name in sorted(set(base) & set(head)):
        if base_units[name] != head_units[name]:
            print(f"UNIT_MISMATCH {name}: base {base_units[name]} / "
                  f"head {head_units[name]}")
            status = 2
            continue
        if base_matched[name] != head_matched[name]:
            print(f"MATCHED_MISMATCH {name}: base "
                  f"{fmt_matched(base_matched[name])} / head "
                  f"{fmt_matched(head_matched[name])}")
            status = 2
        b = statistics.median(base[name])
        h = statistics.median(head[name])
        delta = (h - b) / b if b > 0 else 0.0
        noise = max(relative_mad(base[name]), relative_mad(head[name]))
        rows.append((delta, name, b, h, len(base[name]), len(head[name]), noise))
    rows.sort(reverse=True)

    print(f"{'delta':>8}  {'base':>12}  {'head':>12}  {'n':>7}  {'mad/med':>7}  "
          f"benchmark ({args.metric}, median)")
    for delta, name, b, h, nb, nh, noise in rows:
        flag = "REGRESSION" if delta > args.threshold else ""
        if noise > args.threshold:
            flag = (flag + " NOISY").strip()
        print(f"{delta:+8.2%}  {b:12.4f}  {h:12.4f}  {nb:>3}/{nh:<3}  "
              f"{noise:7.1%}  {name} [{base_units[name]}] {flag}")

    regressions = [r for r in rows if r[0] > args.threshold]
    print(f"\n{len(regressions)} of {len(rows)} benchmarks slower than "
          f"{args.threshold:.0%}")
    if not rows:
        print("ERROR no comparable benchmarks")
        return 2
    if status:
        return status
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main())
