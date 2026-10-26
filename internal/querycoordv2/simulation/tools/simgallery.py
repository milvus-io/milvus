#!/usr/bin/env python3
"""Build a gallery of balancer comparisons from simulator traces.

    python3 simgallery.py -o gallery/ traces/*.json

Traces are named <label>_<case>.json (as bv2run/tracerun.sh writes them);
labels starting with m are master, with v ours. Every case with traces of
both gets a page from simviz.py and a row in index.html with the three
metric groups: memory (node fill min/max over mean at the end), table
distribution (p90 and worst of a table's largest node share over its fair
share, in segments and bytes) and churn (moves, ping-pong).
"""
import argparse
import collections
import glob
import html
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def summarize(path):
    with open(path) as f:
        t = json.load(f)
    cap = {n["id"]: n["mem"] for n in t["nodes"]}
    out = {"branch": t.get("branch", ""), "case": t.get("case", "")}
    # memory at the end: fill over mean, up nodes only
    if t["samples"]:
        last = t["samples"][-1]["n"]
        # Fill over the mean of the node's resource group: groups are
        # sized independently, so only nodes within a group compare.
        rgs = {int(i): g for i, g in (t["keyframes"][-1].get("rg") or {}).items()} if t["keyframes"] else {}
        groups = collections.defaultdict(list)
        for i, v in last.items():
            if v[3] and cap.get(int(i)):
                groups[rgs.get(int(i), "")].append(v[0] / cap[int(i)])
        ratios, fills = [], []
        for fs in groups.values():
            mean = sum(fs) / len(fs)
            fills += fs
            ratios += [f / mean if mean else 0 for f in fs]
        if fills:
            out["mem_min"] = min(ratios)
            out["mem_max"] = max(ratios)
            out["mem_mean"] = sum(fills) / len(fills)
    # table distribution at the end: largest node share / fair share
    if t["keyframes"]:
        kf = t["keyframes"][-1]["colls"]
        live = {int(i) for i, v in t["samples"][-1]["n"].items() if v[3]} if t["samples"] else set()
        # With resource groups a table's fair share is over the nodes of the
        # groups it lives in, not the whole cluster.
        rgs = {int(i): g for i, g in (t["keyframes"][-1].get("rg") or {}).items()}
        segr, byter = [], []
        for coll, nodes in kf.items():
            if coll == "0":
                continue
            segs = sum(v[0] for v in nodes.values())
            bts = sum(v[1] for v in nodes.values())
            n = max(1, len(live))
            if rgs:
                mine = {rgs.get(int(i)) for i in nodes}
                n = max(1, sum(1 for i in live if rgs.get(i) in mine))
            if segs >= 2 * n:
                segr.append(max(v[0] for v in nodes.values()) / (segs / n))
            if bts > 0 and segs >= 2 * n:
                byter.append(max(v[1] for v in nodes.values()) / (bts / n))

        def p90(xs):
            xs = sorted(xs)
            return xs[min(len(xs) - 1, int(0.9 * len(xs)))] if xs else 0

        out["seg_p90"], out["seg_worst"] = p90(segr), max(segr) if segr else 0
        out["bytes_p90"], out["bytes_worst"] = p90(byter), max(byter) if byter else 0
    # churn
    tasks = t["tasks"]
    out["moves"] = sum(1 for x in tasks if x["kind"] == "move")
    out["grows"] = sum(1 for x in tasks if x["kind"] == "grow")
    by = collections.Counter(x.get("reason") or x["kind"] for x in tasks if x["kind"] in ("move", "grow"))
    out["by_reason"] = dict(by)
    # ping-pong: a segment balance moved off a node and later loaded back on
    # it. Loads back after a user release and reload of the table, or after a
    # node restart, are not the balancer's doing and do not count.
    moves = sorted((x["t"], x["seg"], x["from"]) for x in tasks if x["kind"] == "move" and x.get("seg"))
    off = collections.defaultdict(set)
    mi = 0
    pingpong = 0
    for e in sorted(t["events"], key=lambda e: e["t"]):
        while mi < len(moves) and moves[mi][0] <= e["t"]:
            off[moves[mi][1]].add(moves[mi][2])
            mi += 1
        if e["kind"] == "load" and e["node"] in off[e.get("seg")]:
            pingpong += 1
    out["pingpong"] = pingpong
    out["seconds"] = t["samples"][-1]["t"] if t["samples"] else 0
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("traces", nargs="+")
    ap.add_argument("--simviz", default=os.path.join(HERE, "simviz.py"))
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    cases = collections.defaultdict(dict)
    for p in args.traces:
        base = os.path.basename(p)[:-5]
        m = re.match(r"([mv][0-9A-Za-z]*)_(.+)$", base)
        if not m:
            continue
        label, case = m.group(1), m.group(2).strip("_")
        cases[case][label] = p
    rows = []
    for case in sorted(cases):
        labels = sorted(cases[case], key=lambda l: (l[0] != "m", l))
        paths = [cases[case][l] for l in labels]
        page = case + ".html"
        subprocess.run([sys.executable, args.simviz, "-o", os.path.join(args.out, page), "--title", case] + paths, check=True)
        sums = [summarize(p) for p in paths]
        rows.append((case, page, labels, sums))
        print(case, " ".join("%s: moves=%d pp=%d mem=%.2f/%.2f seg90=%.2f byt90=%.2f" % (
            s["branch"], s["moves"], s["pingpong"], s.get("mem_min", 0), s.get("mem_max", 0), s.get("seg_p90", 0), s.get("bytes_p90", 0)) for s in sums))

    def cell(s, k, fmt="%.2f"):
        return fmt % s.get(k, 0)

    # Colour our rows against the master row of the same case: green when
    # better, red when worse by more than a small margin. Lower is better
    # for every column except mem min.
    def grade(ours, master, k, lower_better=True, margin=0.03):
        a, b = ours.get(k, 0), master.get(k, 0)
        if abs(a - b) <= margin * max(abs(b), 1):
            return ""
        better = a < b if lower_better else a > b
        return ' class="%s"' % ("good" if better else "bad")

    cols = [("mem_min", "min/mean", False), ("mem_max", "max/mean", True),
            ("seg_p90", "seg p90", True), ("seg_worst", "seg worst", True), ("bytes_p90", "bytes p90", True), ("bytes_worst", "bytes worst", True),
            ("moves", "moves", True), ("pingpong", "ping-pong", True)]
    h = ["<!doctype html><meta charset=utf-8><title>Balancer gallery</title>",
         "<style>body{font:14px system-ui;margin:24px}table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:4px 8px;text-align:right}th{background:#f3f3f3}"
         "td:first-child,td:nth-child(2){text-align:left}.m{color:#888}.good{background:#e3f6e3}.bad{background:#fbe3e3}th.g{border-bottom:2px solid #999}tr.tot td{font-weight:600;border-top:2px solid #999}</style>",
         "<h1>Balancer comparison: master vs two-level</h1>",
         "<p>Per case, one row per balancer; the two-level rows are coloured against master (green better, red worse, lower is better except min/mean). "
         "1. Memory: node fill over the mean of its resource group at the end (min and max). "
         "2. Tables: a table's largest node share over its fair share (over the nodes of its resource group), p90 and worst across tables with at least two segments per node, in segments and in bytes. "
         "3. Churn: segment moves and ping-pong (a segment loaded back onto a node that had released it).</p>",
         "<table><tr><th rowspan=2>case</th><th rowspan=2>balancer</th><th class=g colspan=2>1. memory</th><th class=g colspan=4>2. tables</th><th class=g colspan=2>3. churn</th><th rowspan=2>seconds</th></tr>",
         "<tr>" + "".join("<th>%s</th>" % t for _, t, _ in cols) + "</tr>"]
    totals = collections.defaultdict(lambda: collections.Counter())
    for case, page, labels, sums in rows:
        master = sums[0] if labels and labels[0].startswith("m") else None
        for i, s in enumerate(sums):
            name = '<a href="%s">%s</a>' % (html.escape(page), html.escape(case)) if i == 0 else '<span class=m>〃</span>'
            tds = []
            for k, _, lower in cols:
                cls = grade(s, master, k, lower) if master is not None and s is not master else ""
                tds.append("<td%s>%s</td>" % (cls, ("%d" % s[k]) if k in ("moves", "pingpong") else cell(s, k)))
            h.append("<tr><td>%s</td><td>%s</td>%s<td>%.0f</td></tr>" % (name, html.escape(s["branch"]), "".join(tds), s["seconds"]))
            t = totals["master" if s is master else "two-level"]
            t["moves"] += s["moves"]
            t["pingpong"] += s["pingpong"]
            t["cases"] += 1
            if master is not None and s is not master:
                for k, _, lower in cols:
                    g = grade(s, master, k, lower)
                    t[k + ("_better" if "good" in g else "_worse" if "bad" in g else "_same")] += 1
    for who in ("master", "two-level"):
        t = totals[who]
        if who == "master":
            tds = "".join("<td></td>" for _ in cols[:6]) + "<td>%d</td><td>%d</td>" % (t["moves"], t["pingpong"])
        else:
            tds = "".join("<td>%d / %d / %d</td>" % (t[k + "_better"], t[k + "_same"], t[k + "_worse"]) for k, _, _ in cols[:6]) + "<td>%d</td><td>%d</td>" % (t["moves"], t["pingpong"])
        h.append('<tr class=tot><td>total (%d cases)</td><td>%s</td>%s<td></td></tr>' % (t["cases"], who, tds))
    h.append("</table><p>Totals row for two-level: cases better / same / worse than master per column (3%% margin).</p>")
    with open(os.path.join(args.out, "index.html"), "w") as f:
        f.write("\n".join(h))
    print("wrote", os.path.join(args.out, "index.html"))


if __name__ == "__main__":
    main()
