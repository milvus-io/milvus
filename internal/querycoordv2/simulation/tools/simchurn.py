#!/usr/bin/env python3
"""Where does the churn in a trace come from?

    python3 simchurn.py trace.json [--show N]

Per trace: moves by phase (the case timeline's marks) and by reason;
reversals (a segment moved A->B and then B->A within --window seconds) by
the pair of reasons; ping-pong loads (a copy loaded back onto a node that
had released it) by the reason of the task that caused the load; and the
full history of the first --show reversed segments.
"""
import argparse
import bisect
import collections
import json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("trace")
    ap.add_argument("--window", type=float, default=300)
    ap.add_argument("--show", type=int, default=3)
    args = ap.parse_args()
    with open(args.trace) as f:
        t = json.load(f)
    marks = [(m["t"], m["label"]) for m in t["marks"] if "@" in m["label"] or m["label"] in ("scale out", "replay")]
    mt = [m[0] for m in marks]

    def phase(x):
        i = bisect.bisect_right(mt, x) - 1
        return marks[i][1].split("@")[0] if i >= 0 else "start"

    def label(x):
        return x.get("reason") or (x["kind"] + "/" + x["src"])

    tasks = sorted(t["tasks"], key=lambda x: x["t"])
    moves = [x for x in tasks if x["kind"] == "move"]
    print("%s %s: tasks=%d (dropped %d) moves=%d" % (t["case"], t["branch"], len(tasks), t.get("dropped_tasks", 0), len(moves)))
    print("moves by phase :", collections.Counter(phase(x["t"]) for x in moves).most_common())
    print("moves by reason:", collections.Counter(label(x) for x in moves).most_common())

    last = {}
    rev = collections.Counter()
    revphase = collections.Counter()
    revsegs = []
    for x in moves:
        k = x.get("seg")
        if k in last and last[k][1] == x["to"] and last[k][2] == x["from"] and x["t"] - last[k][0] < args.window:
            rev[(last[k][3], label(x))] += 1
            revphase[phase(x["t"])] += 1
            if len(revsegs) < args.show:
                revsegs.append(k)
        last[k] = (x["t"], x["from"], x["to"], label(x))
    print("reversals <%ds: %d" % (args.window, sum(rev.values())), rev.most_common(8))
    print("reversals by phase:", revphase.most_common())

    bykey = collections.defaultdict(list)
    for x in tasks:
        if x["kind"] in ("move", "grow"):
            bykey[(x.get("seg"), x["to"])].append((x["t"], label(x)))
    released = collections.defaultdict(set)
    pp = collections.Counter()
    ppphase = collections.Counter()
    for e in t["events"]:
        k = e.get("seg")
        if e["kind"] == "release":
            released[k].add(e["node"])
        elif e["kind"] == "load" and e["node"] in released[k]:
            c = [r for (tt, r) in bykey.get((k, e["node"]), []) if tt <= e["t"]]
            pp[c[-1] if c else "?"] += 1
            ppphase[phase(e["t"])] += 1
    print("pingpong: %d" % sum(pp.values()), pp.most_common(8))
    print("pingpong by phase:", ppphase.most_common())

    for s in revsegs:
        print("== segment %s" % s)
        rows = [(x["t"], "TASK  %-6s %-22s %s->%s" % (x["kind"], label(x), x["from"], x["to"])) for x in tasks if x.get("seg") == s]
        rows += [(e["t"], "EVENT %-8s node %s" % (e["kind"], e["node"])) for e in t["events"] if e.get("seg") == s]
        for r in sorted(rows):
            print("  %9.1f [%s] %s" % (r[0], phase(r[0]), r[1]))


if __name__ == "__main__":
    main()
