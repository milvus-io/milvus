#!/usr/bin/env python3
"""simviz: render QueryCoord balancer simulation traces as one static HTML page.

    python3 simviz.py -o out.html traceA.json [traceB.json ...] [--title T] [--bin 10]
                      [--max-grows 20000] [--max-events 50000] [--max-keyframes 120]

Each trace (see trace.go, written when MILVUS_SIM_TRACE=<file> is set) becomes
one column; columns share the time axis, a single time cursor and the keyframe
slider. Typical use: the same scenario run under master ("m…" branch) and under
our balancer ("v…" branch).

Day-long traces carry 100k-200k tasks and 200k-300k events; to keep the page
usable, grow tasks beyond --max-grows and load events beyond --max-events are
uniformly subsampled (all other tasks / events are kept), keyframes are capped
at --max-keyframes (labeled ones are always kept). All summary statistics are
computed from the full data before subsampling; the histogram scales sampled
grows back up by the sampling factor; the task list shows what is embedded.

Python 3.9 stdlib only. Plotly is loaded from jsdelivr by the generated page;
everything else (data, JS, CSS) is inline.
"""

import argparse
import base64
import gzip
import html
import json
import math
import os
import sys
from collections import Counter

MAX_POINTS = 2000      # per node, memory timeline
TOP_TABLES_BAR = 12    # tables shown individually in the per-node byte bars
MOVE_LIST_LIMIT = 500
DEFAULT_MAX_GROWS = 20000
DEFAULT_MAX_EVENTS = 50000
DEFAULT_MAX_KEYFRAMES = 120
COMPRESS_THRESHOLD = 8 * 1024 * 1024   # --compress auto: gzip the payload above this size

# Reasons that master (and our balancer, through the shared checkers) attach to
# tasks; they carry no information beyond kind/src, so the label is derived.
GENERIC_REASONS = {"", "lacks of segment", "segment unbalanced", "redundancies of segment",
                   "segment not exists in target", "dirty segment exists"}

KINDS = ["move", "grow", "reduce", "update", "watch", "unsub", "load", "release"]  # row kind index
KIND_IDX = {k: i for i, k in enumerate(KINDS)}

LABEL_COLORS = {
    "balance": "#1f77b4",
    "placement": "#aec7e8",
    "recovery": "#ff7f0e",
    "release": "#7f7f7f",
    "level": "#2ca02c",
    "level_swap": "#98df8a",
    "level_unsettling": "#d62728",
    "table_move": "#9467bd",
    "table_exchange": "#c5b0d5",
    "table_swap": "#e377c2",
    "table_swap_release": "#f7b6d2",
    "watch": "#bcbd22",
    "unsub": "#dbdb8d",
    "update": "#17becf",
    "load": "#74c476",
}
LOADER_JS = r"""
(function () {
  var el = document.getElementById('simviz-data');
  var p;
  if (el.getAttribute('type') === 'application/json') {
    p = Promise.resolve(JSON.parse(el.textContent));
  } else {
    p = new Promise(function (resolve, reject) {
      try {
        if (typeof DecompressionStream === 'undefined') throw new Error('DecompressionStream unsupported');
        var bin = atob(el.textContent.replace(/\s+/g, ''));
        var bytes = new Uint8Array(bin.length);
        for (var i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
        var stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
        new Response(stream).text().then(function (txt) { resolve(JSON.parse(txt)); }, reject);
      } catch (e) { reject(e); }
    });
  }
  window.SIMVIZ_READY = p.then(function (M) { window.SIMVIZ = M; simvizMain(M); return M; }, function (e) {
    document.body.insertAdjacentHTML('afterbegin', '<div style="color:#b00;padding:8px">failed to decode embedded data: ' + e +
      ' — this page needs a browser with DecompressionStream (Chrome 80+, Firefox 113+, Safari 16.4+); re-run simviz with --compress off</div>');
    throw e;
  });
})();
"""

EXTRA_COLORS = ["#8c564b", "#c49c94", "#393b79", "#637939", "#8c6d31", "#843c39", "#7b4173"]


# ----------------------------------------------------------------------------
# loading / labelling
# ----------------------------------------------------------------------------

def load_trace(path):
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    for k in ("nodes", "marks", "samples", "keyframes", "tasks", "events"):
        if d.get(k) is None:
            d[k] = []
    d.setdefault("case", os.path.basename(path))
    d.setdefault("branch", "")
    d.setdefault("params", "")
    d.setdefault("period_ms", 500)
    d["_path"] = path
    return d


def task_label(task, first_load):
    """Reason for our balancer; otherwise derived from kind/src."""
    reason = task.get("reason") or ""
    if reason not in GENERIC_REASONS:
        return reason
    kind = task.get("kind", "")
    src = task.get("src", "") or ""
    if kind == "move":
        return "balance"
    if kind == "grow":
        if src.startswith("balance"):
            return "balance"
        seg = task.get("seg")
        fl = first_load.get(seg)
        if fl is not None and fl < task.get("t", 0):
            return "recovery"
        return "placement"
    if kind == "reduce":
        return "release"
    return kind or "task"


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(math.ceil(p * len(s))) - 1))
    return s[k]


def uniform_keep(n, limit):
    """Indices of a uniform subsample of range(n) with at most `limit` items."""
    if limit <= 0:
        return set()
    if n <= limit:
        return set(range(n))
    return {int(k * n / limit) for k in range(limit)}


class Interner:
    def __init__(self):
        self.items = []
        self.idx = {}

    def __call__(self, s):
        s = s or ""
        i = self.idx.get(s)
        if i is None:
            i = len(self.items)
            self.items.append(s)
            self.idx[s] = i
        return i


# ----------------------------------------------------------------------------
# per-trace model
# ----------------------------------------------------------------------------

def build_model(d, bin_s, max_grows, max_events, max_keyframes):
    nodes = sorted(d["nodes"], key=lambda n: n["id"])
    node_ids = [n["id"] for n in nodes]
    node_mem = {n["id"]: n["mem"] for n in nodes}
    samples = d["samples"]
    tmax = 0.0
    for coll in (samples, d["keyframes"], d["tasks"], d["events"], d["marks"]):
        for e in coll:
            tmax = max(tmax, float(e.get("t", 0)))

    # -- memory timeline (downsampled) ---------------------------------------
    stride = max(1, int(math.ceil(len(samples) / MAX_POINTS)))
    idx = list(range(0, len(samples), stride))
    if samples and idx[-1] != len(samples) - 1:
        idx.append(len(samples) - 1)
    ts = [round(samples[i]["t"], 2) for i in idx]
    mem = {}
    for nid in node_ids:
        key = str(nid)
        pct, loads, segs = [], [], []
        cap = node_mem.get(nid) or 1
        for i in idx:
            v = samples[i]["n"].get(key)
            if v is None or v[3] == 0:
                pct.append(None)
                loads.append(None)
                segs.append(None)
            else:
                pct.append(round(100.0 * v[0] / cap, 2))
                loads.append(v[1])
                segs.append(v[2])
        mem[key] = {"pct": pct, "loads": loads, "segs": segs}

    # -- events: stats from the FULL list, then subsample loads ----------------
    events = sorted(d["events"], key=lambda e: e.get("t", 0))
    first_load = {}
    released = set()
    pingpong = 0
    for e in events:
        seg = e.get("seg")
        if seg is None:
            continue
        kind = e.get("kind")
        if kind == "load":
            if seg not in first_load:
                first_load[seg] = e["t"]
            if (seg, e.get("node")) in released:
                pingpong += 1
        elif kind == "release":
            released.add((seg, e.get("node")))
    event_counts = Counter(e.get("kind", "?") for e in events)
    n_loads = event_counts.get("load", 0)
    keep_loads = uniform_keep(n_loads, max_events)
    ev_rows = []
    li = 0
    for e in events:
        kind = e.get("kind", "")
        if kind == "load":
            keep = li in keep_loads
            li += 1
            if not keep:
                continue
        seg = e.get("seg")
        ev_rows.append([round(float(e.get("t", 0)), 2), KIND_IDX.get(kind, 6), e.get("node", -1), e.get("coll", 0),
                        seg if seg is not None else (e.get("ch") or "")])
    events_total = len(events)
    events_shown = len(ev_rows)

    # -- tasks -> labels; subsample grows ---------------------------------------
    tasks = sorted(d["tasks"], key=lambda t: t.get("t", 0))
    n_grows = sum(1 for t in tasks if t.get("kind") == "grow")
    keep_grows = uniform_keep(n_grows, max_grows)
    grow_factor = (n_grows / len(keep_grows)) if keep_grows else 1.0
    labels = Interner()
    srcs = Interner()
    reasons = Interner()
    task_rows = []
    label_counts = Counter()      # full counts (summary)
    moves = 0
    cum_t, cum_y = [0.0], [0]
    nbins = max(1, int(math.floor(tmax / bin_s)) + 1)
    hist = {}                     # label -> weighted counts per bin
    gi = 0
    for t in tasks:
        lab = task_label(t, first_load)
        label_counts[lab] += 1
        kind = t.get("kind", "")
        tt = float(t.get("t", 0))
        b = min(nbins - 1, int(tt // bin_s))
        if kind == "move":
            moves += 1
            cum_t.append(tt)
            cum_y.append(moves)
        if kind == "grow":
            keep = gi in keep_grows
            gi += 1
            if not keep:
                continue
            w = grow_factor
        else:
            w = 1.0
        hist.setdefault(lab, [0.0] * nbins)[b] += w
        seg = t.get("seg")
        task_rows.append([round(tt, 2), KIND_IDX.get(kind, 3), labels(lab), t.get("coll", 0),
                          seg if seg is not None else (t.get("ch") or ""),
                          t.get("from", -1), t.get("to", -1), srcs(t.get("src", "")), reasons(t.get("reason", ""))])
    tasks_total = len(tasks)
    grows_shown = len(keep_grows)
    for l in hist:
        hist[l] = [round(x, 1) for x in hist[l]]
    labels_order = [l for l, _ in label_counts.most_common() if l in hist]
    # cumulative moves line, downsampled
    ck = sorted(uniform_keep(len(cum_t), MAX_POINTS))
    cum_t = [round(cum_t[i], 2) for i in ck] + [round(tmax, 2)]
    cum_y = [cum_y[i] for i in ck] + [moves]

    # -- keyframes -----------------------------------------------------------------
    sample_ts = [s["t"] for s in samples]

    def up_nodes_at(t):
        if not samples:
            return list(node_ids)
        lo, hi = 0, len(sample_ts) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if sample_ts[mid] < t:
                lo = mid + 1
            else:
                hi = mid
        cand = [lo]
        if lo > 0:
            cand.append(lo - 1)
        best = min(cand, key=lambda i: abs(sample_ts[i] - t))
        n = samples[best]["n"]
        return [nid for nid in node_ids if str(nid) in n and n[str(nid)][3] == 1]

    def encode_keyframe(kf):
        colls = kf.get("colls") or {}
        live = up_nodes_at(kf["t"])
        live_keys = [str(n) for n in live]
        totals = {}
        for cid, per_node in colls.items():
            totals[cid] = (sum(v[1] for v in per_node.values()), sum(v[0] for v in per_node.values()))
        order = sorted((c for c in totals if c != "0"), key=lambda c: (-totals[c][0], int(c) if c.isdigit() else 0))
        if "0" in totals:
            order.append("0")
        segs = [[colls[c].get(k, (0, 0))[0] for k in live_keys] for c in order]
        mb = [[int(round(colls[c].get(k, (0, 0))[1] / 1048576.0)) for k in live_keys] for c in order]
        return {"t": round(float(kf["t"]), 2), "label": kf.get("label") or "", "live": live,
                "tables": order, "segs": segs, "mb": mb}

    all_kf = d["keyframes"]
    labeled = [i for i, kf in enumerate(all_kf) if kf.get("label")]
    unlabeled = [i for i, kf in enumerate(all_kf) if not kf.get("label")]
    keep_un = uniform_keep(len(unlabeled), max(0, max_keyframes - len(labeled)))
    kf_idx = sorted(set(labeled) | {unlabeled[i] for i in keep_un})
    keyframes = [encode_keyframe(all_kf[i]) for i in kf_idx]

    # -- summary (full data) -----------------------------------------------------
    summary = {}
    summary["duration_s"] = round(tmax, 1)
    summary["nodes"] = len(node_ids)
    summary["moves"] = moves
    summary["grows_by_src"] = dict(Counter((t.get("src") or "?") for t in tasks if t.get("kind") == "grow"))
    summary["labels"] = dict(label_counts)
    summary["events"] = dict(event_counts)
    summary["tasks_total"] = tasks_total
    summary["tasks_shown"] = len(task_rows)
    summary["grows_total"] = n_grows
    summary["grows_shown"] = grows_shown
    summary["grow_factor"] = round(grow_factor, 2)
    summary["events_total"] = events_total
    summary["events_shown"] = events_shown
    summary["loads_total"] = n_loads
    summary["loads_shown"] = len(keep_loads)
    summary["dropped_tasks"] = int(d.get("dropped_tasks") or 0)
    summary["keyframes_total"] = len(all_kf)
    summary["keyframes_shown"] = len(keyframes)
    summary["pingpong"] = pingpong
    if samples:
        last = samples[-1]["n"]
        pcts = []
        for nid in node_ids:
            v = last.get(str(nid))
            if v and v[3] == 1:
                pcts.append(v[0] / (node_mem.get(nid) or 1))
        if pcts:
            mean = sum(pcts) / len(pcts)
            summary["mem_mean_pct"] = round(100 * mean, 2)
            summary["mem_min_over_mean"] = round(min(pcts) / mean, 3) if mean > 0 else None
            summary["mem_max_over_mean"] = round(max(pcts) / mean, 3) if mean > 0 else None
            summary["mem_up_nodes"] = len(pcts)
    if all_kf:
        kf = all_kf[-1]
        colls = kf.get("colls") or {}
        nlive = max(1, len(up_nodes_at(kf["t"])))
        rs, rb = [], []
        for cid, per_node in colls.items():
            if cid == "0":
                continue
            tot_s = sum(v[0] for v in per_node.values())
            tot_b = sum(v[1] for v in per_node.values())
            if tot_s < 2:
                continue
            rs.append(max(v[0] for v in per_node.values()) / (tot_s / nlive))
            if tot_b > 0:
                rb.append(max(v[1] for v in per_node.values()) / (tot_b / nlive))
        summary["even_tables"] = len(rs)
        summary["even_seg_p90"] = round(percentile(rs, 0.9), 2) if rs else None
        summary["even_seg_worst"] = round(max(rs), 2) if rs else None
        summary["even_bytes_p90"] = round(percentile(rb, 0.9), 2) if rb else None
        summary["even_bytes_worst"] = round(max(rb), 2) if rb else None

    # marks: merge labels closer than 0.6% of the run so annotations don't overlap
    marks = []
    for m in sorted(d["marks"], key=lambda m: m.get("t", 0)):
        t = float(m.get("t", 0))
        if marks and t - marks[-1]["t"] < 0.006 * max(tmax, 1):
            marks[-1]["label"] += " | " + m.get("label", "")
            continue
        marks.append({"t": round(t, 2), "label": m.get("label", "")})

    return {
        "name": os.path.splitext(os.path.basename(d["_path"]))[0],
        "case": d["case"],
        "branch": d["branch"],
        "params": d["params"],
        "period_ms": d["period_ms"],
        "tmax": round(tmax, 2),
        "nodes": [{"id": n["id"], "mem": n["mem"], "cpu": n.get("cpu", 0), "added_at": round(float(n.get("added_at", 0)), 2)} for n in nodes],
        "marks": marks,
        "mem": {"t": ts, "nodes": mem},
        "hist": {"bin": bin_s, "nbins": nbins, "labels": labels_order, "counts": hist, "cum_t": cum_t, "cum_y": cum_y,
                 "grow_factor": round(grow_factor, 2)},
        "keyframes": keyframes,
        "tasks": {"labels": labels.items, "srcs": srcs.items, "reasons": reasons.items, "rows": task_rows},
        "events": ev_rows,
        "summary": summary,
    }


def fmt_bytes(b):
    b = float(b)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024 or unit == "TB":
            return ("%.0f %s" if unit == "B" else "%.2f %s") % (b, unit)
        b /= 1024.0
    return "%.2f TB" % b


# ----------------------------------------------------------------------------
# HTML
# ----------------------------------------------------------------------------

CSS = """
* { box-sizing: border-box; }
body { margin: 0; padding: 12px 16px 40px; font: 13px/1.4 -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; color: #222; background: #fafafa; }
h1 { font-size: 20px; margin: 0 0 4px; }
h2 { font-size: 15px; margin: 18px 0 6px; padding-bottom: 3px; border-bottom: 1px solid #ddd; }
.sub { color: #666; margin-bottom: 8px; }
.cols { display: grid; grid-template-columns: repeat(var(--ncols), minmax(0, 1fr)); gap: 14px; }
.col { min-width: 0; background: #fff; border: 1px solid #e4e4e4; border-radius: 6px; padding: 8px 10px; }
.col h3 { margin: 0 0 4px; font-size: 14px; }
.col .params { color: #666; font-size: 11px; word-break: break-all; margin-bottom: 6px; }
table.summary { border-collapse: collapse; width: 100%; font-size: 12px; }
table.summary td { padding: 2px 6px; border-bottom: 1px solid #f0f0f0; vertical-align: top; }
table.summary td:first-child { color: #555; white-space: nowrap; width: 38%; }
table.summary tr.warn td { color: #b00; }
.plot { width: 100%; }
.controls { display: flex; align-items: center; gap: 12px; margin: 6px 0 8px; flex-wrap: wrap; }
.controls input[type=range] { flex: 1; min-width: 300px; }
.controls .kf { font-family: Menlo, Consolas, monospace; font-size: 12px; white-space: nowrap; }
.controls button { font-size: 12px; padding: 2px 8px; }
.hint { color: #777; font-size: 11px; }
table.tasks { border-collapse: collapse; width: 100%; font-size: 11.5px; font-family: Menlo, Consolas, monospace; }
table.tasks th { text-align: left; background: #f3f3f3; padding: 3px 5px; position: sticky; top: 0; }
table.tasks td { padding: 2px 5px; border-bottom: 1px solid #f2f2f2; white-space: nowrap; }
.tasks-wrap { max-height: 520px; overflow: auto; border: 1px solid #eee; }
.badge { display: inline-block; padding: 0 5px; border-radius: 3px; color: #fff; font-size: 10.5px; }
input.filter { width: 320px; padding: 3px 6px; font-size: 12px; }
.legend-note { font-size: 11px; color: #666; }
"""

JS = r"""
function simvizMain(M) {
  var traces = M.traces;
  var KINDS = M.kinds;
  var COLORS = M.colors;
  var EXTRA = M.extraColors;
  var NODE_PALETTE = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2','#7f7f7f','#bcbd22','#17becf',
                      '#393b79','#637939','#8c6d31','#843c39','#7b4173','#3182bd','#e6550d','#31a354','#756bb1','#636363'];
  var TABLE_PALETTE = ['#4e79a7','#f28e2b','#e15759','#76b7b2','#59a14f','#edc948','#b07aa1','#ff9da7','#9c755f','#bab0ac','#86bcb6','#d37295'];
  var MB = 1048576;
  var XMAX = 0;
  traces.forEach(function (tr) { XMAX = Math.max(XMAX, tr.tmax); });
  var XRANGE = [0, XMAX * 1.02 + 0.5];

  function fmtBytes(b) {
    var u = ['B','KB','MB','GB','TB']; var i = 0; b = +b;
    while (Math.abs(b) >= 1024 && i < u.length - 1) { b /= 1024; i++; }
    return (i === 0 ? b.toFixed(0) : b.toFixed(2)) + ' ' + u[i];
  }
  function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
  function labelColor(l) {
    if (COLORS[l]) return COLORS[l];
    var h = 0; for (var i = 0; i < l.length; i++) h = (h * 31 + l.charCodeAt(i)) >>> 0;
    return EXTRA[h % EXTRA.length];
  }
  function nodeColor(i) { return NODE_PALETTE[i % NODE_PALETTE.length]; }
  function rgba(hex, a) {
    var r = parseInt(hex.slice(1, 3), 16), g = parseInt(hex.slice(3, 5), 16), b = parseInt(hex.slice(5, 7), 16);
    return 'rgba(' + r + ',' + g + ',' + b + ',' + a + ')';
  }

  // decode compact task / event rows into objects (once)
  traces.forEach(function (tr) {
    var T = tr.tasks;
    tr.taskObjs = T.rows.map(function (r) {
      return {t: r[0], kind: KINDS[r[1]], label: T.labels[r[2]], coll: r[3], seg: r[4], from: r[5], to: r[6], src: T.srcs[r[7]], reason: T.reasons[r[8]]};
    });
    tr.eventObjs = tr.events.map(function (r) {
      var k = KINDS[r[1]];
      var isIn = (k === 'load' || k === 'watch');
      return {t: r[0], kind: k, label: k, coll: r[3], seg: r[4], from: isIn ? -1 : r[2], to: isIn ? r[2] : -1, src: 'querynode', reason: ''};
    });
  });

  var timePlots = [];   // divs with a time x-axis (memory + moves), for linking + cursor
  var cursorIdx = {};   // div id -> index of cursor shape
  var syncing = false;

  function markShapes(tr) {
    var shapes = [], ann = [];
    tr.marks.forEach(function (m) {
      shapes.push({type: 'line', xref: 'x', yref: 'paper', x0: m.t, x1: m.t, y0: 0, y1: 1,
                   line: {color: '#555', width: 1, dash: 'dash'}});
      var label = m.label.length > 40 ? m.label.slice(0, 38) + '…' : m.label;
      ann.push({x: m.t, y: 1, xref: 'x', yref: 'paper', text: esc(label), hovertext: esc(m.label), showarrow: false,
                xanchor: 'left', yanchor: 'bottom', font: {size: 10, color: '#444'}, bgcolor: 'rgba(255,255,255,0.7)'});
    });
    return {shapes: shapes, annotations: ann};
  }
  function cursorShape(t) {
    return {type: 'line', xref: 'x', yref: 'paper', x0: t, x1: t, y0: 0, y1: 1, line: {color: '#e41a1c', width: 1.5}};
  }

  // ---------------- memory timeline ----------------
  function plotMemory(tr, divId) {
    var data = [];
    var ids = tr.nodes.map(function (n) { return n.id; });
    var cap = {}; tr.nodes.forEach(function (n) { cap[n.id] = n.mem; });
    ids.forEach(function (id, i) {
      var s = tr.mem.nodes[String(id)];
      if (!s) return;
      var txt = s.pct.map(function (p, k) {
        if (p === null) return 'down';
        return 'qn' + id + ' ' + p.toFixed(2) + '% · ' + fmtBytes(p / 100 * cap[id]) + ' · segs ' + s.segs[k] + ' · loading ' + s.loads[k];
      });
      data.push({type: 'scatter', mode: 'lines', name: 'qn' + id, x: tr.mem.t, y: s.pct, text: txt, hoverinfo: 'text+x',
                 line: {width: 1.2, color: nodeColor(i)}, connectgaps: false, legendgroup: 'qn' + id});
    });
    ids.forEach(function (id, i) {
      var s = tr.mem.nodes[String(id)];
      if (!s) return;
      data.push({type: 'scatter', mode: 'lines', name: 'qn' + id, x: tr.mem.t, y: s.loads, yaxis: 'y2', stackgroup: 'loads',
                 line: {width: 0.5, color: nodeColor(i)}, fillcolor: rgba(nodeColor(i), 0.5), hoverinfo: 'name+y+x',
                 showlegend: false, legendgroup: 'qn' + id, connectgaps: false});
    });
    var ms = markShapes(tr);
    var layout = {
      height: 380, margin: {l: 48, r: 10, t: 34, b: 34},
      xaxis: {range: XRANGE.slice(), title: {text: 's', standoff: 2}, domain: [0, 1]},
      yaxis: {title: {text: 'used / capacity %'}, domain: [0.30, 1], rangemode: 'tozero', zeroline: false},
      yaxis2: {title: {text: 'loading'}, domain: [0, 0.22], rangemode: 'tozero'},
      legend: {orientation: 'h', font: {size: 9}, y: -0.14, x: 0},
      shapes: ms.shapes.concat([cursorShape(0)]),
      annotations: ms.annotations,
      hovermode: 'closest', showlegend: true,
    };
    cursorIdx[divId] = ms.shapes.length;
    Plotly.newPlot(divId, data, layout, {responsive: true, displaylogo: false, modeBarButtonsToRemove: ['lasso2d', 'select2d']});
    timePlots.push(divId);
  }

  // ---------------- moves histogram ----------------
  function plotMoves(tr, divId) {
    var h = tr.hist;
    var xs = [];
    for (var b = 0; b < h.nbins; b++) xs.push(b * h.bin + h.bin / 2);
    var data = [];
    h.labels.forEach(function (l) {
      data.push({type: 'bar', name: l, x: xs, y: h.counts[l], width: h.bin * 0.9, marker: {color: labelColor(l)},
                 hovertemplate: l + ': %{y}<extra>%{x:.0f}s bin</extra>'});
    });
    data.push({type: 'scatter', mode: 'lines', name: 'cumulative moves', x: h.cum_t, y: h.cum_y, yaxis: 'y2',
               line: {color: '#111', width: 1.5, shape: 'hv'}, hovertemplate: 'moves so far: %{y}<extra></extra>'});
    var ms = markShapes(tr);
    var layout = {
      height: 300, margin: {l: 48, r: 48, t: 34, b: 34}, barmode: 'stack',
      xaxis: {range: XRANGE.slice(), title: {text: 's', standoff: 2}},
      yaxis: {title: {text: 'tasks / ' + h.bin + 's' + (h.grow_factor > 1 ? ' (grows ×' + h.grow_factor + ')' : '')}, rangemode: 'tozero'},
      yaxis2: {title: {text: 'cum. moves'}, overlaying: 'y', side: 'right', rangemode: 'tozero', showgrid: false},
      legend: {orientation: 'h', font: {size: 10}, y: -0.16, x: 0},
      shapes: ms.shapes.concat([cursorShape(0)]), annotations: ms.annotations,
      hovermode: 'x unified',
    };
    cursorIdx[divId] = ms.shapes.length;
    Plotly.newPlot(divId, data, layout, {responsive: true, displaylogo: false, modeBarButtonsToRemove: ['lasso2d', 'select2d']});
    timePlots.push(divId);
  }

  // ---------------- keyframes ----------------
  function nearestKeyframe(tr, t) {
    var best = -1, bd = Infinity;
    tr.keyframes.forEach(function (kf, i) { var d = Math.abs(kf.t - t); if (d < bd) { bd = d; best = i; } });
    return best;
  }
  var HEAT_SCALE = [[0,'#2166ac'],[0.25,'#2166ac'],[0.25,'#c6dbef'],[0.375,'#c6dbef'],[0.375,'#f7f7f7'],[0.625,'#f7f7f7'],
                    [0.625,'#fcbba1'],[0.75,'#fcbba1'],[0.75,'#b2182b'],[1,'#b2182b']];
  var heatMode = 'segs';
  var TOP_BARS = M.topTablesBar;

  function plotKeyframe(tr, ti, t) {
    var ki = nearestKeyframe(tr, t);
    var barDiv = 'bars' + ti, heatDiv = 'heat' + ti, lbl = document.getElementById('kflabel' + ti);
    if (ki < 0) { lbl.textContent = 'no keyframes'; Plotly.purge(barDiv); Plotly.purge(heatDiv); return; }
    var kf = tr.keyframes[ki];
    var nlive = Math.max(1, kf.live.length);
    var ynames = kf.live.map(function (id) { return 'qn' + id; });
    var tnames = kf.tables.map(function (c) { return c === '0' ? 'others(0)' : 't' + c; });
    var totS = kf.segs.map(function (r) { return r.reduce(function (a, b) { return a + b; }, 0); });
    var totMB = kf.mb.map(function (r) { return r.reduce(function (a, b) { return a + b; }, 0); });
    lbl.textContent = 'keyframe @' + kf.t.toFixed(1) + 's' + (kf.label ? ' [' + kf.label + ']' : '') + ' · ' + kf.live.length + ' live nodes · ' + kf.tables.length + ' tables';

    // (a) bytes per node by table: top tables by bytes (already ordered), rest grouped
    var capByNode = {}; tr.nodes.forEach(function (n) { capByNode[n.id] = n.mem; });
    var data = [];
    var restMB = null, restSegs = null, restN = 0;
    kf.tables.forEach(function (c, i) {
      if (c !== '0' && i < TOP_BARS) {
        data.push({type: 'bar', orientation: 'h', name: tnames[i], y: ynames, x: kf.mb[i].map(function (v) { return v / 1024; }),
                   marker: {color: TABLE_PALETTE[i % TABLE_PALETTE.length]},
                   text: kf.segs[i].map(function (s, k) { return tnames[i] + ': ' + s + ' segs, ' + fmtBytes(kf.mb[i][k] * MB); }), hoverinfo: 'text+y'});
      } else {
        if (!restMB) { restMB = kf.mb[i].slice(); restSegs = kf.segs[i].slice(); }
        else { for (var k = 0; k < restMB.length; k++) { restMB[k] += kf.mb[i][k]; restSegs[k] += kf.segs[i][k]; } }
        restN++;
      }
    });
    if (restMB) {
      data.push({type: 'bar', orientation: 'h', name: 'others(' + restN + ')', y: ynames, x: restMB.map(function (v) { return v / 1024; }),
                 marker: {color: '#bbb'}, text: restSegs.map(function (s, k) { return 'others: ' + s + ' segs, ' + fmtBytes(restMB[k] * MB); }), hoverinfo: 'text+y'});
    }
    var totals = kf.live.map(function (id, k) { var s = 0; kf.mb.forEach(function (r) { s += r[k]; }); return s * MB; });
    var ann = kf.live.map(function (id, k) {
      var cap = capByNode[id] || 0;
      return {x: totals[k] / 1073741824, y: 'qn' + id, xref: 'x', yref: 'y', xanchor: 'left', showarrow: false, font: {size: 9, color: '#333'},
              text: ' ' + fmtBytes(totals[k]) + (cap ? ' (' + (100 * totals[k] / cap).toFixed(1) + '%)' : '')};
    });
    var rowH = kf.live.length > 24 ? 14 : 26;
    var barLayout = {height: Math.max(220, rowH * kf.live.length + 90), margin: {l: 44, r: 90, t: 26, b: 34}, barmode: 'stack',
                     xaxis: {title: {text: 'GB loaded (top ' + Math.min(TOP_BARS, data.length) + ' tables by bytes, rest grouped)', standoff: 2}, rangemode: 'tozero'},
                     yaxis: {autorange: 'reversed', type: 'category', tickfont: {size: kf.live.length > 24 ? 9 : 11}},
                     legend: {orientation: 'h', font: {size: 10}, y: -0.25, x: 0}, annotations: ann, hovermode: 'closest'};
    Plotly.react(barDiv, data, barLayout, {responsive: true, displaylogo: false});

    // (b) heatmap: share vs fair share
    var src = heatMode === 'segs' ? kf.segs : kf.mb;
    var tot = heatMode === 'segs' ? totS : totMB;
    var z = src.map(function (row, i) { var fair = tot[i] / nlive; return row.map(function (v) { return fair > 0 ? Math.min(v / fair, 2) : 0; }); });
    var text = kf.segs.map(function (row, i) {
      var fairS = totS[i] / nlive, fairB = totMB[i] / nlive;
      return row.map(function (s, k) {
        return s + ' / fair ' + fairS.toFixed(1) + ' segs (' + (fairS > 0 ? (s / fairS).toFixed(2) : '0') + '×) · ' + fmtBytes(kf.mb[i][k] * MB) +
               ' / fair ' + fmtBytes(fairB * MB) + ' (' + (fairB > 0 ? (kf.mb[i][k] / fairB).toFixed(2) : '0') + '×)';
      });
    });
    var heat = [{type: 'heatmap', z: z, x: ynames, y: tnames, text: text, hoverinfo: 'text+x+y',
                 zmin: 0, zmax: 2, colorscale: HEAT_SCALE, xgap: 1, ygap: 1,
                 colorbar: {title: {text: heatMode === 'segs' ? 'segs / fair' : 'bytes / fair'}, thickness: 10, len: 0.8,
                            tickvals: [0, 0.5, 1, 1.5, 2], ticktext: ['0', '0.5', '1', '1.5', '≥2']}}];
    var heatLayout = {height: Math.max(240, 16 * tnames.length + 80), margin: {l: 80, r: 10, t: 30, b: 34},
                      xaxis: {type: 'category', side: 'top', tickfont: {size: kf.live.length > 24 ? 8 : 11}},
                      yaxis: {type: 'category', autorange: 'reversed', tickfont: {size: 10},
                              tickvals: tnames, ticktext: tnames.map(function (n, i) { return n + ' (' + totS[i] + ')'; })}};
    Plotly.react(heatDiv, heat, heatLayout, {responsive: true, displaylogo: false});
  }

  // ---------------- cursor / slider ----------------
  var uTimes = [];
  (function () {
    var set = {};
    traces.forEach(function (tr) { tr.keyframes.forEach(function (kf) { set[kf.t.toFixed(1)] = true; }); });
    uTimes = Object.keys(set).map(parseFloat).sort(function (a, b) { return a - b; });
    if (!uTimes.length) uTimes = [0];
  })();
  var slider = document.getElementById('kfslider');
  slider.max = uTimes.length - 1;
  slider.value = uTimes.length - 1;

  function setCursor(t) {
    document.getElementById('cursorlabel').textContent = 't = ' + t.toFixed(1) + 's';
    timePlots.forEach(function (id) {
      var i = cursorIdx[id]; var u = {}; u['shapes[' + i + '].x0'] = t; u['shapes[' + i + '].x1'] = t;
      Plotly.relayout(id, u);
    });
    traces.forEach(function (tr, ti) { plotKeyframe(tr, ti, t); });
  }
  function applySlider() { setCursor(uTimes[+slider.value]); }
  slider.addEventListener('input', applySlider);
  function nearestSliderIdx(t) {
    var best = 0, bd = Infinity;
    uTimes.forEach(function (u, i) { var d = Math.abs(u - t); if (d < bd) { bd = d; best = i; } });
    return best;
  }
  document.getElementById('kfprev').addEventListener('click', function () { slider.value = Math.max(0, +slider.value - 1); applySlider(); });
  document.getElementById('kfnext').addEventListener('click', function () { slider.value = Math.min(uTimes.length - 1, +slider.value + 1); applySlider(); });
  document.getElementById('kfend').addEventListener('click', function () { slider.value = uTimes.length - 1; applySlider(); });
  document.getElementById('heatmode').addEventListener('change', function (e) { heatMode = e.target.value; applySlider(); });
  document.addEventListener('keydown', function (e) {
    if (e.target && (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT')) return;
    if (e.key === 'ArrowLeft') { slider.value = Math.max(0, +slider.value - 1); applySlider(); }
    if (e.key === 'ArrowRight') { slider.value = Math.min(uTimes.length - 1, +slider.value + 1); applySlider(); }
  });

  // ---------------- render ----------------
  traces.forEach(function (tr, ti) { plotMemory(tr, 'mem' + ti); plotMoves(tr, 'moves' + ti); });
  timePlots.forEach(function (id) {
    var div = document.getElementById(id);
    div.on('plotly_relayout', function (ev) {
      if (syncing) return;
      var u = {};
      if (ev['xaxis.range[0]'] !== undefined) { u['xaxis.range'] = [ev['xaxis.range[0]'], ev['xaxis.range[1]']]; }
      else if (ev['xaxis.range']) { u['xaxis.range'] = ev['xaxis.range']; }
      else if (ev['xaxis.autorange']) { u['xaxis.range'] = XRANGE.slice(); }
      else return;
      syncing = true;
      timePlots.forEach(function (o) { if (o !== id) Plotly.relayout(o, u); });
      syncing = false;
    });
    div.on('plotly_click', function (ev) {
      if (!ev.points || !ev.points.length) return;
      slider.value = nearestSliderIdx(+ev.points[0].x); applySlider();
    });
  });
  applySlider();

  // ---------------- task list ----------------
  var filterInput = document.getElementById('taskfilter');
  var kindSel = document.getElementById('taskkind');
  function renderTasks() {
    var q = filterInput.value.trim().toLowerCase();
    var kind = kindSel.value;
    var terms = q ? q.split(/\s+/) : [];
    traces.forEach(function (tr, ti) {
      var pool = kind === 'events' ? tr.eventObjs : tr.taskObjs;
      var rows = pool.filter(function (r) {
        if (kind === 'move' && r.kind !== 'move') return false;
        if (kind === 'segment' && (r.kind === 'watch' || r.kind === 'unsub')) return false;
        if (!terms.length) return true;
        var hay = (r.label + ' ' + r.reason + ' ' + r.kind + ' ' + r.src + ' coll' + r.coll + ' t' + r.coll + ' ' + r.seg + ' qn' + r.from + ' qn' + r.to + ' ' + r.from + ' ' + r.to).toLowerCase();
        return terms.every(function (t) { return hay.indexOf(t) >= 0; });
      });
      var limit = M.moveListLimit;
      var html = '<table class="tasks"><thead><tr><th>t (s)</th><th>kind</th><th>label</th><th>coll</th><th>seg / channel</th><th>from → to</th><th>src</th></tr></thead><tbody>';
      rows.slice(0, limit).forEach(function (r) {
        var ft = (r.from >= 0 ? 'qn' + r.from : '·') + ' → ' + (r.to >= 0 ? 'qn' + r.to : '·');
        html += '<tr><td>' + r.t.toFixed(2) + '</td><td>' + esc(r.kind) + '</td><td><span class="badge" style="background:' + labelColor(r.label) + '">' + esc(r.label) + '</span>' +
                (r.reason && r.reason !== r.label ? ' <span class="hint">' + esc(r.reason) + '</span>' : '') + '</td><td>t' + esc(r.coll) + '</td><td>' + esc(r.seg) + '</td><td>' + ft + '</td><td>' + esc(r.src) + '</td></tr>';
      });
      html += '</tbody></table>';
      document.getElementById('tasks' + ti).innerHTML = html;
      var s = tr.summary;
      var note = kind === 'events' ? ' (embedded ' + s.events_shown + ' of ' + s.events_total + ' events)' : ' (embedded ' + s.tasks_shown + ' of ' + s.tasks_total + ' tasks)';
      document.getElementById('taskcount' + ti).textContent = 'showing ' + Math.min(rows.length, limit) + ' of ' + rows.length + note;
    });
  }
  filterInput.addEventListener('input', renderTasks);
  kindSel.addEventListener('change', renderTasks);
  renderTasks();
}
"""


def summary_rows(s):
    def f(v, d=2):
        return "–" if v is None else (("%." + str(d) + "f") % v if isinstance(v, float) else str(v))
    grows = ", ".join("%s: %d" % (k.replace("_checker", ""), v) for k, v in sorted(s.get("grows_by_src", {}).items()))
    labels = ", ".join("%s %d" % (k, v) for k, v in sorted(s.get("labels", {}).items(), key=lambda kv: -kv[1]))
    ev = s.get("events", {})
    rows = [
        ("run", "%.1f s · %d nodes" % (s.get("duration_s", 0), s.get("nodes", 0))),
        ("moves", str(s.get("moves", 0))),
        ("grows by source", grows or "–"),
        ("tasks by label", labels or "–"),
        ("QN events", "load %d · release %d · watch %d · unsub %d" % (ev.get("load", 0), ev.get("release", 0), ev.get("watch", 0), ev.get("unsub", 0))),
        ("final memory", "mean %s%% over %s up nodes · min/mean %s · max/mean %s" % (
            f(s.get("mem_mean_pct")), s.get("mem_up_nodes", "?"), f(s.get("mem_min_over_mean"), 3), f(s.get("mem_max_over_mean"), 3))),
        ("final evenness (segs)", "p90 %s · worst %s  (max node share / fair share, %s tables)" % (
            f(s.get("even_seg_p90")), f(s.get("even_seg_worst")), s.get("even_tables", 0))),
        ("final evenness (bytes)", "p90 %s · worst %s" % (f(s.get("even_bytes_p90")), f(s.get("even_bytes_worst")))),
        ("pingpong loads", "%d  (segment loaded onto a node that had released it earlier)" % s.get("pingpong", 0)),
    ]
    gs, gt = s.get("grows_shown", 0), s.get("grows_total", 0)
    if gs < gt:
        rows.append(("grows shown", "%d of %d (sampled, histogram scaled ×%s)" % (gs, gt, s.get("grow_factor"))))
    else:
        rows.append(("grows shown", "%d of %d" % (gs, gt)))
    es, et = s.get("events_shown", 0), s.get("events_total", 0)
    if es < et:
        rows.append(("events shown", "%d of %d (loads sampled %d of %d; releases all kept)" % (es, et, s.get("loads_shown", 0), s.get("loads_total", 0))))
    else:
        rows.append(("events shown", "%d of %d" % (es, et)))
    rows.append(("keyframes shown", "%d of %d" % (s.get("keyframes_shown", 0), s.get("keyframes_total", 0))))
    if s.get("dropped_tasks", 0) > 0:
        rows.append(("!dropped tasks", "%d tasks NOT recorded (recorder cap hit); task counts above are lower bounds" % s["dropped_tasks"]))
    return rows


def render_html(models, title, compress="auto"):
    n = len(models)
    case = models[0]["case"] if models else ""
    title = title or case or "simviz"
    parts = []
    parts.append("<!DOCTYPE html><html><head><meta charset='utf-8'><title>%s</title>" % html.escape(title))
    parts.append("<script src='https://cdn.jsdelivr.net/npm/plotly.js-dist-min'></script>")
    parts.append("<style>%s</style></head><body style='--ncols:%d'>" % (CSS, n))

    # 1. header
    parts.append("<h1>%s</h1>" % html.escape(title))
    cases = sorted(set(m["case"] for m in models))
    parts.append("<div class='sub'>case: %s · %d trace%s · sample period %s ms</div>" % (
        html.escape(", ".join(cases)), n, "s" if n != 1 else "", models[0]["period_ms"] if models else "?"))
    parts.append("<div class='cols'>")
    for m in models:
        parts.append("<div class='col'><h3>%s <span class='hint'>(%s)</span></h3>" % (html.escape(m["branch"] or "?"), html.escape(m["name"])))
        parts.append("<div class='params'>%s</div>" % (html.escape(m["params"]) or "<i>default params</i>"))
        parts.append("<table class='summary'>")
        for k, v in summary_rows(m["summary"]):
            cls = " class='warn'" if k.startswith("!") else ""
            parts.append("<tr%s><td>%s</td><td>%s</td></tr>" % (cls, html.escape(k.lstrip("!")), html.escape(v)))
        parts.append("</table></div>")
    parts.append("</div>")

    # time cursor controls (shared)
    parts.append("<h2>Time cursor</h2>")
    parts.append("<div class='controls'><button id='kfprev'>◀</button><input type='range' id='kfslider' min='0' max='0' value='0'>"
                 "<button id='kfnext'>▶</button><button id='kfend'>end</button><span class='kf' id='cursorlabel'></span>"
                 "<label class='hint'>heatmap: <select id='heatmode'><option value='segs'>segments / fair share</option><option value='bytes'>bytes / fair share</option></select></label></div>")
    parts.append("<div class='hint'>Slider walks the union of keyframe times (marks, every 10 s, end); each column shows its nearest keyframe. "
                 "Click any point in the timelines to jump there; ← → keys step. Drag to zoom a timeline, zoom is shared across all time plots.</div>")

    # 2. memory
    parts.append("<h2>Node memory (used / capacity) and loads in flight</h2><div class='cols'>")
    for i, m in enumerate(models):
        parts.append("<div class='col'><h3>%s</h3><div class='plot' id='mem%d'></div></div>" % (html.escape(m["branch"] or m["name"]), i))
    parts.append("</div>")

    # 3. moves
    parts.append("<h2>Tasks per %ds bin by reason, cumulative moves</h2><div class='cols'>" % (models[0]["hist"]["bin"] if models else 10))
    for i, m in enumerate(models):
        parts.append("<div class='col'><h3>%s</h3><div class='plot' id='moves%d'></div></div>" % (html.escape(m["branch"] or m["name"]), i))
    parts.append("</div><div class='legend-note'>Labels: our balancer's reason when set (level, level_swap, table_move, …); otherwise "
                 "<b>balance</b> = balance checker, <b>placement</b> = first load of a segment, <b>recovery</b> = segment checker re-growing a segment that had been loaded before, "
                 "<b>release</b> = reduce. When grows are sampled, grow bars are scaled by the sampling factor (see header). Click legend entries to hide placement noise.</div>")

    # 4. keyframes
    parts.append("<h2>Table layout at the cursor</h2><div class='cols'>")
    for i, m in enumerate(models):
        parts.append("<div class='col'><h3>%s <span class='kf' id='kflabel%d'></span></h3>"
                     "<div class='plot' id='bars%d'></div><div class='plot' id='heat%d'></div></div>" % (html.escape(m["branch"] or m["name"]), i, i, i))
    parts.append("</div><div class='legend-note'>Heatmap cell = node's segments (or bytes) of that table ÷ fair share (table total ÷ live nodes). "
                 "Blue &lt; 0.5, white ≈ 1, red &gt; 1.5. Row label shows total segments; <b>others(0)</b> = small tables summed by the tracer.</div>")

    # 5. move list
    parts.append("<h2>Task list</h2>")
    parts.append("<div class='controls'><input class='filter' id='taskfilter' placeholder='filter: reason / coll / node (e.g. level qn3 t12), space = AND'>"
                 "<select id='taskkind'><option value='move'>moves only</option><option value='segment'>segment tasks</option><option value='all'>all tasks</option>"
                 "<option value='events'>QN events (load/release)</option></select>"
                 "<span class='hint'>limit %d rows; grows and loads may be sampled (see header)</span></div>" % MOVE_LIST_LIMIT)
    parts.append("<div class='cols'>")
    for i, m in enumerate(models):
        parts.append("<div class='col'><h3>%s <span class='hint' id='taskcount%d'></span></h3><div class='tasks-wrap' id='tasks%d'></div></div>" % (
            html.escape(m["branch"] or m["name"]), i, i))
    parts.append("</div>")

    payload = {"traces": models, "kinds": KINDS, "colors": LABEL_COLORS, "extraColors": EXTRA_COLORS,
               "moveListLimit": MOVE_LIST_LIMIT, "topTablesBar": TOP_TABLES_BAR}
    data_json = json.dumps(payload, separators=(",", ":"))
    raw_size = len(data_json)
    do_compress = compress == "on" or (compress == "auto" and raw_size > COMPRESS_THRESHOLD)
    if do_compress:
        b64 = base64.b64encode(gzip.compress(data_json.encode("utf-8"), compresslevel=6)).decode("ascii")
        lines = [b64[i:i + 120] for i in range(0, len(b64), 120)]
        parts.append("<script id='simviz-data' type='application/gzip+base64'>\n%s\n</script>" % "\n".join(lines))
    else:
        parts.append("<script id='simviz-data' type='application/json'>%s</script>" % data_json.replace("</", "<\\/"))
    parts.append("<script>%s</script>" % JS)
    parts.append("<script>%s</script>" % LOADER_JS)
    parts.append("</body></html>")
    return "\n".join(parts), raw_size, do_compress


def main(argv=None):
    ap = argparse.ArgumentParser(description="Render balancer simulation traces to one HTML page.")
    ap.add_argument("-o", "--output", required=True, help="output HTML file")
    ap.add_argument("--title", default=None, help="page title (default: case name)")
    ap.add_argument("--bin", type=float, default=10.0, help="histogram bin in seconds (default 10)")
    ap.add_argument("--max-grows", type=int, default=DEFAULT_MAX_GROWS,
                    help="embed at most N grow tasks per trace, uniformly sampled (default %d); other tasks are all kept" % DEFAULT_MAX_GROWS)
    ap.add_argument("--max-events", type=int, default=DEFAULT_MAX_EVENTS,
                    help="embed at most N load events per trace, uniformly sampled (default %d); releases are all kept" % DEFAULT_MAX_EVENTS)
    ap.add_argument("--max-keyframes", type=int, default=DEFAULT_MAX_KEYFRAMES,
                    help="embed at most N keyframes per trace (default %d); labeled keyframes are always kept" % DEFAULT_MAX_KEYFRAMES)
    ap.add_argument("--compress", choices=["auto", "on", "off"], default="auto",
                    help="gzip+base64 the embedded data (decoded in the browser via DecompressionStream); auto = when the raw payload exceeds %d MB" % (COMPRESS_THRESHOLD // (1024 * 1024)))
    ap.add_argument("traces", nargs="+", help="trace JSON files (one column each)")
    args = ap.parse_args(argv)

    models = []
    for p in args.traces:
        try:
            d = load_trace(p)
        except (OSError, ValueError) as e:
            print("simviz: cannot read %s: %s" % (p, e), file=sys.stderr)
            return 2
        models.append(build_model(d, args.bin, args.max_grows, args.max_events, args.max_keyframes))
        del d
    out, raw_size, compressed = render_html(models, args.title, args.compress)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(out)
    for m in models:
        s = m["summary"]
        print("%-6s %-40s moves=%-6d pingpong=%-5d mem max/mean=%s even(segs) p90=%s worst=%s | grows %d/%d events %d/%d kf %d/%d%s" % (
            m["branch"], m["case"][:40], s.get("moves", 0), s.get("pingpong", 0),
            s.get("mem_max_over_mean"), s.get("even_seg_p90"), s.get("even_seg_worst"),
            s["grows_shown"], s["grows_total"], s["events_shown"], s["events_total"], s["keyframes_shown"], s["keyframes_total"],
            (" DROPPED %d" % s["dropped_tasks"]) if s["dropped_tasks"] else ""))
    print("wrote %s (%.1f MB; embedded data %.1f MB raw, %s)" % (
        args.output, os.path.getsize(args.output) / 1e6, raw_size / 1e6, "gzip+base64" if compressed else "plain JSON"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
