#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Storyline pages: one case, master and two-level side by side, as a day
unfolds. Three lanes under an event band, in the order the balancer is
judged: node fill (min/mean/max band), tables off their share, moves per
bin with ping-pong in red.

    python3 simstory.py -o story/ --master traces/m77np_day_13_typical_saas_.json \
        --ours traces/v79_day_13_typical_saas_.json

Writes <out>/<case>.html. With several --pair the index lists every case
with a three-bar verdict.
"""
import argparse
import collections
import html
import json
import os
import re

SAMPLE_EVERY = 4  # 500ms samples -> 2s points
BIN_SECONDS = 30
OFF_SHARE = 1.5  # a table's largest node share over its fair share

OPS = [
    (r"kill_qn@", "节点宕机"), (r"restart_qn@", "节点重启"), (r"add_qn@", "扩容"),
    (r"stop_qn@", "缩容(排空)"), (r"rolling_replace@", "滚动升级"), (r"restart_qc@", "QC 重启"),
    (r"ingest_rate@", "写入速率变化"), (r"load_tables@", "新建表并 load"), (r"release_tables@", "释放表"),
    (r"reload_tables@", "重新 load"), (r"kill_sn@", "流节点宕机"), (r"add_sn@", "加流节点"),
    (r"slow_qn@", "慢节点"),
]


def load(path):
    with open(path) as f:
        return json.load(f)


class Clock:
    """Maps a trace's real seconds to the case's own seconds (the event
    table's clock, ten times slower than production) through the '@' marks,
    so master and two-level line up on the same events whatever their
    replay speed. Before the replay mark (initial load) time is negative."""

    def __init__(self, tr):
        pts = []
        for m in tr["marks"]:
            if m["label"] == "replay":
                pts.append((m["t"], 0.0))
            elif "@" in m["label"]:
                sim = float(m["label"].split("@")[1].rstrip("s"))
                pts.append((m["t"], sim))
        pts.sort()
        if not pts:
            pts = [(0.0, 0.0), (1.0, 1.0)]
        if len(pts) == 1:
            pts.append((pts[0][0] + 1.0, pts[0][1] + 1.0))
        self.pts = pts
        self.scale = (pts[-1][1] - pts[0][1]) / max(1e-9, pts[-1][0] - pts[0][0])

    def __call__(self, t):
        pts = self.pts
        if t <= pts[0][0]:
            return pts[0][1] - (pts[0][0] - t) * self.scale
        for (a, sa), (b, sb) in zip(pts, pts[1:]):
            if t <= b:
                return sa + (t - a) * (sb - sa) / max(1e-9, b - a)
        a, sa = pts[-1]
        return sa + (t - a) * self.scale


def rg_at(t, keyframes):
    rg = {}
    for kf in keyframes:
        if kf["t"] > t:
            break
        if kf.get("rg"):
            rg = {int(i): g for i, g in kf["rg"].items()}
    return rg


def fill_series(tr, clk):
    cap = {n["id"]: n["mem"] for n in tr["nodes"] if n["mem"] > 0}
    out = []
    for s in tr["samples"][::SAMPLE_EVERY]:
        rg = rg_at(s["t"], tr["keyframes"])
        groups = collections.defaultdict(list)
        loading = 0
        for i, v in s["n"].items():
            i = int(i)
            if v[3] and i in cap:
                groups[rg.get(i, "")].append(v[0] / cap[i])
                loading += v[1]
        if not groups:
            continue
        fills = [f for fs in groups.values() for f in fs]
        ratio = 0.0
        for fs in groups.values():
            mean = sum(fs) / len(fs)
            if mean > 0.005:
                ratio = max(ratio, max(fs) / mean)
        out.append([round(clk(s["t"]), 1), round(min(fills), 4), round(sum(fills) / len(fills), 4),
                    round(max(fills), 4), round(ratio, 3), len(fills), loading])
    return out


def share_series(tr, clk):
    """Per keyframe: fraction of tables (with at least two segments per
    node) whose largest node share exceeds OFF_SHARE of their fair share,
    and the p90 of that ratio."""
    samples = tr["samples"]
    out = []
    si = 0
    for kf in tr["keyframes"]:
        while si + 1 < len(samples) and samples[si + 1]["t"] <= kf["t"]:
            si += 1
        live = {int(i) for i, v in samples[si]["n"].items() if v[3]} if samples else set()
        rg = {int(i): g for i, g in (kf.get("rg") or {}).items()}
        ratios = []
        for coll, nodes in kf["colls"].items():
            if coll == "0":
                continue
            segs = sum(v[0] for v in nodes.values())
            n = max(1, len(live))
            if rg:
                mine = {rg.get(int(i)) for i in nodes}
                n = max(1, sum(1 for i in live if rg.get(i) in mine))
            if segs >= 2 * n:
                ratios.append(max(v[0] for v in nodes.values()) / (segs / n))
        if not ratios:
            out.append([round(clk(kf["t"]), 1), 0, 0, 0])
            continue
        ratios.sort()
        off = sum(1 for r in ratios if r > OFF_SHARE)
        out.append([round(clk(kf["t"]), 1), round(off / len(ratios), 4), round(ratios[min(len(ratios) - 1, int(0.9 * len(ratios)))], 3), len(ratios)])
    return out


def churn_series(tr, clk, end):
    nbins = int(end // BIN_SECONDS) + 1
    moves = [0] * nbins
    pp = [0] * nbins
    for x in tr["tasks"]:
        if x["kind"] == "move":
            b = int(clk(x["t"]) // BIN_SECONDS)
            if 0 <= b < nbins:
                moves[b] += 1
    moves_t = sorted((x["t"], x["seg"], x["from"]) for x in tr["tasks"] if x["kind"] == "move" and x.get("seg"))
    off = collections.defaultdict(set)
    mi = 0
    for e in sorted(tr["events"], key=lambda e: e["t"]):
        while mi < len(moves_t) and moves_t[mi][0] <= e["t"]:
            off[moves_t[mi][1]].add(moves_t[mi][2])
            mi += 1
        if e["kind"] == "load" and e["node"] in off[e.get("seg")]:
            b = int(clk(e["t"]) // BIN_SECONDS)
            if 0 <= b < nbins:
                pp[b] += 1
    return moves, pp


def events(tr, clk):
    """Case events from the marks: the '@' marks carry the op; the raw
    harness marks right after them are folded in."""
    out = []
    for m in tr["marks"]:
        lab = m["label"]
        if "@" not in lab:
            continue
        name = lab.split("@")[0]
        zh = next((z for pat, z in OPS if re.match(pat, lab)), name)
        out.append({"t": round(clk(m["t"]), 1), "op": name, "label": zh, "raw": lab})
    return out


def build_case(case, master, ours, descs):
    tm, tv = load(master), load(ours)
    cm, cv = Clock(tm), Clock(tv)
    end = max(cm(tm["samples"][-1]["t"]), cv(tv["samples"][-1]["t"]))
    start = min(cm(tm["samples"][0]["t"]), cv(tv["samples"][0]["t"]))
    data = {"case": case, "desc": descs.get(case, ""), "end": round(end, 1), "start": round(start, 1), "branches": []}
    for name, tr, clk in (("master", tm, cm), ("two-level", tv, cv)):
        evs = events(tr, clk)
        mv, pp = churn_series(tr, clk, end)
        data["branches"].append({
            "name": name, "events": evs,
            "fill": fill_series(tr, clk), "share": share_series(tr, clk), "moves": mv, "pingpong": pp,
            "total_moves": sum(mv), "total_pp": sum(pp),
        })
    return data


PAGE = r"""<title>__TITLE__</title>
<style>
:root{--bg:#f6f4ef;--panel:#fffdf8;--ink:#1f2a33;--mute:#6b7680;--line:#e3ded3;--master:#8a8f98;--ours:#1f6fb2;--ours-soft:#cfe0f2;--master-soft:#dfe1e5;--bad:#c8463a;--band:#f0e9d8;--event:#b8860b;color-scheme:light}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){--bg:#15181c;--panel:#1d2126;--ink:#e6e3dc;--mute:#9aa3ad;--line:#2d333b;--master:#9aa0a8;--ours:#6fb1ec;--ours-soft:#243b52;--master-soft:#2f353c;--bad:#ff7b6e;--band:#2a2620;--event:#e0b040;color-scheme:dark}}
:root[data-theme="dark"]{--bg:#15181c;--panel:#1d2126;--ink:#e6e3dc;--mute:#9aa3ad;--line:#2d333b;--master:#9aa0a8;--ours:#6fb1ec;--ours-soft:#243b52;--master-soft:#2f353c;--bad:#ff7b6e;--band:#2a2620;--event:#e0b040;color-scheme:dark}
body{background:var(--bg);color:var(--ink);font:14px/1.5 -apple-system,"PingFang SC","Helvetica Neue",Arial,sans-serif;margin:0;padding-block:18px;padding-inline:16px}
.wrap{max-width:1180px;margin:0 auto}
h1{font:600 22px/1.3 inherit;margin:0 0 4px;text-wrap:balance}
.sub{color:var(--mute);margin:0 0 14px;max-width:80ch}
.verdict{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:0 0 18px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.card h2{font:600 13px/1.3 inherit;margin:0 0 8px;color:var(--mute);letter-spacing:.04em;text-transform:uppercase}
.card .row{display:flex;justify-content:space-between;align-items:baseline;gap:8px;font-variant-numeric:tabular-nums}
.card .row b{font-size:20px;font-weight:600}
.card .row.m b{color:var(--master)}.card .row.v b{color:var(--ours)}
.card .note{color:var(--mute);font-size:12px;margin-top:6px}
.lane{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 14px 6px;margin-bottom:12px}
.lane h2{font:600 15px/1.3 inherit;margin:0 0 2px}
.lane p{color:var(--mute);margin:0 0 6px;font-size:13px;max-width:90ch}
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--mute);margin:4px 0 2px}
.legend i{display:inline-block;width:14px;height:3px;vertical-align:middle;margin-right:5px;border-radius:2px}
svg{width:100%;height:auto;display:block;overflow:visible}
.axis text,.ev text{font-size:11px;fill:var(--mute)}
.ev line{stroke:var(--event);stroke-dasharray:3 3}
.ev text{fill:var(--event)}
.tip{position:fixed;pointer-events:none;background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:6px 8px;font-size:12px;color:var(--ink);box-shadow:0 2px 8px rgba(0,0,0,.12);display:none;white-space:pre;font-variant-numeric:tabular-nums;z-index:5}
.how{color:var(--mute);font-size:12px;margin:10px 0 0;max-width:90ch}
@media (max-width:640px){.verdict{grid-template-columns:1fr}}
</style>
<div class="wrap">
<h1>__CASE__</h1>
<p class="sub">__DESC__ 横轴换算成线上等效小时（模拟器比线上快 10 倍，day 系列再压 6 倍，事件按 case 时间表对齐）。灰色是 master（ScoreBased），蓝色是 two-level。</p>
<div class="verdict" id="verdict"></div>
<div id="lanes"></div>
<p class="how">怎么看：第一条看水位带是否收窄、事件之后多久回到均值附近；第二条看份额外的表占比是否归零；第三条看柱子高低和红色占比，红色是“搬出去又搬回同一台”的无效搬迁。把鼠标放到图上可读数。</p>
</div>
<div class="tip" id="tip"></div>
<script>
const D=__DATA__;
const W=1100,PADL=46,PADR=14;
const fmt=(x,d)=>Number(x).toFixed(d);
const pct=x=>(x*100).toFixed(0)+'%';
function xs(t){return PADL+(W-PADL-PADR)*(t-D.start)/(D.end-D.start)}
const hours=t=>(t*10/3600);
function verdict(){
  const m=D.branches[0],v=D.branches[1];
  const lastF=b=>b.fill[b.fill.length-1], lastS=b=>b.share[b.share.length-1];
  const cards=[
    {h:'1. 内存水位（结束时 max/均值）',m:fmt(lastF(m)[4],2),v:fmt(lastF(v)[4],2),note:'1.00 = 所有节点一样满；节点很空时这个比值没有意义，看下面的带'},
    {h:'2. 表的分配（结束时份额外的表）',m:pct(lastS(m)[1]),v:pct(lastS(v)[1]),note:'一张表在某台上的段数超过公平份额 1.5 倍即算份额外；p90 份额比 '+fmt(lastS(m)[2],2)+' / '+fmt(lastS(v)[2],2)},
    {h:'3. 抖动（一天的搬迁 / ping-pong）',m:m.total_moves.toLocaleString()+' / '+m.total_pp.toLocaleString(),v:v.total_moves.toLocaleString()+' / '+v.total_pp.toLocaleString(),note:'ping-pong = 均衡把段搬离某台后又装回该台（用户释放/重 load、节点重启带回的不算）'},
  ];
  document.getElementById('verdict').innerHTML=cards.map(c=>`<div class="card"><h2>${c.h}</h2><div class="row m"><span>master</span><b>${c.m}</b></div><div class="row v"><span>two-level</span><b>${c.v}</b></div><div class="note">${c.note}</div></div>`).join('');
}
function xaxis(top,H){let g='';const step=D.end-D.start>20000?7200:(D.end-D.start>4000?3600:600);
  for(let t=0;t<=D.end;t+=step){g+=`<g class="axis"><line x1="${xs(t)}" x2="${xs(t)}" y1="${top+H}" y2="${top+H+4}" stroke="var(--mute)"/><text x="${xs(t)}" y="${top+H+15}" text-anchor="middle">${hours(t).toFixed(step>=3600?0:1)}h</text></g>`}
  if(D.start<0)g+=`<rect x="${xs(D.start)}" y="${top}" width="${xs(0)-xs(D.start)}" height="${H}" fill="var(--band)" opacity=".6"/><text x="${xs(D.start)+4}" y="${top+H-4}" font-size="11" fill="var(--mute)">初始装载</text>`;
  return g}
function eventBand(b,y){
  let s='',last=-1e9,lane=0;
  b.events.forEach(e=>{const x=xs(e.t);lane=(e.t-last<D.end*0.06)?(lane+1)%3:0;last=e.t;
    s+=`<g class="ev"><line x1="${x}" x2="${x}" y1="${y}" y2="${y+300}"/><text x="${x+3}" y="${y+10+lane*12}">${e.label}</text></g>`});
  return s;
}
function lane1(){
  const H=150,top=24;
  const sv=b=>{const col=b.name==='master'?'var(--master)':'var(--ours)',soft=b.name==='master'?'var(--master-soft)':'var(--ours-soft)';
    const yy=f=>top+H-H*Math.min(1,f);
    let band=b.fill.map(p=>`${xs(p[0])},${yy(p[3])}`).join(' ')+' '+b.fill.slice().reverse().map(p=>`${xs(p[0])},${yy(p[1])}`).join(' ');
    let mean=b.fill.map((p,i)=>(i?'L':'M')+xs(p[0])+','+yy(p[2])).join('');
    let mx=b.fill.map((p,i)=>(i?'L':'M')+xs(p[0])+','+yy(p[3])).join('');
    let g=`<svg viewBox="0 0 ${W} ${H+top+18}" data-kind="fill" data-b="${b.name}">`+xaxis(top,H);
    g+=[0,.25,.5,.75,1].map(f=>`<g class="axis"><line x1="${PADL}" x2="${W-PADR}" y1="${yy(f)}" y2="${yy(f)}" stroke="var(--line)"/><text x="${PADL-6}" y="${yy(f)+4}" text-anchor="end">${f*100}%</text></g>`).join('');
    g+=`<polygon points="${band}" fill="${soft}"/><path d="${mx}" stroke="${col}" fill="none" stroke-width="1" opacity=".7"/><path d="${mean}" stroke="${col}" fill="none" stroke-width="2"/>`;
    g+=eventBand(b,top-18);
    g+=`<text x="${PADL+4}" y="${top+12}" fill="${col}" font-size="12" font-weight="600">${b.name}</text>`;
    g+=`<rect x="${PADL}" y="${top}" width="${W-PADL-PADR}" height="${H}" fill="transparent" class="hit"/><line class="cursor" y1="${top}" y2="${top+H}" stroke="var(--ink)" opacity="0" /></svg>`;
    return g};
  return `<div class="lane"><h2>1. 节点水位</h2><p>带子是最空到最满节点的内存使用率，粗线是均值。健康的集群带子很窄；故障或扩容后带子张开，看它多久收回来。</p>${D.branches.map(sv).join('')}<div class="legend"><i style="background:var(--master)"></i>master <i style="background:var(--ours)"></i>two-level <i style="background:var(--event);height:2px"></i>事件</div></div>`;
}
function lane2(){
  const H=90,top=22;
  const sv=b=>{const col=b.name==='master'?'var(--master)':'var(--ours)',soft=b.name==='master'?'var(--master-soft)':'var(--ours-soft)';
    const yy=f=>top+H-H*Math.min(1,f);
    let area=`M${xs(b.share[0][0])},${yy(0)} `+b.share.map(p=>`L${xs(p[0])},${yy(p[1])}`).join(' ')+` L${xs(b.share[b.share.length-1][0])},${yy(0)} Z`;
    let g=`<svg viewBox="0 0 ${W} ${H+top+18}" data-kind="share" data-b="${b.name}">`+xaxis(top,H);
    g+=[0,.5,1].map(f=>`<g class="axis"><line x1="${PADL}" x2="${W-PADR}" y1="${yy(f)}" y2="${yy(f)}" stroke="var(--line)"/><text x="${PADL-6}" y="${yy(f)+4}" text-anchor="end">${f*100}%</text></g>`).join('');
    g+=`<path d="${area}" fill="${soft}" stroke="${col}" stroke-width="1.5"/>`;
    g+=`<text x="${PADL+4}" y="${top+12}" fill="${col}" font-size="12" font-weight="600">${b.name}</text>`;
    g+=`<rect x="${PADL}" y="${top}" width="${W-PADL-PADR}" height="${H}" fill="transparent" class="hit"/><line class="cursor" y1="${top}" y2="${top+H}" stroke="var(--ink)" opacity="0"/></svg>`;
    return g};
  return `<div class="lane"><h2>2. 表的分配</h2><p>份额外的表占比：某台上的段数超过这张表公平份额 1.5 倍的表，占所有表（每台至少两段的）的比例。越低越好，理想是贴着 0。</p>${D.branches.map(sv).join('')}</div>`;
}
function lane3(){
  const H=90,top=22;
  const mx=Math.max(1,...D.branches.flatMap(b=>b.moves));
  const sv=b=>{const col=b.name==='master'?'var(--master)':'var(--ours)';
    const yy=v=>top+H-H*v/mx; const bw=(W-PADL-PADR)/b.moves.length;
    let g=`<svg viewBox="0 0 ${W} ${H+top+18}" data-kind="moves" data-b="${b.name}">`+xaxis(top,H);
    g+=[0,.5,1].map(f=>`<g class="axis"><line x1="${PADL}" x2="${W-PADR}" y1="${yy(f*mx)}" y2="${yy(f*mx)}" stroke="var(--line)"/><text x="${PADL-6}" y="${yy(f*mx)+4}" text-anchor="end">${Math.round(f*mx)}</text></g>`).join('');
    b.moves.forEach((m,i)=>{const x=PADL+i*bw;const p=Math.min(m,b.pingpong[i]);
      if(m>0)g+=`<rect x="${x}" y="${yy(m)}" width="${Math.max(1,bw-0.5)}" height="${top+H-yy(m)}" fill="${col}" opacity=".8"/>`;
      if(p>0)g+=`<rect x="${x}" y="${yy(p)}" width="${Math.max(1,bw-0.5)}" height="${top+H-yy(p)}" fill="var(--bad)"/>`;});
    g+=`<text x="${PADL+4}" y="${top+12}" fill="${col}" font-size="12" font-weight="600">${b.name}：${b.total_moves.toLocaleString()} 次搬迁，${b.total_pp.toLocaleString()} 次 ping-pong</text>`;
    g+=`<rect x="${PADL}" y="${top}" width="${W-PADL-PADR}" height="${H}" fill="transparent" class="hit"/><line class="cursor" y1="${top}" y2="${top+H}" stroke="var(--ink)" opacity="0"/></svg>`;
    return g};
  return `<div class="lane"><h2>3. 抖动</h2><p>每 ${BIN} 个 case 秒（线上 5 分钟）的段搬迁次数，红色部分是 ping-pong。两张图同一刻度。</p>${D.branches.map(sv).join('')}<div class="legend"><i style="background:var(--bad)"></i>ping-pong</div></div>`;
}
const BIN=__BIN__;
verdict();
document.getElementById('lanes').innerHTML=lane1()+lane2()+lane3();
const tip=document.getElementById('tip');
function nearest(arr,t){let lo=0,hi=arr.length-1;while(lo<hi){const m=(lo+hi)>>1;if(arr[m][0]<t)lo=m+1;else hi=m}return arr[lo]}
document.querySelectorAll('svg').forEach(svg=>{
  svg.addEventListener('mousemove',ev=>{
    const r=svg.getBoundingClientRect();const x=(ev.clientX-r.left)/r.width*W;const t=Math.max(D.start,Math.min(D.end,D.start+(x-PADL)/(W-PADL-PADR)*(D.end-D.start)));
    document.querySelectorAll('.cursor').forEach(c=>{c.setAttribute('x1',xs(t));c.setAttribute('x2',xs(t));c.setAttribute('opacity',.5)});
    let lines=[`线上等效 ${hours(t).toFixed(2)} 小时（case 第 ${t.toFixed(0)} 秒）`];
    D.branches.forEach(b=>{const f=nearest(b.fill,t),s=nearest(b.share,t),i=Math.min(b.moves.length-1,Math.floor(t/BIN));
      lines.push(`${b.name.padEnd(9)} 水位 ${pct(f[1])}–${pct(f[3])} 均 ${pct(f[2])} max/均 ${fmt(f[4],2)} | 份额外 ${pct(s[1])} p90 ${fmt(s[2],2)} | 搬迁 ${b.moves[i]} 红 ${b.pingpong[i]}`)});
    tip.textContent=lines.join('\n');tip.style.display='block';tip.style.left=(ev.clientX+14)+'px';tip.style.top=(ev.clientY+14)+'px';
  });
  svg.addEventListener('mouseleave',()=>{tip.style.display='none';document.querySelectorAll('.cursor').forEach(c=>c.setAttribute('opacity',0))});
});
</script>
"""


def render(data):
    return (PAGE.replace("__TITLE__", html.escape(data["case"]))
            .replace("__CASE__", html.escape(data["case"]))
            .replace("__DESC__", html.escape(data["desc"]))
            .replace("__BIN__", str(BIN_SECONDS))
            .replace("__DATA__", json.dumps(data, separators=(",", ":"))))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--master", action="append", default=[])
    ap.add_argument("--ours", action="append", default=[])
    ap.add_argument("--desc", default="", help="json file: case -> one-line description")
    args = ap.parse_args()
    descs = json.load(open(args.desc)) if args.desc else {}
    os.makedirs(args.out, exist_ok=True)
    for m, v in zip(args.master, args.ours):
        case = re.sub(r"^[mv][0-9A-Za-z]*_", "", os.path.basename(v)[:-5]).strip("_")
        data = build_case(case, m, v, descs)
        path = os.path.join(args.out, case + ".html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(render(data))
        print("wrote", path, os.path.getsize(path) // 1024, "KB")


if __name__ == "__main__":
    main()
