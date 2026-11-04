"""Produce an English, syntax-highlighted report of the mixed-traffic experiment."""

import json, textwrap, subprocess
from pathlib import Path
from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import HtmlFormatter
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

root = Path("artifacts/multisearch-mixed")
data = json.loads((root / "summary.json").read_text())
manifest = json.loads((root / "manifest.json").read_text())
rows = data["runs"]
ag = data["aggregates"]
assert (
    len(rows) == 14 and len(ag) == 7 and all(a["repetitions"] == 2 for a in ag)
), "complete both repetitions first"


def get(mode, rate):
    return next(a for a in ag if a["mode"] == mode and a["offered_secondary_logical_qps"] == rate)


def fmt(x):
    return f"{x:,.1f}"


def snippet(s, language="go"):
    return (
        '<pre class="code-block"><code>'
        + highlight(
            textwrap.dedent(s).strip().expandtabs(4),
            get_lexer_by_name(language),
            HtmlFormatter(style="github-dark", nowrap=True),
        )
        + "</code></pre>"
    )


def code(path, start, end):
    s = subprocess.check_output(
        ["git", "show", "da46e613b189bb26e532eaa1bf436bf1a6cc21bb:" + path], text=True
    )
    s = s[s.index(start) :]
    s = s[: s.index(end)] if end else s
    return snippet(s)


# Aligned time-series: the same resident workload stays alive through injection.
fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True, layout="constrained")
for mode, color, label in [
    ("search", "#345e8b", "Add ordinary Search"),
    ("multi", "#087f80", "Add MultiSearch"),
]:
    arraysA = []
    arraysB = []
    for rep in (0, 1):
        d = json.loads((root / f"r{rep}-{mode}-rate480.json").read_text())
        seconds = {r["second"]: r for r in d["seconds"]}
        arraysA.append([seconds.get(t, {}).get("resident_successes", 0) for t in range(65)])
        arraysB.append([seconds.get(t, {}).get("secondary_successes", 0) for t in range(65)])
    for ax, arrays in zip(axes, (arraysA, arraysB)):
        aa = np.array(arrays)
        xx = np.arange(5, 65)
        ax.plot(xx, aa.mean(axis=0)[5:], color=color, label=label, lw=1.7)
        ax.fill_between(xx, aa.min(axis=0)[5:], aa.max(axis=0)[5:], color=color, alpha=0.12)
for ax in axes:
    ax.axvspan(20, 50, color="#eab75a", alpha=0.16, label="Injection active")
    ax.axvline(20, color="#b18a42", ls="--", lw=0.8)
    ax.axvline(50, color="#b18a42", ls="--", lw=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(alpha=0.15)
    ax.set_ylabel("Successful logical searches/s")
    ax.set_ylim(bottom=0)
axes[0].set_title("Resident individual Search — 128 workers continue throughout")
axes[1].set_title("Injected traffic — target 480 logical searches/s")
axes[0].legend(loc="lower left", fontsize=8, frameon=False)
axes[1].set_xlabel("Seconds since load generator start")
fig.suptitle("Competition and recovery at a two-CPU limit", fontsize=15)
fig.savefig(root / "mixed-timeline.svg")
fig.savefig(root / "mixed-timeline.png", dpi=150)
p = root / "mixed-timeline.svg"
p.write_text("\n".join(x.rstrip() for x in p.read_text().splitlines()) + "\n")
# Generate comparisons from measured data without assuming fairness in advance.
comparisons = []
for rate in (160, 480, 0):
    a, b = get("search", rate), get("multi", rate)
    comparisons.append(
        {
            "offered_secondary_qps": rate,
            "resident_retention_delta_pp": 100
            * (b["resident_retention"] - a["resident_retention"]),
            "resident_mixed_p99_delta_percent": 100
            * (b["resident_p99_ms"]["mixed"] / a["resident_p99_ms"]["mixed"] - 1),
            "secondary_qps_delta_percent": 100 * (b["secondary_qps"] / a["secondary_qps"] - 1),
            "secondary_group_p99_delta_percent": 100
            * (b["secondary_group_p99_ms"] / a["secondary_group_p99_ms"] - 1),
        }
    )
(root / "comparisons.json").write_text(json.dumps(comparisons, indent=2))
body = """<div class="eyebrow">Milvus native MultiSearch · mixed-traffic experiment · 1 November 2026</div><h1>Does MultiSearch compete fairly with an already saturated Search workload?</h1><p class="lead">Continuous individual Search traffic saturates the server first. A second client then adds either ordinary Search or MultiSearch work, and is removed while the original client continues running.</p>"""
body += (
    '<div class="callout"><strong>Measured comparison against added ordinary Search</strong><ul>'
)
for c in comparisons:
    label = (
        f"{c['offered_secondary_qps']} additional logical searches/s"
        if c["offered_secondary_qps"]
        else "continuously backlogged secondary client"
    )
    body += f"<li>At {label}, MultiSearch changes resident throughput retention by <b>{c['resident_retention_delta_pp']:+.2f} percentage points</b> and resident p99 by <b>{c['resident_mixed_p99_delta_percent']:+.1f}%</b> versus the matched ordinary-Search injection.</li>"
body += "</ul><p>These compare API behavior under the same workload. They do not establish a general scheduler fairness guarantee or per-tenant isolation.</p></div>"
body += """<h2>1. What is shared, and what the executor means</h2><p><code>multiSearch *proxy.MultiSearchExecutor</code> is a stateful dispatcher owned by the existing gRPC Server. It has no listener or independent service registration. The existing <code>MilvusService</code> receives the MultiSearch call on the same endpoint. Shared semaphores bound the active batches and child searches across all envelopes.</p><p>Every item re-enters the existing Search interceptor chain and calls the existing <code>Server.Search → Proxy.Search → DQL queue</code>. The coordination object is separate from leaf search execution; it does not provide a new search engine or reserved execution pool. In this prototype the gRPC Server owns this coordination state; it is not implemented as a <code>Proxy.MultiSearch()</code> method.</p>"""
body += code(
    "internal/distributed/proxy/multisearch.go",
    "\tentry := proxyEntryInterceptor",
    "\treturn proxy.NewMultiSearchExecutor",
)
body += """<p>Proxy admission appends search tasks to the shared queue and removes them from its front. Ordinary search tasks inherit <code>BaseTask.IsSubTask() == false</code>; MultiSearch does not set the scheduler's special subtask flag. Thus the separate pool for scheduler-created subtasks is not selected merely because a search belongs to an envelope. QueryNode uses the configured FIFO policy with compatible-task merging. These paths contain no priority distinction based on whether the external call was Search or MultiSearch.</p>"""
body += code(
    "internal/proxy/taskmodel/task.go",
    "func (bt *BaseTask) IsSubTask()",
    "func (bt *BaseTask) SetExecutingTime",
)
body += """<p>FIFO with task merging is not strict per-client fairness. Batch bursts, the wait-for-all barrier, admission limits, transport overhead, task merging and execution cost can all change observed shares. The experiment therefore uses a matched ordinary-Search injection control.</p>
<h2>2. Workload and controls</h2><div class="timeline"><span>5 s warmup</span><span>15 s Search only</span><span class="on">30 s Search + injected traffic</span><span>15 s Search only</span></div>
<ul><li><b>Resident client A:</b> 128 independent workers, each continuously calling ordinary Search and immediately issuing its next request when the prior one completes. A remains active through all phases; it is not a light probe.</li><li><b>Injected client B:</b> each job contains eight searches. The control issues eight concurrent Search RPCs and waits for all eight. MultiSearch sends one eight-item RPC, with concurrency eight. B uses a separate gRPC connection with the same test identity.</li><li><b>Fixed-rate conditions:</b> 160 and 480 additional logical searches/s (20 and 60 groups/s). Client group caps are 16 and 32 respectively, providing headroom while recording any rejected or late load.</li><li><b>Continuously backlogged condition:</b> B has 16 group workers, at most 128 logical searches per wave. The group barrier applies to both B variants. This is a demand-driven capacity-sharing test, not an equal offered-QPS test.</li><li><b>No-injection control:</b> A runs alone through the same three phase windows, to reveal time drift without B.</li><li><b>Ordering:</b> two valid repetitions with opposing API order. Fourteen valid formal runs follow a separate harness smoke check. The first 160/s pair was replaced after a binary-checksum command consumed CPU inside the test container during its MultiSearch phase (seconds 44–46). Both original runs remain in the excluded-data archive; both were rerun in Search-then-MultiSearch order after the main suite. No conclusion uses the contaminated pair.</li></ul>
<p>The unchanged prototype binary is based on v3.0.5. It uses a two-CPU quota/cpuset, GOMAXPROCS=4, and the existing 160,000-vector fixture: eight FLAT collections across two databases, nq=1, topK 5–8, dimensions 128/192/256 and L2/IP/COSINE. MultiSearch has 128 shared child permits and 32 admitted batches in this benchmark configuration; these exceed the API defaults of 64 and eight. There is no capacity reservation for A.</p>
<p>Each phase excludes its first three seconds from steady-state statistics. The resulting windows are 12 seconds of baseline, 27 seconds of mixed traffic, and 12 seconds of recovery. QPS counts successful completions in the window; latency includes calls that both start and complete inside it. A latency measures a single Search; B latency measures completion of all eight searches. Comparing A's p99 directly to B's p99 would not diagnose priority.</p>
<h2>3. Impact on the existing Search client</h2><table><tr><th>Injected B workload</th><th>A baseline QPS</th><th>A mixed QPS</th><th>A retained</th><th>A mixed p99, ms</th><th>A recovery QPS</th><th>CPU baseline → mixed</th></tr>"""
for r in ag:
    label = (
        "None"
        if r["mode"] == "none"
        else ("Search" if r["mode"] == "search" else "MultiSearch")
        + " / "
        + (
            str(r["offered_secondary_logical_qps"]) + "/s"
            if r["offered_secondary_logical_qps"]
            else "backlogged"
        )
    )
    body += f"<tr><td>{label}</td><td>{fmt(r['resident_qps']['baseline'])}</td><td>{fmt(r['resident_qps']['mixed'])}</td><td>{r['resident_retention']:.1%}</td><td>{r['resident_p99_ms']['mixed']:.2f}</td><td>{fmt(r['resident_qps']['recovery'])}</td><td>{r['cpu_utilization']['baseline']:.1%} → {r['cpu_utilization']['mixed']:.1%}</td></tr>"
body += "</table><p>Throughput values are means across repetitions. Retention is the mean of each run’s mixed/baseline throughput ratio; p99 pools the raw latency samples across repetitions. The two-run retention ranges and all per-run measurements are in the summary JSON. No confidence intervals or statistical-significance claim is made.</p>"
svg = (root / "mixed-timeline.svg").read_text()
body += (
    '<div class="chart">'
    + svg[svg.index("<svg") :]
    + '</div><p class="small">480/s condition. Lines average the two aligned repetitions; shading spans the two observations for each second. Orange marks the injection interval. The resident client is never stopped or restarted between phases.</p>'
)
body += """<h2>4. Does the added traffic get served?</h2><table><tr><th>B workload</th><th>B successful logical/s</th><th>B group p99, ms</th><th>A + B logical/s</th><th>B share of completions</th><th>Minimum A completions in any measured second</th></tr>"""
for r in ag:
    if r["mode"] == "none":
        continue
    label = (
        ("Search" if r["mode"] == "search" else "MultiSearch")
        + " / "
        + (
            str(r["offered_secondary_logical_qps"]) + "/s"
            if r["offered_secondary_logical_qps"]
            else "backlogged"
        )
    )
    total = r["resident_qps"]["mixed"] + r["secondary_qps"]
    body += f"<tr><td>{label}</td><td>{fmt(r['secondary_qps'])}</td><td>{r['secondary_group_p99_ms']:.2f}</td><td>{fmt(total)}</td><td>{r['secondary_qps']/total:.1%}</td><td>{r['min_resident_successes_per_second']}</td></tr>"
body += """</table><p>A 50/50 share is not the expected invariant for the backlogged condition: A independently replenishes each of 128 calls, while B replenishes groups only after their slowest search finishes. Some slots in a B group are idle while its remaining searches finish. The ordinary-Search B control has the same barrier, so differences between B variants are more informative than an assumed equal-share target. Client envelope concurrency is also not an exact measurement of active backend child work.</p>"""
body += f"<p>Across all 14 formal runs, {data['total_completed_all_phases_including_warmup']:,} logical searches completed, including warmup and transitions. Recorded RPC/item errors: <b>{data['errors_all']}</b>; client admission drops: <b>{data['drops']}</b>; generator-late groups: <b>{data['generator_late_groups']}</b>. The initial smoke check is excluded from these totals.</p>"
body += "<h2>5. Interpretation and limits</h2>"
if all(
    c["resident_retention_delta_pp"] >= 0 and c["resident_mixed_p99_delta_percent"] <= 0
    for c in comparisons
):
    body += "<p><b>No additional harm to resident Search was observed from MultiSearch in these matched comparisons.</b> Injecting either API reduced resident throughput and raised its latency under CPU saturation. At each tested demand level, MultiSearch retained more resident throughput and had a lower resident p99 than the ordinary-Search injection. Both clients continued completing work, fixed-rate injection targets were met, and resident throughput recovered after injection stopped. This is consistent with ordinary competition for shared resources; it does not prove a scheduler fairness guarantee.</p>"
body += """<p>Use the fixed-rate pairs to determine whether MultiSearch imposes an extra cost on an already saturated ordinary-Search client at matched logical demand. Use the backlogged pairs to examine throughput sharing when both clients compete continuously. Recovery after B stops distinguishes temporary contention from lingering capacity loss. The no-injection control checks whether time drift alone could explain the changes.</p><p>The measurements and code-path audit support conclusions only for this same-identity, single-standalone, FLAT workload with the stated limits. CPU is measured for the entire standalone process, including QueryNode and background components. This does not isolate Proxy CPU or establish fairness across tenants, clusters, heterogeneous costs, much larger batches, different indexes, failures or long durations. The host is shared, affinity does not reserve sibling hyperthreads or NUMA placement, and no PSC/network fault is injected. Default MultiSearch admission settings can produce different backpressure.</p><p>No scheduler priority, server code or native binary was changed for this experiment. There is no inference that a dispatcher field constitutes an independent serving service. The new artifacts are the mixed-load driver, resource monitor, raw results and this report.</p>
"""
body += "<h2>6. Reproduce and audit</h2>"
body += snippet(
    """go build -o bin/multisearch-mixed-load ./examples/multisearch/mixed_load
.venv/bin/python examples/multisearch/run_mixed.py
.venv/bin/python examples/multisearch/summarize_mixed.py
python examples/multisearch/report_mixed.py""",
    "bash",
)
body += "<p>The runner saves and restores the owned containers' CPU settings. It checks a short run for exactly 100 admitted eight-item groups and no RPC errors/drops before the formal suite. Samples include per-phase distributions, per-second completion counts, active client-call samples, CPU/memory/Go runtime and Proxy queue metrics. No production collections are used.</p>"
body += (
    "<details><summary>Exact experiment manifest</summary>"
    + snippet(json.dumps(manifest, indent=2), "json")
    + "</details>"
)
body += "<footer>Native MultiSearch prototype · mixed-traffic addendum · source and results accompany PR #54940.</footer>"
css = """*{box-sizing:border-box}body{background:#f3f6f8;color:#1b2c40;font:16px/1.6 system-ui,sans-serif;margin:0}main{max-width:1140px;background:white;padding:50px;margin:28px auto;border-radius:14px}h1{font-size:38px;line-height:1.18;letter-spacing:-.8px}h2{margin-top:35px;padding-top:20px;border-top:1px solid #dce5ed;font-size:25px}.eyebrow{color:#087b84;text-transform:uppercase;font-size:12px;letter-spacing:1.2px}.lead{font-size:20px;color:#506176}.callout{background:#edf6f8;padding:20px 26px;border-left:5px solid #087f80}.timeline{display:flex;gap:4px;margin:22px 0}.timeline span{background:#edf2f7;padding:14px;flex:1;font-size:13px}.timeline .on{background:#fff0ce;flex:2}table{border-collapse:collapse;width:100%;font-size:12px;margin:20px 0}td,th{text-align:left;padding:10px 8px;border-bottom:1px solid #dce5ed}th{background:#edf3f8}tr:nth-child(even){background:#fafcfd}pre{background:#0d1117;color:#e6edf3;padding:20px;border-radius:8px;font:12px/1.5 ui-monospace,monospace;white-space:pre-wrap;overflow-wrap:anywhere}pre code{font-size:1em}code{font-family:ui-monospace,monospace;font-size:.88em}.chart svg{width:100%;height:auto}.small,footer{color:#60758b;font-size:12px}li{margin:8px 0}footer{margin-top:28px;border-top:1px solid #dce5ed;padding-top:16px}.code-block,.code-block *{-webkit-print-color-adjust:exact;print-color-adjust:exact}@media print{body{background:white;font-size:11px}main{margin:0;padding:0}h1{font-size:28px}h2{font-size:19px}pre{font-size:9px}table{font-size:9px}tr,.chart,.timeline{break-inside:avoid}h2{break-after:avoid}details{display:none}@page{size:A4;margin:16mm}}""" + HtmlFormatter(
    style="github-dark"
).get_style_defs(
    ".code-block"
)
body = (
    '<p class="callout"><strong>Historical build:</strong> this report records the Server-owned coordinator at commit da46e613b1. See the <a href="../multisearch-proxy-refactor/report.html">Proxy ownership refactor report</a> for the current implementation and new-binary validation.</p>'
    + body
)
(root / "mixed-report.html").write_text(
    '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MultiSearch and resident Search — CPU contention and resource sharing</title><style>'
    + css
    + "</style><main>"
    + body
    + "</main></html>"
)
print(root / "mixed-report.html")
