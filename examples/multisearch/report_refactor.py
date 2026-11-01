"""Current Proxy-owned integration and measured validation; historical runs stay separate."""

import html
import json
import textwrap
from pathlib import Path

from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name

root = Path("artifacts/multisearch-proxy-refactor")
summary = json.loads((root / "summary.json").read_text())
manifest = json.loads((root / "manifest.json").read_text())
assert len(summary["runs"]) == 4
assert len(summary["aggregates"]) == 2
assert all(r["repetitions"] == 2 for r in summary["aggregates"])
formatter = HtmlFormatter(style="github-dark", nowrap=True)


def snippet(source, language="go"):
    return (
        '<pre class="code-block"><code>'
        + highlight(
            textwrap.dedent(source).strip().expandtabs(4), get_lexer_by_name(language), formatter
        )
        + "</code></pre>"
    )


def code(path, start, end=None):
    s = Path(path).read_text()
    s = s[s.index(start) :]
    if end:
        s = s[: s.index(end)]
    return snippet(s)


body = """<div class="eyebrow">Milvus native MultiSearch · Proxy ownership refactor · 1 November 2026</div>
<h1>One Proxy, one existing Search execution path</h1>
<p class="lead">Server.MultiSearch now delegates to Proxy.MultiSearch. The separate MultiSearchExecutor type and its Server field have been removed. The existing DQL scheduler still executes every child search.</p>
<div class="callout"><b>What changed:</b> batch coordination and shared admission state now belong to the existing Proxy. The shared Search entry callback terminates directly at Proxy.Search. There is no child call back through Server.Search, no new serving endpoint, and no replacement search worker pool.</div>
<h2>1. Integration: thin transport, Proxy-owned coordination</h2>
<p>The public service and Python method are unchanged: MilvusClient.multi_search sends one MilvusService.MultiSearch RPC containing independent SearchRequest items. Each item chooses its own database, collection and parameters; results remain independent and ordered.</p>
"""
body += code("internal/distributed/proxy/multisearch.go", "func (s *Server) MultiSearch(")
body += """<div class="flow"><div>Existing gRPC Server → Proxy.MultiSearch</div><div>Validate envelope · acquire batch permit · bounded dispatch</div><div>Each child: shared Search entry checks → Proxy.Search</div><div>NewSearchTask → existing DqQueue → existing worker pool</div><div>PreExecute → QueryNode shard calls → PostExecute</div><div>Independent child outcomes → one ordered response</div></div>
<p>The parent and its bounded dispatch goroutines stay outside the DQL worker pool. Only existing SearchTasks enter the queue. Waiting parks Go goroutines but retains state; executing tasks still occupy logical pool slots while waiting for QueryNodes. Admission limits bound pressure without establishing priority or reserving ordinary-Search capacity.</p>
<p>The new state lives on the existing Proxy:</p>"""
body += snippet(
    """// Fields on the existing Proxy, beside its existing scheduler.
sched *scheduler.TaskScheduler
multiSearchInit sync.Once
multiSearchLimits multiSearchLimits
multiSearchBatches chan struct{}
multiSearchActive chan struct{}
multiSearchEntry atomic.Pointer[multiSearchEntry]"""
)
body += """<h2>2. Keep ordinary Search entry checks</h2>
<p>Calling Proxy.Search directly does not run gRPC interceptors. During startup, the transport installs the same Search entry chain used by ordinary RPCs. Its terminal handler calls the existing Proxy component directly. The chain preserves database normalization, authentication, per-collection RBAC, quota checks, hooks and independent request statistics.</p>"""
body += code(
    "internal/distributed/proxy/multisearch.go",
    "func newMultiSearchEntry(",
    "func (s *Server) MultiSearch(",
)
body += """<p>Installation happens once. The callback is published atomically after limits and shared channels are initialized. A second installation cannot reset permits while work is active. Before installation, MultiSearch returns a retriable ServiceNotReady status; there is no unchecked Search fallback. Active batches inherit both the request deadline and cancellation of the Proxy component context.</p>
<h2>3. Actual dispatch through the existing path</h2><p>Excerpt from Proxy.MultiSearch, after validation and batch admission. The callback below performs entry checks and invokes ordinary Proxy.Search, which constructs and enqueues its existing SearchTask.</p>"""
body += code(
    "internal/proxy/multisearch.go",
    "\t// The RPC handler and these bounded workers",
    "\tif overflow.Load()",
)
body += """<p>Caller-owned requests are cloned before entry processing. Each child acquires a shared permit until its Search call returns. Item status or transport errors are recorded independently. A deadline or aggregate response-size overflow fails the unary envelope, as before. Permit release does not prove all downstream work has already stopped after cancellation.</p>
<h2>4. Validation of the rebuilt binary</h2><table><tr><th>Check</th><th>Result</th></tr>
<tr><td>Proxy coordination tests, race enabled</td><td>12 test functions passed: independent outcomes, validation, pointer/database isolation, shared concurrency/admission, deadlines, response limits, requested concurrency, cancellation while waiting, uninitialized entry, one-time publication, Proxy-context cancellation and publication races.</td></tr>
<tr><td>Real gRPC integration, race enabled</td><td>Proxy.MultiSearch receives the RPC. Two children charge exactly 3 + 7 Search tokens; the second is rate-limited while the first succeeds. One subsequent ordinary Search charges five more tokens. The envelope is not charged as a Search. Missing authentication is rejected before execution.</td></tr>
<tr><td>Existing server/stream regressions and isolated statistics</td><td>Passed with race detector. All Go checks use dynamic,test and -gcflags='all=-N -l'.</td></tr>
<tr><td>Python SDK tests</td><td>Three passed; protocol and SDK code were unchanged by this refactor.</td></tr>"""
for filename, label in [
    ("functional.json", "Standalone Docker / 160,000 vectors"),
    ("kubernetes-functional-1.json", "Kubernetes Proxy replica 1"),
    ("kubernetes-functional-2.json", "Kubernetes Proxy replica 2"),
]:
    d = json.loads((root / filename).read_text())
    assert d["all_passed"] and len(d["tests"]) == 12
    body += f'<tr><td>{label}</td><td>All 12 native client checks passed; server reports {html.escape(d["server_version"])}.</td></tr>'
body += """</table><p>The native checks include 50-item equivalence with ordinary Search, independent databases with identical collection names, per-database RBAC, missing/invalid authentication, mixed item failures, envelope limits, deadline and recovery. Kubernetes reuses the earlier 2,048-vector distributed fixture; only the two Proxy replicas were upgraded. QueryNode and other components remain on the preceding v3.0.5 prototype image. Both new Proxy binary hashes match the standalone build.</p>
<p>The new gRPC test initially lacked the access-logger initialization normally performed by server startup. Its fixture was corrected and the final race-enabled run passed. This was a test-fixture failure; no production logging behavior was changed.</p>
<h2>5. Fresh mixed-traffic measurements on this binary</h2>
<p>128 ordinary Search workers first saturate a two-CPU standalone server. A second client then adds 480 logical searches/s, using either eight concurrent Search RPCs or one eight-item MultiSearch RPC. The resident client continues through baseline, injection and recovery. Two repetitions use opposing API order; each run has 5 s warmup, 15 s baseline, 30 s injection and 15 s recovery. Steady-state statistics exclude the first three seconds of each phase.</p>
<table><tr><th>Added API</th><th>Resident baseline QPS</th><th>Resident mixed QPS</th><th>Resident p99, ms</th><th>Injected logical/s</th><th>Resident recovery QPS</th><th>CPU baseline → mixed</th></tr>"""
for r in summary["aggregates"]:
    body += f'<tr><td>{"Search" if r["mode"] == "search" else "MultiSearch"}</td><td>{r["resident_qps"]["baseline"]:.1f}</td><td>{r["resident_qps"]["mixed"]:.1f}</td><td>{r["resident_p99_ms"]["mixed"]:.2f}</td><td>{r["secondary_qps"]:.1f}</td><td>{r["resident_qps"]["recovery"]:.1f}</td><td>{r["cpu_utilization"]["baseline"]:.1%} → {r["cpu_utilization"]["mixed"]:.1%}</td></tr>'
a = next(r for r in summary["aggregates"] if r["mode"] == "search")
b = next(r for r in summary["aggregates"] if r["mode"] == "multi")
retention = 100 * (b["resident_retention"] - a["resident_retention"])
p99 = 100 * (b["resident_p99_ms"]["mixed"] / a["resident_p99_ms"]["mixed"] - 1)
body += f"""</table><p>Compared with ordinary-Search injection, MultiSearch changed resident throughput retention by <b>{retention:+.2f} percentage points</b> and resident p99 by <b>{p99:+.1f}%</b>. Across four formal runs, <b>{summary['total_completed_all_phases_including_warmup']:,}</b> logical searches completed, including warmup and transitions. Recorded RPC/item errors: <b>{summary['errors_all']}</b>; admission drops: <b>{summary['drops']}</b>; late-generated groups: <b>{summary['generator_late_groups']}</b>.</p>"""
body += """<p>QPS is averaged across repetitions; p99 pools latency samples. CPU includes the whole standalone process. This repeats the resource-contention check on the new binary; it is not a matched before/after measurement proving the ownership refactor improves performance. The earlier stock-v3.0.5 comparison and 14-run mixed-traffic report remain historical evidence for their recorded binary.</p>
<p>The fixture uses eight FLAT collections across two databases, nq=1, topK 5–8, dimensions 128/192/256, L2/IP/COSINE and bounded tmpfs storage. GOMAXPROCS remains four. Benchmark admission limits are 128 shared children/32 batches, above defaults of 64/eight. The shared host and short runs do not establish tenant fairness, production latency, PSC resilience or long-term stability. No confidence intervals are claimed.</p>
<h2>6. Build identity and review scope</h2>"""
body += snippet(json.dumps(manifest, indent=2), "json")
body += """<p>Go was rebuilt with the release tags using unchanged native libraries from the pinned stock v3.0.5 image. Focused regression tests and native standalone/Kubernetes checks were run; the full repository Go suite and C++/Rust rebuild were not. Unpublished protocol and SDK companion patches are still required. REST, async Python, Cloud gateway/billing validation, full gRPC error details and resumable partial results remain outside this prototype.</p>
<p>The failure-path audit and raw measurements accompany this report. Previous reports are explicitly labeled historical. The PR's primary report now describes this Proxy-owned implementation.</p>
<h2>7. Reproduce the measurements</h2>"""
body += snippet(
    """# Follow examples/multisearch/README.md to prepare the refactor image and fixture.
.venv/bin/python examples/multisearch/run_refactor_bench.py
.venv/bin/python examples/multisearch/summarize_mixed.py \\
  --input-dir artifacts/multisearch-proxy-refactor --server-binary-changed
python examples/multisearch/report_refactor.py""",
    "bash",
)
body += (
    "<footer>PR #54940 · Native Milvus MultiSearch · current Proxy-owned implementation</footer>"
)
css = (
    """*{box-sizing:border-box}body{background:#f2f5f8;color:#1c3045;font:16px/1.6 system-ui,sans-serif;margin:0}main{max-width:1120px;padding:45px;margin:25px auto;background:#fff;border-radius:14px}h1{font-size:39px;line-height:1.2}h2{font-size:24px;margin-top:34px;border-top:1px solid #d8e3ec;padding-top:20px}.eyebrow{font-size:12px;letter-spacing:1px;text-transform:uppercase;color:#087e84}.lead{font-size:20px;color:#50677d}.callout{background:#edf7f7;border-left:5px solid #087e84;padding:20px}.flow{display:grid;gap:5px;margin:20px 0}.flow div{padding:9px 16px;background:#eff4f8;border-left:3px solid #31638d}table{border-collapse:collapse;width:100%;font-size:12px}td,th{padding:10px;border-bottom:1px solid #dce5ed;text-align:left}th{background:#eaf1f7}pre{background:#0d1117;color:#e6edf3;padding:18px;border-radius:7px;font:12px/1.5 ui-monospace,monospace;white-space:pre-wrap;overflow-wrap:anywhere}pre code{font-size:1em}code{font-family:ui-monospace,monospace;font-size:.9em}.code-block,.code-block *{-webkit-print-color-adjust:exact;print-color-adjust:exact}footer{margin-top:25px;color:#61768b;font-size:12px}@media print{body{background:white;font-size:11px}main{margin:0;padding:0}h1{font-size:28px}h2{font-size:19px;break-after:avoid}pre{font-size:8px;break-inside:avoid}table{font-size:9px}tr,.flow{break-inside:avoid}@page{size:A4;margin:16mm}}"""
    + formatter.get_style_defs(".code-block")
)
(root / "report.html").write_text(
    '<!doctype html><html lang="en"><meta charset="utf-8"><title>MultiSearch: existing Proxy ownership and validation</title><style>'
    + css
    + "</style><main>"
    + body
    + "</main></html>"
)
print(root / "report.html")
