"""Render a standalone English engineering report from recorded test artifacts."""

import html
import json
import statistics
from pathlib import Path

root = Path("artifacts/multisearch-v2")
summary = json.loads((root / "performance-summary.json").read_text())
manifest = json.loads((root / "build-manifest.json").read_text())
verification = json.loads((root / "verification-summary.json").read_text())
rows = summary["runs"]
escape = html.escape


def code(path, start=None, end=None):
    s = Path(path).read_text()
    if start:
        s = s[s.index(start) :]
    if end:
        s = s[: s.index(end)]
    return "<pre><code>" + escape(s.strip()) + "</code></pre>"


def quantile(values, p=0.99):
    values = sorted(values)
    return values[int((len(values) - 1) * p)] if values else 0


def group(cpu, cap, kind, mode):
    selected = [
        r
        for r in rows
        if r["cpu_limit"] == cpu
        and r["logical_inflight_cap"] == cap
        and r["kind"] == kind
        and r["mode"] == mode
        and not r["offered_logical_qps"]
    ]
    if not selected:
        return None
    group_lat, probe_lat = [], []
    threads, queued = [], []
    for r in selected:
        raw = json.loads((root / (r["name"] + ".json")).read_text())
        group_lat += raw["groups"]["latencies_ms"]
        probe_lat += raw["ordinary_search_probe"]["latencies_ms"]
        resources = json.loads((root / (r["name"] + ".resources.json")).read_text())
        for s in resources["samples"]:
            if 5 <= s["elapsed_s"] < 5 + r["duration_seconds"]:
                if "go_threads" in s:
                    threads.append(s["go_threads"])
                queued += [
                    v
                    for k, v in s.items()
                    if k.startswith("milvus_proxy_queue_task_num{") and '"unissued"' in k
                ]
    return dict(
        qps=statistics.mean(r["logical_qps"] for r in selected),
        cpu=statistics.mean(r["cpu_utilization"] for r in selected),
        p99=quantile(group_lat),
        probe=quantile(probe_lat),
        probe_samples=len(probe_lat),
        errors=sum(r["errors"] for r in selected),
        drops=sum(r["dropped_groups"] for r in selected),
        goroutines=max(r["peak_goroutines"] for r in selected),
        memory=max(r["peak_memory_mib"] for r in selected),
        throttle=statistics.mean(r["throttled_period_fraction"] or 0 for r in selected),
        threads=max(threads) if threads else None,
        repeats=len(selected),
    )


labels = [
    ("stock", "search", "Stock v3.0.5 / Search"),
    ("modified", "search", "Modified / Search"),
    ("modified", "multi", "Modified / MultiSearch"),
]


def comparison(cpu, cap):
    body = ""
    for kind, mode, label in labels:
        g = group(cpu, cap, kind, mode)
        if g:
            body += f"<tr><td>{label}</td><td>{g['qps']:,.1f}</td><td>{g['cpu']:.1%}</td><td>{g['p99']:.2f}</td><td>{g['probe']:.2f}</td><td>{g['errors']}</td></tr>"
    return (
        "<table><thead><tr><th>Path</th><th>Logical searches/s</th><th>CPU / limit</th><th>Group p99, ms</th><th>Search probe p99, ms</th><th>Errors</th></tr></thead><tbody>"
        + body
        + "</tbody></table>"
    )


high = [group(2, 128, k, m) for k, m, _ in labels]
if not all(high):
    raise SystemExit("Wait for the saturation experiment before publishing the report")
stock, ordinary, multi = high
qps_delta = (multi["qps"] / stock["qps"] - 1) * 100
lat_delta = (multi["p99"] / stock["p99"] - 1) * 100
probe_delta = (multi["probe"] / stock["probe"] - 1) * 100
ordinary_delta = (ordinary["qps"] / stock["qps"] - 1) * 100
ordinary_latency_delta = (ordinary["p99"] / stock["p99"] - 1) * 100

body = f"""<div class="eyebrow">Milvus engineering prototype · 31 October 2026</div>
<h1>MultiSearch through the existing Milvus service</h1>
<p class="lead">A native Python method and gRPC API that package independent searches across databases and collections into one call, with bounded dispatch through the existing Proxy Search path.</p>
<div class="verdict"><strong>What this experiment demonstrates</strong><p>The corrected implementation works over native gRPC on standalone Milvus. Under the two-CPU contention workload, MultiSearch achieved {qps_delta:+.1f}% logical throughput versus stock v3.0.5, with group p99 changing {lat_delta:+.1f}% and ordinary-Search probe p99 changing {probe_delta:+.1f}%. The modified ordinary Search path showed a {ordinary_latency_delta:+.1f}% group-p99 increase and {ordinary_delta:+.1f}% throughput change, which remains a review caveat. Observed CPU utilization was {stock['cpu']:.1%} for stock and {multi['cpu']:.1%} for MultiSearch.</p><p>These are measurements of this workload, not a general latency or production-readiness guarantee. The Kubernetes evidence and remaining limits appear below.</p></div>
<h2>1. The product contract</h2>
<p>A consultation may need searches in multiple medical collections, each with its own vector dimension, metric, filter, output fields and topK. MultiSearch transports a list of those independent requests. Every item includes its own database and collection. Results stay separate and retain input order; there is no score fusion or cross-collection atomic snapshot.</p>
<pre><code>client.multi_search([
    {{"id": "papers", "db_name": "literature", "collection_name": "papers",
     "data": [paper_vector], "anns_field": "embedding", "limit": 10}},
    {{"id": "guidelines", "db_name": "clinical", "collection_name": "guidelines",
     "data": [guideline_vector], "anns_field": "vector", "limit": 3}},
], max_concurrent_searches=8, timeout=5)</code></pre>
<p>This extends the actual <code>MilvusClient</code>, <code>GrpcHandler</code> and <code>MilvusService</code>. The public RPC is <code>/milvus.proto.milvus.MilvusService/MultiSearch</code>. It uses the existing authenticated client channel. The protocol and SDK changes are provided as companion patches to their actual repositories; they are unpublished dependencies.</p>
<p>Transport batching reduces client-to-Proxy RPC count, not the underlying search work. Under an illustrative independent 3% per-RPC failure model, 100 calls have a 95.2% probability of at least one failure. One batched call has one transport exposure, but loses all its results if that call fails. Real PSC failures can be correlated; this prototype does not establish a 3% production question failure rate or validate any GCP SLA claim.</p>
<h2>2. Why the wait does not consume a parent search worker</h2>
<div class="flow"><div>Python multi_search()</div><b>↓ one existing-service RPC</b><div>Proxy envelope admission + bounded dispatcher goroutines</div><b>↓ each item, independent DB and parameters</b><div>Ordinary Search entry chain: authentication · RBAC · quota · hooks</div><b>↓</b><div>Server.Search → Proxy.Search → ordinary DQL queue → QueryNode</div><b>↓</b><div>Ordered independent outcomes → one response</div></div>
<p>The concern about tying up threads is valid if the parent is put in the same finite worker pool as its children: all pool slots could wait for children that cannot run. Here, the parent stays outside the DQL pool. <code>WaitGroup.Wait()</code> and channel receives park Go goroutines instead of spinning or reserving an OS thread. The ordinary Search tasks still occupy their normal scheduler slots while executing. Waiting retains request/result state, and scheduling, serialization, allocation and leaf computation still cost CPU.</p>
<p>Current HybridSearch converts one database/collection request into a SearchRequest with SubReqs, then enqueues one search task and performs collection-local execution/reduction. It is a useful example of waiting for search completion, but its shared collection and ranking semantics do not provide independent cross-database dispatch. MultiSearch reuses the ordinary Search path separately for each item.</p>
<h3>Existing task wait</h3>"""
body += code(
    "internal/proxy/taskmodel/condition.go", "func (tc *TaskCondition) WaitToFinish()", "// Notify"
)
body += """<h3>Actual dispatcher implementation</h3><p>The code below is extracted from the prototype. The semaphore is shared by all batches on this Proxy. The worker count is bounded per batch. The parent waits outside the search scheduler.</p>"""
body += code(
    "internal/proxy/multisearch.go", "\tparallelism := e.limits.Parallelism", "\tif overflow.Load()"
)
body += """<h3>Actual integration with the ordinary Search entry</h3><p>A Go method call does not automatically invoke gRPC interceptors. The callback explicitly re-enters the shared Search chain before calling the existing server method. Request cloning and independent mutable RPC statistics prevent sibling state sharing.</p>"""
body += code(
    "internal/distributed/proxy/multisearch.go",
    "\tentry := proxyEntryInterceptor",
    "\treturn proxy.NewMultiSearchExecutor",
)
body += "<h3>Actual public protocol additions</h3>"
body += code(".cache/milvus-proto/proto/milvus.proto", "message MultiSearchRequest {")
body += "<h3>Actual Python RPC call and item preparation</h3>"
body += code(
    ".cache/pymilvus/pymilvus/client/grpc_handler.py",
    "    def multi_search(self, searches",
    "        outcomes = []",
)
body += """<h2>3. Concurrency controls and established precedent</h2>
<p><a href="https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-msearch">Elasticsearch's established _msearch API</a> batches independent searches, including searches targeting different indices. Its <code>max_concurrent_searches</code> bounds concurrent sub-searches; the documented default is max(1, data nodes × min(search thread-pool size, 10)). Its <code>max_concurrent_shard_requests</code> separately limits each sub-search's concurrent shard requests per node, defaulting to 5. These distinguish request-level and shard-level pressure. This is a mature API pattern, not evidence that Milvus automatically inherits Elasticsearch's performance or fairness properties.</p>
<table><tr><th>Prototype control</th><th>Default</th><th>Purpose</th></tr>
<tr><td>max_concurrent_searches (request)</td><td>0 = server default</td><td>Caller can lower item concurrency; cannot exceed the server cap.</td></tr>
<tr><td>proxy.multiSearch.parallelism</td><td>16</td><td>Maximum dispatcher workers per envelope.</td></tr>
<tr><td>globalParallelism</td><td>64 per Proxy</td><td>Shared active child Search calls across envelopes.</td></tr>
<tr><td>maxBatches / maxItems</td><td>8 / 128</td><td>Fail-fast envelope admission and bounded fan-out.</td></tr>
<tr><td>Request / response limits</td><td>4 / 16 MiB</td><td>Bound accepted payload and retained aggregate response.</td></tr>
<tr><td>timeoutSeconds</td><td>30</td><td>Upper bound on parent context; an earlier client deadline wins.</td></tr></table>
<p>These controls are not a reservation for ordinary Search, a per-tenant fair scheduler or a cluster-wide concurrency limit. The prototype adds no new shard-concurrency control. The capacity tests use 128 shared permits and 32 admitted batches so MultiSearch is not compared with an artificially smaller logical-inflight limit.</p>
<h2>4. Reproducible build and functional evidence</h2>"""
body += f"""<p>The stock reference is the latest stable 3.0 release found at test time: <a href="https://github.com/milvus-io/milvus/releases/tag/v3.0.5">v3.0.5</a>, source <code>{manifest['milvus_base_commit']}</code>. This is a released-build comparison, not a claim about every later master commit. The modified Go binary uses the stock image's unchanged native libraries.</p>"""
body += "<pre><code>" + escape(json.dumps(manifest, indent=2)) + "</code></pre>"
body += """<p>The standalone fixture contains 160,000 deterministic vectors across two databases, each with the same four collection names. Dimensions are 128/192/256; metrics are L2/IP/COSINE; indexes are FLAT. Fifty-item functional requests vary topK and compare IDs, fields and scores with ordinary Search.</p>"""
body += "<table><tr><th>Verification</th><th>Result</th></tr>"
for name, value in verification.items():
    body += f'<tr><td>{escape(name.replace("_"," "))}</td><td>{escape(str(value))}</td></tr>'
body += "</table>"
for name in ["functional.json", "kubernetes-functional.json", "kubernetes-functional-proxy2.json"]:
    p = root / name
    if p.exists():
        d = json.loads(p.read_text())
        body += f'<details><summary>{escape(name)}: {len(d["tests"])} checks, all passed = {d.get("all_passed",False)}</summary><ul>'
        for t in d["tests"]:
            body += f'<li>{escape(t["name"])} — {"PASS" if t["passed"] else "FAIL"}</li>'
        body += "</ul></details>"
body += """<p>Coverage includes independent cross-database RBAC, missing databases, invalid vector dimensions, duplicate IDs, item and byte limits, deadlines, cancellation recovery, permit cleanup and status preservation. Race tests exercise dispatch and mutable statistics. The full repository test suite was not run. Dynamic quota rejection was traced through the shared interceptor but not fault-injected in this v3.0.5 run.</p>
<h3>Distributed Kubernetes check</h3><p>The same image is deployed in an isolated namespace on a single-node Kubernetes v1.30 development cluster: two Proxy replicas plus separate mixcoord, QueryNode, DataNode, StreamingNode, etcd and MinIO pods. A separate fixture has 2,048 vectors across the same two-database/eight-collection layout. Both Proxy replicas are addressed directly through local port forwarding, and the full native API verification suite is run against each. The running binary hash is recorded. This is a functional distributed-path check, not a multi-node availability or performance test.</p>
<h2>5. Performance methodology</h2>
<p>All paths execute identical groups of eight logical searches. Search sends eight concurrent ordinary RPCs; MultiSearch sends one RPC with eight items. Latency measures the time to complete the whole group in both cases. Performance items use nq=1 and topK 5–8; loaded batches contain eight items. Fifty-item batches are tested functionally, not benchmarked here. A compiled Go client avoids measuring Python fan-out limitations as server performance. Requests are pre-built and use the same vectors, parameters and authorization.</p>
<p>Each four-core capacity condition runs twice with reversed variant order, after five seconds of warmup and for 30 seconds of measurement. Logical concurrency caps are 8, 64 and 128. Offered-load runs at 480 and 1,200 logical searches/s are reported separately. Two-core saturation conditions run twice for 45 seconds each. CPU means are normalized to the actual quota and cover the entire standalone process, including Proxy, QueryNode and background components; they do not isolate Proxy CPU cost. GOMAXPROCS remains 4 in both binaries during the two-core stress; this deliberately tests contention rather than a tuned two-core deployment.</p>
<p>The active standalone process is pinned to CPUs 64–67 (64–65 for saturation); the inactive instance remains alive on a separate cpuset. The client uses CPUs 32–39. Each container has a 16 GiB memory limit and bounded 4 GiB tmpfs data storage. The host is shared; CPU affinity does not reserve sibling hyperthreads or NUMA memory placement. This is an in-memory CPU/read benchmark, not a durable-storage, PSC or WAN experiment. Initial pause/resume runs were discarded after embedded-etcd leases expired.</p>
<p>An ordinary Search probe shares the server with each load. It attempts a request every 50 ms but waits for each response, so slow responses reduce its sampling rate. Its p99 is diagnostic, not a constant-arrival SLA measurement. Tables pool raw latency observations across repeats; throughput and CPU are means. No confidence intervals or statistical-significance claims are made.</p>
<h2>6. Results at high CPU utilization</h2>"""
chart = root / "performance-chart.svg"
if chart.exists():
    svg = chart.read_text()
    body += '<div class="chart">' + svg[svg.index("<svg") :] + "</div>"
body += comparison(2, 128)
normal_multi = group(4, 128, "modified", "multi")
normal_stock = group(4, 128, "stock", "search")
body += f"<p><strong>Absolute degradation under contention:</strong> reducing the CPU budget from four cores to two changes MultiSearch group p99 from {normal_multi['p99']:.2f} to {multi['p99']:.2f} ms ({(multi['p99']/normal_multi['p99']-1)*100:+.1f}%), while throughput changes from {normal_multi['qps']:,.1f} to {multi['qps']:,.1f} logical searches/s. Stock Search group p99 changes from {normal_stock['p99']:.2f} to {stock['p99']:.2f} ms. The claim is about relative overhead at the same CPU budget, not immunity to CPU saturation.</p>"
body += f"""<p>At two cores, modified ordinary Search throughput changed {ordinary_delta:+.1f}% relative to stock, while its group p99 changed {ordinary_latency_delta:+.1f}%. That small observed tail-latency regression needs more repeats and a matched rebuild control before attribution to the API integration; it is not dismissed as noise. MultiSearch changed throughput {qps_delta:+.1f}%, group p99 {lat_delta:+.1f}%, and the co-running ordinary-Search probe p99 {probe_delta:+.1f}%. Interpret both throughput and latency: more completed work is not sufficient evidence if it worsens unrelated requests.</p>"""
body += "<table><tr><th>Path</th><th>Throttled periods</th><th>Peak Go threads*</th><th>Peak goroutines</th><th>Peak cgroup memory, MiB</th><th>Probe samples</th></tr>"
for (_, _, label), g in zip(labels, high):
    body += f"<tr><td>{label}</td><td>{g['throttle']:.1%}</td><td>{g['threads'] or 'not sampled'}</td><td>{g['goroutines']:.0f}</td><td>{g['memory']:.0f}</td><td>{g['probe_samples']}</td></tr>"
body += '</table><p class="small">*Go runtime threads are not the total native thread count. Cgroup memory includes cache/tmpfs accounting; it is not Go heap size. Bounded peak goroutines in short runs do not prove absence of a long-duration leak.</p>'
body += "<h3>Four-core capacity: 128 logical requests in flight</h3>" + comparison(4, 128)
body += "<h3>Four-core capacity: 64 logical requests in flight</h3>" + comparison(4, 64)
body += "<h3>Four-core capacity: 8 logical requests in flight</h3>" + comparison(4, 8)
body += "<h3>Matched offered load (four cores)</h3><table><tr><th>Run</th><th>Offered logical/s</th><th>Completed logical/s</th><th>CPU</th><th>Group p99, ms</th><th>Probe p99, ms</th><th>Errors / dropped groups</th></tr>"
for r in rows:
    if r["offered_logical_qps"]:
        body += f"<tr><td>{r['name']}</td><td>{r['offered_logical_qps']:.0f}</td><td>{r['logical_qps']:.1f}</td><td>{r['cpu_utilization']:.1%}</td><td>{r['group_p99_ms']:.2f}</td><td>{r['probe_p99_ms']:.2f}</td><td>{r['errors']} / {r['dropped_groups']}</td></tr>"
body += f"""</table><p>Across {len(rows)} recorded performance runs: {summary['total_logical_searches']:,} logical group searches, {summary['total_errors']} group/probe errors, {summary['dropped_groups']} dropped groups, {summary['measurement_seconds']:.0f} measurement seconds, excluding warmup. Raw samples and per-run resource telemetry accompany the report.</p>
<h2>7. Failure semantics and production follow-ups</h2>
<ul><li>Successful siblings survive individual item failures. Ordinary SearchResults and Status are preserved; transport errors retain code/message only. Full gRPC details and trailers are not retained.</li>
<li>The SDK does not automatically retry the entire batch. A lost envelope response can repeat successful work if an application retries it; selective retry policy needs an explicit design.</li>
<li>Envelope deadline/cancellation and aggregate response overflow fail the unary response. There is no partial-result streaming or resume token.</li>
<li>Permits bound active Search calls, not all backend work remaining after cancellation. Transient allocations can exceed the retained-response budget.</li>
<li>Large batches concentrate work on one Proxy. Tenant fairness, distributed balancing, larger batches, HNSW/other indexes, larger results, slower heterogeneous backends and long-duration saturation require additional testing. Kubernetes testing here is functional, not a cluster performance claim.</li>
<li>Private Cloud gateway/billing hooks, envelope byte accounting, hot configuration updates and broader SDK data types need production review. REST and AsyncMilvusClient are not implemented.</li>
<li>The protocol and PyMilvus patches need normal upstream releases and dependency pins. The build recipe uses released native libraries, not a full C++/Rust rebuild. The checked-in PGO profile is empty; exact release PGO provenance was not reconstructed.</li></ul>
<h2>8. Engineering assessment</h2>
<p>The prototype demonstrates that independent multi-database searches can be added at the existing MilvusService/Proxy boundary without changing QueryNode execution or imposing common schemas and metrics. Bounded dispatch keeps the parent out of the DQL pool and preserves per-item authorization. The recorded CPU-contention measurements address the waiting/thread concern for this fixture; they do not eliminate the need for resource limits or establish fairness for arbitrary workloads.</p>
<p>Review <code>internal/proxy/multisearch.go</code>, <code>internal/distributed/proxy/multisearch.go</code>, the shared interceptor integration in <code>service.go</code>, and the two companion patches. Reproduction scripts, unit tests, functional tests and the load driver live under <code>examples/multisearch</code>. The design document is <code>docs/design-docs/multisearch.md</code>.</p>
<footer>Evidence generated from local test artifacts. Latest-stable release reference checked on 2026-10-31. Prototype scope and unverified behavior are stated explicitly.</footer>"""
css = """*{box-sizing:border-box}body{margin:0;background:#f3f5f8;color:#1c293a;font:16px/1.6 system-ui,-apple-system,sans-serif}main{max-width:1160px;margin:32px auto;background:white;padding:56px;border-radius:16px}h1{font-size:42px;line-height:1.14;letter-spacing:-1.2px;margin:12px 0 22px}h2{font-size:25px;margin-top:42px;border-top:1px solid #dce4ee;padding-top:24px}h3{font-size:19px;margin-top:28px}.eyebrow{font-size:13px;letter-spacing:1.4px;text-transform:uppercase;color:#176681}.lead{font-size:20px;color:#506176}.verdict{background:#ecf6f8;border-left:5px solid #08798b;padding:22px 26px;margin:28px 0}.verdict p:last-child{margin-bottom:0}.chart svg{width:100%;height:auto}table{border-collapse:collapse;width:100%;font-size:13px;margin:18px 0;overflow-wrap:anywhere}td,th{text-align:left;border-bottom:1px solid #dce4ee;padding:10px 9px}th{background:#eef3f7;font-weight:650}tr:nth-child(even){background:#fafbfd}pre{background:#142538;color:#e7f0f7;padding:22px;border-radius:9px;overflow:auto;font:12px/1.55 ui-monospace,monospace;white-space:pre-wrap;overflow-wrap:anywhere}code{font:0.88em ui-monospace,monospace}a{color:#08778c}.flow{background:#f5f8fb;padding:20px;text-align:center;border:1px solid #dce4ee;border-radius:12px}.flow div{border:1px solid #bad1dc;background:white;padding:12px;max-width:680px;margin:auto;border-radius:6px}.flow b{display:block;font-size:13px;color:#60768a;margin:8px}.small,footer{font-size:13px;color:#607087}footer{border-top:1px solid #dce4ee;padding-top:20px;margin-top:35px}li{margin:9px 0}details{background:#f5f8fb;padding:14px;margin:10px 0}summary{cursor:pointer;font-weight:600}@media print{body{background:white;font-size:11px}main{margin:0;padding:10px;max-width:none}h1{font-size:30px}h2{font-size:20px}h3{font-size:16px}pre{font-size:9px}table{font-size:10px}h2,h3{break-after:avoid}tr,.verdict,.flow{break-inside:avoid}details{display:none}@page{size:A4;margin:16mm}}"""
(root / "multisearch-report.html").write_text(
    '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Native Milvus MultiSearch — design and performance evidence</title><style>'
    + css
    + "</style><main>"
    + body
    + "</main></html>"
)
(root / "comparison.json").write_text(
    json.dumps(
        {
            "two_cpu": dict(zip([l for _, _, l in labels], high)),
            "multisearch_vs_stock_percent": {
                "qps": qps_delta,
                "group_p99": lat_delta,
                "probe_p99": probe_delta,
            },
            "modified_search_vs_stock_qps_percent": ordinary_delta,
            "modified_search_vs_stock_p99_percent": ordinary_latency_delta,
        },
        indent=2,
    )
)
print(root / "multisearch-report.html")
