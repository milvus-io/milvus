"""Bounded performance telemetry, interference, and package-cache workloads."""

import copy
import hashlib
import io
import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from urllib.parse import urlparse

import numpy as np
import requests
from common.pyudf_helpers import build_wheel
from pymilvus import FunctionChain, FunctionChainStage, MilvusClient
from pymilvus.function_chain import col
from pymilvus.function_chain.chain import FunctionChainExpr
from workloads.pyudf.benchmark import SearchActivity, SearchWireCounter, run_abba, run_batch_measurements
from workloads.pyudf.runner import save
from workloads.pyudf.workload import DOCUMENT_FIELDS, FIELDS, MODULE, RAG_QUERY_TIME, check_stop_file

# The instrumented subclass preserves the original transform and its column checks.
# Snapshots read process-local counters atomically through the same UDF instance.
COUNTER_FIELDS = ("pid", "count", "completed", "active", "peak")
COUNTED_MODULE = (
    MODULE
    + """
class CountedUDF(UDF):
    def __init__(self, context):
        super().__init__(context)
        self.active = self.peak = self.completed = 0

    def transform_query(self, params, columns):
        with self.lock:
            if params.get('counter_query'):
                values = (os.getpid(), self.count, self.completed, self.active, self.peak)
                if params.get('reset_peak'):
                    self.peak = self.active
                # Search scores are float32; reject counters that would lose precision.
                assert all(0 <= value <= 2**24 for value in values), values
                return [pa.array([values[index.as_py()] for index in columns[0]], type=pa.float32())]
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            return super().transform_query(params, columns)
        finally:
            with self.lock:
                self.completed += 1
                self.active -= 1

def create_udf(context):
    return CountedUDF(context)
"""
)

INTERFERENCE_MODULE = """import os
import time
import pyarrow as pa

class UDF:
    def __init__(self, context):
        self.context = context

    def transform_query(self, params, columns):
        started = time.time()
        self.context.logger.warning('PYUDF_R2_ENTER resource=%s pid=%s call=%s mode=%s unix=%s', self.context.resource_name, os.getpid(), params['call'], params['mode'], started)
        try:
            # Queued calls cannot extend CPU pressure beyond the observation window.
            deadline = min(params['deadline'], started + 1)
            if params['mode'] == 'wait':
                time.sleep(max(0, deadline - time.time()))
            else:
                value = 1
                while time.time() < deadline:
                    value = (value * 1664525 + 1013904223) % 4294967296
            return [pa.array([v.as_py() * 2 for v in columns[0]], type=pa.float32())]
        finally:
            self.context.logger.warning('PYUDF_R2_END resource=%s pid=%s call=%s mode=%s unix=%s', self.context.resource_name, os.getpid(), params['call'], params['mode'], time.time())

def create_udf(context):
    return UDF(context)
"""


def register(workload, root, prefix, sources):
    checker, store, config = workload.checker, workload.storage, workload.config
    registered = {item.name for item in checker.client.list_file_resources()}
    wheels = []
    for number, source in enumerate(sources):
        check_stop_file(root.parent / "STOP")
        name = f"{prefix}_{number:02d}"
        assert name not in registered, f"Refuse to overwrite {name}"
        wheel = build_wheel(name + "_pkg", source)
        path = f"pyudf_r2/{name}.whl"
        wheels.append({"resource": name, "path": path, "sha256": hashlib.sha256(wheel).hexdigest()})
        save(root / "wheels.json", wheels)
        store.put_object(config.bucket, path, io.BytesIO(wheel), len(wheel))
        checker.client.add_file_resource(name, path, timeout=30)
        (root / (name + ".whl")).write_bytes(wheel)
    return wheels


def instrumentation_comparison(checker, clients, original_resource, counted_resource, output):
    """Bound the observed end-to-end instrumentation cost with an ABBA control."""
    records = []
    for mode in ("original", "counted", "counted", "original"):
        resource = original_resource if mode == "original" else counted_resource
        started, wall = time.monotonic(), time.time()
        values = []
        sequence = 0
        while time.monotonic() - started < 30:
            check_stop_file(output.parent / "STOP")
            before = time.monotonic()
            query = sequence % 1000
            hits = checker.search(clients[sequence % 2], checker.queries[query], checker.chain(resource, 2, 0))
            checker.verify_udf(hits, checker.candidates[query], 2, 0)
            values.append(1000 * (time.monotonic() - before))
            sequence += 1
        records.append(
            {
                "mode": mode,
                "started_unix": wall,
                "elapsed": time.monotonic() - started,
                "requests": len(values),
                "latency_ms_p50_p95_p99": np.percentile(values, [50, 95, 99]).tolist(),
                "concurrency": 1,
                "limitation": "End-to-end paired observation; not an isolated CPU-cost attribution",
            }
        )
        save(output / "instrumentation-comparison.json", records)


def verify_single_worker_proxies(config):
    """Require explicit single-worker configuration on each directly addressed Proxy."""
    endpoints = config.proxy_management_endpoints
    assert endpoints and len(endpoints) == len(config.proxy_uris), (
        "Performance counters require one proxy_management_endpoints entry per Proxy"
    )
    assert len(set(endpoints)) == len(endpoints), "Management endpoints must address distinct Proxies"
    key = "function.pyUDF.server.workerCount"
    observations = []
    for proxy, (uri, endpoint) in enumerate(zip(config.proxy_uris, endpoints)):
        assert urlparse(uri).hostname == urlparse(endpoint).hostname, (
            "Search and management endpoints must directly address the same Proxy host"
        )
        response = requests.get(endpoint.rstrip("/") + "/management/config/get", params={"keys": key}, timeout=10)
        response.raise_for_status()
        entries = response.json()["configs"]
        assert len(entries) == 1 and entries[0]["key"] == key, entries
        assert entries[0].get("value") == "1", (
            "Performance counters require explicit workerCount=1 on every Proxy",
            proxy,
            entries,
        )
        observations.append({"proxy": proxy, "worker_count": 1})
    return observations


def snapshot_worker_counters(checker, clients, stop_file, reset_peak=False):
    """Read PID and counters atomically in one invocation per Proxy/resource."""
    observations = []
    for proxy, client in enumerate(clients):
        for resource in checker.resources:
            check_stop_file(stop_file)
            chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score",
                FunctionChainExpr(
                    "py_udf",
                    args=(col("id"),),
                    params={"resource_name": resource, "udf_params": {"counter_query": True, "reset_peak": reset_peak}},
                ),
            )
            # Bind scores by primary key, since Search may reorder output rows.
            hits = checker.search(client, checker.queries[0], chain, limit=len(COUNTER_FIELDS), filter_expr="id < 5")
            values = {hit["id"]: hit["distance"] for hit in hits}
            assert len(hits) == len(COUNTER_FIELDS) and set(values) == set(range(len(COUNTER_FIELDS))), hits
            assert all(value == int(value) and 0 <= value <= 2**24 for value in values.values()), values
            counts = {field: int(values[index]) for index, field in enumerate(COUNTER_FIELDS)}
            observation = {"proxy": proxy, "resource": resource, **counts}
            assert counts["active"] == 0 and counts["count"] == counts["completed"], observation
            observations.append(observation)
    return observations


def abba(workload):
    root, config = workload.root, workload.config
    checker, clients = copy.copy(workload.checker), workload.clients
    prerequisites = verify_single_worker_proxies(config)
    output = root / "performance"
    output.mkdir()
    save(output / "worker-prerequisites.json", prerequisites)
    checker.evidence = output
    prefix = config.collection + "_perf"
    wheels = register(workload, output, prefix, [COUNTED_MODULE] * 2)
    original_resource = checker.resources[0]
    checker.resources = [item["resource"] for item in wheels]
    save(
        output / "instrumentation.json",
        {
            "method": "Process-local counters per resource/worker; start/end lock overhead",
            "original_control_resources": workload.checker.resources,
            "native_heap": "not covered by Go heap",
            "component_spans": "not available; no encode/execute/decode time decomposition",
        },
    )
    # Only a newly registered resource's missing snapshot is a prerequisite wait.
    readiness = []
    for proxy, client in enumerate(clients):
        for resource in checker.resources:
            deadline = time.monotonic() + 30
            while True:
                check_stop_file(root / "STOP")
                try:
                    checker.verify_udf(
                        checker.search(client, checker.queries[0], checker.chain(resource, 2, 0)),
                        checker.candidates[0],
                        2,
                        0,
                    )
                    break
                except Exception as error:
                    message = str(error)
                    transient = (
                        "snapshot is not ready" in message or "file resource" in message and "not found" in message
                    )
                    readiness.append({"proxy": proxy, "resource": resource, "unix": time.time(), "error": message})
                    save(output / "readiness-attempts.json", readiness)
                    if time.monotonic() >= deadline or not transient:
                        raise
                    time.sleep(0.2)
    instrumentation_comparison(checker, clients, original_resource, checker.resources[0], output)
    addresses = config.proxy_uris

    worker_snapshot = partial(snapshot_worker_counters, checker, clients, root / "STOP")

    windows = run_abba(
        checker,
        addresses[0],
        os.environ["MILVUS_TOKEN"],
        proxy_uris=addresses,
        worker_snapshot=worker_snapshot,
        raise_on_failure=False,
        stop_file=root / "STOP",
    )
    failed = [
        result for result in windows if result.get("outcome") == "FAIL" or result.get("internal_outcome") == "FAIL"
    ]
    save(
        output / "terminal.json",
        {"outcome": "FAIL" if failed else "MEASURED", "failed_windows": len(failed), "unix": time.time()},
    )
    assert not failed, "Strict benchmark gate failed; independent measurements retained"
    save(output / "post-performance-audit.json", checker.audit_all_rows())


def batches(workload):
    """Run the batch matrix independently of ABBA and worker instrumentation."""
    checker = copy.copy(workload.checker)
    checker.evidence = workload.root / "batches"
    checker.evidence.mkdir()
    addresses = workload.config.proxy_uris
    run_batch_measurements(
        checker, addresses[0], os.environ["MILVUS_TOKEN"], proxy_uris=addresses, stop_file=checker.stop_file
    )
    save(checker.evidence / "post-batch-audit.json", checker.audit_all_rows())


def peer_baseline(checker, clients, output, seconds=15):
    samples = []
    started = time.monotonic()
    sequence = 0
    while time.monotonic() - started < seconds:
        for number, client in enumerate(clients):
            check_stop_file(output.parent.parent / "STOP")
            before = time.monotonic()
            hits = checker.search(client, checker.queries[sequence % 1000])
            checker.verify_control(hits, sequence % 1000)
            samples.append({"proxy": number, "latency_ms": 1000 * (time.monotonic() - before)})
        sequence += 1
        time.sleep(0.05)
    save(output, {"outcome": "PASS", "unix": time.time(), "samples": samples})


def interference(workload):
    root, config = workload.root, workload.config
    checker, clients = copy.copy(workload.checker), workload.clients
    attackers = []
    try:
        output = root / "interference"
        output.mkdir()
        checker.evidence = output
        prefix = config.collection + "_interference"
        resource = register(workload, output, prefix, [INTERFERENCE_MODULE])[0]["resource"]
        addresses = config.proxy_uris
        for number in range(32):
            attackers.append(MilvusClient(addresses[number % 2], token=os.environ["MILVUS_TOKEN"], dedicated=True))
        wire = [SearchWireCounter(client) for client in attackers]
        for mode in ("cpu", "wait"):
            peer_baseline(checker, clients, output / f"{mode}-baseline.json")
            rpc_activity = SearchActivity()
            for counter in wire:
                counter.activity = rpc_activity
            history, peer, failures = [], [], []
            lock = threading.Lock()
            started = time.monotonic()
            wall = time.time()
            active = peak = 0
            area = 0.0
            changed = started
            barrier = threading.Barrier(34)

            def actor(number):
                nonlocal active, peak, area, changed
                barrier.wait(timeout=30)
                sequence = number
                while time.monotonic() - started < 60:
                    with lock:
                        now = time.monotonic()
                        area += active * (now - changed)
                        changed = now
                        active += 1
                        peak = max(peak, active)
                    before = time.monotonic()
                    try:
                        check_stop_file(root / "STOP")
                        chain = (
                            FunctionChain(FunctionChainStage.L2_RERANK)
                            .map(
                                "$score",
                                FunctionChainExpr(
                                    "py_udf",
                                    args=(col("ts"),),
                                    params={
                                        "resource_name": resource,
                                        "udf_params": {"mode": mode, "deadline": wall + 60, "call": sequence},
                                    },
                                ),
                            )
                            .sort(col("$score"), desc=True, tie_break_col=col("$id"))
                            .limit(20)
                        )
                        hits = checker.search(attackers[number], checker.queries[sequence % 1000], chain)
                        checker.verify_udf(hits, checker.candidates[sequence % 1000], 2, 0)
                        with lock:
                            history.append(
                                {
                                    "call": sequence,
                                    "proxy": number % 2,
                                    "elapsed": time.monotonic() - started,
                                    "latency_ms": 1000 * (time.monotonic() - before),
                                }
                            )
                    except Exception as error:
                        with lock:
                            failures.append({"call": sequence, "type": type(error).__name__, "error": str(error)})
                        break
                    finally:
                        with lock:
                            now = time.monotonic()
                            area += active * (now - changed)
                            changed = now
                            active -= 1
                    sequence += 32

            def peer_actor(number):
                barrier.wait(timeout=30)
                sequence = 0
                while time.monotonic() - started < 60:
                    before = time.monotonic()
                    try:
                        check_stop_file(root / "STOP")
                        hits = checker.search(clients[number], checker.queries[sequence % 1000])
                        checker.verify_control(hits, sequence % 1000)
                        item = {"proxy": number, "latency_ms": 1000 * (time.monotonic() - before)}
                    except Exception as error:
                        item = {"proxy": number, "error": str(error)}
                    with lock:
                        peer.append(item)
                    if "error" in item:
                        break
                    sequence += 1
                    time.sleep(0.05)

            with ThreadPoolExecutor(max_workers=34) as pool:
                try:
                    futures = [pool.submit(actor, n) for n in range(32)] + [
                        pool.submit(peer_actor, n) for n in range(2)
                    ]
                    for future in futures:
                        future.result()
                except BaseException:
                    (root / "STOP").touch()
                    raise
            elapsed = time.monotonic() - started
            rpc_measurement = rpc_activity.snapshot()
            save(
                output / f"{mode}.json",
                {
                    "mode": mode,
                    "started_unix": wall,
                    "elapsed": elapsed,
                    "peak_client_inflight": peak,
                    "time_weighted_client_inflight": area / elapsed,
                    "search_rpc_activity": rpc_measurement,
                    "requests": history,
                    "peer": peer,
                    "failures": failures,
                    "outcome": "FAIL"
                    if failures or any("error" in item for item in peer)
                    else "PASS"
                    if peak == 32 and rpc_measurement["peak"] == 32
                    else "INCONCLUSIVE",
                },
            )
            for number, client in enumerate(clients):
                checker.verify_control(checker.search(client, checker.queries[0]), 0)
                checker.once(client, 1)
            peer_baseline(checker, clients, output / f"{mode}-recovery.json")
        outcomes = {mode: json.loads((output / f"{mode}.json").read_text())["outcome"] for mode in ("cpu", "wait")}
        save(output / "terminal.json", {"outcomes": outcomes, "unix": time.time()})
        assert all(value == "PASS" for value in outcomes.values()), outcomes
        save(output / "post-interference-audit.json", checker.audit_all_rows())
    finally:
        for client in attackers:
            client.close()


# Each wheel contains its own policy, so resource mix-ups change the expected ranking.
RAG_RERANK_MODULE = """import numpy as np
import pyarrow as pa

class DocumentReranker:
    def transform_query(self, params, columns):
        distances, published, sources, quality = columns
        distances = distances.to_numpy(zero_copy_only=False).astype(np.float64)
        if not np.all(np.isfinite(distances)) or np.any(distances < 0):
            raise ValueError("Document reranking requires finite nonnegative L2 distances")
        age_days = np.maximum(0, params['as_of'] - published.to_numpy(zero_copy_only=False)) / 86400
        freshness = np.exp2(-age_days / POLICY['half_life_days'])
        trust = np.array([{'official': 1.0, 'community': 0.65}.get(source, 0.25)
                          for source in sources.to_pylist()])
        quality = np.clip(np.nan_to_num(quality.to_numpy(zero_copy_only=False), nan=0.5), 0, 1)
        scores = (0.6 / (1 + distances) + POLICY['freshness_weight'] * freshness
                  + (0.3 - POLICY['freshness_weight']) * trust + 0.1 * quality)
        return [pa.array(scores, type=pa.float32())]

def create_udf(context):
    context.logger.info('Document reranker policy=%s resource=%s', POLICY, context.resource_name)
    return DocumentReranker()
"""


def rag_chain(resource):
    return (
        FunctionChain(FunctionChainStage.L2_RERANK)
        .map(
            "$score",
            FunctionChainExpr(
                "py_udf",
                args=tuple(col(name) for name in ["$score", *DOCUMENT_FIELDS]),
                params={"resource_name": resource, "udf_params": {"as_of": RAG_QUERY_TIME}},
            ),
        )
        .sort(col("$score"), desc=True, tie_break_col=col("$id"))
        .limit(20)
    )


def verify_document_ranking(checker, hits, query_index, policy):
    """Compute the business oracle from retained inputs, independently of the UDF."""
    scores = {}
    for pk in checker.candidates[query_index]:
        document = checker.documents[pk]
        distance = float(np.sum((checker.vectors[pk] - checker.queries[query_index]) ** 2))
        age = max(0, RAG_QUERY_TIME - document["published_at"]) / 86400
        freshness = math.exp(-math.log(2) * age / policy["half_life_days"])
        trust = 1.0 if document["source"] == "official" else 0.65 if document["source"] == "community" else 0.25
        quality = 0.5 if document["quality"] is None else min(1.0, max(0.0, document["quality"]))
        score = 0.6 / (1 + distance) + policy["freshness_weight"] * freshness
        score += (0.3 - policy["freshness_weight"]) * trust + 0.1 * quality
        scores[pk] = float(np.float32(score))
    expected_ids = sorted(scores, key=lambda pk: (-scores[pk], pk))[:20]
    assert checker.verify_fields(hits) == expected_ids, "Document ranking differs from the retained-input oracle"
    for hit in hits:
        assert np.isclose(hit["distance"], scores[hit["id"]], rtol=1e-5, atol=1e-6), hit
        assert {field: hit["entity"][field] for field in DOCUMENT_FIELDS} == checker.documents[hit["id"]], hit
    return expected_ids


def package_cache(workload):
    """Check wheel reuse and resource removal; latency does not prove a cache hit."""
    root, config = workload.root, workload.config
    checker, clients = copy.copy(workload.checker), workload.clients
    output = root / "package-cache"
    output.mkdir()
    wheels = []
    first_rankings = set()
    changed_rankings = 0
    for phase in ("baseline", "cold", "replay", "remove", "recovery"):
        check_stop_file(root / "STOP")
        if phase == "cold":
            prefix = config.collection + "_package_cache"
            policies = [
                {"half_life_days": days, "freshness_weight": weight}
                for days in (7, 14, 30, 60, 90, 180, 365, 730)
                for weight in (0.1, 0.15, 0.2, 0.25)
            ]
            sources = [f"POLICY = {policy!r}\n" + RAG_RERANK_MODULE for policy in policies]
            wheels = register(workload, output, prefix, sources)
            for wheel, policy in zip(wheels, policies):
                wheel["policy"] = policy
            save(output / "wheels.json", wheels)
        result = []
        if phase in ("cold", "replay"):
            for repeat in range(1 if phase == "cold" else 10):
                for wheel in wheels:
                    check_stop_file(root / "STOP")
                    before = time.monotonic()
                    hits = checker.search(
                        clients[0],
                        checker.queries[repeat],
                        rag_chain(wheel["resource"]),
                        output_fields=FIELDS + DOCUMENT_FIELDS,
                    )
                    ids = verify_document_ranking(checker, hits, repeat, wheel["policy"])
                    changed_rankings += ids != checker.candidates[repeat][:20]
                    if phase == "cold":
                        first_rankings.add(tuple(ids))
                    result.append(
                        {
                            "resource": wheel["resource"],
                            "repeat": repeat,
                            "ids": ids,
                            "policy": wheel["policy"],
                            "latency_ms": 1000 * (time.monotonic() - before),
                        }
                    )
        elif phase == "remove":
            for wheel in wheels:
                check_stop_file(root / "STOP")
                clients[0].remove_file_resource(wheel["resource"], timeout=30)
            names = {item.name for item in clients[0].list_file_resources()}
            assert not any(wheel["resource"] in names for wheel in wheels)
            for wheel in wheels:
                started = time.monotonic()
                accepted_before_convergence = 0
                while True:
                    check_stop_file(root / "STOP")
                    try:
                        hits = checker.search(
                            clients[0],
                            checker.queries[0],
                            rag_chain(wheel["resource"]),
                            output_fields=FIELDS + DOCUMENT_FIELDS,
                        )
                        verify_document_ranking(checker, hits, 0, wheel["policy"])
                        accepted_before_convergence += 1
                    except Exception as error:
                        message = str(error)
                        assert (
                            getattr(error, "code", None) == 1100
                            and "file resource" in message
                            and "not found" in message
                        ), error
                        result.append(
                            {
                                "resource": wheel["resource"],
                                "rejected": True,
                                "elapsed": time.monotonic() - started,
                                "accepted_before_convergence": accepted_before_convergence,
                                "error": message,
                            }
                        )
                        break
                    assert time.monotonic() - started < 30, (
                        "Removed resource remained callable beyond the observation budget"
                    )
                    time.sleep(0.2)
        elif phase in ("baseline", "recovery"):
            for client in clients:
                checker.verify_control(checker.search(client, checker.queries[0]), 0)
                checker.once(client, 1)
            checker.evidence = output
            checker.audit_all_rows()
        save(
            output / f"{phase}.json",
            {
                "workload": "pyudf_rag_package_lifecycle",
                "query_time": RAG_QUERY_TIME,
                "changed_rankings": changed_rankings,
                "distinct_policy_rankings": len(first_rankings),
                "cache_hit_verification": "NOT_MEASURED",
                "phase": phase,
                "unix": time.time(),
                "outcome": "PASS",
                "operations": result,
                "packages": len(wheels),
                "target_proxy": 0,
            },
        )

    assert changed_rankings > 0, "Business features never changed the vector-only ranking"
    assert len(first_rankings) > 1, "Different wheel policies never changed the ranking"


def concurrency(workload):
    """Follow up at the RPC boundary without repeating the full ABBA campaign."""
    root, config = workload.root, workload.config
    checker = copy.copy(workload.checker)
    output = root / "concurrency"
    output.mkdir()
    checker.evidence = output
    addresses = config.proxy_uris
    summaries = []
    for workers in (1, 8, 32):
        clients = []
        try:
            for number in range(max(2, workers)):
                clients.append(MilvusClient(addresses[number % 2], token=os.environ["MILVUS_TOKEN"], dedicated=True))
            counters = [SearchWireCounter(client) for client in clients]
            for mode in ("ordinary", "python_arithmetic"):
                activity = SearchActivity()
                for counter in counters:
                    counter.activity = activity
                lock = threading.Lock()
                barrier = threading.Barrier(workers)
                started = time.monotonic()
                wall = time.time()
                count = 0
                failures = []

                def invoke():
                    nonlocal count
                    barrier.wait(timeout=30)
                    while time.monotonic() - started < 20:
                        with lock:
                            sequence = count
                            count += 1
                        try:
                            check_stop_file(root / "STOP")
                            chain = checker.chain(checker.resources[0], 2, 0) if mode.startswith("python") else None
                            hits = checker.search(
                                clients[sequence % len(clients)], checker.queries[sequence % 1000], chain
                            )
                            if chain is None:
                                checker.verify_control(hits, sequence % 1000)
                            else:
                                checker.verify_udf(hits, checker.candidates[sequence % 1000], 2, 0)
                        except Exception as error:
                            with lock:
                                failures.append(str(error))
                            return

                with ThreadPoolExecutor(max_workers=workers) as pool:
                    try:
                        futures = [pool.submit(invoke) for _ in range(workers)]
                        for future in futures:
                            future.result()
                    except BaseException:
                        (root / "STOP").touch()
                        raise
                measured = activity.snapshot()
                summary = {
                    "mode": mode,
                    "workers": workers,
                    "started_unix": wall,
                    "seconds": 20,
                    "offered": count,
                    "rpc": measured,
                    "failures": failures,
                    "outcome": "FAIL" if failures else "PASS" if measured["peak"] == workers else "INCONCLUSIVE",
                }
                summaries.append(summary)
                save(output / "summary.json", summaries)
                assert not failures, summary
        finally:
            for client in clients:
                client.close()
    assert all(item["outcome"] == "PASS" for item in summaries), "RPC concurrency target not observed"
    save(output / "post-concurrency-audit.json", checker.audit_all_rows())
