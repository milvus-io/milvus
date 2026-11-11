"""Bounded performance telemetry, interference, and package-cache workloads."""

import hashlib
import io
import json
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
from workloads.pyudf.runner import WorkloadConfig, load_checker, save
from workloads.pyudf.workload import MODULE, check_stop_file

# The mmap counters measure real transform entries/completions without a syscall
# on every invocation. They add a lock/write cost, explicitly retained in metadata.
COUNTER_IMPORT = """import mmap
import struct
from pathlib import Path
"""
COUNTER_INIT = """        self.active = 0
        self.peak = 0
        self.completed = 0
        path = Path('/tmp') / ('pyudf-r2-counter-' + context.resource_name + '-' + str(os.getpid()))
        self.counter_file = path.open('w+b')
        self.counter_file.truncate(32)
        self.counter = mmap.mmap(self.counter_file.fileno(), 32)
        context.logger.warning('PYUDF_R2_FACTORY resource=%s pid=%s', context.resource_name, os.getpid())
"""
COUNTER_START = """            self.active += 1
            self.peak = max(self.peak, self.active)
            self.counter[:] = struct.pack('<QQQQ', self.count, self.completed, self.active, self.peak)
"""
COUNTER_END = """        with self.lock:
            self.completed += 1
            self.active -= 1
            self.counter[:] = struct.pack('<QQQQ', self.count, self.completed, self.active, self.peak)
"""
COUNTER_FIELDS = ("pid", "count", "completed", "active", "peak")


def counted_module():
    source = MODULE.replace("import pyarrow as pa", "import pyarrow as pa\n" + COUNTER_IMPORT)
    source = source.replace(
        "        self.lock = threading.Lock()", "        self.lock = threading.Lock()\n" + COUNTER_INIT
    )
    source = source.replace("            count = self.count", "            count = self.count\n" + COUNTER_START)
    source = source.replace(
        "    def transform_query(self, params, columns):",
        """    def transform_query(self, params, columns):
        if params.get('counter_query'):
            with self.lock:
                values = (os.getpid(), self.count, self.completed, self.active, self.peak)
                if params.get('reset_peak'):
                    self.peak = self.active
            # Search scores are float32; reject counters that would lose precision.
            assert all(0 <= value <= 2**24 for value in values), values
            return [pa.array([values[index.as_py()] for index in columns[0]], type=pa.float32())]""",
    )
    body_start = source.index("        values = columns[0].to_pylist()")
    body_end = source.index("\ndef create_udf", body_start)
    body = source[body_start:body_end].rstrip()
    wrapped = "        try:\n" + "\n".join("    " + line for line in body.splitlines())
    wrapped += "\n        finally:\n" + "\n".join("    " + line for line in COUNTER_END.rstrip().splitlines()) + "\n"
    return source[:body_start] + wrapped + source[body_end:]


INTERFERENCE_MODULE = (
    """import os
import threading
import time
import pyarrow as pa
"""
    + COUNTER_IMPORT
    + """
class UDF:
    def __init__(self, context):
        self.context = context
        self.count = 0
        self.lock = threading.Lock()
"""
    + COUNTER_INIT
    + """
    def transform_query(self, params, columns):
        with self.lock:
            self.count += 1
"""
    + COUNTER_START
    + """
        started = time.time()
        self.context.logger.warning('PYUDF_R2_ENTER resource=%s pid=%s call=%s mode=%s unix=%s', self.context.resource_name, os.getpid(), params['call'], params['mode'], started)
        try:
            # The shared absolute deadline prevents a queue from extending CPU
            # pressure beyond the approved 60-second experiment window.
            deadline = min(params['deadline'], started + 1)
            if params['mode'] == 'wait':
                time.sleep(max(0, deadline - time.time()))
            else:
                value = 1
                while time.time() < deadline:
                    value = (value * 1664525 + 1013904223) % 4294967296
            return [pa.array([v.as_py() * 2 for v in columns[0]], type=pa.float32())]
        finally:
            with self.lock:
                self.completed += 1
                self.active -= 1
                self.counter[:] = struct.pack('<QQQQ', self.count, self.completed, self.active, self.peak)
            self.context.logger.warning('PYUDF_R2_END resource=%s pid=%s call=%s mode=%s unix=%s', self.context.resource_name, os.getpid(), params['call'], params['mode'], time.time())

def create_udf(context):
    return UDF(context)
"""
)


def register(checker, root, prefix, count, source, config):
    store = config.storage()
    registered = {item.name for item in checker.client.list_file_resources()}
    wheels = []
    for number in range(count):
        name = f"{prefix}_{number:02d}"
        assert name not in registered, f"Refuse to overwrite {name}"
        wheel = build_wheel(name + "_pkg", source)
        path = f"pyudf_r2/{name}.whl"
        store.put_object(config.bucket, path, io.BytesIO(wheel), len(wheel))
        checker.client.add_file_resource(name, path, timeout=30)
        (root / (name + ".whl")).write_bytes(wheel)
        wheels.append({"resource": name, "path": path, "sha256": hashlib.sha256(wheel).hexdigest()})
        save(root / "wheels.json", wheels)
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


def performance(root):
    config = WorkloadConfig.read(root)
    prerequisites = verify_single_worker_proxies(config)
    checker, clients = load_checker(root)
    try:
        output = root / "performance"
        output.mkdir()
        save(output / "worker-prerequisites.json", prerequisites)
        checker.evidence = output
        prefix = config.collection + "_perf"
        wheels = register(checker, output, prefix, 2, counted_module(), config)
        original_resource = checker.resources[0]
        checker.resources = [item["resource"] for item in wheels]
        save(
            output / "instrumentation.json",
            {
                "method": "32-byte mmap counters per resource/worker; start/end lock and memory writes",
                "original_control_wheels": json.loads((root / "input/metadata.json").read_text())["wheels"],
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
        run_batch_measurements(
            checker, addresses[0], os.environ["MILVUS_TOKEN"], proxy_uris=addresses, stop_file=root / "STOP"
        )
        failed = [
            result for result in windows if result.get("outcome") == "FAIL" or result.get("internal_outcome") == "FAIL"
        ]
        save(
            output / "terminal.json",
            {"outcome": "FAIL" if failed else "MEASURED", "failed_windows": len(failed), "unix": time.time()},
        )
        assert not failed, "Strict benchmark gate failed; independent measurements retained"
    finally:
        for client in clients:
            client.close()


def peer_baseline(checker, clients, output, seconds=15):
    samples = []
    started = time.monotonic()
    sequence = 0
    while time.monotonic() - started < seconds:
        for number, client in enumerate(clients):
            before = time.monotonic()
            hits = checker.search(client, checker.queries[sequence % 1000])
            checker.verify_control(hits, sequence % 1000)
            samples.append({"proxy": number, "latency_ms": 1000 * (time.monotonic() - before)})
        sequence += 1
        time.sleep(0.05)
    save(output, {"outcome": "PASS", "unix": time.time(), "samples": samples})


def interference(root):
    config = WorkloadConfig.read(root)
    checker, clients = load_checker(root)
    output = root / "interference"
    output.mkdir()
    prefix = config.collection + "_interference"
    resource = register(checker, output, prefix, 1, INTERFERENCE_MODULE, config)[0]["resource"]
    addresses = config.proxy_uris
    attackers = [MilvusClient(addresses[n % 2], token=os.environ["MILVUS_TOKEN"], dedicated=True) for n in range(32)]
    wire = [SearchWireCounter(client) for client in attackers]
    try:
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
                barrier.wait()
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
                barrier.wait()
                sequence = 0
                while time.monotonic() - started < 60:
                    before = time.monotonic()
                    try:
                        hits = checker.search(clients[number], checker.queries[sequence % 1000])
                        checker.verify_control(hits, sequence % 1000)
                        item = {"proxy": number, "latency_ms": 1000 * (time.monotonic() - before)}
                    except Exception as error:
                        item = {"proxy": number, "error": str(error)}
                    with lock:
                        peer.append(item)
                    sequence += 1
                    time.sleep(0.05)

            with ThreadPoolExecutor(max_workers=34) as pool:
                futures = [pool.submit(actor, n) for n in range(32)] + [pool.submit(peer_actor, n) for n in range(2)]
                for future in futures:
                    future.result()
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
    finally:
        for client in clients + attackers:
            client.close()


def package_cache(root, phase):
    """Check wheel reuse and resource removal; latency does not prove a cache hit."""
    config = WorkloadConfig.read(root)
    checker, clients = load_checker(root)
    output = root / "package-cache"
    output.mkdir(exist_ok=True)
    try:
        if phase == "baseline":
            wheels = []
        elif phase == "cold":
            assert not (output / "wheels.json").exists()
            prefix = config.collection + "_package_cache"
            package_source = MODULE.replace(
                "def create_udf(context):\n    return UDF(context)",
                "def create_udf(context):\n    context.logger.warning('PYUDF_R2_FACTORY resource=%s pid=%s', context.resource_name, os.getpid())\n    return UDF(context)",
            )
            wheels = register(checker, output, prefix, 32, package_source, config)
        else:
            wheels = json.loads((output / "wheels.json").read_text())
        result = []
        if phase in ("cold", "replay"):
            for repeat in range(1 if phase == "cold" else 10):
                for wheel in wheels:
                    before = time.monotonic()
                    hits = checker.search(clients[0], checker.queries[repeat], checker.chain(wheel["resource"], 2, 0))
                    checker.verify_udf(hits, checker.candidates[repeat], 2, 0)
                    result.append(
                        {
                            "resource": wheel["resource"],
                            "repeat": repeat,
                            "latency_ms": 1000 * (time.monotonic() - before),
                        }
                    )
        elif phase == "remove":
            for wheel in wheels:
                clients[0].remove_file_resource(wheel["resource"], timeout=30)
            names = {item.name for item in clients[0].list_file_resources()}
            assert not any(wheel["resource"] in names for wheel in wheels)
            for wheel in wheels:
                started = time.monotonic()
                accepted_before_convergence = 0
                while True:
                    try:
                        hits = checker.search(clients[0], checker.queries[0], checker.chain(wheel["resource"], 2, 0))
                        checker.verify_udf(hits, checker.candidates[0], 2, 0)
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
                "workload": "pyudf_package_cache",
                "cache_hit_verification": "NOT_MEASURED",
                "phase": phase,
                "unix": time.time(),
                "outcome": "PASS",
                "operations": result,
                "packages": len(wheels),
                "target_proxy": 0,
            },
        )
    finally:
        for client in clients:
            client.close()


def concurrency(root):
    """Follow up at the RPC boundary without repeating the full ABBA campaign."""
    config = WorkloadConfig.read(root)
    checker, controls = load_checker(root)
    output = root / "concurrency"
    output.mkdir()
    addresses = config.proxy_uris
    summaries = []
    try:
        for workers in (1, 8, 32):
            clients = [
                MilvusClient(addresses[n % 2], token=os.environ["MILVUS_TOKEN"], dedicated=True)
                for n in range(max(2, workers))
            ]
            counters = [SearchWireCounter(client) for client in clients]
            try:
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
                        barrier.wait()
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
                        futures = [pool.submit(invoke) for _ in range(workers)]
                        for future in futures:
                            future.result()
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
    finally:
        for client in controls:
            client.close()


def cleanup(root):
    config = WorkloadConfig.read(root)
    checker, clients = load_checker(root)
    try:
        names = {item.name for item in clients[0].list_file_resources()}
        removed = []
        for phase in ("performance", "interference"):
            path = root / phase / "wheels.json"
            if not path.exists():
                continue
            for item in json.loads(path.read_text()):
                name = item["resource"]
                assert name.startswith(config.collection + "_")
                if name in names:
                    clients[0].remove_file_resource(name, timeout=30)
                    removed.append(name)
        save(
            root / "extra-resource-cleanup.json",
            {"removed": removed, "wheel_objects_retained": True, "unix": time.time()},
        )
    finally:
        for client in clients:
            client.close()
