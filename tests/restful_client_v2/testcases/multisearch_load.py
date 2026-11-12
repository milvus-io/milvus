"""On-demand REST comparison. Pass this file explicitly to the REST E2E runner."""

import json
import math
import random
import resource
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import requests
from api.milvus import CollectionClient, VectorClient
from utils.utils import gen_unique_str

BATCH_SIZE = 8
RESIDENT_WORKERS = 12
PHASE_SECONDS = 8


@dataclass
class Measurements:
    latency_ms: list = field(default_factory=list)
    failures: int = 0
    max_round_result_bytes: int = 0

    def add(self, started, succeeded, result_bytes):
        self.latency_ms.append((time.perf_counter() - started) * 1000)
        self.failures += not succeeded
        self.max_round_result_bytes = max(self.max_round_result_bytes, result_bytes)

    def summary(self, seconds, items_per_round=1):
        ordered = sorted(self.latency_ms)

        def percentile(p):
            return round(ordered[math.ceil(len(ordered) * p) - 1], 2) if ordered else None

        return {
            "rounds": len(ordered),
            "successful_rounds": len(ordered) - self.failures,
            "failures": self.failures,
            "offered_items_per_second": round(len(ordered) * items_per_round / seconds, 1),
            "p50_ms": percentile(0.50),
            "p95_ms": percentile(0.95),
            "p99_ms": percentile(0.99),
            "max_round_result_bytes": self.max_round_result_bytes,
        }


def prepare_collections(endpoint, token, request):
    collections, vectors = CollectionClient(endpoint, token), VectorClient(endpoint, token)
    names = []
    rng = random.Random(145)
    for _ in range(2):
        name = gen_unique_str("multisearch_load")
        response = collections.collection_create(
            {
                "collectionName": name,
                "schema": {
                    "autoId": False,
                    "fields": [
                        {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                        {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "32"}},
                    ],
                },
                "indexParams": [
                    {"fieldName": "vector", "indexName": "flat", "indexType": "FLAT", "metricType": "L2"},
                ],
            }
        )
        assert response["code"] == 0, response
        request.addfinalizer(lambda collection=name: collections.collection_drop({"collectionName": collection}))
        for base in range(0, 4096, 1024):
            response = vectors.vector_insert(
                {
                    "collectionName": name,
                    "data": [{"id": i, "vector": [rng.random() for _ in range(32)]} for i in range(base, base + 1024)],
                }
            )
            assert response["code"] == 0, response
        response = collections.flush(name)
        assert response["code"] == 0, response
        collections.wait_load_completed(name, timeout=90)
        names.append(name)
    return [
        {
            "collectionName": names[i % 2],
            "data": [[rng.random() for _ in range(32)]],
            "limit": 8,
            "filter": f"id >= {i}",
            "outputFields": ["vector"],
        }
        for i in range(BATCH_SIZE)
    ]


def make_requests(endpoint, token):
    clients = threading.local()

    def call(operation, payload):
        if not hasattr(clients, "session"):
            clients.session = requests.Session()
            clients.session.headers["Authorization"] = f"Bearer {token}"
        try:
            response = clients.session.post(
                f"{endpoint}/v2/vectordb/entities/{operation}",
                json=payload,
                timeout=30,
            )
            body = response.json()
            if response.status_code != 200 or body.get("code") != 0:
                return False, len(response.content)
            if operation == "multi_search":
                results = body.get("data", [])
                return len(results) == BATCH_SIZE and all(item["search"].get("code") == 0 for item in results), len(
                    response.content
                )
            return bool(body.get("data")), len(response.content)
        except (requests.RequestException, ValueError, KeyError):
            return False, 0

    return call


def read_metrics(endpoint):
    host = urlsplit(endpoint).hostname
    try:
        response = requests.get(f"http://{host}:9091/metrics", timeout=1)
        response.raise_for_status()
    except requests.RequestException:
        return {}
    names = (
        "process_cpu_seconds_total",
        "process_resident_memory_bytes",
        "go_memstats_heap_inuse_bytes",
        "milvus_proxy_multisearch_retries_total",
        "milvus_proxy_multisearch_attempts_total",
    )
    values = {}
    for line in response.text.splitlines():
        name = line.split(" ", 1)[0].split("{", 1)[0]
        if name in names:
            values[name] = values.get(name, 0) + float(line.rsplit(" ", 1)[1])
    return values


def phase(endpoint, call, searches, mode, concurrency, offered_rate=None, mixed=False):
    resident_stop = threading.Event()
    resident_measurements = []
    group_measurements = Measurements()
    metrics_before = read_metrics(endpoint)
    cpu_before = time.process_time()

    def resident(i):
        measurements = Measurements()
        while not resident_stop.is_set():
            started = time.perf_counter()
            measurements.add(started, *call("search", searches[i % BATCH_SIZE]))
        return measurements

    with (
        ThreadPoolExecutor(max_workers=concurrency) as fanout,
        ThreadPoolExecutor(max_workers=RESIDENT_WORKERS) as residents,
    ):
        resident_jobs = [residents.submit(resident, i) for i in range(RESIDENT_WORKERS)] if mixed else []
        start = time.perf_counter()
        while time.perf_counter() - start < PHASE_SECONDS:
            started = time.perf_counter()
            if mode == "batch":
                payload = {"requests": [{"search": search} for search in searches]}
                if concurrency != 4:
                    payload["maxConcurrency"] = concurrency
                group_measurements.add(started, *call("multi_search", payload))
            elif mode == "fanout":
                results = list(fanout.map(lambda search: call("search", search), searches))
                group_measurements.add(started, all(ok for ok, _ in results), sum(size for _, size in results))
            else:
                resident_stop.wait(PHASE_SECONDS)
            if offered_rate and group_measurements.latency_ms:
                next_round = start + len(group_measurements.latency_ms) / offered_rate
                resident_stop.wait(max(0, next_round - time.perf_counter()))
        resident_stop.set()
        elapsed = time.perf_counter() - start
        resident_measurements = [job.result() for job in resident_jobs]

    resident_combined = Measurements()
    for measurement in resident_measurements:
        resident_combined.latency_ms.extend(measurement.latency_ms)
        resident_combined.failures += measurement.failures
    metrics_after = read_metrics(endpoint)
    result = {
        "mode": mode,
        "concurrency": concurrency,
        "mixed": mixed,
        "target_rounds_per_second": offered_rate,
        "seconds": round(elapsed, 2),
        "group": group_measurements.summary(elapsed, BATCH_SIZE),
        "resident": resident_combined.summary(elapsed),
        "runner_cpu_seconds": round(time.process_time() - cpu_before, 3),
        "runner_process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "server_metrics_before": metrics_before,
        "server_metrics_after": metrics_after,
    }
    print("MULTISEARCH_LOAD " + json.dumps(result), flush=True)
    return result


def test_multisearch_load(endpoint, token, request):
    searches = prepare_collections(endpoint, token, request)
    call = make_requests(endpoint, token)
    for search in searches:
        assert call("search", search)[0]
    samples = [
        phase(endpoint, call, searches, mode, concurrency)
        for mode, concurrency in [("fanout", 4), ("batch", 4), ("batch", 8), ("fanout", 8)]
    ]
    offered_rate = min(sample["group"]["rounds"] / sample["seconds"] for sample in samples[:2]) * 0.5
    phase(endpoint, call, searches, "resident", 4, mixed=True)
    for mode in ["fanout", "batch", "batch", "fanout"]:
        phase(endpoint, call, searches, mode, 4, offered_rate=offered_rate, mixed=True)
