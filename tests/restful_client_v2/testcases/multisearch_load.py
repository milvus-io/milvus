"""On-demand REST comparison. Pass this file explicitly to the REST E2E runner.

HTTP response body sizes are measured at the client. Heap and RSS samples
cover the Milvus process serving Proxy, which includes all roles in standalone
mode. Result-buffer allocations have no separate measurement.

The fan-out deadline starts before client dispatch and covers HTTP I/O.
MultiSearch starts its execution budget on receipt and allows response delivery
within the separate transport timeout.
"""

import json
import math
import os
import random
import resource
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import requests
from api.milvus import CollectionClient, VectorClient
from urllib3.util import Timeout
from utils.utils import gen_unique_str

BATCH_SIZE = 8
RESIDENT_WORKERS = 12
PHASE_SECONDS = 8
EXECUTION_BUDGET_MS = 5000
TRANSPORT_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 10
INITIAL_BACKOFF_SECONDS = 0.2
MAX_BACKOFF_SECONDS = 3
METRICS_SAMPLE_SECONDS = 0.5
ENTITIES_PATH = "/v2/vectordb/entities"
RATE_LIMIT_CODE = 8
QUEUE_FULL_CODE = 4
REST_RATE_LIMIT_CODE = 1807


@dataclass(frozen=True)
class RoundOutcome:
    succeeded: bool
    response_body_bytes: int
    client_http_attempts: int
    client_http_retries: int = 0


@dataclass
class Measurements:
    latency_ms: list = field(default_factory=list)
    failures: int = 0
    max_round_http_response_body_bytes: int = 0
    client_http_attempts: int = 0
    client_http_retries: int = 0

    def add(self, started, outcome):
        self.latency_ms.append((time.perf_counter() - started) * 1000)
        self.failures += not outcome.succeeded
        self.max_round_http_response_body_bytes = max(
            self.max_round_http_response_body_bytes, outcome.response_body_bytes
        )
        self.client_http_attempts += outcome.client_http_attempts
        self.client_http_retries += outcome.client_http_retries

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
            "max_round_http_response_body_bytes": self.max_round_http_response_body_bytes,
            "client_http_attempts": self.client_http_attempts,
            "client_http_retries": self.client_http_retries,
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

    def call(operation, payload, timeout, headers=None):
        if not hasattr(clients, "session"):
            clients.session = requests.Session()
            clients.session.headers.update({"Authorization": f"Bearer {token}", "Accept-Encoding": "identity"})
        body_bytes = 0
        try:
            response = clients.session.post(
                f"{endpoint}{ENTITIES_PATH}/{operation}",
                json=payload,
                headers=headers,
                timeout=Timeout(total=timeout),
                allow_redirects=False,
            )
            body_bytes = len(response.content)
            body = response.json()
            if not isinstance(body, dict):
                return False, body_bytes, False
            code = body.get("code")
            if response.status_code != 200 or code != 0:
                # REST wraps ordinary quota rejections in code 1807. The
                # inner message distinguishes a rate limit from force-deny.
                retryable = code in (QUEUE_FULL_CODE, RATE_LIMIT_CODE) or (
                    code == REST_RATE_LIMIT_CODE and "rate limit exceeded" in body.get("message", "")
                )
                return False, body_bytes, retryable
            results = body.get("data")
            if not isinstance(results, list):
                return False, body_bytes, False
            if operation == "multi_search":
                succeeded = len(results) == len(payload["requests"]) and all(
                    item["search"].get("code") == 0 for item in results
                )
                return succeeded, body_bytes, False
            return True, body_bytes, False
        except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError):
            return False, body_bytes, False

    return call


def run_search(call, search, deadline, max_attempts=MAX_ATTEMPTS):
    attempts, body_bytes, succeeded = 0, 0, False
    backoff = INITIAL_BACKOFF_SECONDS
    while attempts < max_attempts:
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            break
        # Request-Timeout accepts whole seconds. The client uses the
        # remaining budget for I/O, including queue time in this round.
        succeeded, received_bytes, retryable = call(
            "search", search, remaining, {"Request-Timeout": str(math.ceil(remaining))}
        )
        attempts += 1
        body_bytes += received_bytes
        if succeeded or not retryable or attempts == max_attempts or deadline - time.perf_counter() <= backoff:
            break
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
    return RoundOutcome(succeeded, body_bytes, attempts, max(0, attempts - 1))


def batch_payload(searches, concurrency=None):
    payload = {"requests": [{"search": search} for search in searches], "timeoutMs": EXECUTION_BUDGET_MS}
    if concurrency is not None:
        payload["maxConcurrency"] = concurrency
    return payload


COUNTER_KEYS = (
    "search_http_requests",
    "multisearch_http_requests",
    "multisearch_attempts",
    "multisearch_rate_limit_retries",
    "multisearch_queue_full_retries",
)
PROCESS_GAUGE_KEYS = ("process_resident_memory_bytes", "go_memstats_heap_inuse_bytes")
PROCESS_KEYS = (*PROCESS_GAUGE_KEYS, "process_cpu_seconds_total", "process_start_time_seconds")


def read_metrics(metrics_url):
    try:
        response = requests.get(metrics_url, timeout=1)
        response.raise_for_status()
    except requests.RequestException:
        return {}
    values = {}
    for line in response.text.splitlines():
        series, _, value = line.partition(" ")
        name, _, labels = series.partition("{")
        if name in PROCESS_KEYS:
            key = name
        elif name == "milvus_proxy_multisearch_attempts_total":
            key = "multisearch_attempts"
        elif name == "milvus_proxy_multisearch_retries_total" and 'reason="rate_limit"' in labels:
            key = "multisearch_rate_limit_retries"
        elif name == "milvus_proxy_multisearch_retries_total" and 'reason="queue_full"' in labels:
            key = "multisearch_queue_full_retries"
        elif name == "milvus_proxy_restful_api_req_count" and f'path="{ENTITIES_PATH}/search"' in labels:
            key = "search_http_requests"
        elif name == "milvus_proxy_restful_api_req_count" and f'path="{ENTITIES_PATH}/multi_search"' in labels:
            key = "multisearch_http_requests"
        else:
            continue
        values[key] = values.get(key, 0) + float(value.split()[0])
    return values


def checked_metrics(metrics_url):
    sample = read_metrics(metrics_url)
    missing = set((*COUNTER_KEYS, *PROCESS_KEYS)) - sample.keys()
    assert not missing, f"Missing Proxy metrics from {metrics_url}: {sorted(missing)}"
    return sample


def metric_deltas(before, after):
    assert before["process_start_time_seconds"] == after["process_start_time_seconds"], "Metrics process changed"
    deltas = {key: int(after[key] - before[key]) for key in COUNTER_KEYS}
    assert all(value >= 0 for value in deltas.values()), deltas
    return deltas


def phase(metrics_url, call, searches, mode, concurrency, offered_rate=None, mixed=False):
    resident_stop = threading.Event()
    sampler_stop = threading.Event()
    group_measurements = Measurements()
    metrics_before = checked_metrics(metrics_url)
    samples = []
    payload = batch_payload(searches, concurrency)

    def sample_process():
        while not sampler_stop.wait(METRICS_SAMPLE_SECONDS):
            sample = read_metrics(metrics_url)
            if sample.get("process_start_time_seconds") == metrics_before["process_start_time_seconds"]:
                samples.append(sample)

    def resident(i):
        measurements = Measurements()
        while not resident_stop.is_set():
            started = time.perf_counter()
            deadline = started + EXECUTION_BUDGET_MS / 1000
            measurements.add(started, run_search(call, searches[i % BATCH_SIZE], deadline, max_attempts=1))
        return measurements

    sampler = threading.Thread(target=sample_process)
    sampler.start()
    try:
        cpu_before = time.process_time()
        start = time.perf_counter()
        with (
            ThreadPoolExecutor(max_workers=concurrency) as fanout,
            ThreadPoolExecutor(max_workers=RESIDENT_WORKERS) as residents,
        ):
            resident_jobs = [residents.submit(resident, i) for i in range(RESIDENT_WORKERS)] if mixed else []
            try:
                while time.perf_counter() - start < PHASE_SECONDS:
                    started = time.perf_counter()
                    if mode == "batch":
                        ok, body_bytes, _ = call("multi_search", payload, TRANSPORT_TIMEOUT_SECONDS)
                        outcome = RoundOutcome(ok, body_bytes, 1)
                    elif mode == "fanout":
                        # One deadline covers both queued and running items.
                        deadline = started + EXECUTION_BUDGET_MS / 1000
                        results = list(fanout.map(lambda search: run_search(call, search, deadline), searches))
                        outcome = RoundOutcome(
                            all(item.succeeded for item in results),
                            sum(item.response_body_bytes for item in results),
                            sum(item.client_http_attempts for item in results),
                            sum(item.client_http_retries for item in results),
                        )
                    else:
                        resident_stop.wait(PHASE_SECONDS)
                        break
                    group_measurements.add(started, outcome)
                    if offered_rate:
                        next_round = start + len(group_measurements.latency_ms) / offered_rate
                        resident_stop.wait(max(0, min(next_round, start + PHASE_SECONDS) - time.perf_counter()))
            finally:
                resident_stop.set()
            resident_measurements = [job.result() for job in resident_jobs]
        elapsed = time.perf_counter() - start
        runner_cpu_seconds = time.process_time() - cpu_before
    finally:
        sampler_stop.set()
        sampler.join()

    resident_combined = Measurements()
    for measurement in resident_measurements:
        resident_combined.latency_ms.extend(measurement.latency_ms)
        resident_combined.failures += measurement.failures
        resident_combined.client_http_attempts += measurement.client_http_attempts
        resident_combined.client_http_retries += measurement.client_http_retries
        resident_combined.max_round_http_response_body_bytes = max(
            resident_combined.max_round_http_response_body_bytes, measurement.max_round_http_response_body_bytes
        )
    metrics_after = checked_metrics(metrics_url)
    samples = [metrics_before, *samples, metrics_after]
    result = {
        "mode": mode,
        "concurrency": concurrency,
        "execution_budget_ms": EXECUTION_BUDGET_MS,
        "retry_policy": {
            "max_attempts": MAX_ATTEMPTS,
            "initial_backoff_seconds": INITIAL_BACKOFF_SECONDS,
            "max_backoff_seconds": MAX_BACKOFF_SECONDS,
        },
        "mixed": mixed,
        "target_rounds_per_second": offered_rate,
        "seconds": round(elapsed, 2),
        "group": group_measurements.summary(elapsed, BATCH_SIZE),
        "resident": resident_combined.summary(elapsed),
        "runner_cpu_seconds": round(runner_cpu_seconds, 3),
        "runner_process_peak_rss_to_date_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "server_counter_deltas": metric_deltas(metrics_before, metrics_after),
        "milvus_process": {
            "scope": "process serving Proxy, including all Milvus roles in standalone",
            "samples": len(samples),
            "cpu_seconds": round(
                metrics_after["process_cpu_seconds_total"] - metrics_before["process_cpu_seconds_total"], 3
            ),
            "sampled_memory_bytes": {
                key: {
                    "start": int(metrics_before[key]),
                    "peak": int(max(sample[key] for sample in samples if key in sample)),
                    "end": int(metrics_after[key]),
                }
                for key in PROCESS_GAUGE_KEYS
            },
            "proxy_result_buffer_heap_bytes": None,
        },
    }
    print("MULTISEARCH_LOAD " + json.dumps(result), flush=True)
    return result


def measure_batch_admission(endpoint, metrics_url, token, searches, concurrent_batches):
    start = threading.Barrier(concurrent_batches)
    payload = batch_payload(searches)
    url = f"{endpoint}{ENTITIES_PATH}/multi_search"
    headers = {"Authorization": f"Bearer {token}"}

    def submit(_):
        start.wait(timeout=10)
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=TRANSPORT_TIMEOUT_SECONDS)
            body = response.json()
            if response.status_code == 429 and body.get("code") == QUEUE_FULL_CODE:
                return "queue_full"
            if response.status_code == 200 and body.get("code") == 0:
                items = body.get("data", [])
                if len(items) == BATCH_SIZE and all(item["search"]["code"] == 0 for item in items):
                    return "complete"
        except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError):
            pass
        return "failed"

    metrics_before = checked_metrics(metrics_url)
    with ThreadPoolExecutor(max_workers=concurrent_batches) as pool:
        outcomes = list(pool.map(submit, range(concurrent_batches)))
    print(
        "MULTISEARCH_ADMISSION "
        + json.dumps(
            {
                "concurrent_batches": concurrent_batches,
                "client_http_attempts": len(outcomes),
                "complete": outcomes.count("complete"),
                "queue_full": outcomes.count("queue_full"),
                "failed": outcomes.count("failed"),
                "server_counter_deltas": metric_deltas(metrics_before, checked_metrics(metrics_url)),
            }
        ),
        flush=True,
    )


def test_multisearch_load(endpoint, token, request):
    host = urlsplit(endpoint).hostname
    metrics_url = os.environ.get("MULTISEARCH_METRICS_URL", f"http://{host}:9091/metrics")
    searches = prepare_collections(endpoint, token, request)
    call = make_requests(endpoint, token)
    for search in searches:
        assert call("search", search, TRANSPORT_TIMEOUT_SECONDS)[0]
    assert call("multi_search", batch_payload(searches, 4), TRANSPORT_TIMEOUT_SECONDS)[0]
    samples = [
        phase(metrics_url, call, searches, mode, concurrency)
        for mode, concurrency in [("fanout", 4), ("batch", 4), ("batch", 8), ("fanout", 8)]
    ]
    # Leave headroom for ordinary Search so each mode can sustain the same
    # scheduled batch rate during the mixed comparison.
    offered_rate = min(sample["group"]["rounds"] / sample["seconds"] for sample in samples[:2]) * 0.2
    phase(metrics_url, call, searches, "resident", 4, mixed=True)
    for mode in ["fanout", "batch", "batch", "fanout"]:
        phase(metrics_url, call, searches, mode, 4, offered_rate=offered_rate, mixed=True)

    for concurrent_batches in (8, 32, 80):
        measure_batch_admission(endpoint, metrics_url, token, searches, concurrent_batches)
