"""On-demand REST comparison. Pass this file explicitly to the REST E2E runner.

HTTP response body sizes are measured at the client. Heap and RSS samples
cover the Milvus process serving Proxy, which includes all roles in standalone
mode. Result-buffer allocations have no separate measurement.

The fan-out deadline starts before client dispatch and covers HTTP I/O.
MultiSearch starts its execution budget on receipt and allows response delivery
within the separate transport timeout.

Mixed phases use the same paced schedule and check actual first-attempt
arrivals before comparing ordinary Search throughput and latency.
"""

import json
import math
import multiprocessing
import os
import random
import re
import resource
import threading
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from itertools import count
from urllib.parse import urlsplit

import pytest
import requests
from api.milvus import CollectionClient, VectorClient
from urllib3.util import Timeout
from utils.utils import gen_unique_str

BATCH_SIZE = 8
DRIVER_PROCESSES = 4
GROUP_WORKERS = 8
RESIDENT_WORKERS = 24
PHASE_SECONDS = 8
MIXED_GROUPED_SHARE = 0.30
ARRIVAL_TOLERANCE = 0.02
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
PROXY_DQL_QUEUE_FULL = re.compile(r"too many concurrent requests, queue is full\[limit=\d+\]")


@dataclass(frozen=True)
class RoundOutcome:
    offered_items: int
    successful_items: int
    response_body_bytes: int
    client_http_attempts: int
    client_retries: Counter = field(default_factory=Counter)


@dataclass
class Measurements:
    latency_ms: list = field(default_factory=list)
    failures: int = 0
    offered_items: int = 0
    successful_items: int = 0
    max_round_http_response_body_bytes: int = 0
    client_http_attempts: int = 0
    client_retries: Counter = field(default_factory=Counter)

    def add(self, started, outcome, items_per_round):
        self.latency_ms.append((time.perf_counter() - started) * 1000)
        self.failures += outcome.successful_items != items_per_round
        self.offered_items += outcome.offered_items
        self.successful_items += outcome.successful_items
        self.max_round_http_response_body_bytes = max(
            self.max_round_http_response_body_bytes, outcome.response_body_bytes
        )
        self.client_http_attempts += outcome.client_http_attempts
        self.client_retries.update(outcome.client_retries)

    def merge(self, other):
        self.latency_ms.extend(other.latency_ms)
        self.failures += other.failures
        self.offered_items += other.offered_items
        self.successful_items += other.successful_items
        self.max_round_http_response_body_bytes = max(
            self.max_round_http_response_body_bytes, other.max_round_http_response_body_bytes
        )
        self.client_http_attempts += other.client_http_attempts
        self.client_retries.update(other.client_retries)

    def summary(self, arrival_seconds, elapsed):
        ordered = sorted(self.latency_ms)

        def percentile(p):
            return round(ordered[math.ceil(len(ordered) * p) - 1], 2) if ordered else None

        return {
            "rounds": len(ordered),
            "successful_rounds": len(ordered) - self.failures,
            "failures": self.failures,
            "offered_items": self.offered_items,
            "offered_items_per_second": round(self.offered_items / arrival_seconds, 1),
            "successful_items": self.successful_items,
            "successful_items_per_second": round(self.successful_items / elapsed, 1),
            "p50_ms": percentile(0.50),
            "p95_ms": percentile(0.95),
            "p99_ms": percentile(0.99),
            "max_round_http_response_body_bytes": self.max_round_http_response_body_bytes,
            "client_http_attempts": self.client_http_attempts,
            "client_http_retries": self.client_retries.total(),
            "client_retries_by_reason": dict(self.client_retries),
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


def search_retry_reason(http_status, code, message):
    if code == QUEUE_FULL_CODE:
        # Proxy returns 429 before decode or its bare queue error when the
        # queue fills before enqueue. QueryNode refusals carry downstream
        # context even when they use the same Milvus code.
        if http_status == 429 or (http_status == 200 and PROXY_DQL_QUEUE_FULL.fullmatch(message)):
            return "queue_full"
    if code == RATE_LIMIT_CODE or (code == REST_RATE_LIMIT_CODE and "rate limit exceeded" in message):
        return "rate_limit"
    return None


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
                return 0, body_bytes, None
            code = body.get("code")
            if response.status_code != 200 or code != 0:
                reason = None
                if operation == "search":
                    reason = search_retry_reason(response.status_code, code, body.get("message") or "")
                return 0, body_bytes, reason
            results = body.get("data")
            if not isinstance(results, list):
                return 0, body_bytes, None
            if operation == "multi_search":
                if len(results) != len(payload["requests"]):
                    return 0, body_bytes, None
                return sum(item["search"].get("code") == 0 for item in results), body_bytes, None
            return 1, body_bytes, None
        except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError):
            return 0, body_bytes, None

    return call


def run_search(call, search, deadline, stop_at, max_attempts=MAX_ATTEMPTS):
    offered_items, successful_items, attempts, body_bytes = 0, 0, 0, 0
    retries = Counter()
    reason = None
    backoff = INITIAL_BACKOFF_SECONDS
    while attempts < max_attempts:
        now = time.perf_counter()
        remaining = deadline - now
        if remaining <= 0:
            break
        # Request-Timeout accepts whole seconds. The client uses the
        # remaining budget for I/O, including queue time in this round.
        if reason:
            retries[reason] += 1
        else:
            offered_items = int(now < stop_at)
        successful_items, received_bytes, reason = call(
            "search", search, remaining, {"Request-Timeout": str(math.ceil(remaining))}
        )
        attempts += 1
        body_bytes += received_bytes
        if successful_items or not reason or attempts == max_attempts or deadline - time.perf_counter() <= backoff:
            break
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
    return RoundOutcome(offered_items, successful_items, body_bytes, attempts, retries)


def batch_payload(searches, concurrency=None):
    payload = {"requests": [{"search": search} for search in searches], "timeoutMs": EXECUTION_BUDGET_MS}
    if concurrency is not None:
        payload["maxConcurrency"] = concurrency
    return payload


def run_group(call, searches, payload, fanout, started, stop_at):
    if fanout is None:
        successful_items, body_bytes, _ = call("multi_search", payload, TRANSPORT_TIMEOUT_SECONDS)
        return RoundOutcome(len(searches), successful_items, body_bytes, 1)

    # Both the queued and running Search calls use the group's deadline.
    deadline = started + EXECUTION_BUDGET_MS / 1000
    outcomes = list(fanout.map(lambda search: run_search(call, search, deadline, stop_at), searches))
    retries = Counter()
    for outcome in outcomes:
        retries.update(outcome.client_retries)
    return RoundOutcome(
        sum(outcome.offered_items for outcome in outcomes),
        sum(outcome.successful_items for outcome in outcomes),
        sum(outcome.response_body_bytes for outcome in outcomes),
        sum(outcome.client_http_attempts for outcome in outcomes),
        retries,
    )


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


def drive(worker_id, endpoint, token, searches, mode, concurrency, scheduled_groups, barrier, start_time):
    mixed = scheduled_groups is not None or mode == "resident"
    group_count = 0 if mode == "resident" else GROUP_WORKERS // DRIVER_PROCESSES
    resident_count = RESIDENT_WORKERS // DRIVER_PROCESSES if mixed else 0
    call = make_requests(endpoint, token)
    next_slot = count(worker_id, DRIVER_PROCESSES)

    def resident(i):
        measurements = Measurements()
        while True:
            started = time.perf_counter()
            if started >= stop_at:
                break
            deadline = started + EXECUTION_BUDGET_MS / 1000
            outcome = run_search(call, searches[i % BATCH_SIZE], deadline, stop_at, max_attempts=1)
            measurements.add(started, outcome, 1)
        return measurements

    def grouped():
        measurements = Measurements()
        payload = batch_payload(searches, concurrency)
        fanout = ThreadPoolExecutor(max_workers=concurrency) if mode == "fanout" else None
        try:
            while time.perf_counter() < stop_at:
                if scheduled_groups is not None:
                    slot = next(next_slot)
                    if slot >= scheduled_groups:
                        break
                    interval = PHASE_SECONDS / scheduled_groups
                    launch_at = start + slot * interval
                    wait = launch_at - time.perf_counter()
                    if wait > 0:
                        time.sleep(wait)
                started = time.perf_counter()
                if started >= stop_at:
                    break
                if scheduled_groups is not None and started >= launch_at + interval * DRIVER_PROCESSES:
                    # Skip missed slots instead of sending a catch-up burst.
                    continue
                outcome = run_group(call, searches, payload, fanout, started, stop_at)
                measurements.add(started, outcome, BATCH_SIZE)
        finally:
            if fanout is not None:
                fanout.shutdown()
        return measurements

    group_measurements, resident_measurements = Measurements(), Measurements()
    # All drivers wait for the same start timestamp, including after spawn.
    barrier.wait(timeout=30)
    barrier.wait(timeout=30)
    start = start_time.value
    stop_at = start + PHASE_SECONDS
    cpu_before = time.process_time()
    with ThreadPoolExecutor(max_workers=group_count + resident_count) as workers:
        group_jobs = [workers.submit(grouped) for _ in range(group_count)]
        resident_jobs = [workers.submit(resident, worker_id * resident_count + i) for i in range(resident_count)]
        for job in group_jobs:
            group_measurements.merge(job.result())
        for job in resident_jobs:
            resident_measurements.merge(job.result())
    return (
        group_measurements,
        resident_measurements,
        time.process_time() - cpu_before,
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    )


def phase(metrics_url, endpoint, token, searches, mode, concurrency, scheduled_groups=None):
    samples = []
    sampler_stop = threading.Event()

    def sample_process():
        while not sampler_stop.wait(METRICS_SAMPLE_SECONDS):
            sample = read_metrics(metrics_url)
            if sample.get("process_start_time_seconds") == metrics_before["process_start_time_seconds"]:
                samples.append(sample)

    # Independent Python interpreters keep one GIL from limiting the
    # fan-out driver before the Proxy reaches its CPU limit.
    processes = multiprocessing.get_context("spawn")
    with processes.Manager() as control, ProcessPoolExecutor(DRIVER_PROCESSES, mp_context=processes) as drivers:
        barrier = control.Barrier(DRIVER_PROCESSES + 1)
        start_time = control.Value("d", 0)
        jobs = [
            drivers.submit(
                drive, i, endpoint, token, searches, mode, concurrency, scheduled_groups, barrier, start_time
            )
            for i in range(DRIVER_PROCESSES)
        ]
        barrier.wait(timeout=30)
        metrics_before = checked_metrics(metrics_url)
        sampler = threading.Thread(target=sample_process)
        sampler.start()
        group_measurements, resident_measurements = Measurements(), Measurements()
        runner_cpu_seconds, peak_rss_kib = 0, 0
        try:
            # Process startup is outside the arrival window.
            start_time.value = start = time.perf_counter()
            barrier.wait(timeout=30)
            for job in jobs:
                group, resident, cpu_seconds, rss_kib = job.result()
                group_measurements.merge(group)
                resident_measurements.merge(resident)
                runner_cpu_seconds += cpu_seconds
                peak_rss_kib = max(peak_rss_kib, rss_kib)
            elapsed = time.perf_counter() - start
            metrics_after = checked_metrics(metrics_url)
        finally:
            sampler_stop.set()
            sampler.join()
    samples = [metrics_before, *samples, metrics_after]
    proxy_cpu_seconds = metrics_after["process_cpu_seconds_total"] - metrics_before["process_cpu_seconds_total"]
    mixed = scheduled_groups is not None or mode == "resident"
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
        "driver_processes": DRIVER_PROCESSES,
        "group_workers": 0 if mode == "resident" else GROUP_WORKERS,
        "resident_workers": RESIDENT_WORKERS if mixed else 0,
        "scheduled_groups": scheduled_groups,
        "arrival_seconds": PHASE_SECONDS,
        "seconds": round(elapsed, 2),
        "group": group_measurements.summary(PHASE_SECONDS, elapsed),
        "resident": resident_measurements.summary(PHASE_SECONDS, elapsed),
        "runner_cpu_seconds": round(runner_cpu_seconds, 3),
        "max_driver_process_peak_rss_to_date_kib": peak_rss_kib,
        "server_counter_deltas": metric_deltas(metrics_before, metrics_after),
        "milvus_process": {
            "scope": "process serving Proxy, including all Milvus roles in standalone",
            "samples": len(samples),
            "cpu_seconds": round(proxy_cpu_seconds, 3),
            "cpu_cores": round(proxy_cpu_seconds / elapsed, 2),
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


def mixed_phases_are_comparable(samples, scheduled_items):
    offered = [sample["group"]["offered_items"] for sample in samples]
    if min(offered) < scheduled_items * (1 - ARRIVAL_TOLERANCE):
        return False
    if max(offered) - min(offered) > scheduled_items * ARRIVAL_TOLERANCE:
        return False
    return all(
        sample["resident"]["offered_items"] > 0
        and sample["server_counter_deltas"]["search_http_requests"]
        == sample["resident"]["client_http_attempts"]
        + (sample["group"]["client_http_attempts"] if sample["mode"] == "fanout" else 0)
        and sample["server_counter_deltas"]["multisearch_http_requests"]
        == (sample["group"]["client_http_attempts"] if sample["mode"] == "batch" else 0)
        for sample in samples
    )


@pytest.mark.parametrize(
    ("status", "code", "message", "expected"),
    [
        (429, 4, "too many concurrent requests, queue is full[limit=8]", "queue_full"),
        (200, 4, "too many concurrent requests, queue is full[limit=8]", "queue_full"),
        (
            200,
            4,
            "failed to search: limit by queryNode.scheduler.maxUnsolvedQueueSize: "
            "too many concurrent requests, queue is full[limit=8]",
            None,
        ),
        (200, 8, "rate limit exceeded[rate=8]", "rate_limit"),
        (200, 1807, "request is rejected by grpc RateLimiter middleware: rate limit exceeded[rate=8]", "rate_limit"),
        (200, 1807, "quota exceeded[reason=deny]", None),
        (200, 9, "quota exceeded[reason=deny]", None),
    ],
)
def test_multisearch_load_retry_policy(status, code, message, expected):
    assert search_retry_reason(status, code, message) == expected


def test_multisearch_load(endpoint, token, request):
    host = urlsplit(endpoint).hostname
    metrics_url = os.environ.get("MULTISEARCH_METRICS_URL", f"http://{host}:9091/metrics")
    searches = prepare_collections(endpoint, token, request)
    call = make_requests(endpoint, token)
    for search in searches:
        assert call("search", search, TRANSPORT_TIMEOUT_SECONDS)[0]
    assert call("multi_search", batch_payload(searches, 4), TRANSPORT_TIMEOUT_SECONDS)[0] == len(searches)
    samples = [
        phase(metrics_url, endpoint, token, searches, mode, concurrency)
        for mode, concurrency in [("fanout", 4), ("batch", 4), ("batch", 8), ("fanout", 8)]
    ]
    # Set an identical schedule from the slower pure-load mode, leaving
    # headroom for resident Search. Validate the realized arrivals below.
    at_eight = (sample for sample in samples if sample["concurrency"] == 8)
    grouped_rate = min(sample["group"]["successful_items_per_second"] for sample in at_eight) * MIXED_GROUPED_SHARE
    scheduled_groups = int(grouped_rate * PHASE_SECONDS / BATCH_SIZE)
    assert scheduled_groups > 0, samples
    phase(metrics_url, endpoint, token, searches, "resident", 8)
    mixed = [
        phase(metrics_url, endpoint, token, searches, mode, 8, scheduled_groups=scheduled_groups)
        for mode in ["fanout", "batch", "batch", "fanout"]
    ]
    scheduled_items = scheduled_groups * BATCH_SIZE
    comparable = mixed_phases_are_comparable(mixed, scheduled_items)
    print(
        "MULTISEARCH_MIXED "
        + json.dumps(
            {
                "comparable": comparable,
                "scheduled_items_per_phase": scheduled_items,
                "actual_grouped_items_per_phase": [sample["group"]["offered_items"] for sample in mixed],
            }
        ),
        flush=True,
    )
    assert comparable, "Mixed phases missed the shared schedule or did not match the Proxy request counters"

    for concurrent_batches in (8, 32, 80):
        measure_batch_admission(endpoint, metrics_url, token, searches, concurrent_batches)
