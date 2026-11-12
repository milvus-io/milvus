"""Closed-loop ABBA measurements retaining strict semantic gates and concurrency."""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
from pymilvus import FunctionChain, FunctionChainStage, MilvusClient
from pymilvus.function_chain import col, fn
from pymilvus.function_chain.chain import FunctionChainExpr
from workloads.pyudf.workload import check_stop_file


class SearchActivity:
    """Measure synchronous Search stub occupancy with peak interval witnesses."""

    def __init__(self):
        self.lock = threading.Lock()
        self.started = self.changed = time.monotonic()
        self.active = {}
        self.entered = self.completed = self.errors = self.peak = 0
        self.area = 0.0
        self.peak_time = None
        self.witnesses = {}

    def enter(self):
        with self.lock:
            now = time.monotonic()
            self.area += len(self.active) * (now - self.changed)
            self.changed = now
            sequence = self.entered
            self.entered += 1
            self.active[sequence] = now
            if len(self.active) > self.peak:
                self.peak = len(self.active)
                self.peak_time = now
                self.witnesses = {key: {"start": value} for key, value in self.active.items()}
            return sequence

    def leave(self, sequence, failed=False):
        with self.lock:
            now = time.monotonic()
            self.area += len(self.active) * (now - self.changed)
            self.changed = now
            del self.active[sequence]
            self.completed += 1
            self.errors += int(failed)
            if sequence in self.witnesses:
                self.witnesses[sequence]["end"] = now

    def snapshot(self):
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.started
            return {
                "boundary": "Synchronous SDK Search stub invocation; excludes oracle and journal",
                "entered": self.entered,
                "completed": self.completed,
                "errors": self.errors,
                "active": len(self.active),
                "peak": self.peak,
                "mean": (self.area + len(self.active) * (now - self.changed)) / elapsed,
                "elapsed": elapsed,
                "peak_monotonic": self.peak_time,
                "peak_interval_witnesses": self.witnesses.copy(),
            }


class SearchWireCounter:
    """Measure protobuf bytes at the SDK Search boundary, excluding gRPC framing."""

    def __init__(self, client, activity=None):
        self.original = client._get_connection()._stub.Search
        self.activity = activity
        self.lock = threading.Lock()
        self.requests = self.request_bytes = self.response_bytes = 0
        client._get_connection()._stub.Search = self

    def __call__(self, request, **kwargs):
        request_bytes = request.ByteSize()
        sequence = self.activity.enter() if self.activity else None
        failed = True
        try:
            response = self.original(request, **kwargs)
            failed = False
        finally:
            if self.activity:
                self.activity.leave(sequence, failed)
        with self.lock:
            self.requests += 1
            self.request_bytes += request_bytes
            self.response_bytes += response.ByteSize()
        return response

    def snapshot(self):
        with self.lock:
            return [self.requests, self.request_bytes, self.response_bytes]


def wire_delta(counters, before):
    after = [counter.snapshot() for counter in counters]
    totals = [sum(end[k] - start[k] for end, start in zip(after, before)) for k in range(3)]
    return {
        "boundary": "SDK Search protobuf; excludes gRPC framing and internal PyUDF RPC",
        "requests": totals[0],
        "request_bytes": totals[1],
        "response_bytes": totals[2],
    }


def worker_accounting(before, after, expected_starts):
    """Compare the same worker instances; never subtract unrelated snapshots."""

    def indexed(snapshot):
        return {(item["proxy"], item["resource"], item["pid"]): item["count"] for item in snapshot}

    starts, ends = indexed(before), indexed(after)
    result = {"worker_expected_starts": expected_starts, "worker_actual_starts": None, "internal_outcome": "FAIL"}
    if not starts or starts.keys() != ends.keys() or len(starts) != len(before) or len(ends) != len(after):
        return {**result, "internal_error": "Worker identities changed or snapshots are incomplete"}
    deltas = [ends[key] - value for key, value in starts.items()]
    if any(delta < 0 for delta in deltas):
        return {**result, "internal_error": "Worker counters decreased during the window"}
    actual_starts = sum(deltas)
    return {
        **result,
        "worker_actual_starts": actual_starts,
        "internal_outcome": "PASS" if actual_starts == expected_starts else "FAIL",
    }


def run_abba(
    checker,
    uri,
    token,
    window_seconds=120,
    worker_counts=(1, 8, 32),
    proxy_uris=None,
    worker_snapshot=None,
    raise_on_failure=True,
    stop_file=None,
):
    """Bound client concurrency; measure offered/completed load without rate pacing."""
    assert 0 < window_seconds <= 120
    assert all(0 < workers <= 32 for workers in worker_counts)
    stop_file = Path(stop_file) if stop_file is not None else checker.evidence / "STOP"
    check_stop_file(stop_file)
    identity = FunctionChain(FunctionChainStage.L2_RERANK).map(
        "$score",
        FunctionChainExpr(
            "py_udf",
            args=(col("$score"),),
            params={"resource_name": checker.resources[1], "udf_params": {"factor": 1, "bias": 0}},
        ),
    )
    builtin = (
        FunctionChain(FunctionChainStage.L2_RERANK)
        .map("$score", fn.num_combine(col("ts"), col("ts"), mode="sum"))
        .sort(col("$score"), desc=True, tie_break_col=col("$id"))
        .limit(20)
    )
    chains = {
        "ordinary": None,
        "python_identity": identity,
        "builtin_arithmetic": builtin,
        "python_arithmetic": checker.chain(checker.resources[0], 2, 0),
    }

    def once(client, mode, query):
        started = time.monotonic()
        hits = checker.search(client, checker.queries[query], chains[mode])
        latency = (time.monotonic() - started) * 1000
        if mode in ("ordinary", "python_identity"):
            checker.verify_control(hits, query)
            assert [hit["id"] for hit in hits] == checker.candidates[query], (mode, query)
        else:
            checker.verify_udf(hits, checker.candidates[query], 2, 0)
        return latency

    windows = []
    rejected_modes = {}
    addresses = proxy_uris or [uri]
    journal = (checker.evidence / "benchmark-requests.jsonl").open("w")
    try:
        for workers in worker_counts:
            clients = []
            try:
                for number in range(max(workers, len(addresses))):
                    clients.append(MilvusClient(addresses[number % len(addresses)], token=token, dedicated=True))
                wire = [SearchWireCounter(client) for client in clients]
                for mode in (
                    "ordinary",
                    "python_identity",
                    "python_identity",
                    "ordinary",
                    "builtin_arithmetic",
                    "python_arithmetic",
                    "python_arithmetic",
                    "builtin_arithmetic",
                ):
                    check_stop_file(stop_file)
                    if mode in rejected_modes:
                        result = {
                            "window": len(windows),
                            "mode": mode,
                            "workers": workers,
                            "outcome": "NOT_RUN",
                            "reason": "Semantic gate failed in earlier window",
                            "first_failure": rejected_modes[mode],
                        }
                        windows.append(result)
                        print(json.dumps(result), flush=True)
                        continue
                    internal_before = worker_snapshot(reset_peak=True) if worker_snapshot else None
                    wire_before = [counter.snapshot() for counter in wire]
                    started = time.monotonic()
                    wall = time.time()
                    results, failures = [], []
                    lock = threading.Lock()
                    stop = threading.Event()
                    barrier = threading.Barrier(workers)
                    active = peak = offered = 0
                    area = 0.0
                    changed = started
                    per_proxy = [0] * len(addresses)

                    def worker(number):
                        nonlocal active, peak, offered, area, changed
                        sequence = number
                        barrier.wait(timeout=30)
                        while time.monotonic() < started + window_seconds and not stop.is_set():
                            with lock:
                                now = time.monotonic()
                                area += active * (now - changed)
                                changed = now
                                active += 1
                                sequence = offered
                                offered += 1
                                peak = max(peak, active)
                            try:
                                check_stop_file(stop_file)
                                query = sequence % len(checker.queries)
                                client_index = sequence % len(clients)
                                latency = once(clients[client_index], mode, query)
                                with lock:
                                    results.append(latency)
                                    per_proxy[client_index % len(addresses)] += 1
                                    journal.write(
                                        json.dumps(
                                            {
                                                "window": len(windows),
                                                "query": query,
                                                "proxy": client_index % len(addresses),
                                                "latency_ms": latency,
                                            }
                                        )
                                        + "\n"
                                    )
                            except Exception as error:
                                with lock:
                                    failures.append(
                                        {
                                            "query": sequence % len(checker.queries),
                                            "type": type(error).__name__,
                                            "error": str(error),
                                        }
                                    )
                                stop.set()
                            finally:
                                with lock:
                                    now = time.monotonic()
                                    area += active * (now - changed)
                                    changed = now
                                    active -= 1
                            sequence += workers

                    with ThreadPoolExecutor(max_workers=workers) as pool:
                        try:
                            futures = [pool.submit(worker, number) for number in range(workers)]
                            for future in futures:
                                future.result()
                        except BaseException:
                            stop.set()
                            raise
                    elapsed = time.monotonic() - started
                    measured_wire = wire_delta(wire, wire_before)
                    stopped = stop_file.exists()
                    if stopped and not failures:
                        failures.append({"type": "RuntimeError", "error": "Resource guard requested stop"})
                    internal_after = worker_snapshot() if worker_snapshot and not stopped else None
                    result = {
                        "event": "benchmark_window",
                        "window": len(windows),
                        "mode": mode,
                        "workers": workers,
                        "peak_inflight": peak,
                        "time_weighted_inflight": area / elapsed,
                        "offered": offered,
                        "offered_qps": offered / elapsed,
                        "completed": len(results),
                        "per_proxy": per_proxy,
                        "elapsed": elapsed,
                        "started_unix": wall,
                        "completed_qps": len(results) / elapsed,
                        "latency_ms_p50_p95_p99": np.percentile(results, [50, 95, 99]).tolist() if results else [],
                        "failures": failures,
                        "outcome": "FAIL" if failures else "MEASURED",
                        "wire": measured_wire,
                        "worker_before": internal_before,
                        "worker_after": internal_after,
                    }
                    if internal_before is not None and internal_after is not None:
                        expected_starts = offered if mode.startswith("python_") else 0
                        result.update(worker_accounting(internal_before, internal_after, expected_starts))
                    windows.append(result)
                    if failures:
                        rejected_modes[mode] = result["window"]
                    journal.flush()
                    (checker.evidence / "benchmark-summary.json").write_text(
                        json.dumps({"windows": windows, "rejected_modes": rejected_modes}, indent=2)
                    )
                    print(json.dumps(result), flush=True)
                    # A resource stop aborts all modes, even when semantic
                    # failures are retained for independent measurements.
                    check_stop_file(stop_file)
            finally:
                for client in clients:
                    client.close()
    finally:
        journal.close()
    (checker.evidence / "benchmark-summary.json").write_text(
        json.dumps({"windows": windows, "rejected_modes": rejected_modes}, indent=2)
    )
    if raise_on_failure:
        assert not any(
            result.get("outcome") == "FAIL" or result.get("internal_outcome") == "FAIL" for result in windows
        ), "ABBA correctness or worker-accounting failure; see retained benchmark-summary.json"
    return windows


def run_batch_measurements(checker, uri, token, proxy_uris=None, stop_file=None):
    """Measure bounded NQ/candidate tuples; validate every decoded input column."""
    summaries = []
    stop_file = Path(stop_file) if stop_file is not None else checker.evidence / "STOP"
    check_stop_file(stop_file)
    addresses = proxy_uris or [uri]
    clients = []
    try:
        for number in range(8):
            clients.append(MilvusClient(addresses[number % len(addresses)], token=token, dedicated=True))
        wire = [SearchWireCounter(client) for client in clients]
        for candidate_count in (10, 100, 1000):
            # Retain one baseline per tuple. The two input-layout windows share it.
            queries = checker.queries[:100]
            candidates = []
            for query in queries:
                check_stop_file(stop_file)
                ids = checker.verify_fields(checker.search(checker.client, query, limit=candidate_count))
                assert len(ids) == candidate_count, f"Expected {candidate_count} baseline candidates, got {len(ids)}"
                candidates.append(ids)
            (checker.evidence / f"batch-candidates-{candidate_count}.json").write_text(json.dumps(candidates))
            for nq in (1, 8):
                for mode in ("repeated", "unique"):
                    names = ["ts"] * 4 if mode == "repeated" else ["ts", "label", "flag", "optional"]
                    chain = (
                        FunctionChain(FunctionChainStage.L2_RERANK)
                        .map(
                            "$score",
                            FunctionChainExpr(
                                "py_udf",
                                args=tuple(col(name) for name in names),
                                params={
                                    "resource_name": checker.resources[0],
                                    "udf_params": {"factor": 2, "bias": 0, "verify_mode": mode},
                                },
                            ),
                        )
                        .sort(col("$score"), desc=True, tie_break_col=col("$id"))
                        .limit(20)
                    )

                    def invoke(sequence):
                        check_stop_file(stop_file)
                        selected = [(sequence * nq + number) % len(queries) for number in range(nq)]
                        started = time.monotonic()
                        result = clients[sequence % len(clients)].search(
                            checker.collection,
                            data=[queries[index].tolist() for index in selected],
                            anns_field="vector",
                            limit=candidate_count,
                            search_params=checker.search_params(candidate_count),
                            output_fields=["ts", "label", "flag", "optional"],
                            consistency_level="Strong",
                            function_chains=chain,
                            timeout=30,
                        )
                        latency = (time.monotonic() - started) * 1000
                        assert len(result) == nq
                        for hits, index in zip(result, selected):
                            checker.verify_udf(hits, candidates[index], 2, 0)
                        return latency

                    wire_before = [counter.snapshot() for counter in wire]
                    started = time.monotonic()
                    wall = time.time()
                    results, pending = [], []
                    with ThreadPoolExecutor(max_workers=8) as pool:
                        sequence = 0
                        while time.monotonic() - started < 30:
                            check_stop_file(stop_file)
                            due = started + sequence / 5
                            if due >= started + 30:
                                break
                            time.sleep(max(0, due - time.monotonic()))
                            check_stop_file(stop_file)
                            if len(pending) >= 8:
                                results.append(pending.pop(0).result(timeout=35))
                            pending.append(pool.submit(invoke, sequence))
                            sequence += 1
                            while started + sequence / 5 < time.monotonic():
                                sequence += 1
                        results.extend(future.result(timeout=35) for future in pending)
                    check_stop_file(stop_file)
                    summary = {
                        "event": "batch_window",
                        "nq": nq,
                        "candidates": candidate_count,
                        "input_layout": mode,
                        "started_unix": wall,
                        "elapsed": time.monotonic() - started,
                        "requests": len(results),
                        "query_results": len(results) * nq,
                        "offered_qps": 5,
                        "latency_ms_p50_p95_p99": np.percentile(results, [50, 95, 99]).tolist(),
                        "outcome": "PASS",
                        "wire": wire_delta(wire, wire_before),
                        "component_timings": "Not captured; no inferred encode/execute/decode spans",
                    }
                    summaries.append(summary)
                    (checker.evidence / "batch-summary.json").write_text(json.dumps(summaries, indent=2))
                    print(json.dumps(summary), flush=True)
    except Exception as error:
        summaries.append({"event": "batch_abort", "outcome": "FAIL", "error": str(error)})
        (checker.evidence / "batch-summary.json").write_text(json.dumps(summaries, indent=2))
        raise
    finally:
        for client in clients:
            client.close()
    return summaries
