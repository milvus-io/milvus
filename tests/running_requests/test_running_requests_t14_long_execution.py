"""T14 cancellation with correlated server evidence for wait and compute stages."""

import json
import math
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    assert_canceled,
    assert_search_baseline,
    assert_unregistered,
    cancel_exact,
    check_record,
    list_complete,
    search,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


@pytest.fixture
def stage_probe():
    import importlib

    provider = os.getenv("MILVUS_T14_EVIDENCE_PROVIDER")
    return importlib.import_module(provider) if provider else None


def segment_log_source(line):
    return re.split(r"\[\d{4}/\d{2}/\d{2} ", line, maxsplit=1)[0].strip()


def native_canceled_segments(lines, trace_id):
    """Attribute native cancellation only when one segment is active on that node."""
    active = {}
    native = set()
    failures = set()
    for line in lines:
        source = segment_log_source(line)
        if (
            "[ERROR] [CGO] [exec/Driver.cpp:" in line
            and f"[{trace_id}]" in line
            and "Operator::GetOutput failed" in line
            and "cancelled by the caller" in line
        ):
            candidates = active.get(source, set())
            if len(candidates) == 1:
                native.add(next(iter(candidates)))
            continue
        if f"[traceID={trace_id}]" not in line or "segments/segment.go:" not in line:
            continue
        segment = re.search(r"\[segmentID=(\d+)\]", line)
        span = re.search(r"\[spanID=([0-9a-f]+)\]", line)
        if not segment or not span:
            continue
        key = (source, segment[1], span[1])
        if '["search segment..."]' in line:
            active.setdefault(source, set()).add(key)
        elif any(
            f'["{event}"]' in line
            for event in (
                "search segment done",
                "search filter only segment done",
                "Search failed",
            )
        ):
            if '["Search failed"]' in line and key in native:
                failures.add(key)
            native.discard(key)
            active.get(source, set()).discard(key)
    return failures


def test_t14_compute_cancellation_log_evidence(deployment, workload):
    check_compute_cancellation_log_evidence(deployment, workload)


def check_compute_cancellation_log_evidence(deployment, workload, *, operator=None):
    """Smoke only: correlate segment start and cancellation-related failure.

    Does not establish task-level physical exit or a same-node cancel latency.
    """
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    app, _, collection, vectors = workload
    operator = connect(proxy_b) if operator is None else operator
    kube = [
        "kubectl",
        "--kubeconfig",
        os.getenv(
            "MILVUS_T14_KUBECONFIG",
            os.getenv("MILVUS_L1_KUBECONFIG", str(Path.home() / ".kube/config")),
        ),
        "-n",
        os.getenv("MILVUS_T14_NAMESPACE", os.getenv("MILVUS_L1_NAMESPACE", "requests-l0-1112")),
    ]
    release = os.getenv("MILVUS_L1_RELEASE", "requests-l0")
    since = (datetime.now(UTC) - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def log_pods(container):
        selector = os.getenv(f"MILVUS_T14_{container.upper()}_SELECTOR")
        command = kube + ["get", "pods", "-o", "json"]
        if selector:
            command += ["-l", selector]
        result = subprocess.check_output(
            command,
            text=True,
            timeout=30,
        )
        pods = [
            pod["metadata"]["name"]
            for pod in json.loads(result)["items"]
            if not pod["metadata"].get("deletionTimestamp")
            and (
                selector
                or pod["metadata"].get("labels", {}).get("app.kubernetes.io/instance") == release
                or pod["metadata"]["name"].startswith(f"{release}-milvus-{container}-")
            )
            and any(
                item["type"] == "Ready" and item["status"] == "True" for item in pod["status"].get("conditions", [])
            )
            and any(item["name"] == container for item in pod["spec"]["containers"])
        ]
        assert pods, f"No Ready {container} log sources for release {release!r}, selector {selector!r}"
        return pods

    local_sources = {
        "proxy": [os.getenv("RR_LOCAL_T04_PROXY_A"), os.getenv("RR_LOCAL_PROXY_B")],
        "querynode": [os.getenv("RR_LOCAL_QUERYNODE")],
    }
    local = any(container for group in local_sources.values() for container in group)
    if local:
        assert all(container for group in local_sources.values() for container in group)
    sources = (
        local_sources
        if local
        else {
            "proxy": log_pods("proxy"),
            "querynode": log_pods("querynode"),
        }
    )

    def logs(container):
        lines = []
        for pod in sources[container]:
            if local:
                output = subprocess.check_output(
                    ["docker", "logs", "--since", since, pod],
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=12,
                )
                lines.extend(f"[{pod}] {line}" for line in output.splitlines())
                continue
            output = subprocess.check_output(
                kube + ["logs", pod, "-c", container, "--prefix=true", "--since-time=" + since],
                text=True,
                timeout=12,
            )
            lines.extend(output.splitlines())
        return lines

    def timestamp(line):
        match = re.search(r"\[(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d\.\d+ [+-]\d{2}:\d{2})\]", line)
        assert match, line
        return datetime.strptime(match[1], "%Y/%m/%d %H:%M:%S.%f %z")

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    # Check log access before starting a request.
    logs("proxy")
    logs("querynode")
    reason = f"T14-compute-log-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, app, collection, vectors)
        target = wait_for_records(records, {"Running"}, lambda row: row.state, [future], exact=True)["Running"]
        check_record(target, proxy_a, user, collection)
        assert target.trace_id
        trace = f"[traceID={target.trace_id}]"
        # Registration precedes QueryNode execution; startup/queue delay is
        # not evidence that this request lacks a cancellable compute stage.
        deadline = monotonic() + QUERY_TIMEOUT
        while monotonic() < deadline and not future.done():
            markers = [line for line in logs("querynode") if trace in line]
            active = set()
            for line in markers:
                identity = re.search(r"\[segmentID=(\d+)\].*\[spanID=([0-9a-f]+)\]", line)
                if identity is None:
                    continue
                key = (segment_log_source(line), *identity.groups())
                if '["search segment..."]' in line:
                    active.add(key)
                elif any(
                    f'["{event}"]' in line
                    for event in (
                        "search segment done",
                        "search filter only segment done",
                        "Search failed",
                    )
                ):
                    active.discard(key)
            if active and not future.done():
                break
            sleep(0.05)
        else:
            pytest.fail(f"OBSERVATION_WINDOW: segment start not observed for {target.request_id}")
        cancel_exact(operator, set(uris), [target.request_id], reason)
        assert_canceled(future, reason)
    assert_unregistered(records, {target.request_id})
    audits = [
        line
        for line in logs("proxy")
        if trace in line
        and reason in line
        and '["request canceled by operator"]' in line
        and f"[requestID={target.request_id}]" in line
        # The forwarding Proxy also audits this cancellation with servedByProxyID.
        and "[servedByProxyID=" not in line
    ]
    assert len(audits) == 1, f"Expected one Proxy cancellation audit for {target.request_id}, got {len(audits)}"
    audit = audits[0]
    canceled_at = timestamp(audit)
    deadline = monotonic() + 15
    unqualified_failures = []
    while monotonic() < deadline:
        starts = {}
        querynode_logs = logs("querynode")
        native_failures = native_canceled_segments(querynode_logs, target.trace_id)
        spans = []
        for line in querynode_logs:
            _, marker, payload = line.partition('{"Name":')
            if marker and target.trace_id in payload:
                span_record = json.loads(marker + payload)
                if span_record["SpanContext"]["TraceID"] == target.trace_id:
                    spans.append(span_record)
        canceled_views = {
            span["SpanContext"]["SpanID"]
            for span in spans
            if span["Name"].endswith("/SearchOnView")
            and span["Status"]["Code"] == "Error"
            and re.search(r"cancel(?:l)?ed", span["Status"]["Description"], re.I)
        }
        canceled_children = {
            span["SpanContext"]["SpanID"]
            for span in spans
            if span["Name"] == "schedule" and span["Parent"]["SpanID"] in canceled_views
        }
        for line in querynode_logs:
            if trace not in line or "segments/segment.go:" not in line:
                continue
            segment = re.search(r"\[segmentID=(\d+)\]", line)
            span = re.search(r"\[spanID=([0-9a-f]+)\]", line)
            if not segment or not span:
                continue
            key = (segment_log_source(line), segment[1], span[1])
            if '["search segment..."]' in line:
                starts[key] = timestamp(line)
            cancellation_error = (
                re.search(
                    r"\[error(?:Verbose)?=.*(?:context canceled|context cancelled|request canceled)",
                    line,
                    re.IGNORECASE,
                )
                or span[1] in canceled_children
                or key in native_failures
            )
            if '["Search failed"]' in line and key in starts and not cancellation_error:
                unqualified_failures.append(line)
            if '["Search failed"]' in line and cancellation_error and key in starts and starts[key] < timestamp(line):
                # Proxy and QueryNode clocks may differ; this is correlation,
                # not a proof of cross-node event ordering.
                print(
                    f"request_id={target.request_id} trace_id={target.trace_id} "
                    f"segment_id={segment[1]} started={starts[key]} "
                    f"proxy_cancel={canceled_at} segment_error={timestamp(line)} "
                    "evidence=log_smoke strict_background_exit=not_verified"
                )
                return
        sleep(0.25)
    if unqualified_failures:
        pytest.fail(
            f"EVIDENCE_GAP: request {target.request_id} has matching segment failure logs, "
            "but they do not identify cancellation as the cause; backend exit is not verified"
        )
    pytest.fail(f"OBSERVATION_WINDOW: no matching segment start and error around Proxy Cancel for {target.request_id}")


@pytest.mark.parametrize("stage", ["waiting", "computing"])
def test_t14_cancel_long_search(stage, deployment, stage_probe, request):
    check_cancel_long_search(stage, deployment, stage_probe, request)


def check_cancel_long_search(stage, deployment, stage_probe, request, *, operator=None):
    if stage_probe is None:
        pytest.skip("Strict wait/compute acceptance requires server events for cancel arrival and exit")
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    app, control, collection, vectors = request.getfixturevalue("workload")
    baseline = request.getfixturevalue("workload_baseline")()
    operator = connect(proxy_b) if operator is None else operator
    target_vectors = vectors[:-1]
    if stage == "computing":
        configured = os.getenv("MILVUS_T14_COMPUTE_EXIT_MAX_MS")
        if not configured:
            pytest.skip("Set the reviewed MILVUS_T14_COMPUTE_EXIT_MAX_MS threshold")
        max_exit_ms = float(configured)
        assert math.isfinite(max_exit_ms) and max_exit_ms > 0

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    reason = f"T14-{stage}-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=2) as pool:
        with stage_probe.observe_search(collection=collection, stage=stage, nq=len(target_vectors)) as probe:
            target_call = pool.submit(search, app, collection, target_vectors)
            control_call = pool.submit(search, control, collection, vectors)
            rows = wait_for_records(
                records,
                {len(vectors), len(target_vectors)},
                lambda row: row.nq,
                (target_call, control_call),
                exact=True,
            )
            target = rows[len(target_vectors)]
            for row in rows.values():
                check_record(row, proxy_a, user, collection)
                assert row.type == "Search"
            assert target.trace_id
            started = probe.wait_started(target.request_id, target.trace_id, timeout=10)
            assert started.trace_id == target.trace_id and started.stage == stage
            assert started.task_id
            assert not target_call.done()
            cancel_exact(operator, set(uris), [target.request_id], reason)
            assert_canceled(target_call, reason)
            stopped = probe.wait_canceled_exit(started.task_id, timeout=30)
            assert stopped.trace_id == target.trace_id and stopped.task_id == started.task_id
            assert stopped.canceled
            assert started.started_at < stopped.cancel_arrived_at <= stopped.exited_at
            assert started.node_id and started.node_id == stopped.node_id
            assert started.clock_id and started.clock_id == stopped.clock_id
            if stage == "waiting":
                assert started.business_wait and stopped.wait_ended_by_cancel
                assert stopped.manual_release_at is None or stopped.manual_release_at >= stopped.exited_at
            else:
                assert started.exclusive_execution and stopped.exclusive_execution
                assert not stopped.completed_normally and not stopped.started_followup
                exit_ms = (stopped.exited_at - stopped.cancel_arrived_at) * 1000
                assert exit_ms <= max_exit_ms, (exit_ms, max_exit_ms)
        assert_search_baseline(control_call.result(timeout=QUERY_TIMEOUT), baseline, len(vectors))
    assert_unregistered(records, {row.request_id for row in rows.values()})
