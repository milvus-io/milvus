"""Local Docker provider for the T12 test-image overlay."""
from contextlib import contextmanager
import json
import os
import re
import subprocess
from time import monotonic, sleep
from types import SimpleNamespace

import pytest
from running_requests_topology import ProxyAuthorizationTopology

def proxy_exec_command():
    local = os.environ.get("RR_LOCAL_T04_PROXY_A")
    if local:
        return ["docker", "exec", local]
    topology = ProxyAuthorizationTopology(shared=True)
    pod = topology.proxy_pods()[int(os.environ["PROXY_A_ID"])]
    return [*topology.kube, "exec", pod, "-c", "proxy", "--"]

def proxy_logs():
    local = os.environ.get("RR_LOCAL_T04_PROXY_A")
    if local:
        command = ["docker", "logs", local]
    else:
        topology = ProxyAuthorizationTopology(shared=True)
        pod = topology.proxy_pods()[int(os.environ["PROXY_A_ID"])]
        command = [*topology.kube, "logs", pod, "-c", "proxy", "--tail=20000"]
    result = subprocess.run(command, capture_output=True, text=True, timeout=20, check=True)
    return result.stdout + "\n" + result.stderr



class Checkpoint:
    def __init__(self, collection, path):
        self.path = path
        self.command = proxy_exec_command()
        self.gate = f"/tmp/milvus-rr-t12-{path}-{collection}"

    def run(self, *args, check=True):
        return subprocess.run(
            [*self.command, *args], capture_output=True, text=True, timeout=10, check=check,
        )

    def event(self, phase, timeout):
        deadline = monotonic() + timeout
        while monotonic() < deadline:
            result = self.run("cat", f"{self.gate}.{phase}", check=False)
            if result.returncode == 0:
                return SimpleNamespace(**json.loads(result.stdout))
            sleep(0.1)
        raise AssertionError(f"T12 {self.path} did not publish {phase}")

    def wait(self, phase, timeout):
        event = self.event(phase, timeout)
        assert event.task_id in event.task_ids, "Checkpoint task is missing from the Proxy registry"
        return SimpleNamespace(
            path=self.path, phase=phase, request_id=event.request_id,
            proxy_task_ids=event.task_ids,
            started_task_ids=[event.task_id] if phase == "during" else [],
        )

    def advance(self):
        self.run("touch", self.gate + ".advance")

    def assert_retry_observed(self, request_id, timeout):
        first = self.event("retry", timeout)
        second = self.event("during", timeout)
        assert first.request_id == second.request_id == request_id
        assert first.task_id != second.task_id

    def wait_for_completion(self, request_id, timeout):
        try:
            entered = self.event("during", timeout)
            exited = self.event("exit", timeout)
            assert exited.request_id == entered.request_id == request_id
            assert exited.task_id == entered.task_id and exited.canceled
            unexpected = self.run("test", "-e", self.gate + ".unexpected", check=False)
            assert unexpected.returncode in (0, 1), unexpected.stderr
            self.completed = exited
            return SimpleNamespace(
                exited_task_ids=[exited.task_id],
                tasks_started_after_cancel=unexpected.returncode == 0,
            )
        finally:
            self.clean()

    def assert_requery_canceled(self, request_id, timeout):
        assert self.path == "requery" and self.completed.request_id == request_id
        assert self.completed.canceled

    def clean(self):
        self.run("rm", "-f", *(self.gate + "." + phase for phase in (
            "armed", "advance", "before", "during", "exit", "retry", "unexpected",
        )))


@contextmanager
def hold_internal_stage(collection, path):
    if path not in {"requery", "search_by_id", "retry"}:
        pytest.skip(f"T12 {path} has no checkpoint in this image")
    if not re.fullmatch(r"running_requests_l1_[0-9a-f]{32}", collection):
        raise ValueError("T12 requires an isolated workload collection")
    checkpoint = Checkpoint(collection, path)
    checkpoint.clean()
    checkpoint.run("touch", checkpoint.gate + ".armed")
    try:
        yield checkpoint
    except BaseException:
        checkpoint.clean()
        raise


def proxy_trace_spans(lines, trace_id):
    spans = {}
    for line in lines:
        _, marker, data = line.partition('{"Name":')
        if not marker or trace_id not in data:
            continue
        span = json.loads(marker + data)
        context = span["SpanContext"]
        if context["TraceID"] == trace_id:
            spans[context["SpanID"]] = span
    return list(spans.values())


def dispatched_view_calls(spans):
    methods = {
        "milvus.proto.view.ViewQueryService/SearchOnView": "search",
        "milvus.proto.view.ViewQueryService/SearchOnViewStream": "search",
        "milvus.proto.view.ViewQueryService/QueryOnView": "query",
        "milvus.proto.view.ViewQueryService/QueryOnViewStream": "query",
    }
    return {methods[span["Name"].lstrip("/")] for span in spans
            if span["SpanKind"] == 3 and span["Name"].lstrip("/") in methods}


def read_finished_search_trace(trace_id, expected_tasks):
    deadline = monotonic() + 20
    while monotonic() < deadline:
        spans = proxy_trace_spans(proxy_logs().splitlines(), trace_id)
        search_finished = any(
            span["SpanKind"] == 2 and span["Name"].endswith("MilvusService/Search") for span in spans
        )
        if search_finished and all(
            sum(span["Name"] == task for span in spans) >= count
            for task, count in expected_tasks.items()
        ):
            return spans
        sleep(0.25)
    raise AssertionError(f"T12 trace {trace_id}: missing completed Search or task spans")


def read_dispatches(trace_id):
    assert re.fullmatch(r"[0-9a-f]{32}", trace_id)
    marker = "/tmp/milvus-rr-t12-dispatch-" + trace_id + ".jsonl"
    output = subprocess.run(
        [*proxy_exec_command(), "sh", "-c",
         'if [ -f "$1" ]; then cat "$1"; else echo __NO_DISPATCH__; fi',
         "t12", marker], capture_output=True, text=True, timeout=15, check=True,
    ).stdout
    if output.strip() == "__NO_DISPATCH__":
        return []
    events = [json.loads(line) for line in output.splitlines() if line.strip()]
    assert events and all(event["trace_id"] == trace_id and event["request_id"]
                          and event["task_ids"] for event in events), events
    return events


def downstream_evidence(path, control_trace):
    names = {"SearchOnView": "search", "SearchOnViewStream": "search",
             "QueryOnView": "query", "QueryOnViewStream": "query"}
    control = {names[event["method"]] for event in read_dispatches(control_trace)}
    required = {"search"} if path == "retry" else {"search", "query"}
    assert required <= control, f"T12 {path} positive dispatch control {control_trace}: {control}"

    def check(trace_id):
        observed = {names[event["method"]] for event in read_dispatches(trace_id)}
        expected = {"search"} if path == "requery" else set()
        assert observed == expected, (path, trace_id, observed, expected)

    return check


@contextmanager
def hold_request_exit(collection, state):
    from running_requests_t04_provider import Checkpoint as RequestCheckpoint
    if state not in {"Running", "Queued"}:
        raise ValueError("Unsupported request state")
    if not re.fullmatch(r"running_requests_l1_[0-9a-f]{32}", collection):
        raise ValueError("Checkpoint requires an isolated collection")
    command = proxy_exec_command()
    gate = "/tmp/milvus-rr-t04-" + ("queued-" if state == "Queued" else "") + collection
    subprocess.run([*command, "touch", gate+".armed"], check=True, timeout=10)
    try:
        yield RequestCheckpoint(tuple(command), gate, state)
    finally:
        subprocess.run([*command, "rm", "-f", gate+".armed"], check=True, timeout=10)


def hold_search_group(**kwargs):
    pytest.skip("T10 QueryNode group checkpoint is not implemented in this temporary image")
