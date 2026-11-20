"""Native Java/Go parity: Python shares data, environment and behavioral assertions.

Run serially through run_current_requests_tests.py. SDK calls under test never use
PyMilvus. A short observation window is a failure, not a successful cancellation.
"""

import json
import queue
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import numpy as np
import pytest
from running_requests_l1_support import (
    TOPK,
    assert_search_baseline,
    check_record,
)

ROOT = Path(__file__).parent / "native_sdk"
pytestmark = pytest.mark.tags("L1")


class Native:
    def __init__(self, language, log):
        if language == "go":
            cmd = [str(ROOT / "go-worker")]
        else:
            config = json.loads((ROOT / "java-runtime.json").read_text())
            cmd = [config["java"], "-cp", config["classpath"], "rr.Worker"]
        self.language = language
        self.stderr = open(log, "w")
        self.process = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr, text=True, bufsize=1
        )
        self.replies = queue.Queue()

        def reader():
            for line in self.process.stdout:
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if isinstance(obj, dict) and "ok" in obj:
                    self.replies.put(obj)
            self.replies.put({"ok": False, "message": "native worker exited"})

        threading.Thread(target=reader, daemon=True).start()

    def call(self, op, client="b", **kwargs):
        payload = dict(Op=op, Client=client, **kwargs)
        self.process.stdin.write(json.dumps(payload) + "\n")
        self.process.stdin.flush()
        try:
            return self.replies.get(timeout=150)
        except queue.Empty:
            pytest.fail(f"{self.language} worker reply timeout ({op}); see native stderr")

    def ok(self, op, client="b", **kwargs):
        result = self.call(op, client, **kwargs)
        assert result["ok"], result
        return result

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            self.process.wait(timeout=10)
        self.stderr.close()


@pytest.fixture(params=["go", "java"])
def native(request, deployment, tmp_path):
    uris, token, user, _ = deployment
    worker = Native(request.param, tmp_path / "native.stderr")
    request.addfinalizer(worker.close)
    for name, uri in zip(("a", "b"), uris.values()):
        worker.ok("connect", name, URI=uri, Token=token, DB="default")
    worker.nodes = set(uris)
    worker.owner = next(iter(uris))
    worker.user = user
    return worker


@pytest.fixture
def data(workload, tmp_path):
    _, _, collection, vectors = workload
    path = tmp_path / "vectors.f32"
    np.asarray(vectors, dtype="<f4").tofile(path)
    return dict(Collection=collection, Vectors=str(path), NQ=len(vectors), Limit=TOPK)


def complete(native, result):
    assert result["ok"], result
    assert {n["node_id"] for n in result["nodes"]} == native.nodes, result
    assert all(n["answered"] and not n["unimplemented"] for n in result["nodes"]), result
    return result


def listed(native, **filters):
    return complete(native, native.call("list", **filters))["requests"]


def start(native, data, kind="Search", client="a"):
    call = uuid4().hex
    expr = f"id >= -{int(call[:7], 16)}"
    args = dict(data, Kind=kind, Call=call, Expr=expr)
    if kind == "Query":
        args["Limit"] = 16384
    native.ok("start", client, **args)
    return call, expr


def capture(native, data, call, expr, kind="Search"):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        matches = [
            r
            for r in listed(native, Collection=data["Collection"], User=native.user)
            if r["expr"] == expr and r["type"] == kind
        ]
        if matches:
            assert len(matches) == 1, matches
            row = matches[0]
            check_record(SimpleNamespace(**row), native.owner, native.user, data["Collection"])
            assert row["proxy_id"] == native.owner
            assert row["db_name"] == "default" and row["user"] == native.user
            assert row["client_addr"] and row["start_time_ms"] > 0
            assert row["elapsed_ms"] >= 0 and row["queued_ms"] >= 0
            assert row["state"] in ("Queued", "Running") and row["cancellable"]
            # Registration can precede task creation. Empty task IDs are valid
            # at that point, just as in the Python SDK snapshot contract.
            assert isinstance(row["task_ids"], (list, type(None)))
            assert all(type(x) is int for x in (row["task_ids"] or []))
            assert isinstance(row["trace_id"], str)
            if kind != "Query":
                expected_nq = data["NQ"] * (2 if kind == "HybridSearch" else 1)
                assert row["nq"] == expected_nq and row["topk"] == data["Limit"], row
            return row
        status = native.ok("poll", Call=call)
        diagnostic = {k: v for k, v in (status.get("result") or {}).items() if k != "hits"}
        assert not status["done"], f"Observation window missed: {kind}; result={diagnostic}"
        time.sleep(0.01)
    pytest.fail("Observation window: List did not capture target in 30 seconds")


def canceled(native, ids, reason, **kwargs):
    result = complete(native, native.call("cancel", IDs=ids, Reason=reason, **kwargs))
    assert {r["request_id"] for r in result["canceled"]} == set(ids), result
    assert not result["not_found"] and not result["undetermined"], result
    return result


def terminal(native, call, reason):
    result = native.call("await", Call=call)
    assert not result["ok"] and result["code"] == 3002, {k: v for k, v in result.items() if k != "hits"}
    assert reason in result["message"], result


def test_native_empty_ids_zero_rpc(native):
    before = native.ok("counts")["counts"].get("CancelRequests", 0)
    for ids in ([], None):
        result = native.call("cancel", IDs=ids)
        assert not result["ok"], result
        assert result["code"] == (1101 if native.language == "go" else "INVALID_PARAMS"), result
    assert native.ok("counts")["counts"].get("CancelRequests", 0) == before


@pytest.mark.parametrize("kind", ["Search", "Query", "HybridSearch"])
def test_native_cancel_terminal(native, data, kind, workload_baseline):
    before = native.ok("counts", "a")["counts"].get(kind, 0)
    call, expr = start(native, data, kind)
    row = capture(native, data, call, expr, kind)
    reason = "native-" + uuid4().hex + "\n第二行取消原因"
    response = complete(native, native.call("cancel", IDs=[row["request_id"]], Reason=reason))
    if [r["request_id"] for r in response["canceled"]] != [row["request_id"]]:
        outcome = native.call("await", Call=call)
        diagnostic = {k: v for k, v in outcome.items() if k != "hits"}
        pytest.fail(f"Cancel did not hit target: {response}; original terminal={diagnostic}")
    assert not response["not_found"] and not response["undetermined"], response
    snapshot = response["canceled"][0]
    for field in ("request_id", "proxy_id", "type", "db_name", "collection_name", "user", "expr", "start_time_ms"):
        assert snapshot[field] == row[field], (field, snapshot, row)
    terminal(native, call, reason)
    assert native.ok("counts", "a")["counts"].get(kind, 0) - before == 1
    assert row["request_id"] not in {r["request_id"] for r in listed(native, Collection=data["Collection"])}
    # Same independent baseline as Python; all Search result groups are compared.
    if kind == "Search":
        control, _ = start(native, data)
        result = native.ok("await", Call=control)
        assert_search_baseline(result["hits"], workload_baseline(), data["NQ"])


def test_native_filters(native, data):
    call, expr = start(native, data)
    row = capture(native, data, call, expr)
    rid = row["request_id"]
    for filters in (
        {"DB": "default"},
        {"Collection": data["Collection"]},
        {"User": native.user},
        {"MinMS": 0},
        dict(DB="default", Collection=data["Collection"], User=native.user, MinMS=0),
    ):
        assert rid in {r["request_id"] for r in listed(native, **filters)}, filters
    for filters in (
        {"DB": "absent_" + uuid4().hex},
        {"Collection": "absent_" + uuid4().hex},
        {"User": "absent_" + uuid4().hex},
        {"MinMS": 900000},
    ):
        assert rid not in {r["request_id"] for r in listed(native, **filters)}, filters
    # Ensure negative filters were checked while target still existed.
    assert rid in {r["request_id"] for r in listed(native, Collection=data["Collection"])}
    canceled(native, [rid], "filter-finish")
    terminal(native, call, "filter-finish")


def test_native_batch_and_repeat(native, data, workload_baseline):
    old, expr = start(native, data)
    old_id = capture(native, data, old, expr)["request_id"]
    assert_search_baseline(native.ok("await", Call=old)["hits"], workload_baseline(), data["NQ"])
    targets = [start(native, data) for _ in range(2)]
    ids = [capture(native, data, c, e)["request_id"] for c, e in targets]
    unknown = "999999-unknown-" + uuid4().hex
    result = complete(native, native.call("cancel", IDs=ids + [ids[0], old_id, unknown], Reason="batch"))
    assert {r["request_id"] for r in result["canceled"]} == set(ids), result
    # Same semantics as Python T05: repeated IDs can yield an additional
    # Canceling snapshot (or not_found after exit), not a second Running effect.
    for rid in ids:
        snapshots = [r for r in result["canceled"] if r["request_id"] == rid]
        assert sum(r["state"] == "Running" for r in snapshots) == 1, result
        assert all(r["state"] in ("Running", "Canceling") for r in snapshots), result
    not_found = result["not_found"] or []
    assert not_found.count(old_id) == not_found.count(unknown) == 1, result
    assert set(not_found) <= {old_id, unknown, ids[0]}, result
    assert not result["undetermined"], result
    for c, _ in targets:
        terminal(native, c, "batch")
    repeated = complete(native, native.call("cancel", IDs=ids))
    assert not repeated["canceled"] and not repeated["undetermined"]
    assert set(repeated["not_found"]) == set(ids)


def test_native_cancel_isolates_control(native, data, workload_baseline):
    """L0: one complete List snapshot must contain target AND control."""
    target, expr = start(native, data)
    control, control_expr = start(native, data)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        rows = {
            r["expr"]: r
            for r in listed(native, Collection=data["Collection"], User=native.user)
            if r["expr"] in (expr, control_expr)
        }
        if len(rows) == 2:
            break
        for call in (target, control):
            assert not native.ok("poll", Call=call)["done"], "Observation window missed concurrent target/control"
        time.sleep(0.01)
    else:
        pytest.fail("No complete snapshot contained both concurrent requests")
    assert rows[expr]["request_id"] != rows[control_expr]["request_id"]
    canceled(native, [rows[expr]["request_id"]], "isolation")
    terminal(native, target, "isolation")
    assert_search_baseline(native.ok("await", Call=control)["hits"], workload_baseline(), data["NQ"])
    remaining = {r["request_id"] for r in listed(native, Collection=data["Collection"])}
    assert not {r["request_id"] for r in rows.values()} & remaining


@pytest.mark.parametrize(
    "privileges", [(), ("ListRunningRequests",), ("CancelRequests",), ("ListRunningRequests", "CancelRequests")]
)
def test_native_cross_user_permissions(native, data, deployment, create_rbac_identity, privileges, workload_baseline):
    uris, _, owner, connect = deployment
    admin = connect(list(uris)[1])
    username, token, grant = create_rbac_identity(admin)
    for privilege in privileges:
        grant(privilege)
    native.ok("connect", "operator", URI=list(uris.values())[1], Token=token, DB="default")
    # Poll only known propagation lag; unexpected error codes fail immediately.
    deadline = time.monotonic() + 15
    while True:
        response = native.call("list", "operator", User=owner, Collection=data["Collection"])
        if "ListRunningRequests" not in privileges or response["ok"]:
            break
        assert response["code"] == 1401, response
        assert time.monotonic() < deadline, "List privilege propagation timeout"
        time.sleep(0.2)
    assert response["ok"] == ("ListRunningRequests" in privileges), response
    if not response["ok"]:
        assert response["code"] == 1401
    call, expr = start(native, data)
    rid = capture(native, data, call, expr)["request_id"]
    visible = native.call("list", "operator", User=owner, Collection=data["Collection"])
    if "ListRunningRequests" in privileges:
        complete(native, visible)
        assert rid in {r["request_id"] for r in visible["requests"]}
    else:
        assert not visible["ok"] and visible["code"] == 1401, visible
        own = complete(native, native.call("list", "operator", Collection=data["Collection"]))
        assert rid not in {r["request_id"] for r in own["requests"]}
    response = native.call("cancel", "operator", IDs=[rid], User=owner, Reason="cross-user")
    if "CancelRequests" in privileges:
        complete(native, response)
        assert [r["request_id"] for r in response["canceled"]] == [rid], response
        assert not response["not_found"] and not response["undetermined"]
        terminal(native, call, "cross-user")
    else:
        assert not response["ok"] and response["code"] == 1401, response
        assert rid in {r["request_id"] for r in listed(native, Collection=data["Collection"])}
        own = complete(native, native.call("cancel", "operator", IDs=[rid]))
        assert not own["canceled"] and not own["undetermined"]
        assert own["not_found"] == [rid]
        assert_search_baseline(native.ok("await", Call=call)["hits"], workload_baseline(), data["NQ"])
    assert rid not in {r["request_id"] for r in listed(native, Collection=data["Collection"])}


def test_native_ordinary_user_self(native, data, deployment, create_rbac_identity):
    uris, _, _, connect = deployment
    username, token, grant = create_rbac_identity(connect(list(uris)[1]))
    grant("Search", data["Collection"], "default")
    # Metadata privileges are needed by native clients' collection-schema caches.
    grant("DescribeCollection", data["Collection"], "default")
    for name, uri in zip(("self_a", "self_b"), uris.values()):
        native.ok("connect", name, URI=uri, Token=token, DB="default")
    call, expr = start(native, data, client="self_a")
    native.user = username
    row = capture(native, data, call, expr)
    visible = complete(native, native.call("list", "self_b", Collection=data["Collection"]))
    assert row["request_id"] in {r["request_id"] for r in visible["requests"]}
    assert all(r["user"] == username for r in visible["requests"])
    result = complete(native, native.call("cancel", "self_b", IDs=[row["request_id"]], Reason="self"))
    assert [r["request_id"] for r in result["canceled"]] == [row["request_id"]]
    assert not result["not_found"] and not result["undetermined"]
    terminal(native, call, "self")


@pytest.mark.parametrize(
    "group,can_cancel", [("ClusterReadOnly", False), ("ClusterReadWrite", True), ("ClusterAdmin", True)]
)
def test_native_builtin_groups(native, data, deployment, create_rbac_identity, group, can_cancel, workload_baseline):
    uris, _, owner, connect = deployment
    username, token, grant = create_rbac_identity(connect(list(uris)[1]))
    grant(group)
    native.ok("connect", "group", URI=list(uris.values())[1], Token=token, DB="default")
    call, expr = start(native, data)
    rid = capture(native, data, call, expr)["request_id"]
    response = complete(native, native.call("list", "group", User=owner, Collection=data["Collection"]))
    assert rid in {r["request_id"] for r in response["requests"]}
    response = native.call("cancel", "group", User=owner, IDs=[rid], Reason="builtin")
    if can_cancel:
        complete(native, response)
        assert [r["request_id"] for r in response["canceled"]] == [rid]
        assert not response["not_found"] and not response["undetermined"]
        terminal(native, call, "builtin")
    else:
        assert not response["ok"] and response["code"] == 1401, response
        assert_search_baseline(native.ok("await", Call=call)["hits"], workload_baseline(), data["NQ"])
