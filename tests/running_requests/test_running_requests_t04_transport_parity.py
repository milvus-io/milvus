"""T04 snapshot timing and expression boundaries at the management API."""

import os
import re
from concurrent.futures import ThreadPoolExecutor
from time import monotonic, sleep, time
from uuid import uuid4

import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import (
    TOPK,
    assert_canceled,
    search,
    wait_for_records,
)
from test_running_requests_native_sdk import canceled, listed, terminal
from test_running_requests_t04_snapshot import check_cancel_and_repeat_while_exit_is_held

pytestmark = pytest.mark.tags("L1")


def check_snapshot_timing(records, future, proxy, user, collection, nq, submitted):
    interval = None

    def timed_records():
        nonlocal interval
        before = monotonic()
        result = records()
        interval = (before, monotonic())
        return result

    first = wait_for_records(
        timed_records,
        {"Running"},
        lambda row: row["state"] if row["type"] == "Search" else None,
        [future],
    )["Running"]
    first_start, first_end = interval
    assert first["proxy_id"] == proxy and first["user"] == user, first
    assert first["collection_name"] == collection and first["db_name"] == "default", first
    assert first["nq"] == nq and first["topk"] == TOPK and first["cancellable"], first
    assert first["client_addr"] and re.fullmatch(r"[0-9a-f]{32}", first["trace_id"]), first
    assert first["task_ids"] and all(type(x) is int and x > 0 for x in first["task_ids"]), first
    assert first["elapsed_ms"] >= 0 and first["queued_ms"] >= 0, first
    age_observed = time() * 1000
    skew = float(os.getenv("MILVUS_CLOCK_SKEW_MAX_MS", "5000"))
    assert 0 <= skew <= 60000
    assert submitted - skew <= first["start_time_ms"] <= age_observed + skew, first
    assert abs(age_observed - first["start_time_ms"] - first["elapsed_ms"]) <= (
        skew + (first_end - first_start) * 1000 + 100
    ), first

    request_id = first["request_id"]
    sleep(0.2)
    later = wait_for_records(
        timed_records,
        {request_id},
        lambda row: row["request_id"] if row["elapsed_ms"] > first["elapsed_ms"] else None,
        [future],
    )[request_id]
    for field in ("start_time_ms", "client_addr", "expr", "trace_id", "queued_ms"):
        assert later[field] == first[field], (field, first, later)
    assert set(first["task_ids"]) <= set(later["task_ids"]), later
    later_start, later_end = interval
    delta = later["elapsed_ms"] - first["elapsed_ms"]
    assert (later_start - first_end) * 1000 - 10 <= delta <= (later_end - first_start) * 1000 + 10
    return request_id


def test_t04_native_list_snapshot_timing(native, deployment, workload):
    """Use an SDK-originated query to isolate the native List/Cancel contract."""
    uris, _, user, _ = deployment
    proxy = next(iter(uris))
    app, _, collection, vectors = workload

    def records():
        return listed(native, Collection=collection, User=user)

    reason = "T04-native-" + uuid4().hex
    with ThreadPoolExecutor(max_workers=1) as pool:
        submitted = time() * 1000
        future = pool.submit(search, app, collection, vectors)
        request_id = check_snapshot_timing(records, future, proxy, user, collection, len(vectors), submitted)
        result = canceled(native, [request_id], reason)
        assert [row["request_id"] for row in result["canceled"]] == [request_id]
        assert result["canceled"][0]["state"] == "Running", result
        assert_canceled(future, reason)
    assert request_id not in {row["request_id"] for row in records()}


def test_t04_rest_list_snapshot_timing(deployment, workload):
    """Measure REST List snapshots on a gRPC-originated Search with known nq."""
    uris, token, user, _ = deployment
    proxy_a, proxy_b = uris
    app, _, collection, vectors = workload
    fields = {
        "request_id": "requestId",
        "proxy_id": "proxyId",
        "type": "type",
        "state": "state",
        "user": "user",
        "collection_name": "collectionName",
        "db_name": "dbName",
        "nq": "nq",
        "topk": "topk",
        "cancellable": "cancellable",
        "client_addr": "clientAddr",
        "trace_id": "traceId",
        "task_ids": "taskIds",
        "elapsed_ms": "elapsedMs",
        "queued_ms": "queuedMs",
        "start_time_ms": "startTimeMs",
        "expr": "expr",
    }

    def records():
        result = post_rest(uris[proxy_b], token, set(uris), "list", {"collectionName": collection, "user": user})
        return [{field: row[key] for field, key in fields.items()} for row in result["requests"]]

    reason = "T04-rest-" + uuid4().hex
    with ThreadPoolExecutor(max_workers=1) as pool:
        submitted = time() * 1000
        future = pool.submit(search, app, collection, vectors)
        request_id = check_snapshot_timing(records, future, proxy_a, user, collection, len(vectors), submitted)
        result = post_rest(uris[proxy_b], token, set(uris), "cancel", {"requestIds": [request_id], "reason": reason})
        assert [row["requestId"] for row in result["canceled"]] == [request_id], result
        assert result["canceled"][0]["state"] == "Running", result
        assert result["notFound"] == result["undetermined"] == [], result
        assert_canceled(future, reason)
    assert request_id not in {row["request_id"] for row in records()}


@pytest.mark.parametrize("boundary", [255, 256, 257, "utf8-at-254", "utf8-at-255"])
def test_t04_native_expression_bytes(native, data, boundary):
    if isinstance(boundary, int):
        prefix = f"id >= -{int(uuid4().hex[:7], 16) + 1}"
        expression = prefix.ljust(boundary)
    else:
        offset = int(boundary.rsplit("-", 1)[1])
        prefix = f"id >= -{int(uuid4().hex[:7], 16) + 1} and "
        expression = prefix.ljust(offset - 1) + '"你" == "你"'
        assert expression.encode().index("你".encode()) == offset
    expected = expression.encode()[:256].decode(errors="ignore")
    call = uuid4().hex
    native.ok("start", "a", **dict(data, Kind="Search", Call=call, Expr=expression))
    deadline = monotonic() + 30
    while monotonic() < deadline:
        rows = [
            row
            for row in listed(native, Collection=data["Collection"], User=native.user)
            if row["expr"].startswith(prefix.rstrip()) and row["type"] == "Search"
        ]
        if rows:
            assert len(rows) == 1, rows
            row = rows[0]
            break
        assert not native.ok("poll", Call=call)["done"], "Search ended before List captured its expression"
        sleep(0.02)
    else:
        pytest.fail("Search expression was not observed")
    assert row["expr"] == expected and row["proxy_id"] == native.owner, row
    reason = "T04-expr-" + uuid4().hex
    canceled(native, [row["request_id"]], reason)
    terminal(native, call, reason)
    assert row["request_id"] not in {item["request_id"] for item in listed(native, Collection=data["Collection"])}


@pytest.mark.parametrize("state", ["Queued", "Running"])
def test_t04_transport_cancel_and_repeat_while_exit_is_held(
    state,
    checkpoint_provider,
    deployment,
    workload,
    t13_operator,
):
    check_cancel_and_repeat_while_exit_is_held(
        state,
        checkpoint_provider,
        deployment,
        workload,
        operator=t13_operator,
    )
