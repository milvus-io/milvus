"""T11. Check Java, Go and REST behavior with a newer Proxy A and an older Proxy B."""

import os
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import pytest
from running_requests_l1_rest_support import post_rest_response
from running_requests_l1_support import (
    assert_canceled,
    assert_search_baseline,
    search,
    wait_for_records,
)

pytestmark = [
    pytest.mark.tags("L1"),
    pytest.mark.skipif(
        os.getenv("MILVUS_T11_MIXED_TRANSPORT") != "1",
        reason="Requires an existing mixed-version deployment with newer Proxy A and older Proxy B",
    ),
]


def native_management(worker, action, collection, user, ids=None, reason=""):
    # Proxy A remains the healthy entry; the fixture's default worker is B.
    if action == "list":
        body = worker.call("list", "a", Collection=collection, User=user)
        records = body.get("requests", [])
    else:
        body = worker.call("cancel", "a", IDs=ids, Reason=reason, User=user)
        records = body.get("canceled", [])
    assert body["ok"], body
    return SimpleNamespace(
        node_results=[SimpleNamespace(**node) for node in body["nodes"]],
        ids=[row["request_id"] for row in records],
        proxy_ids=[row["proxy_id"] for row in records],
        not_found=body.get("not_found") or [],
        undetermined=body.get("undetermined") or [],
    )


def rest_management(uri, token, action, collection, user, ids=None, reason=""):
    payload = {"user": user}
    if action == "list":
        payload["collectionName"] = collection
    else:
        payload.update(requestIds=ids, reason=reason)
    body = post_rest_response(uri, token, action, payload)
    assert body["code"] == 0, body
    data = body["data"]
    rows = data["requests" if action == "list" else "canceled"]
    assert isinstance(rows, list), body
    if action == "cancel":
        assert isinstance(data["notFound"], list) and isinstance(data["undetermined"], list), body
    return SimpleNamespace(
        node_results=[
            SimpleNamespace(
                node_id=node["nodeId"],
                answered=node["code"] == 0,
                unimplemented=node["unimplemented"],
                error=node["message"],
            )
            for node in data["nodeResults"]
        ],
        ids=[row["requestId"] for row in rows],
        proxy_ids=[row["proxyId"] for row in rows],
        not_found=data.get("notFound", []),
        undetermined=data.get("undetermined", []),
    )


def check_partial_cancel(deployment, workload, workload_baseline, manage):
    uris, _, user, connect = deployment
    a, b = uris
    _, _, collection, vectors = workload
    target, old_proxy = connect(a), connect(b)
    baseline = workload_baseline()
    reason = "T11-transport-" + uuid4().hex
    unknown_id = f"999-{uuid4().hex[:8]}-9"

    def expect_mixed_nodes(response):
        nodes = response.node_results
        assert len(nodes) == 2, nodes
        assert {node.node_id: (node.answered, node.unimplemented) for node in nodes} == {
            a: (True, False),
            b: (False, True),
        }, nodes

    def visible_ids():
        response = manage("list", collection, user)
        expect_mixed_nodes(response)
        assert all(proxy == a for proxy in response.proxy_ids), response
        return response.ids

    assert visible_ids() == []
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, target, collection, vectors)
        ids = wait_for_records(visible_ids, {a}, lambda _: a, [future])
        request_id = ids[a]
        canceled = manage("cancel", collection, user, [request_id, unknown_id], reason)
        expect_mixed_nodes(canceled)
        assert canceled.ids == [request_id] and canceled.proxy_ids == [a], canceled
        assert canceled.not_found == [unknown_id] and not canceled.undetermined, canceled
        assert_canceled(future, reason)

    assert request_id not in visible_ids()
    assert_search_baseline(search(old_proxy, collection, vectors), baseline, len(vectors))


def test_t11_native_partial_cancel(deployment, workload, workload_baseline, native):
    def manage(action, collection, user, ids=None, reason=""):
        return native_management(native, action, collection, user, ids, reason)

    check_partial_cancel(deployment, workload, workload_baseline, manage)


def test_t11_rest_partial_cancel(deployment, workload, workload_baseline):
    uris, token, _, _ = deployment
    uri = next(iter(uris.values()))

    def manage(action, collection, user, ids=None, reason=""):
        return rest_management(uri, token, action, collection, user, ids, reason)

    check_partial_cancel(deployment, workload, workload_baseline, manage)
