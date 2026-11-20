"""T13 indexed-search cancellation through Java, Go and REST management APIs."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    TOPK,
    assert_canceled,
    assert_unregistered,
    check_record,
    list_complete,
    missing_active_requests,
    wait_for_records,
)
from test_running_requests_native_sdk import canceled, listed
from test_running_requests_t13_search_paths import assert_ranked_control

pytestmark = pytest.mark.tags("L1")
PATHS = ["binary", "sparse", "bm25", "ivf"]


def check_indexed_cancel(deployment, indexed_search, cancel):
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    path, collection, target, control, vectors, invoke = indexed_search
    observer = connect(proxy_b)
    nq = len(vectors)
    baseline = invoke(control, vectors)
    barrier = Barrier(2, timeout=5)

    def records():
        return list_complete(observer, set(uris), collection_name=collection, user=user)

    def search(client, data):
        barrier.wait()
        return invoke(client, data)

    reason = f"T13-transport-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=2) as pool:
        target_rpc = pool.submit(search, target, vectors[:-1])
        control_rpc = pool.submit(search, control, vectors)
        snapshot = wait_for_records(
            records,
            {nq - 1, nq},
            lambda row: row.nq,
            (target_rpc, control_rpc),
            exact=True,
        )
        for row in snapshot.values():
            check_record(row, proxy_a, user, collection)
            assert row.type == "Search" and row.topk == TOPK
        target_id, control_id = (snapshot[size].request_id for size in (nq - 1, nq))
        assert target_id != control_id
        cancel(collection, user, target_id, reason)
        missing = missing_active_requests(records, {control_id})
        assert_canceled(target_rpc, reason)
        actual = control_rpc.result(timeout=QUERY_TIMEOUT)

    assert_unregistered(records, {target_id, control_id})
    assert_ranked_control(actual, baseline, vectors, invoke, control, descending=path in {"bm25", "sparse"})
    assert not missing, f"OBSERVATION_WINDOW: control overlap unverified: {missing}"


@pytest.mark.parametrize("indexed_search", PATHS, indirect=True)
def test_t13_native_cancel_indexed_search(deployment, indexed_search, native):
    def cancel(collection, user, request_id, reason):
        matches = [row for row in listed(native, Collection=collection, User=user) if row["request_id"] == request_id]
        assert len(matches) == 1 and matches[0]["proxy_id"] == native.owner, matches
        result = canceled(native, [request_id], reason)
        assert [row["request_id"] for row in result["canceled"]] == [request_id], result

    check_indexed_cancel(deployment, indexed_search, cancel)


@pytest.mark.parametrize("indexed_search", PATHS, indirect=True)
def test_t13_rest_cancel_indexed_search(deployment, indexed_search):
    uris, token, _, _ = deployment
    proxy_a, proxy_b = uris

    def cancel(collection, user, request_id, reason):
        listed = post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "list",
            {
                "collectionName": collection,
                "user": user,
            },
        )
        matches = [row for row in listed["requests"] if row["requestId"] == request_id]
        assert len(matches) == 1 and matches[0]["proxyId"] == proxy_a, matches
        result = post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "cancel",
            {
                "requestIds": [request_id],
                "reason": reason,
            },
        )
        assert [row["requestId"] for row in result["canceled"]] == [request_id], result
        assert result["notFound"] == result["undetermined"] == [], result

    check_indexed_cancel(deployment, indexed_search, cancel)
