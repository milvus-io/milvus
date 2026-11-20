"""TC-L1 functional scenario T06."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import (
    assert_canceled,
    search,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


def test_t06_rest_round_trip_live_id(deployment, workload):
    """REST Cancel distinguishes nonmatching strings from the exact live ID."""
    uris, token, user, _ = deployment
    proxy_a, proxy_b = uris
    owner, _, collection, vectors = workload

    def post(action, payload):
        return post_rest(uris[proxy_b], token, set(uris), action, payload, headers={"Accept-Type-Allow-Int64": "true"})

    def records():
        data = post("list", {"collectionName": collection, "user": user})
        rows = data["requests"]
        assert rows is None or isinstance(rows, list), data
        return rows or []

    def cancel(ids, reason):
        data = post("cancel", {"requestIds": ids, "reason": reason, "user": user})
        for key in ("canceled", "notFound", "undetermined"):
            assert data[key] is None or isinstance(data[key], list), data
        return data

    reason = f"T06-{uuid4().hex}\n第二行取消原因"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, owner, collection, vectors)
        row = wait_for_records(
            records,
            {"Search"},
            lambda item: item["type"],
            [future],
        )["Search"]
        request_id = row["requestId"]
        assert isinstance(request_id, str) and request_id
        assert row["proxyId"] == proxy_a and row["collectionName"] == collection
        assert row["user"] == user and row["nq"] == len(vectors)
        assert type(row["startTimeMs"]) is int and row["startTimeMs"] > 0
        for field in ("elapsedMs", "queuedMs"):
            if field in row:
                assert type(row[field]) is int and row[field] >= 0, row
        if "taskIds" in row:
            assert isinstance(row["taskIds"], list) and row["taskIds"]
            assert all(type(task_id) is int and task_id > 0 for task_id in row["taskIds"])
        invalid_matches = [
            f"unknown_{uuid4().hex}",
            str(uuid4().int),
            request_id.upper(),
            f"{request_id}-typo",
        ]
        assert request_id not in invalid_matches
        assert not set(invalid_matches).intersection(item["requestId"] for item in records())
        miss = cancel(invalid_matches, reason)
        assert sorted(miss["notFound"]) == sorted(invalid_matches)
        assert not miss["canceled"] and not miss["undetermined"]
        assert any(item["requestId"] == request_id for item in records())
        hit = cancel([request_id], reason)
        assert [item["requestId"] for item in hit["canceled"]] == [request_id]
        assert not hit["notFound"] and not hit["undetermined"]
        repeated = cancel([request_id], reason)
        if repeated["canceled"]:
            assert [(item["requestId"], item["state"]) for item in repeated["canceled"]] == [
                (request_id, "Canceling"),
            ]
            assert not repeated["notFound"], repeated
        else:
            assert repeated["notFound"] == [request_id], repeated
        assert not repeated["undetermined"], repeated
        assert_canceled(future, reason)
    exited = cancel([request_id], reason)
    assert exited["notFound"] == [request_id], exited
    assert not exited["canceled"] and not exited["undetermined"], exited
    assert all(item["requestId"] != request_id for item in records())


@pytest.mark.parametrize(
    "payload",
    [{"dbName": 1}, {"collectionName": {}}, {"user": 1}, {"minElapsedMs": "1"}],
)
def test_t06_list_rejects_wrong_json_types(deployment, payload):
    from running_requests_l1_rest_support import post_rest_response

    uris, token, _, _ = deployment
    body = post_rest_response(list(uris.values())[1], token, "list", payload)
    assert body["code"] == 1801, body


@pytest.mark.parametrize("optional", [{}, {"user": ""}, {"reason": ""}, {"user": None}, {"reason": None}])
def test_t06_optional_cancel_fields(deployment, workload, workload_baseline, create_rbac_identity, optional):
    """Optional fields cancel self only; another user's live ID remains unaffected."""
    from pymilvus import MilvusClient
    from running_requests_l1_support import QUERY_TIMEOUT, assert_search_baseline, list_complete

    uris, _, admin_user, _ = deployment
    proxy_a, proxy_b = uris
    admin, _, collection, vectors = workload
    baseline = workload_baseline()
    user, token, grant = create_rbac_identity(admin)
    grant("Search", collection, "default")
    app = MilvusClient(uri=uris[proxy_a], token=token, db_name="default", timeout=10)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            own = pool.submit(search, app, collection, vectors)
            other = pool.submit(search, admin, collection, vectors)

            def records():
                return list_complete(admin, set(uris), collection_name=collection, user="")

            rows = wait_for_records(records, {user, admin_user}, lambda row: row.user, [own, other], exact=True)
            own_id, other_id = rows[user].request_id, rows[admin_user].request_id
            result = post_rest(
                uris[proxy_b],
                token,
                set(uris),
                "cancel",
                {
                    "requestIds": [own_id, other_id],
                    **optional,
                },
            )
            assert [row["requestId"] for row in result["canceled"]] == [own_id], result
            assert result["notFound"] == [other_id] and result["undetermined"] == [], result
            assert_canceled(own, "request canceled")
            assert any(row.request_id == other_id and row.state in ("Queued", "Running") for row in records())
            assert_search_baseline(other.result(timeout=QUERY_TIMEOUT), baseline, len(vectors))
    finally:
        app.close()


@pytest.mark.parametrize("invalid_first", [False, True], ids=["valid-first", "invalid-first"])
def test_t06_malformed_batch_has_no_side_effects(deployment, workload, invalid_first):
    """Binding rejection must not cancel the valid ID in either batch position."""
    from running_requests_l1_rest_support import post_rest_response
    from running_requests_l1_support import list_complete

    uris, token, user, connect = deployment
    proxy_a, proxy_b = uris
    owner, _, collection, vectors = workload
    operator = connect(proxy_b)

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, owner, collection, vectors)
        row = wait_for_records(records, {"Running"}, lambda r: r.state, [future], exact=True)["Running"]
        ids = [123, row.request_id] if invalid_first else [row.request_id, 123]
        body = post_rest_response(uris[proxy_b], token, "cancel", {"requestIds": ids})
        assert body["code"] == 1801, body
        assert not future.done()
        assert any(r.request_id == row.request_id and r.state == "Running" for r in records())
        reason = f"T06-after-rejection-{uuid4().hex}"
        result = post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "cancel",
            {
                "requestIds": [row.request_id],
                "reason": reason,
            },
        )
        assert [r["requestId"] for r in result["canceled"]] == [row.request_id]
        assert result["notFound"] == result["undetermined"] == []
        assert_canceled(future, reason)
