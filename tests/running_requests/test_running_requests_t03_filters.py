"""TC-L1 functional scenario T03."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    assert_canceled,
    cancel_exact,
    check_record,
    list_complete,
    search,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


def test_t03_sdk_list_filters(deployment, workload):
    """Verify SDK filtering against a known in-flight request with unique ownership."""
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    client, _, collection, vectors = workload
    operator = connect(proxy_b)

    def records(**filters):
        return list_complete(operator, set(uris), **filters)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, client, collection, vectors)
        target = wait_for_records(
            lambda: records(collection_name=collection),
            {collection},
            lambda row: row.collection_name,
            [future],
        )[collection]
        check_record(target, proxy_a, user, collection)
        combined = {"db_name": "default", "collection_name": collection, "user": user}
        target = wait_for_records(
            lambda: records(**combined),
            {target.request_id},
            lambda row: row.request_id if row.elapsed_ms >= 1 else None,
            [future],
        )[target.request_id]
        positive = {**combined, "min_elapsed_ms": target.elapsed_ms}
        for filters in (
            {"db_name": "default"},
            {"collection_name": collection},
            {"user": user},
            {"min_elapsed_ms": 0},
            positive,
        ):
            assert [row.request_id for row in records(**filters)].count(target.request_id) == 1
        for filters in (
            {**positive, "db_name": f"missing_{uuid4().hex}"},
            {**positive, "collection_name": f"missing_{uuid4().hex}"},
            {**positive, "user": f"missing_{uuid4().hex}"},
            {**positive, "min_elapsed_ms": QUERY_TIMEOUT * 1000},
        ):
            assert all(row.request_id != target.request_id for row in records(**filters))
        assert not future.done(), "Search must still be in flight after the filter checks"
        reason = f"T03-{uuid4().hex}"
        cancel_exact(operator, set(uris), [target.request_id], reason)
        assert_canceled(future, reason)


def test_t03_rest_default_and_null_filters(deployment, workload, workload_baseline, create_rbac_identity):
    """Default filters must find a live self-owned request, not another user's."""
    from pymilvus import MilvusClient
    from running_requests_l1_rest_support import post_rest
    from running_requests_l1_support import assert_search_baseline

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
            snapshot = wait_for_records(
                lambda: list_complete(admin, set(uris), collection_name=collection, user=""),
                {user, admin_user},
                lambda row: row.user,
                [own, other],
                exact=True,
            )
            target = snapshot[user]
            base = {"dbName": "default", "collectionName": collection, "user": user, "minElapsedMs": 0}
            for field in base:
                variants = ["omitted", None, 0 if field == "minElapsedMs" else ""]
                for value in variants:
                    payload = dict(base)
                    if value == "omitted":
                        del payload[field]
                    else:
                        payload[field] = value
                    result = post_rest(uris[proxy_b], token, set(uris), "list", payload)
                    assert [row["requestId"] for row in result["requests"]] == [target.request_id], (
                        field,
                        value,
                        result,
                    )
                    assert not own.done() and not other.done(), (
                        "OBSERVATION_WINDOW: requests ended during default-filter checks"
                    )
            reason = f"T03-default-{uuid4().hex}"
            canceled = post_rest(
                uris[proxy_b],
                token,
                set(uris),
                "cancel",
                {
                    "requestIds": [target.request_id],
                    "reason": reason,
                },
            )
            assert [row["requestId"] for row in canceled["canceled"]] == [target.request_id]
            assert_canceled(own, reason)
            assert_search_baseline(other.result(timeout=QUERY_TIMEOUT), baseline, len(vectors))
    finally:
        app.close()


@pytest.fixture
def second_database(request, deployment, workload):
    from pymilvus import MilvusClient
    from setup_running_requests_l0 import DEFAULT_ROWS, create_collection

    uris, token, _, _ = deployment
    proxy_a = next(iter(uris))
    app, _, collection, _ = workload
    other_db = f"rr_t03_{uuid4().hex}"
    app.create_database(other_db, timeout=30)
    request.addfinalizer(lambda: app.drop_database(other_db, timeout=30))
    other = MilvusClient(uri=uris[proxy_a], token=token, db_name=other_db, dedicated=True)
    request.addfinalizer(other.close)
    create_collection(
        other,
        collection,
        DEFAULT_ROWS,
        on_created=lambda: request.addfinalizer(lambda: other.drop_collection(collection, timeout=30)),
    )
    return other_db, other


def test_t03_rest_database_scope_and_precedence(deployment, workload, second_database):
    """Use live, same-name Collections to distinguish default and explicit DB scope."""
    from threading import Barrier

    from running_requests_l1_rest_support import post_rest

    other_db, other = second_database
    uris, token, user, connect = deployment
    proxy_a, proxy_b = uris
    app, _, collection, vectors = workload
    operator = connect(proxy_b)
    assert other.get_load_state(collection, timeout=10)["state"].name == "Loaded"
    start = Barrier(2, timeout=5)

    def invoke(client):
        start.wait()
        return search(client, collection, vectors)

    def rest_records(payload, headers=None):
        data = post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "list",
            payload,
            headers=headers,
        )
        return data["requests"] or []

    base = {"collectionName": collection, "user": user}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {db: pool.submit(invoke, client) for db, client in (("default", app), (other_db, other))}
        snapshot = wait_for_records(
            lambda: rest_records(base),
            set(futures),
            lambda row: row["dbName"],
            futures.values(),
            exact=True,
        )
        ids = {db: row["requestId"] for db, row in snapshot.items()}
        assert len(set(ids.values())) == 2
        assert all(
            row["type"] == "Search"
            and row["proxyId"] == proxy_a
            and row["user"] == user
            and row["collectionName"] == collection
            for row in snapshot.values()
        )
        scoped = rest_records({**base, "dbName": other_db}, {"DB-Name": "default"})
        assert [row["requestId"] for row in scoped] == [ids[other_db]]
        reason = f"T03-db-{uuid4().hex}"
        cancel_exact(operator, set(uris), list(ids.values()), reason)
        for future in futures.values():
            assert_canceled(future, reason)


def test_t03_rest_list_nondefault_db_rbac(deployment, workload, second_database, create_rbac_identity):
    """Verify self scope, explicit cross-user denial, and visibility after granting List."""
    import json
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen

    from pymilvus import MilvusClient
    from running_requests_l1_rest_support import post_rest, post_rest_response
    from test_running_requests_t08_rbac import await_rest_list_grant

    uris, _, _, connect = deployment
    proxy_a, proxy_b = uris
    db, _ = second_database
    _, _, collection, vectors = workload
    admin = connect(proxy_b)
    owner, owner_token, grant_search = create_rbac_identity(admin)
    _, operator_token, grant_list = create_rbac_identity(admin)
    grant_search("Search", collection, db_name=db)
    app_uri, list_uri = uris[proxy_a], uris[proxy_b]

    denied_search = Request(
        f"{app_uri}/v2/vectordb/entities/search",
        data=json.dumps(
            {
                "dbName": "default",
                "collectionName": collection,
                "annsField": "vector",
                "data": vectors[:1].tolist(),
                "limit": 1,
            }
        ).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {owner_token}"},
        method="POST",
    )
    with pytest.raises(HTTPError) as error:
        urlopen(denied_search, timeout=10).close()
    with error.value as response:
        assert response.code == 403
        assert "PrivilegeSearch" in json.load(response)["message"]

    payload = {"dbName": db, "collectionName": collection, "minElapsedMs": 0}
    target_payload = {**payload, "user": owner}
    denied = post_rest_response(list_uri, operator_token, "list", target_payload)
    assert denied["code"] == 1401 and "PrivilegeListRunningRequests" in denied["message"], denied
    grant_list("ListRunningRequests")
    await_rest_list_grant(list_uri, operator_token, target_payload)

    def records(token, filters, headers=None):
        return post_rest(list_uri, token, set(uris), "list", filters, headers=headers)["requests"] or []

    app = MilvusClient(app_uri, token=owner_token, db_name=db, dedicated=True, timeout=10)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(search, app, collection, vectors)
            target = wait_for_records(
                lambda: records(owner_token, payload, {"DB-Name": f"missing_{db}"}),
                {owner},
                lambda row: row["user"],
                [future],
            )[owner]
            assert target["dbName"] == db and target["proxyId"] == proxy_a
            assert [row["requestId"] for row in records(operator_token, target_payload)] == [target["requestId"]]
            assert len(future.result(timeout=QUERY_TIMEOUT)) == len(vectors)
    finally:
        app.close()
