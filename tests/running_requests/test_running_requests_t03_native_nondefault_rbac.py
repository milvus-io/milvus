"""T03. Verify native List visibility and permissions for a non-default database."""

from concurrent.futures import ThreadPoolExecutor
from time import monotonic, sleep
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    wait_for_records,
)
from test_running_requests_native_sdk import complete

pytestmark = pytest.mark.tags("L1")


def test_t03_native_nondefault_list_rbac(
    deployment,
    workload,
    second_database,
    create_rbac_identity,
    native,
    request,
):
    from pymilvus import MilvusClient

    uris, _, _, connect = deployment
    a, b = uris
    database, _ = second_database
    _, _, collection, vectors = workload
    admin = connect(b)
    owner, owner_token, grant_owner = create_rbac_identity(admin)
    _, operator_token, grant_operator = create_rbac_identity(admin)
    grant_owner("Search", collection, db_name=database)

    # Keep management connections in the default DB. List uses its explicit
    # database filter and the two accounts need no database discovery grants.
    for label, token in (("rr-owner", owner_token), ("rr-observer", operator_token)):
        native.ok("connect", label, URI=uris[b], Token=token, DB="default")
    filters = {"DB": database, "Collection": collection}
    explicit = dict(filters, User=owner)
    denied = native.call("list", "rr-observer", **explicit)
    assert not denied["ok"] and denied["code"] == 1401, denied
    assert "PrivilegeListRunningRequests" in denied["message"], denied

    grant_operator("ListRunningRequests")
    deadline = monotonic() + 15
    while monotonic() < deadline:
        result = native.call("list", "rr-observer", **explicit)
        if result["ok"]:
            assert not complete(native, result)["requests"]
            break
        assert result["code"] == 1401, result
        sleep(0.2)
    else:
        pytest.fail("List grant did not propagate to Proxy B")

    app = MilvusClient(uris[a], token=owner_token, db_name=database, dedicated=True, timeout=10)
    request.addfinalizer(app.close)
    expression = f"id >= -{int(uuid4().hex[:7], 16) + 1}"

    def records(client, **selection):
        return complete(native, native.call("list", client, **selection))["requests"]

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            app.search,
            collection,
            vectors,
            anns_field="vector",
            filter=expression,
            limit=10,
            search_params={"metric_type": "L2"},
            timeout=QUERY_TIMEOUT,
        )
        row = wait_for_records(
            lambda: records("rr-owner", **filters),
            {expression},
            lambda item: item["expr"],
            [future],
            exact=True,
        )[expression]
        assert (row["db_name"], row["proxy_id"], row["user"], row["type"]) == (
            database,
            a,
            owner,
            "Search",
        ), row
        request_id = row["request_id"]
        assert [item["request_id"] for item in records("rr-observer", **explicit)] == [request_id]
        assert not records("rr-observer", **dict(explicit, DB="default"))
        assert len(future.result(timeout=QUERY_TIMEOUT)) == len(vectors)
    assert not records("rr-observer", **explicit)
