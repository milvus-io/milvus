"""T06 live cancellation boundary cases for Java and Go."""

from time import monotonic, sleep
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    assert_search_baseline,
)
from test_running_requests_native_sdk import canceled, capture, complete, listed, start, terminal

pytestmark = pytest.mark.tags("L1")


def test_t06_native_long_reason_round_trip(native, data):
    reason = f"reason-start-{uuid4().hex}-" + "取消原因" * 1024 + "-reason-end"
    call, expr = start(native, data)
    request_id = capture(native, data, call, expr)["request_id"]
    result = canceled(native, [request_id], reason)
    assert [row["request_id"] for row in result["canceled"]] == [request_id], result
    terminal(native, call, reason)
    assert request_id not in {row["request_id"] for row in listed(native, Collection=data["Collection"])}


@pytest.mark.parametrize("size", [1, 1024], ids=["empty-id-element", "large-unknown-batch"])
def test_t06_native_unknown_id_boundaries(native, size):
    prefix = f"unknown_{uuid4().hex}"
    ids = [""] if size == 1 else [f"{prefix}_{index}" for index in range(size)]
    result = complete(native, native.call("cancel", IDs=ids))
    assert sorted(result["not_found"]) == sorted(ids), result
    assert not result["canceled"] and not result["undetermined"], result


def test_t06_native_default_cancel_fields_are_self_scoped(
    native,
    data,
    deployment,
    workload_baseline,
    create_rbac_identity,
):
    """Cancel a mixed-owner batch without specifying user or reason."""
    uris, _, admin_user, connect = deployment
    a, b = uris
    owner, token, grant = create_rbac_identity(connect(b))
    assert owner != admin_user
    for privilege in ("Search", "DescribeCollection"):
        grant(privilege, data["Collection"], "default")
    native.ok("connect", "self-query", URI=uris[a], Token=token, DB="default")
    native.ok("connect", "self-manager", URI=uris[b], Token=token, DB="default")
    baseline = workload_baseline()
    own_call, own_expr = start(native, data, client="self-query")
    other_call, other_expr = start(native, data)
    expressions = {own_expr, other_expr}

    def records():
        return listed(native, Collection=data["Collection"])

    deadline = monotonic() + 30
    while monotonic() < deadline:
        matches = [row for row in records() if row["expr"] in expressions]
        assert len(matches) == len({row["expr"] for row in matches}), matches
        if len(matches) == 2:
            snapshot = {row["expr"]: row for row in matches}
            break
        for call in (own_call, other_call):
            assert not native.ok("poll", Call=call)["done"], "Search ended before List captured both owners"
        sleep(0.02)
    else:
        pytest.fail("OBSERVATION_WINDOW: no snapshot contained both owners")

    own, other = snapshot[own_expr], snapshot[other_expr]
    assert (own["user"], other["user"]) == (owner, admin_user)
    assert all(row["proxy_id"] == a and row["type"] == "Search" for row in snapshot.values())
    own_id, other_id = own["request_id"], other["request_id"]
    assert own_id != other_id
    result = complete(native, native.call("cancel", "self-manager", IDs=[own_id, other_id]))
    assert [row["request_id"] for row in result["canceled"]] == [own_id], result
    assert result["not_found"] == [other_id] and not result["undetermined"], result
    terminal(native, own_call, "request canceled")
    active = {row["request_id"] for row in records()}
    assert_search_baseline(native.ok("await", Call=other_call)["hits"], baseline, data["NQ"])
    assert other_id in active, "OBSERVATION_WINDOW: other user's Search was not observed after Cancel"
    assert not {own_id, other_id} & {row["request_id"] for row in records()}
