"""Verify revoking Cancel preserves cross-user List through native SDKs."""

from time import monotonic, sleep
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    assert_search_baseline,
)
from test_running_requests_native_sdk import capture, complete, listed, start

pytestmark = pytest.mark.tags("L1")


def test_native_revoke_cancel_preserves_list(native, data, deployment, workload_baseline, request):
    uris, _, owner, connect = deployment
    proxy_b = next(reversed(uris))
    admin = connect(proxy_b)
    suffix = uuid4().hex[:12]
    role, username, password = f"rr_role_{suffix}", f"rr_user_{suffix}", f"Qa7{suffix}"
    admin.create_role(role, timeout=10)
    request.addfinalizer(lambda: admin.drop_role(role, timeout=10))
    admin.create_user(username, password, timeout=10)
    request.addfinalizer(lambda: admin.drop_user(username, timeout=10))
    admin.grant_role(username, role, timeout=10)
    active_privileges = set()

    def revoke(privilege):
        if privilege in active_privileges:
            admin.revoke_privilege_v2(role, privilege, "*", db_name="*", timeout=10)
            active_privileges.remove(privilege)

    for privilege in ("ListRunningRequests", "CancelRequests"):
        admin.grant_privilege_v2(role, privilege, "*", db_name="*", timeout=10)
        active_privileges.add(privilege)
        request.addfinalizer(lambda privilege=privilege: revoke(privilege))
    native.ok("connect", "restricted", URI=uris[proxy_b], Token=f"{username}:{password}", DB="default")
    probe_id = f"unknown_{suffix}"

    def await_permission(allowed):
        deadline = monotonic() + 15
        while monotonic() < deadline:
            result = native.call("cancel", "restricted", IDs=[probe_id], User=owner)
            if result["ok"]:
                complete(native, result)
                assert result["not_found"] == [probe_id]
                assert not result["canceled"] and not result["undetermined"]
                if allowed:
                    return
            else:
                assert result["code"] == 1401, result
                if not allowed:
                    return
            sleep(0.2)
        pytest.fail(f"Cancel permission did not converge to allowed={allowed}")

    await_permission(True)
    revoke("CancelRequests")
    await_permission(False)
    call, expr = start(native, data)
    target = capture(native, data, call, expr)
    request_id = target["request_id"]
    visible = complete(native, native.call("list", "restricted", User=owner, Collection=data["Collection"]))
    assert request_id in {row["request_id"] for row in visible["requests"]}
    denied = native.call("cancel", "restricted", IDs=[request_id], User=owner)
    assert not denied["ok"] and denied["code"] == 1401, denied
    assert_search_baseline(native.ok("await", Call=call)["hits"], workload_baseline(), data["NQ"])
    assert request_id not in {row["request_id"] for row in listed(native, Collection=data["Collection"])}
