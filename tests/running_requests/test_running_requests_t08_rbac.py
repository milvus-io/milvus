"""TC-L1 functional scenario T08."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    assert_all_proxies_answered,
    assert_canceled,
    assert_registered,
    assert_search_baseline,
    assert_unregistered,
    cancel_exact,
    list_complete,
    required_env,
    search,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


@pytest.fixture
def role_token(role):
    return required_env(f"MILVUS_L1_RBAC_{role}_TOKEN")[0]


@pytest.mark.parametrize(
    ("role", "can_list", "can_cancel"),
    [
        ("NONE", False, False),
        ("LIST", True, False),
        ("CANCEL", False, True),
        ("BOTH", True, True),
    ],
)
def test_t08_cross_user_sdk_privileges(role, can_list, can_cancel, role_token, deployment, workload, workload_baseline):
    """Exercise distinct, pre-provisioned non-admin roles against another user's search."""
    from pymilvus.exceptions import MilvusException

    uris, owner_token, user, connect = deployment
    _, proxy_b = uris
    assert role_token != owner_token and role_token.split(":", 1)[0] != user
    owner, _, collection, vectors = workload
    admin = connect(proxy_b)
    restricted = connect(proxy_b, role_token)
    nq = len(vectors)
    baseline = None if can_cancel else workload_baseline()

    def records():
        return list_complete(admin, set(uris), collection_name=collection, user=user)

    reason = f"T08-{role}-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, owner, collection, vectors)
        target = wait_for_records(records, {nq}, lambda row: row.nq, [future])[nq]
        if can_list:
            visible = list_complete(restricted, set(uris), collection_name=collection, user=user)
            assert [row.request_id for row in visible] == [target.request_id]
        else:
            with pytest.raises(MilvusException) as denied:
                restricted.list_running_requests(collection_name=collection, user=user, timeout=10)
            assert denied.value.code == 1401
            visible = list_complete(restricted, set(uris), collection_name=collection)
            assert all(row.request_id != target.request_id for row in visible)

        assert not future.done(), "Owner's Search must still be in flight during Cancel"
        if can_cancel:
            result = restricted.cancel_requests(
                [target.request_id],
                user=user,
                reason=reason,
                timeout=10,
            )
            assert_all_proxies_answered(result, set(uris))
            assert [row.request_id for row in result.canceled] == [target.request_id]
            assert not result.not_found and not result.undetermined
            assert_canceled(future, reason)
        else:
            with pytest.raises(MilvusException) as denied:
                restricted.cancel_requests([target.request_id], user=user, reason=reason, timeout=10)
            assert denied.value.code == 1401
            assert any(row.request_id == target.request_id for row in records())
            cancel_exact(restricted, set(uris), [], reason, not_found=[target.request_id])
            assert_registered(records, {target.request_id})
            assert_search_baseline(future.result(timeout=QUERY_TIMEOUT), baseline, nq)
    assert_unregistered(records, {target.request_id})


def test_t08_ordinary_user_manages_own_request(deployment, workload):
    """An ordinary caller can list and cancel their own Search across Proxies."""
    (token,) = required_env("MILVUS_L1_RBAC_SELF_TOKEN")
    uris, _, owner, connect = deployment
    proxy_a, proxy_b = uris
    user = token.split(":", 1)[0]
    assert user != owner
    _, _, collection, vectors = workload
    app, operator = connect(proxy_a, token), connect(proxy_b, token)

    def records():
        return list_complete(operator, set(uris), collection_name=collection)

    reason = f"T08-self-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, app, collection, vectors)
        target = wait_for_records(records, {len(vectors)}, lambda row: row.nq, [future])[len(vectors)]
        assert target.user == user and target.proxy_id == proxy_a
        cancel_exact(operator, set(uris), [target.request_id], reason)
        assert_canceled(future, reason)
    assert_unregistered(records, {target.request_id})


def test_t08_authorized_cancel_with_wrong_user(deployment, workload, workload_baseline):
    check_authorized_cancel_with_wrong_user(deployment, workload, workload_baseline)


def check_authorized_cancel_with_wrong_user(
    deployment,
    workload,
    workload_baseline,
    *,
    connect_operator=None,
):
    """A cross-user privilege still respects the explicitly specified owner."""
    (token,) = required_env("MILVUS_L1_RBAC_CANCEL_TOKEN")
    uris, _, user, connect = deployment
    _, proxy_b = uris
    owner, _, collection, vectors = workload
    admin = connect(proxy_b)
    operator = connect(proxy_b, token) if connect_operator is None else connect_operator(uris[proxy_b], token)
    wrong_user = token.split(":", 1)[0]
    assert wrong_user != user
    nq = len(vectors)
    baseline = workload_baseline()

    def records():
        return list_complete(admin, set(uris), collection_name=collection, user=user)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, owner, collection, vectors)
        target = wait_for_records(records, {nq}, lambda row: row.nq, [future])[nq]
        result = operator.cancel_requests([target.request_id], user=wrong_user, timeout=10)
        assert_all_proxies_answered(result, set(uris))
        assert result.not_found == [target.request_id], result
        assert not result.canceled and not result.undetermined, result
        assert_registered(records, {target.request_id})
        assert_search_baseline(future.result(timeout=QUERY_TIMEOUT), baseline, nq)
    assert_unregistered(records, {target.request_id})


def test_t08_auth_disabled_smoke():
    check_auth_lifecycle()


@pytest.mark.parametrize("language", ["java", "go"])
def test_t08_auth_disabled_smoke_native(language, tmp_path):
    check_auth_lifecycle(language=language, logs=tmp_path)


def check_auth_lifecycle(*, language=None, logs=None):
    """Verify the same cross-user requests before disabling auth, after restart, and after restoration."""
    import os
    from contextlib import ExitStack, closing, contextmanager
    from urllib.error import HTTPError

    from pymilvus import MilvusClient
    from running_requests_l1_rest_support import post_rest, post_rest_response
    from running_requests_topology import ProxyAuthorizationTopology
    from test_running_requests_l0 import search_vectors

    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.fail("Run the authorization lifecycle as a separate serial pytest step")
    admin_token, collection = required_env("MILVUS_TOKEN", "MILVUS_L0_COLLECTION")
    topology = ProxyAuthorizationTopology()
    original = topology.configuration()
    suffix = uuid4().hex[:12]
    owner, operator, role = (f"t08_{label}_{suffix}" for label in ("owner", "operator", "role"))
    password = f"Qa7{suffix}"
    owner_token, operator_token = (f"{user}:{password}" for user in (owner, operator))
    filters = {"dbName": "default", "collectionName": collection, "user": owner}
    reason = f"T08-no-auth-{suffix}"
    vectors = search_vectors(default_nq=16384)
    cleanup = []

    def check_auth(endpoints, *, enabled):
        from time import monotonic, sleep

        ids = {node_id for _, node_id in endpoints}
        deadline = monotonic() + 60
        pending = dict(endpoints)
        errors = {}
        while pending and monotonic() < deadline:
            for uri in tuple(pending):
                try:
                    if enabled:
                        try:
                            post_rest_response(uri, None, "list", {})
                        except HTTPError as rejected:
                            with rejected:
                                assert rejected.code == 401, rejected.code
                        else:
                            raise AssertionError("Anonymous REST List was accepted")
                    post_rest(uri, admin_token if enabled else None, ids, "list", filters)
                except (AssertionError, OSError) as error:
                    errors[pending[uri]] = str(error)
                else:
                    del pending[uri]
            if pending:
                sleep(0.5)
        if pending:
            raise AssertionError(
                f"Proxy REST List did not converge to authorization={enabled}, expected node IDs {ids}, "
                f"pending node errors { {node: errors[node] for node in pending.values()} }"
            )

    @contextmanager
    def management(uri, ids, phase, credential):
        if language is None:

            def call(action, payload, *, denied=False):
                if denied:
                    return post_rest_response(uri, credential, action, payload)
                return post_rest(uri, credential, ids, action, payload)

            yield call
            return

        # Connect after every rollout; workers must not reuse the old Pod or channel.
        from test_running_requests_native_sdk import Native, complete

        worker = Native(language, logs / f"{phase}.stderr")
        with closing(worker):
            worker.nodes = ids
            worker.ok("connect", "b", URI=uri, Token=credential, DB="default")

            def call(action, payload, *, denied=False):
                fields = {
                    "dbName": "DB",
                    "collectionName": "Collection",
                    "user": "User",
                    "requestIds": "IDs",
                    "reason": "Reason",
                }
                result = worker.call(action, **{fields[key]: value for key, value in payload.items()})
                if denied:
                    assert not result["ok"], result
                    return result
                complete(worker, result)
                if action == "list":
                    return {
                        "requests": [
                            {
                                "requestId": row["request_id"],
                                "proxyId": row["proxy_id"],
                                "nq": row["nq"],
                                "type": row["type"],
                                "user": row["user"],
                            }
                            for row in result["requests"]
                        ]
                    }
                return {
                    "canceled": [{"requestId": row["request_id"]} for row in result["canceled"]],
                    "notFound": result["not_found"],
                    "undetermined": result["undetermined"],
                }

            yield call

    def check_denied(call, request_id):
        for action, payload, privilege in (
            ("list", filters, "ListRunningRequests"),
            ("cancel", {"requestIds": [request_id], "user": owner}, "CancelRequests"),
        ):
            result = call(action, payload, denied=True)
            assert result["code"] == 1401 and privilege in result["message"], result

    def exercise(endpoints, *, enabled):
        (uri_a, proxy_a), (uri_b, proxy_b) = endpoints
        ids = {proxy_a, proxy_b}
        with ExitStack() as stack:
            app = stack.enter_context(
                closing(
                    MilvusClient(
                        uri_a,
                        token=owner_token,
                        timeout=10,
                        dedicated=True,
                    )
                )
            )
            restricted = stack.enter_context(
                management(
                    uri_b,
                    ids,
                    "enabled-restricted" if enabled else "disabled",
                    operator_token,
                )
            )
            list_call = restricted
            if enabled:
                list_call = stack.enter_context(management(uri_b, ids, "enabled-admin", admin_token))
            pool = stack.enter_context(ThreadPoolExecutor(max_workers=1))
            future = pool.submit(search, app, collection, vectors)
            row = wait_for_records(
                lambda: list_call("list", filters)["requests"],
                {len(vectors)},
                lambda item: item["nq"],
                [future],
            )[len(vectors)]
            assert (row["proxyId"], row["type"], row["user"]) == (proxy_a, "Search", owner)
            request_id = row["requestId"]
            if enabled:
                check_denied(restricted, request_id)
                assert len(future.result(timeout=QUERY_TIMEOUT)) == len(vectors)
            else:
                result = restricted(
                    "cancel",
                    {
                        "requestIds": [request_id],
                        "user": owner,
                        "reason": reason,
                    },
                )
                assert [item["requestId"] for item in result["canceled"]] == [request_id]
                assert not result["notFound"] and not result["undetermined"], result
                assert_canceled(future, reason)
            assert request_id not in {row["requestId"] for row in list_call("list", filters)["requests"]}

    def restore_verified():
        with topology.endpoints() as endpoints:
            check_auth(endpoints, enabled=True)
            ids = {node_id for _, node_id in endpoints}
            for uri, node_id in endpoints:
                with management(uri, ids, f"restored-{node_id}", operator_token) as restricted:
                    check_denied(restricted, f"missing_{suffix}")
        print("T08 authorization restored and verified on both Proxies", flush=True)

    try:
        print("T08 verifying authorization enabled", flush=True)
        with topology.endpoints() as endpoints:
            check_auth(endpoints, enabled=True)
            with closing(MilvusClient(endpoints[0][0], token=admin_token, timeout=10)) as admin:
                admin.create_role(role, timeout=10)
                cleanup.append(("drop_role", (role,), {}))
                for name in (owner, operator):
                    admin.create_user(name, password, timeout=10)
                    cleanup.append(("drop_user", (name,), {}))
                admin.grant_role(owner, role, timeout=10)
                admin.grant_privilege_v2(role, "Search", collection, db_name="default", timeout=10)
                cleanup.append(("revoke_privilege_v2", (role, "Search", collection), {"db_name": "default"}))
            exercise(endpoints, enabled=True)

        print("T08 disabling authorization and restarting both Proxies", flush=True)
        with topology.disabled(original, restore_verified):
            with topology.endpoints() as endpoints:
                check_auth(endpoints, enabled=False)
                print("T08 verifying cross-user List and Cancel with authorization disabled", flush=True)
                exercise(endpoints, enabled=False)
    finally:
        if cleanup and topology.configuration() == original:
            with topology.endpoints() as endpoints:
                with closing(MilvusClient(endpoints[0][0], token=admin_token, timeout=10)) as admin:
                    with ExitStack() as stack:
                        for method, args, kwargs in cleanup:
                            stack.callback(getattr(admin, method), *args, **kwargs, timeout=10)


def test_t08_revoking_cancel_preserves_list(deployment, workload):
    check_revoking_cancel_preserves_list(deployment, workload)


def check_revoking_cancel_preserves_list(deployment, workload, *, connect_operator=None):
    """Revoke Cancel from an isolated role and verify its List grant survives."""
    from time import monotonic, sleep

    from pymilvus.exceptions import MilvusException

    uris, _, owner_user, connect = deployment
    _, proxy_b = uris
    admin = connect(proxy_b)
    owner, _, collection, vectors = workload
    suffix = uuid4().hex[:12]
    role, username = f"t08_role_{suffix}", f"t08_user_{suffix}"
    password = uuid4().hex
    probe_id = f"unknown_{suffix}"

    granted = set()
    admin.create_role(role, timeout=10)
    try:
        admin.create_user(username, password, timeout=10)
        try:
            admin.grant_role(username, role, timeout=10)
            for privilege in ("ListRunningRequests", "CancelRequests"):
                admin.grant_privilege_v2(role, privilege, "*", db_name="*", timeout=10)
                granted.add(privilege)
            credential = f"{username}:{password}"
            restricted = (
                connect(proxy_b, credential)
                if connect_operator is None
                else connect_operator(uris[proxy_b], credential, set(uris))
            )

            def await_cancel_permission(expected):
                deadline = monotonic() + 15
                while monotonic() < deadline:
                    try:
                        result = restricted.cancel_requests([probe_id], user=owner_user, timeout=3)
                    except MilvusException as error:
                        assert error.code == 1401, error
                        if not expected:
                            return
                    else:
                        assert_all_proxies_answered(result, set(uris))
                        assert result.not_found == [probe_id] and not result.canceled and not result.undetermined
                        if expected:
                            return
                    sleep(0.2)
                pytest.fail(f"Cancel permission did not converge to {expected}")

            await_cancel_permission(True)
            admin.revoke_privilege_v2(role, "CancelRequests", "*", db_name="*", timeout=10)
            granted.remove("CancelRequests")
            await_cancel_permission(False)

            def records():
                return list_complete(restricted, set(uris), collection_name=collection, user=owner_user)

            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(search, owner, collection, vectors)
                nq = len(vectors)
                target = wait_for_records(records, {nq}, lambda row: row.nq, [future])[nq]
                with pytest.raises(MilvusException) as denied:
                    restricted.cancel_requests([target.request_id], user=owner_user, timeout=10)
                assert denied.value.code == 1401
                assert_registered(records, {target.request_id})
                reason = f"T08-cleanup-{suffix}"
                cancel_exact(admin, set(uris), [target.request_id], reason)
                assert_canceled(future, reason)
            assert_unregistered(records, {target.request_id})
        finally:
            admin.drop_user(username, timeout=10)
    finally:
        for privilege in sorted(granted):
            admin.revoke_privilege_v2(role, privilege, "*", db_name="*", timeout=10)
        admin.drop_role(role, timeout=10)


@pytest.fixture
def create_rbac_identity(request):
    """Register cleanup for each isolated user, role, and privilege grant."""

    def create(admin):
        suffix = uuid4().hex[:12]
        role, username, password = f"t08_role_{suffix}", f"t08_user_{suffix}", f"Qa7{suffix}"
        admin.create_role(role, timeout=10)
        request.addfinalizer(lambda: admin.drop_role(role, timeout=10))
        admin.create_user(username, password, timeout=10)
        request.addfinalizer(lambda: admin.drop_user(username, timeout=10))
        admin.grant_role(username, role, timeout=10)

        def grant(privilege, collection="*", db_name="*"):
            admin.grant_privilege_v2(role, privilege, collection, db_name=db_name, timeout=10)
            request.addfinalizer(
                lambda: admin.revoke_privilege_v2(role, privilege, collection, db_name=db_name, timeout=10)
            )

        return username, f"{username}:{password}", grant

    return create


def await_rest_list_grant(uri, token, payload):
    """Wait for the targeted Proxy to see the role update before launching Search."""
    from time import monotonic, sleep

    from running_requests_l1_rest_support import post_rest_response

    deadline = monotonic() + 15
    while monotonic() < deadline:
        body = post_rest_response(uri, token, "list", payload)
        if body["code"] == 0:
            return
        assert body["code"] == 1401, body
        sleep(0.2)
    pytest.fail("ListRunningRequests grant did not propagate to Proxy B")


@pytest.mark.parametrize(
    ("group", "can_cancel"),
    [("ClusterReadOnly", False), ("ClusterReadWrite", True), ("ClusterAdmin", True)],
)
def test_t08_builtin_group_cross_user_rest(group, can_cancel, deployment, workload, create_rbac_identity):
    """Verify the built-in catalog and the cross-user permissions it grants."""
    from running_requests_l1_rest_support import post_rest, post_rest_response

    uris, _, owner_user, connect = deployment
    proxy_a, proxy_b = uris
    admin = connect(proxy_b)
    owner, _, collection, vectors = workload
    groups = {item["privilege_group"]: item["privileges"] for item in admin.list_privilege_groups(timeout=10)}
    assert "ListRunningRequests" in groups[group]
    assert ("CancelRequests" in groups[group]) == can_cancel

    operator_user, token, grant = create_rbac_identity(admin)
    assert operator_user != owner_user
    grant(group)
    uri = uris[proxy_b]
    payload = {"dbName": "default", "collectionName": collection, "user": owner_user}
    await_rest_list_grant(uri, token, payload)

    def records():
        return post_rest(uri, token, set(uris), "list", payload)["requests"] or []

    reason = f"T08-{group}-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, owner, collection, vectors)
        row = wait_for_records(records, {len(vectors)}, lambda item: item["nq"], [future])[len(vectors)]
        assert row["proxyId"] == proxy_a and row["user"] == owner_user
        request_id = row["requestId"]
        cancel = {"requestIds": [request_id], "user": owner_user, "reason": reason}
        if can_cancel:
            result = post_rest(uri, token, set(uris), "cancel", cancel)
            assert [item["requestId"] for item in result["canceled"]] == [request_id]
            assert not result["notFound"] and not result["undetermined"], result
            assert_canceled(future, reason)
        else:
            result = post_rest_response(uri, token, "cancel", cancel)
            assert result["code"] == 1401 and "PrivilegeCancelRequests" in result["message"], result
            assert len(future.result(timeout=QUERY_TIMEOUT)) == len(vectors)
