"""T05 duplicate IDs: one cancellation side effect, audit and counter increment."""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from time import monotonic, sleep
from uuid import uuid4

import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import (
    assert_canceled,
    assert_unregistered,
    list_complete,
    search,
    wait_for_records,
)
from running_requests_topology import ProxyAuthorizationTopology, run
from test_running_requests_native_sdk import complete
from test_running_requests_t08_rbac import await_rest_list_grant

pytestmark = pytest.mark.tags("L1")


def test_t05_duplicate_live_id_audit_and_counter(deployment, workload, create_rbac_identity):
    check_duplicate_live_id(deployment, workload, create_rbac_identity)


def test_t05_native_duplicate_live_id_audit_and_counter(deployment, workload, create_rbac_identity, native):
    check_duplicate_live_id(deployment, workload, create_rbac_identity, native)


def check_duplicate_live_id(deployment, workload, create_rbac_identity, worker=None):
    """Submit a live ID twice; audit at both hops and count cancellation once.

    A repeated entry can already have exited. Deterministic Canceling snapshots
    still require the separate held-exit scenario.
    """
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.fail("Run cancellation counter checks serially on a dedicated instance")
    uris, token, user, connect = deployment
    proxy_a, proxy_b = uris
    owner, _, collection, vectors = workload
    operator = connect(proxy_b)
    operator_user, token, grant = create_rbac_identity(operator)
    assert operator_user != user
    grant("ListRunningRequests")
    grant("CancelRequests")
    if worker is None:
        await_rest_list_grant(uris[proxy_b], token, {"user": user, "collectionName": collection})
    else:
        worker.ok("connect", "audit-operator", URI=uris[proxy_b], Token=token, DB="default")
        deadline = monotonic() + 15
        while monotonic() < deadline:
            result = worker.call("list", "audit-operator", User=user, Collection=collection)
            if result["ok"]:
                complete(worker, result)
                break
            assert result["code"] == 1401, result
            sleep(0.2)
        else:
            pytest.fail("Native List grant did not propagate to Proxy B")

    def cancel(ids, reason):
        if worker is None:
            return post_rest(
                uris[proxy_b],
                token,
                set(uris),
                "cancel",
                {
                    "requestIds": ids,
                    "reason": reason,
                    "user": user,
                },
            )
        result = complete(
            worker,
            worker.call(
                "cancel",
                "audit-operator",
                IDs=ids,
                Reason=reason,
                User=user,
            ),
        )
        return {
            "canceled": [{"requestId": row["request_id"], "state": row["state"]} for row in result["canceled"]],
            "notFound": result["not_found"],
            "undetermined": result["undetermined"],
        }

    topology = ProxyAuthorizationTopology()
    pods = topology.proxy_pods()
    target, entry = pods[proxy_a], pods[proxy_b]
    since = (datetime.now(UTC) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")

    def counter():
        metrics = run(
            [
                *topology.kube,
                "exec",
                target,
                "-c",
                "proxy",
                "--",
                "curl",
                "-fsS",
                "--max-time",
                "5",
                "http://127.0.0.1:9091/metrics",
            ]
        )
        values = []
        for line in metrics.splitlines():
            if (
                line.startswith("milvus_proxy_request_canceled_total{")
                and f'node_id="{proxy_a}"' in line
                and 'query_type="Search"' in line
            ):
                values.append(float(line.rsplit(" ", 1)[1]))
        assert len(values) <= 1, values
        return values[0] if values else 0

    def audits(resource, request_id, reason):
        lines = run([*topology.kube, "logs", resource, "-c", "proxy", "--since-time=" + since])
        return [
            line
            for line in lines.splitlines()
            if '["request canceled by operator"]' in line
            and f"[requestID={request_id}]" in line
            and f"[reason={reason}]" in line
        ]

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    before = counter()
    missing = f"unknown_{uuid4().hex}"
    reason = f"T05-duplicate-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, owner, collection, vectors)
        row = wait_for_records(records, {"Running"}, lambda r: r.state, [future], exact=True)["Running"]
        result = cancel([row.request_id, row.request_id, missing], reason)
        canceled = result["canceled"] or []
        assert canceled and all(item["requestId"] == row.request_id for item in canceled), result
        assert sum(item["state"] == "Running" for item in canceled) == 1, result
        assert all(item["state"] in ("Running", "Canceling") for item in canceled), result
        assert not result["undetermined"], result
        not_found = result["notFound"] or []
        assert not_found.count(missing) == 1 and set(not_found) <= {missing, row.request_id}, result
        assert_canceled(future, reason)
    assert_unregistered(records, {row.request_id})
    # A second public call after exit must not create another successful audit.
    repeated = cancel([row.request_id], reason)
    assert repeated["notFound"] == [row.request_id] and not repeated["canceled"]
    assert not repeated["undetermined"]
    deadline = monotonic() + 10
    while monotonic() < deadline:
        target_audits = audits(target, row.request_id, reason)
        entry_audits = audits(entry, row.request_id, reason)
        if target_audits and entry_audits:
            break
        sleep(0.2)
    assert len(target_audits) == len(entry_audits) == 1, (target_audits, entry_audits)
    assert "[servedByProxyID=" not in target_audits[0]
    assert f"[servedByProxyID={proxy_a}]" in entry_audits[0]
    for name, value in (("operator", operator_user), ("user", user), ("collection", collection), ("db", "default")):
        assert f"[{name}={value}]" in entry_audits[0], entry_audits[0]
    assert counter() - before == 1
    print(f"duplicate_id={row.request_id} target_audits=1 entry_audits=1 counter_delta=1")
