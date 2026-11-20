"""Real per-type cancellation counter deltas, batch and rejected branches."""

import os
import re
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from pymilvus import AnnSearchRequest, RRFRanker
from pymilvus.exceptions import MilvusException
from running_requests_l1_support import (
    assert_all_proxies_answered,
    assert_canceled,
    list_complete,
    wait_for_records,
)
from running_requests_topology import ProxyAuthorizationTopology, run

pytestmark = pytest.mark.tags("L1")


@pytest.fixture(autouse=True)
def exclusive_counter_run():
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.fail("Counter deltas require serial tests on a dedicated instance")


def counters():
    local = (os.environ.get("RR_LOCAL_T04_PROXY_A"), os.environ.get("RR_LOCAL_PROXY_B"))
    if any(local):
        assert all(local), "Docker metrics require both Proxy containers"
        commands = [["docker", "exec", container] for container in local]
    else:
        topology = ProxyAuthorizationTopology()
        commands = [[*topology.kube, "exec", pod, "-c", "proxy", "--"] for pod in topology.proxy_pods().values()]
    values = {}
    for command in commands:
        raw = run([*command, "curl", "-fsS", "--max-time", "5", "http://127.0.0.1:9091/metrics"])
        for line in raw.splitlines():
            if line.startswith("milvus_proxy_request_canceled_total{"):
                labels = dict(re.findall(r'(\w+)="([^"]*)"', line))
                key = (int(labels["node_id"]), labels["query_type"])
                assert key not in values
                values[key] = float(line.rsplit(" ", 1)[1])
    return values


def delta(before, expected):
    after = counters()
    actual = {key: after.get(key, 0) - before.get(key, 0) for key in set(before) | set(after)}
    actual = {k: v for k, v in actual.items() if v != 0}
    assert actual == expected, (actual, expected)
    print("counter_delta=", actual)


@pytest.mark.parametrize("kind", ["HybridSearch", "Query"])
def test_cancel_type_counter(kind, deployment, workload):
    check_cancel_type_counter(kind, deployment, workload)


def check_cancel_type_counter(kind, deployment, workload, *, operator=None):
    uris, _, user, connect = deployment
    a, b = uris
    app, _, collection, vectors = workload
    operator = connect(b) if operator is None else operator
    before = counters()
    captured = False
    for attempt in range(5):
        expr = f"id >= -{int(uuid4().hex[:7], 16)}"

        def invoke():
            if kind == "Query":
                return app.query(collection, filter=expr, output_fields=["id", "vector"], limit=16384, timeout=90)
            requests = [
                AnnSearchRequest(vectors, "vector", {"metric_type": "L2"}, limit=10, expr=expr) for _ in range(2)
            ]
            return app.hybrid_search(collection, requests, RRFRanker(), limit=10, timeout=90)

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(invoke)

            def rows():
                return list_complete(operator, set(uris), collection_name=collection, user=user)

            # Only retry a completed observation window; do not swallow API failures.
            from time import monotonic, sleep

            deadline = monotonic() + 30
            row = None
            while monotonic() < deadline:
                matches = [r for r in rows() if r.type == kind and expr in r.expr]
                if matches:
                    assert len(matches) == 1
                    row = matches[0]
                    break
                if future.done():
                    future.result()
                    break
                sleep(0.01)
            if row is None:
                future.result(timeout=90)
                continue
            result = operator.cancel_requests([row.request_id], reason="metric-type", timeout=10)
            assert_all_proxies_answered(result, set(uris))
            assert result.undetermined == []
            if result.not_found:
                assert result.not_found == [row.request_id] and result.canceled == []
                future.result(timeout=90)
                continue
            assert [r.request_id for r in result.canceled] == [row.request_id]
            assert result.canceled[0].type == kind and row.proxy_id == a
            assert_canceled(future, "metric-type")
            captured = True
            break
    assert captured, "OBSERVATION_WINDOW: no live cancellation in five attempts"
    delta(before, {(a, kind): 1})


def test_batch_and_rejected_counter_branches(deployment, workload, create_rbac_identity):
    check_batch_and_rejected_counter_branches(deployment, workload, create_rbac_identity)


def check_batch_and_rejected_counter_branches(
    deployment,
    workload,
    create_rbac_identity,
    *,
    operator=None,
    denied_cancel=None,
):
    uris, _, user, connect = deployment
    a, b = uris
    app, _, collection, vectors = workload
    admin = connect(b)
    _, credential, _ = create_rbac_identity(admin)
    manager = admin if operator is None else operator
    if denied_cancel is None:
        restricted = connect(b, credential)

        def denied_cancel(ids, user, credential):
            with pytest.raises(MilvusException) as error:
                restricted.cancel_requests(ids, user=user, timeout=10)
            assert error.value.code == 1401

    before = counters()
    unknown = "unknown-" + uuid4().hex
    missing = manager.cancel_requests([unknown], timeout=10)
    assert_all_proxies_answered(missing, set(uris))
    assert missing.not_found == [unknown] and missing.canceled == missing.undetermined == []
    delta(before, {})
    expressions = [f"id >= -{int(uuid4().hex[:7], 16)}" for _ in range(2)]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            expr: pool.submit(
                client.search,
                collection,
                vectors,
                anns_field="vector",
                filter=expr,
                limit=10,
                search_params={"metric_type": "L2"},
                timeout=90,
            )
            for expr, client in zip(expressions, [app, connect(a)])
        }

        def rows():
            return list_complete(manager, set(uris), collection_name=collection, user=user)

        found = wait_for_records(rows, set(expressions), lambda r: r.expr, futures.values(), exact=True)
        ids = [found[e].request_id for e in expressions]
        assert len(set(ids)) == 2
        denied_cancel(ids, user, credential)
        delta(before, {})
        assert set(ids) <= {r.request_id for r in rows()}, "Targets ended before the valid batch cancel"
        result = manager.cancel_requests(ids, reason="metric-batch", timeout=10)
        assert_all_proxies_answered(result, set(uris))
        assert {r.request_id for r in result.canceled} == set(ids)
        assert len(result.canceled) == 2 and result.not_found == result.undetermined == []
        for future in futures.values():
            assert_canceled(future, "metric-batch")
    delta(before, {(a, "Search"): 2})
    repeated = manager.cancel_requests(ids, timeout=10)
    assert_all_proxies_answered(repeated, set(uris))
    assert set(repeated.not_found) == set(ids) and repeated.canceled == repeated.undetermined == []
    delta(before, {(a, "Search"): 2})
