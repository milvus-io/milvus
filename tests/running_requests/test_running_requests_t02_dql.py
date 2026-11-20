"""TC-L1 functional scenario T02."""

import math
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from statistics import median
from threading import Barrier, Event
from time import perf_counter
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    TOPK,
    ObservationWindowError,
    assert_canceled,
    assert_registered,
    assert_search_baseline,
    assert_unregistered,
    cancel_exact,
    check_record,
    list_complete,
    missing_active_requests,
    required_env,
    search,
    search_hits,
    wait_for_records,
)
from running_requests_topology import ProxyAuthorizationTopology, run
from setup_running_requests_l0 import VECTOR_DIMENSION
from test_running_requests_l0 import assert_all_proxies_answered

pytestmark = pytest.mark.tags("L1")


@pytest.fixture
def hybrid_workload(request, deployment):
    import numpy as np
    from pymilvus import AnnSearchRequest, DataType, RRFRanker

    uris, _, _, connect = deployment
    client = connect(next(iter(uris)))
    name = f"running_requests_l1_hybrid_{uuid4().hex}"
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    for field in ("vector_a", "vector_b"):
        schema.add_field(field, DataType.FLOAT_VECTOR, dim=128)
    client.create_collection(name, schema=schema, consistency_level="Strong", timeout=120)
    request.addfinalizer(lambda: client.drop_collection(name, timeout=30))
    random = np.random.default_rng()
    rows = int(os.environ.get("MILVUS_L0_ROWS", "100000"))
    for start in range(0, rows, 5000):
        count = min(5000, rows - start)
        vectors_a, vectors_b = random.random((2, count, 128), dtype=np.float32).tolist()
        client.insert(
            name,
            [
                {"id": start + offset, "vector_a": a, "vector_b": b}
                for offset, (a, b) in enumerate(zip(vectors_a, vectors_b))
            ],
            timeout=60,
        )
    client.flush(name, timeout=120)
    indexes = client.prepare_index_params()
    for field in ("vector_a", "vector_b"):
        indexes.add_index(field, index_type="FLAT", metric_type="L2")
    client.create_index(name, indexes, timeout=120)
    client.load_collection(name, timeout=120)
    nq = int(os.environ.get("MILVUS_L1_HYBRID_NQ", "2048"))
    inputs = random.random((2, nq, 128), dtype=np.float32).tolist()

    def query(app=client, *, trim=0, **kwargs):
        requests = [
            AnnSearchRequest(
                data=data[: nq - trim],
                anns_field=field,
                param={"metric_type": "L2"},
                limit=TOPK,
            )
            for field, data in zip(("vector_a", "vector_b"), inputs)
        ]
        return app.hybrid_search(
            name,
            requests,
            RRFRanker(),
            limit=TOPK,
            timeout=QUERY_TIMEOUT,
            **kwargs,
        )

    return name, query


@pytest.mark.parametrize("query_type", ["Search", "HybridSearch"])
def test_t02_list_and_cancel_dql(query_type, deployment, request):
    """Cancel one DQL call and verify its concurrent control still returns the expected result."""
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    operator = connect(proxy_b)
    if query_type == "HybridSearch":
        collection, hybrid = request.getfixturevalue("hybrid_workload")
        target_client, control_client = connect(proxy_a), connect(proxy_a)
        nq = int(os.environ.get("MILVUS_L1_HYBRID_NQ", "2048"))
        assert nq >= 2
        baseline = search_hits(hybrid(control_client), nq, TOPK)
        # HybridSearch reports the sum of nq across the two ANN subrequests.
        target_key, control_key = 2 * (nq - 1), 2 * nq
        calls = {
            target_key: lambda: hybrid(target_client, trim=1),
            control_key: lambda: hybrid(control_client),
        }
    else:
        target_client, control_client, collection, vectors = request.getfixturevalue("workload")
        nq = len(vectors)
        baseline = request.getfixturevalue("workload_baseline")()
        calls = {
            nq - 1: lambda: search(target_client, collection, vectors[:-1]),
            nq: lambda: search(control_client, collection, vectors),
        }
        target_key, control_key = nq - 1, nq

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    reason = f"T02-{query_type}-{uuid4().hex}"
    start = Barrier(2)

    def invoke(call):
        start.wait(timeout=5)
        return call()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {key: pool.submit(invoke, call) for key, call in calls.items()}
        snapshot = wait_for_records(
            records,
            {(query_type, key) for key in calls},
            lambda row: (row.type, row.nq),
            futures.values(),
            exact=True,
        )
        target, control = (snapshot[query_type, key] for key in (target_key, control_key))
        for row in (target, control):
            check_record(row, proxy_a, user, collection)
            assert row.topk == TOPK
        assert target.request_id != control.request_id
        assert not futures[target_key].done(), "Target completed before Cancel"
        cancel_exact(operator, set(uris), [target.request_id], reason)
        assert_registered(records, {control.request_id})
        assert_canceled(futures[target_key], reason)
        assert_search_baseline(futures[control_key].result(timeout=QUERY_TIMEOUT), baseline, nq)
    assert_unregistered(records, {target.request_id, control.request_id})


def test_t02_inflight_insert_is_not_listed(deployment, request):
    """Confirm an active Insert is outside the running request registry."""
    _assert_non_dql_excluded("Insert", deployment, request)


@pytest.mark.parametrize("operation", ["CreateIndex"])
def test_t02_inflight_non_dql_is_not_listed(operation, deployment, request):
    """Confirm an active CreateIndex RPC is outside the running request registry."""
    _assert_non_dql_excluded(operation, deployment, request)


@pytest.mark.parametrize("operation", ["Insert", "CreateIndex"])
def test_t02_rest_list_excludes_non_dql(operation, deployment, request):
    _assert_non_dql_excluded(operation, deployment, request, rest=True)


@pytest.mark.parametrize("operation", ["Insert", "CreateIndex"])
@pytest.mark.parametrize("language", ["java", "go"])
def test_t02_native_list_excludes_non_dql(operation, language, deployment, request, tmp_path):
    _assert_non_dql_excluded(operation, deployment, request, language=language, log_dir=tmp_path)


def _assert_non_dql_excluded(operation, deployment, request, *, rest=False, language=None, log_dir=None):
    if os.environ.get("MILVUS_T02_ACCESS_LOG", os.environ.get("MILVUS_T02_INSERT_ACCESS_LOG")) != "1":
        pytest.skip("Enable the T02 Proxy access log formatter to verify RPC overlap")

    uris, token, user, connect = deployment
    proxy_a = next(iter(uris))
    topology = ProxyAuthorizationTopology()
    pod = topology.proxy_pods()[proxy_a]

    rest_method = "/v2/vectordb/requests/running/list"
    list_method = rest_method if rest else "ListRunningRequests"

    def timing_logs():
        logs = run([*topology.kube, "logs", pod, "-c", "proxy", "--since=10m"])
        timings = {}
        for request_id, method, start, end in re.findall(
            r"^\[T02-RPC\] \[([0-9a-f]{32})\] \[(Insert|CreateIndex|ListRunningRequests|/v2/vectordb/requests/running/list)\] "
            r"\[([^\]]+)\] \[([^\]]+)\]$",
            logs,
            re.MULTILINE,
        ):
            bounds = tuple(datetime.strptime(value, "%Y/%m/%d %H:%M:%S.%f %z") for value in (start, end))
            timings.setdefault((request_id, method), []).append(bounds)
        return timings

    samplers = 1 if operation == "Insert" else 4
    if language:
        from test_running_requests_native_sdk import Native, complete

        workers = []
        for slot in range(samplers):
            worker = Native(language, log_dir / f"list-{slot}.stderr")
            request.addfinalizer(worker.close)
            worker.nodes = set(uris)
            worker.ok("connect", "a", URI=uris[proxy_a], Token=token, DB="default")
            workers.append(worker)

        def sample_list(list_id, collection=None, slot=0):
            worker = workers[slot]
            response = worker.call("list", "a", User=user, Collection=collection or "", RequestID=list_id)
            return complete(worker, response)["requests"]
    elif rest:
        from running_requests_l1_rest_support import post_rest

        def sample_list(list_id, collection=None, slot=0):
            payload = {"user": user}
            if collection is not None:
                payload["collectionName"] = collection
            traceparent = f"00-{list_id}-{uuid4().hex[:16]}-01"
            return post_rest(uris[proxy_a], token, set(uris), "list", payload, headers={"traceparent": traceparent})[
                "requests"
            ]
    else:
        operator = connect(proxy_a)

        def sample_list(list_id, collection=None, slot=0):
            filters = {"user": user, "client_request_id": list_id}
            if collection is not None:
                filters["collection_name"] = collection
            return list_complete(operator, set(uris), **filters)

    warmup_id = uuid4().hex
    sample_list(warmup_id)
    assert (warmup_id, list_method) in timing_logs(), f"Missing correlated T02 access log for {list_method}"

    import numpy as np
    from pymilvus import DataType

    writer = connect(proxy_a)
    schema = writer.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=VECTOR_DIMENSION)

    batch_size = int(os.environ.get("MILVUS_T02_INSERT_ROWS", "32768"))
    if operation == "Insert":
        assert batch_size > 0
        vector = np.random.default_rng(2026).random(VECTOR_DIMENSION, dtype=np.float32).tolist()
        data = [{"id": offset, "vector": vector} for offset in range(batch_size)]
    else:
        indexes = writer.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")

    def invoke(collection, request_id):
        if operation == "Insert":
            result = writer.insert(collection, data, timeout=QUERY_TIMEOUT, client_request_id=request_id)
            assert result["insert_count"] == batch_size
        else:
            # The SDK's build polling can outlive the CreateIndex RPC on Proxy.
            writer.create_index(collection, indexes, sync=False, timeout=QUERY_TIMEOUT, client_request_id=request_id)

    attempts = 4 if operation == "Insert" else 12
    with ThreadPoolExecutor(max_workers=samplers) as pool:
        for _ in range(attempts):
            collection = f"running_requests_l1_{operation.lower()}_{uuid4().hex}"
            writer.create_collection(collection, schema=schema, timeout=30)
            request.addfinalizer(lambda name=collection: writer.drop_collection(name, timeout=30))

            finished = Event()
            ready = Barrier(samplers + 1)

            def sample(number):
                snapshots = {}
                ready.wait(timeout=10)
                # Spread the initial List calls across the short CreateIndex RPC.
                finished.wait(number * 0.005)
                while not finished.is_set():
                    list_id = uuid4().hex
                    snapshots[list_id] = sample_list(list_id, collection, number)
                return snapshots

            samples = [pool.submit(sample, number) for number in range(samplers)]
            rpc_id = uuid4().hex
            try:
                ready.wait(timeout=10)
                invoke(collection, rpc_id)
            finally:
                finished.set()

            snapshots = {}
            for sample_call in samples:
                snapshots.update(sample_call.result())
            timings = timing_logs()
            spans = timings.get((rpc_id, operation), [])
            assert len(spans) == 1, f"Expected one access log for {operation} {rpc_id}: {spans}"
            ((rpc_start, rpc_end),) = spans
            for list_id, snapshot in snapshots.items():
                bounds = timings.get((list_id, list_method), [])
                assert len(bounds) == 1, f"Expected one access log for List {list_id}: {bounds}"
                ((list_start, list_end),) = bounds
                if rpc_start < list_start <= list_end < rpc_end:
                    print(
                        f"{pod} {operation} {rpc_id}: {rpc_start.isoformat()} → {rpc_end.isoformat()} "
                        f"List {list_id}: {list_start.isoformat()} → {list_end.isoformat()}"
                    )
                    assert not snapshot, f"Non-DQL collection unexpectedly appeared in List: {snapshot}"
                    return
    raise ObservationWindowError(f"No complete List RPC was within a {operation} RPC on Proxy A")


@pytest.fixture
def query_data(deployment):
    (collection,) = required_env("MILVUS_L0_COLLECTION")
    uris, _, _, connect = deployment
    app = connect(next(iter(uris)))
    assert app.get_load_state(collection, timeout=10)["state"].name == "Loaded"
    rows = int(app.get_collection_stats(collection, timeout=10)["row_count"])
    assert rows >= 2
    limit = int(os.environ.get("MILVUS_L1_QUERY_LIMIT", "12000"))
    assert 1 <= limit and 2 * limit <= rows
    return collection, limit


def fetch_rows(client, collection, expr, limit):
    return client.query(
        collection,
        filter=expr,
        output_fields=["id", "vector"],
        limit=limit,
        timeout=QUERY_TIMEOUT,
    )


def assert_rows(result, parity, limit):
    assert sorted(row["id"] for row in result) == list(range(parity, 2 * limit, 2))
    for row in result:
        vector = row["vector"]
        assert len(vector) == VECTOR_DIMENSION
        assert all(math.isfinite(value) for value in vector)


def test_t02_query_calibrate(deployment, query_data):
    """Measure the SDK round trip; use server traces for the execution window."""
    uris, _, _, connect = deployment
    app = connect(next(iter(uris)))
    collection, limit = query_data
    timings = []
    expr = f"id < {2 * limit} and id % 2 == 0"
    for _ in range(3):
        start = perf_counter()
        result = fetch_rows(app, collection, expr, limit)
        timings.append(perf_counter() - start)
        assert_rows(result, 0, limit)
    print(f"returned_rows={limit} client_rpc_seconds={timings} median={median(timings):.3f}")


def test_t02_query_cancel_with_concurrent_control(deployment, query_data):
    """Verify in-flight cancellation; List alone does not prove QueryNode execution."""
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    operator = connect(proxy_b)
    collection, limit = query_data
    target_expr, control_expr = (f"id < {2 * limit} and id % 2 == {parity}" for parity in (0, 1))
    clients = {expr: connect(proxy_a) for expr in (target_expr, control_expr)}

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    def attempt():
        start = Barrier(2)

        def invoke(expr):
            start.wait(timeout=5)
            return fetch_rows(clients[expr], collection, expr, limit)

        reason = f"T02-Query-{uuid4().hex}"
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {expr: pool.submit(invoke, expr) for expr in clients}
            snapshot = wait_for_records(
                records,
                set(clients),
                lambda row: row.expr if row.type == "Query" else None,
                futures.values(),
                exact=True,
            )
            target, control = snapshot[target_expr], snapshot[control_expr]
            for row in (target, control):
                check_record(row, proxy_a, user, collection)
                print(f"expr={row.expr!r} request_id={row.request_id} trace_id={row.trace_id}")
            assert target.request_id != control.request_id
            result = operator.cancel_requests([target.request_id], reason=reason, timeout=10)
            assert_all_proxies_answered(result, set(uris))
            assert not result.undetermined, result
            ids = {target.request_id, control.request_id}
            if not result.canceled and list(result.not_found) == [target.request_id]:
                assert_rows(futures[target_expr].result(timeout=QUERY_TIMEOUT), 0, limit)
                assert_rows(futures[control_expr].result(timeout=QUERY_TIMEOUT), 1, limit)
                assert_unregistered(records, ids)
                raise ObservationWindowError(f"Target {target.request_id} completed before Cancel")
            assert [row.request_id for row in result.canceled] == [target.request_id], result
            assert not result.not_found, result
            missing = missing_active_requests(records, {control.request_id})
            assert_canceled(futures[target_expr], reason)
            assert_rows(futures[control_expr].result(timeout=QUERY_TIMEOUT), 1, limit)
        assert_unregistered(records, ids)
        if missing:
            raise ObservationWindowError(f"Control overlap with Cancel is unverified: {missing}")

    misses = []
    for _ in range(3):
        try:
            attempt()
            return
        except ObservationWindowError as exc:
            misses.append(str(exc))
    pytest.fail(f"OBSERVATION_WINDOW: no valid Query window in 3 attempts: {misses}")
