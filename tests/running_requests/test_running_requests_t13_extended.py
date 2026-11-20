"""Real Iterator batch RPCs and StructArray cancellation paths."""

import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, nullcontext
from importlib import import_module
from time import monotonic, sleep
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    assert_all_proxies_answered,
    assert_canceled,
    assert_unregistered,
    list_complete,
)

pytestmark = pytest.mark.tags("L1")


def iterator_finish_checkpoint(collection, kind):
    if kind.lower() != "search" or os.getenv("MILVUS_T13_ITERATOR_CHECKPOINT") != "1":
        return nullcontext()
    provider = import_module(os.environ["MILVUS_L1_CHECKPOINT_PROVIDER"])
    return provider.hold_request_exit(collection=collection, state="Running")


def cancel_inflight(
    operator,
    proxy_ids,
    collection,
    user,
    expr,
    invoke,
    expected_type,
    expected_proxy,
    rpc_calls=None,
    *,
    finish_checkpoint=None,
):
    """Observe only the selected actual RPC; never predict request IDs."""

    def records():
        return list_complete(operator, proxy_ids, collection_name=collection, user=user)

    reason = f"T13-extended-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool, ExitStack() as held:
        checkpoint = held.enter_context(finish_checkpoint) if finish_checkpoint else None
        future = pool.submit(invoke)
        hit = checkpoint.wait(timeout=15) if checkpoint else None
        deadline = monotonic() + 90
        while monotonic() < deadline:
            rows = [r for r in records() if expr in r.expr and r.state in ("Running", "Queued")]
            if rows:
                assert len(rows) == 1, rows
                row = rows[0]
                assert (row.type, row.proxy_id, row.user, row.collection_name) == (
                    expected_type,
                    expected_proxy,
                    user,
                    collection,
                ), f"TARGET_MISMATCH: {row}"
                if hit:
                    assert row.request_id == hit.request_id
                if rpc_calls is not None:
                    assert len(rpc_calls) == 1, "Expected one actual batch RPC"
                result = operator.cancel_requests([row.request_id], reason=reason, timeout=10)
                held.close()
                print(f"type={row.type} request_id={row.request_id} trace_id={row.trace_id} cancel={result}")
                assert_all_proxies_answered(result, proxy_ids)
                assert result.undetermined == [], f"UNDETERMINED_CANCEL: {result}"
                if result.not_found:
                    assert result.not_found == [row.request_id] and result.canceled == [], result
                    # Exceptions (including 3002) propagate: not_found alone is
                    # not sufficient evidence that the request finished normally.
                    completed = future.result(timeout=90)
                    assert len(completed) > 0, "Unexpected empty successful batch"
                    assert_unregistered(records, {row.request_id})
                    if rpc_calls is not None:
                        assert len(rpc_calls) == 1, "Batch RPC was retransmitted"
                    pytest.fail(
                        f"OBSERVATION_WINDOW: {row.request_id} not_found with complete nodes; original call completed normally ({len(completed)} rows)"
                    )
                assert result.not_found == [], result
                assert [r.request_id for r in result.canceled] == [row.request_id], result
                canceled = result.canceled[0]
                assert (canceled.type, canceled.proxy_id, canceled.user, canceled.collection_name) == (
                    expected_type,
                    expected_proxy,
                    user,
                    collection,
                ), canceled
                assert_canceled(future, reason)
                if rpc_calls is not None:
                    assert len(rpc_calls) == 1, "Canceled batch RPC was retried"
                break
            if future.done():
                future.result()
                pytest.fail("OBSERVATION_WINDOW: batch/request completed before List captured it")
            sleep(0.01)
        else:
            pytest.fail("OBSERVATION_WINDOW: no matching request within 90 seconds")
    assert_unregistered(records, {row.request_id})


@pytest.mark.parametrize("kind", ["search", "query"], ids=["search-iterator", "query-iterator"])
def test_t13_iterator_batch_cancel(kind, deployment, workload, monkeypatch):
    check_iterator_cancel(kind, deployment, workload, monkeypatch)


def check_iterator_cancel(kind, deployment, workload, monkeypatch, *, operator=None):
    uris, _, user, connect = deployment
    a, b = uris
    app, control, collection, vectors = workload
    operator = connect(b) if operator is None else operator
    expr = f"id >= -{int(uuid4().hex[:7], 16)}"

    def make(client):
        if kind == "search":
            return client.search_iterator(
                collection,
                data=vectors[:1].tolist(),
                anns_field="vector",
                batch_size=8192,
                limit=24576,
                filter=expr,
                search_params={"metric_type": "L2"},
                timeout=90,
            )
        return client.query_iterator(
            collection, batch_size=16384, limit=49152, filter=expr, output_fields=["id", "vector"], timeout=90
        )

    target = make(app)
    reference = make(control)
    try:
        baseline = reference.next()
        assert len(baseline) > 0
        calls = []
        handler = app._get_connection()
        method = "Search" if kind == "search" else "Query"
        original = getattr(handler._stub, method)

        def counted(*args, **kwargs):
            calls.append(1)
            return original(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(handler._stub, method, counted)
            cancel_inflight(
                operator,
                set(uris),
                collection,
                user,
                expr,
                target.next,
                method,
                a,
                calls,
                finish_checkpoint=iterator_finish_checkpoint(collection, kind),
            )
        # Independent iterator remains usable after target batch cancellation.
        following = reference.next()
        assert len(following) > 0
        if kind == "search":
            assert set(baseline.ids()).isdisjoint(following.ids())
        else:
            assert {r["id"] for r in baseline}.isdisjoint(r["id"] for r in following)
            assert all(len(r["vector"]) == 128 for r in following)
    finally:
        target.close()
        reference.close()


@pytest.fixture
def struct_data(deployment, request):
    import numpy as np
    from pymilvus import DataType

    uris, _, _, connect = deployment
    app = connect(next(iter(uris)))
    name = "running_struct_" + uuid4().hex[:12]
    schema = app.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    struct = app.create_struct_field_schema()
    struct.add_field("value", DataType.INT64)
    struct.add_field("embedding", DataType.FLOAT_VECTOR, dim=32)
    schema.add_field("chunks", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=struct, max_capacity=2)
    app.create_collection(name, schema=schema, consistency_level="Strong", timeout=90)
    request.addfinalizer(lambda: app.drop_collection(name, timeout=60))
    count = int(os.getenv("MILVUS_T13_STRUCT_ROWS", "20000"))
    rng = np.random.default_rng(1313)
    for start in range(0, count, 1000):
        data = [
            {
                "id": i,
                "chunks": [
                    {"value": i % 2, "embedding": rng.random(32).tolist()},
                    {"value": i % 2, "embedding": rng.random(32).tolist()},
                ],
            }
            for i in range(start, min(start + 1000, count))
        ]
        app.insert(name, data, timeout=60)
    app.flush(name, timeout=120)
    indexes = app.prepare_index_params()
    indexes.add_index(
        "chunks[embedding]", index_type="HNSW", metric_type="MAX_SIM_COSINE", params={"M": 8, "efConstruction": 64}
    )
    app.create_index(name, indexes, timeout=120)
    app.load_collection(name, timeout=120)
    return app, name, count


@pytest.mark.parametrize("kind", ["search", "query"], ids=["struct-search", "struct-query"])
def test_t13_struct_cancel(kind, deployment, struct_data):
    check_struct_cancel(kind, deployment, struct_data)


def check_struct_cancel(kind, deployment, struct_data, *, operator=None):
    import numpy as np
    from pymilvus.client.embedding_list import EmbeddingList

    uris, _, user, connect = deployment
    a, b = uris
    app, collection, count = struct_data
    operator = connect(b) if operator is None else operator
    control = connect(a)
    expr = "MATCH_ANY(chunks, $[value] == 0)"
    query = EmbeddingList()
    query.add(np.random.default_rng(14).random(32).tolist())

    def invoke(client, nq):
        if kind == "search":
            return client.search(
                collection,
                [query] * nq,
                anns_field="chunks[embedding]",
                filter=expr,
                limit=10,
                search_params={"metric_type": "MAX_SIM_COSINE", "params": {"ef": 64}},
                timeout=90,
            )
        return client.query(
            collection, filter=expr, output_fields=["id", "chunks"], limit=min(count // 2, 16000), timeout=90
        )

    baseline = invoke(control, 1)
    if kind == "query":
        assert sorted(r["id"] for r in baseline) == list(range(0, min(count, 32000), 2))
        assert all(c["value"] == 0 and len(c["embedding"]) == 32 for r in baseline for c in r["chunks"])
    else:
        assert len(baseline[0]) == 10
        assert all(h["id"] % 2 == 0 for h in baseline[0])
    cancel_inflight(
        operator,
        set(uris),
        collection,
        user,
        expr,
        lambda: invoke(app, 2048),
        "Search" if kind == "search" else "Query",
        a,
    )
    actual = invoke(control, 1)
    if kind == "query":
        assert sorted(list(actual), key=lambda r: r["id"]) == sorted(list(baseline), key=lambda r: r["id"])
    else:
        assert [h["id"] for h in actual[0]] == [h["id"] for h in baseline[0]]
        assert [h["distance"] for h in actual[0]] == pytest.approx([h["distance"] for h in baseline[0]])
