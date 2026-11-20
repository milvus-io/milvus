"""T13 cancellation on representative vector search paths."""

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    TOPK,
    assert_canceled,
    assert_unregistered,
    cancel_exact,
    check_record,
    list_complete,
    missing_active_requests,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


@pytest.fixture
def indexed_search(request, deployment):
    import numpy as np
    from pymilvus import DataType, Function, FunctionType

    path = request.param
    uris, _, _, connect = deployment
    proxy_a = next(iter(uris))
    target, control = connect(proxy_a), connect(proxy_a)
    collection = f"running_t13_{path}_{uuid4().hex[:12]}"
    rows = int(os.getenv("MILVUS_T13_ROWS", "100000"))
    nq = int(os.getenv(f"MILVUS_T13_{path.upper()}_NQ", os.getenv("MILVUS_T13_NQ", "8192")))
    assert rows >= TOPK and 2 <= nq <= 16384
    schema = target.create_schema(auto_id=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    rng = np.random.default_rng(13)
    index_config, search_config = {}, {}
    if path == "binary":
        schema.add_field("vector", DataType.BINARY_VECTOR, dim=128)
        index, metric = "BIN_FLAT", "HAMMING"

        def samples(count):
            return [row.tobytes() for row in rng.integers(0, 256, (count, 16), dtype=np.uint8)]
    elif path in {"sparse", "bm25"}:
        schema.add_field("vector", DataType.SPARSE_FLOAT_VECTOR)
        index, metric = "SPARSE_INVERTED_INDEX", "BM25" if path == "bm25" else "IP"
        if path == "bm25":
            schema.add_field("text", DataType.VARCHAR, max_length=128, enable_analyzer=True)
            schema.add_function(
                Function(
                    name="bm25",
                    input_field_names=["text"],
                    output_field_names=["vector"],
                    function_type=FunctionType.BM25,
                )
            )

        def samples(count):
            if path == "bm25":
                return ["request cancellation" if i % 2 else "request cancellation search" for i in range(count)]
            return [{0: 1.0, int(i): float(rng.random())} for i in rng.integers(1, 64, count)]
    else:
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=128)
        index, metric = "IVF_FLAT", "L2"
        index_config, search_config = {"nlist": 64}, {"nprobe": 64}

        def samples(count):
            return rng.random((count, 128), dtype=np.float32).tolist()

    target.create_collection(collection, schema=schema, consistency_level="Strong", timeout=120)
    request.addfinalizer(lambda: target.drop_collection(collection, timeout=30))
    input_field = "text" if path == "bm25" else "vector"
    for first in range(0, rows, 5000):
        data = samples(min(5000, rows - first))
        target.insert(collection, [{"id": first + i, input_field: value} for i, value in enumerate(data)], timeout=60)
    target.flush(collection, timeout=120)
    indexes = target.prepare_index_params()
    indexes.add_index("vector", index_type=index, metric_type=metric, params=index_config)
    target.create_index(collection, indexes, timeout=120)
    target.load_collection(collection, timeout=120)
    if path == "ivf":
        segments = target.list_loaded_segments(collection, timeout=10)
        assert segments and all(segment.state_name == "Sealed" and segment.index_id > 0 for segment in segments), (
            segments
        )

    def invoke(client, data, *, filter_expr="", limit=TOPK):
        return client.search(
            collection,
            data,
            anns_field="vector",
            limit=limit,
            filter=filter_expr,
            search_params={"metric_type": metric, "params": search_config},
            timeout=QUERY_TIMEOUT,
        )

    return path, collection, target, control, samples(nq), invoke


def assert_ranked_control(actual, baseline, vectors, invoke, control, *, descending):
    assert len(actual) == len(baseline) == len(vectors)
    verified = set()
    for hits, expected, vector in zip(actual, baseline, vectors):
        scores = [hit["distance"] for hit in hits]
        expected_scores = [hit["distance"] for hit in expected]
        assert len(scores) == len(expected_scores) == TOPK
        assert scores == sorted(scores, reverse=descending)
        assert scores == pytest.approx(expected_scores)
        ids = {hit["id"] for hit in hits}
        assert len(ids) == TOPK
        cutoff = expected_scores[-1]
        assert {hit["id"] for hit in expected if hit["distance"] != cutoff} <= ids
        extra = ids - {hit["id"] for hit in expected}
        if not extra:
            continue
        assert all(hit["distance"] == pytest.approx(cutoff) for hit in hits if hit["id"] in extra)
        key = repr(vector), frozenset(extra), cutoff
        if key in verified:
            continue
        (candidates,) = invoke(control, [vector], filter_expr=f"id in {sorted(extra)}", limit=len(extra))
        assert {hit["id"] for hit in candidates} == extra
        assert all(hit["distance"] == pytest.approx(cutoff) for hit in candidates)
        verified.add(key)


@pytest.mark.parametrize("indexed_search", ["binary", "sparse", "bm25", "ivf"], indirect=True)
def test_t13_cancel_indexed_search_with_control(deployment, indexed_search):
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    path, collection, target, control, vectors, invoke = indexed_search
    operator = connect(proxy_b)
    nq = len(vectors)
    baseline = invoke(control, vectors)
    start = Barrier(2, timeout=5)

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    def run(client, data):
        start.wait()
        return invoke(client, data)

    reason = f"T13-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=2) as pool:
        canceled = pool.submit(run, target, vectors[:-1])
        surviving = pool.submit(run, control, vectors)
        snapshot = wait_for_records(records, {nq - 1, nq}, lambda row: row.nq, (canceled, surviving), exact=True)
        for row in snapshot.values():
            check_record(row, proxy_a, user, collection)
            assert row.type == "Search" and row.topk == TOPK
        target_id, control_id = (snapshot[size].request_id for size in (nq - 1, nq))
        assert target_id != control_id
        cancel_exact(operator, set(uris), [target_id], reason)
        missing = missing_active_requests(records, {control_id})
        assert_canceled(canceled, reason)
        actual = surviving.result(timeout=QUERY_TIMEOUT)
    assert_unregistered(records, {target_id, control_id})
    assert_ranked_control(actual, baseline, vectors, invoke, control, descending=path in {"bm25", "sparse"})
    assert not missing, f"OBSERVATION_WINDOW: control overlap unverified: {missing}"
