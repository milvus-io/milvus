"""Original-client Search and cancellation on MMAP, nullable and evolved schemas."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import numpy as np
import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import wait_for_records
from test_running_requests_native_sdk import canceled, capture, listed, terminal
from test_running_requests_rest_cross_proxy import entity_request
from test_running_requests_schema_regression import prepare_schema_search

pytestmark = pytest.mark.tags("L1")
VARIANTS = ["mmap", "nullable", "add_field"]


@pytest.fixture
def schema_search(request, deployment, variant):
    uris, _, _, connect = deployment
    name, vectors = prepare_schema_search(variant, connect(next(iter(uris))), request)
    expr = f"id >= -{int(uuid4().hex[:7], 16) + 1}"
    if variant == "nullable":
        expr += " and extra is null"
    return name, vectors, expr


def assert_control(hit, fields, variant):
    assert int(hit["id"]) == 0
    assert hit["distance"] == pytest.approx(0, abs=1e-5)
    if variant == "nullable":
        assert fields["extra"] is None
    elif variant == "add_field":
        assert fields["extra"] == 7


@pytest.mark.parametrize("variant", VARIANTS)
def test_t13_native_search_schema_cancel(variant, native, schema_search, tmp_path):
    collection, vectors, expr = schema_search
    vector_file = tmp_path / "schema-vectors.f32"
    np.asarray(vectors[:16384], dtype="<f4").tofile(vector_file)
    data = dict(Collection=collection, Kind="Search", Vectors=str(vector_file), Limit=1)
    fields = {} if variant == "mmap" else {"OutputFields": ["extra"]}

    def check():
        call = uuid4().hex
        native.ok("start", "a", **data, **fields, Call=call, Expr=expr, NQ=1)
        hits = native.ok("await", Call=call)["hits"]
        assert len(hits) == len(hits[0]) == 1, hits
        hit = hits[0][0]
        assert_control(hit, hit.get("entity", {}), variant)

    check()
    target = uuid4().hex
    target_data = dict(data, NQ=16384)
    native.ok("start", "a", **target_data, **fields, Call=target, Expr=expr)
    request_id = capture(native, target_data, target, expr)["request_id"]
    reason = "t13-schema-" + uuid4().hex
    result = canceled(native, [request_id], reason)
    assert [row["request_id"] for row in result["canceled"]] == [request_id], result
    terminal(native, target, reason)
    assert request_id not in {row["request_id"] for row in listed(native, Collection=collection)}
    check()


@pytest.mark.parametrize("variant", VARIANTS)
def test_t13_rest_search_schema_cancel(variant, deployment, schema_search):
    uris, token, user, _ = deployment
    a, b = uris
    collection, vectors, expr = schema_search
    inputs = vectors[:8192].astype("float64").round(6).tolist()

    def invoke(query_vectors):
        return entity_request(
            uris[a],
            token,
            "Search",
            {
                "collectionName": collection,
                "annsField": "vector",
                "data": query_vectors,
                "limit": 1,
                "filter": expr,
                "outputFields": [] if variant == "mmap" else ["extra"],
            },
        )

    def records():
        return post_rest(
            uris[b],
            token,
            set(uris),
            "list",
            {
                "collectionName": collection,
                "user": user,
            },
        )["requests"]

    def check():
        result = invoke(inputs[:1])
        assert result["code"] == 0 and result["topks"] == [1] and len(result["data"]) == 1, result
        hit = result["data"][0]
        assert_control(hit, hit, variant)

    check()
    reason = "t13-rest-schema-" + uuid4().hex
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(invoke, inputs)
        row = wait_for_records(records, {expr}, lambda item: item["expr"], [future])[expr]
        assert row["proxyId"] == a and row["type"] == "Search" and row["cancellable"], row
        request_id = row["requestId"]
        result = post_rest(
            uris[b],
            token,
            set(uris),
            "cancel",
            {
                "requestIds": [request_id],
                "reason": reason,
            },
        )
        assert [item["requestId"] for item in result["canceled"]] == [request_id], result
        assert result["notFound"] == result["undetermined"] == [], result
        outcome = future.result(timeout=95)
        assert outcome["code"] == 3002 and reason in outcome["message"], outcome
    assert request_id not in {row["requestId"] for row in records()}
    check()
