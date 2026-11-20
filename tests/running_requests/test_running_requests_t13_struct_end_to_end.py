"""Cancel original StructArray Search and Query on Java, Go and REST."""

import json
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import numpy as np
import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import wait_for_records
from test_running_requests_native_sdk import canceled, capture, listed, terminal
from test_running_requests_rest_cross_proxy import entity_request

pytestmark = pytest.mark.tags("L1")
STRUCT_FILTER = "MATCH_ANY(chunks, $[value] == 0)"
QUERY_COUNT = 2048


def embedding_inputs():
    return [[np.random.default_rng(14).random(32).tolist()]] * QUERY_COUNT


def assert_query_row(row):
    assert int(row["id"]) == 0
    chunks = row["chunks"]
    if isinstance(chunks, dict):
        # Go exposes the struct as a map of columns.
        values, embeddings = chunks["value"], chunks["embedding"]
    else:
        values = [item["value"] for item in chunks]
        embeddings = [item["embedding"] for item in chunks]
    assert values == [0, 0]
    assert len(embeddings) == 2 and all(len(vector) == 32 for vector in embeddings)


def assert_search_control(hits, baseline):
    assert len(hits) == 10 and all(int(hit["id"]) % 2 == 0 for hit in hits)
    assert [int(hit["id"]) for hit in hits] == [int(hit["id"]) for hit in baseline]
    assert [hit["distance"] for hit in hits] == pytest.approx([hit["distance"] for hit in baseline])


@pytest.mark.parametrize("kind", ["Search", "Query"])
def test_t13_native_struct_cancel(kind, native, struct_data, tmp_path):
    _, collection, count = struct_data
    expr = f"id >= -{int(uuid4().hex[:7], 16) + 1} and {STRUCT_FILTER}"
    data = dict(Collection=collection, Kind=kind)
    if kind == "Search":
        path = tmp_path / "struct-embeddings.json"
        path.write_text(json.dumps(embedding_inputs()), encoding="utf-8")
        data.update(
            Vectors=str(path), VectorKind="struct-embedding", AnnsField="chunks[embedding]", NQ=QUERY_COUNT, Limit=10
        )
    else:
        data.update(OutputFields=["id", "chunks"], Limit=min(count // 2, 16000))

    def control():
        call = uuid4().hex
        args = dict(data, NQ=1) if kind == "Search" else dict(data, Limit=1, ReturnQueryRows=True)
        native.ok("start", "a", **args, Call=call, Expr=expr if kind == "Search" else f"id == 0 and {expr}")
        result = native.ok("await", Call=call)
        if kind == "Search":
            assert len(result["hits"]) == 1
            return result["hits"][0]
        assert result["count"] == len(result["rows"]) == 1, result
        assert_query_row(result["rows"][0])

    baseline = control()
    call = uuid4().hex
    native.ok("start", "a", **data, Call=call, Expr=expr)
    request_id = capture(native, data, call, expr, kind=kind)["request_id"]
    reason = "t13-native-struct-" + uuid4().hex
    result = canceled(native, [request_id], reason)
    assert [row["request_id"] for row in result["canceled"]] == [request_id], result
    terminal(native, call, reason)
    assert request_id not in {row["request_id"] for row in listed(native, Collection=collection)}
    actual = control()
    if kind == "Search":
        assert_search_control(actual, baseline)


@pytest.mark.parametrize("kind", ["Search", "Query"])
def test_t13_rest_struct_cancel(kind, deployment, struct_data):
    uris, token, user, _ = deployment
    a, b = uris
    _, collection, count = struct_data
    expr = f"id >= -{int(uuid4().hex[:7], 16) + 1} and {STRUCT_FILTER}"
    vectors = embedding_inputs() if kind == "Search" else None

    def invoke(control=False):
        payload = {"collectionName": collection, "filter": expr}
        if kind == "Search":
            payload.update(
                data=vectors[:1] if control else vectors,
                annsField="chunks[embedding]",
                limit=10,
                searchParams={"params": {"ef": 64}},
            )
        else:
            if control:
                payload["filter"] = f"id == 0 and {expr}"
            payload.update(outputFields=["id", "chunks"], limit=1 if control else min(count // 2, 16000))
        return entity_request(uris[a], token, kind, payload)

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

    def control():
        result = invoke(control=True)
        assert result["code"] == 0, result
        if kind == "Search":
            assert result["topks"] == [10], result
            return result["data"]
        assert len(result["data"]) == 1, result
        assert_query_row(result["data"][0])

    baseline = control()
    reason = "t13-rest-struct-" + uuid4().hex
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(invoke)
        row = wait_for_records(records, {expr}, lambda item: item["expr"], [future])[expr]
        assert row["proxyId"] == a and row["type"] == kind and row["cancellable"], row
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
    actual = control()
    if kind == "Search":
        assert_search_control(actual, baseline)
