"""Originating-client cancellation on indexed T13 collections."""

import json
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import numpy as np
import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import TOPK, wait_for_records
from test_running_requests_native_sdk import canceled, capture, listed, start, terminal
from test_running_requests_rest_cross_proxy import entity_request
from test_running_requests_t13_search_paths import assert_ranked_control

pytestmark = pytest.mark.tags("L1")
NPROBE = 64


@pytest.mark.parametrize("indexed_search", ["ivf"], indirect=True)
def test_t13_native_ivf_search_cancel(indexed_search, native, tmp_path):
    check_native_indexed_search_cancel(indexed_search, native, tmp_path)


@pytest.mark.parametrize("indexed_search", ["sparse", "bm25", "binary"], indirect=True)
def test_t13_native_specialized_search_cancel(indexed_search, native, tmp_path):
    check_native_indexed_search_cancel(indexed_search, native, tmp_path)


def wire_vectors(path, inputs):
    if path == "binary":
        return [b64encode(value).decode("ascii") for value in inputs]
    return inputs


def check_native_indexed_search_cancel(indexed_search, native, tmp_path):
    path, collection, _, control, vectors, python_search = indexed_search
    binary = tmp_path / "ivf-vectors.f32"
    data = dict(Collection=collection, NQ=len(vectors), Limit=TOPK)
    if path == "ivf":
        np.asarray(vectors, dtype="<f4").tofile(binary)
        data.update(Vectors=str(binary), NProbe=NPROBE)
    else:
        inputs = tmp_path / "indexed-inputs.json"
        inputs.write_text(json.dumps(wire_vectors(path, vectors), ensure_ascii=False), encoding="utf-8")
        data.update(Vectors=str(inputs), VectorKind=path)
    baseline = python_search(control, vectors[:1])

    call, expr = start(native, data)
    row = capture(native, data, call, expr)
    request_id = row["request_id"]
    reason = f"t13-native-{path}-" + uuid4().hex
    result = canceled(native, [request_id], reason)
    assert [item["request_id"] for item in result["canceled"]] == [request_id], result
    terminal(native, call, reason)
    assert request_id not in {item["request_id"] for item in listed(native, Collection=collection)}

    control_call, _ = start(native, dict(data, NQ=1))
    actual = native.ok("await", Call=control_call)["hits"]
    assert_ranked_control(actual, baseline, vectors[:1], python_search, control, descending=path in {"sparse", "bm25"})


@pytest.mark.parametrize("indexed_search", ["ivf"], indirect=True)
def test_t13_rest_ivf_search_cancel(deployment, indexed_search):
    check_rest_indexed_search_cancel(deployment, indexed_search)


@pytest.mark.parametrize("indexed_search", ["sparse", "bm25"], indirect=True)
def test_t13_rest_sparse_and_bm25_search_cancel(deployment, indexed_search):
    check_rest_indexed_search_cancel(deployment, indexed_search)


@pytest.mark.parametrize("indexed_search", ["binary"], indirect=True)
def test_t13_rest_binary_search_cancel(deployment, indexed_search):
    check_rest_indexed_search_cancel(deployment, indexed_search)


def check_rest_indexed_search_cancel(deployment, indexed_search):
    uris, token, user, _ = deployment
    proxy_a, proxy_b = uris
    path, collection, _, control, vectors, python_search = indexed_search
    inputs = vectors[:8192]
    if path == "ivf":
        inputs = np.asarray(inputs, dtype="float64").round(6).tolist()
    params = {"nprobe": NPROBE} if path == "ivf" else {}
    expr = f"id >= -{int(uuid4().hex[:7], 16) + 1}"
    reason = f"t13-rest-{path}-" + uuid4().hex

    def search(query_vectors):
        return entity_request(
            uris[proxy_a],
            token,
            "Search",
            {
                "dbName": "default",
                "collectionName": collection,
                "annsField": "vector",
                "data": wire_vectors(path, query_vectors),
                "filter": expr,
                "limit": TOPK,
                "searchParams": {"params": params},
            },
        )

    def records():
        return post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "list",
            {
                "dbName": "default",
                "collectionName": collection,
                "user": user,
            },
        )["requests"]

    baseline = python_search(control, inputs[:1])
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, inputs)
        target = wait_for_records(records, {expr}, lambda row: row["expr"], [future])[expr]
        assert target["proxyId"] == proxy_a and target["type"] == "Search" and target["cancellable"], target
        request_id = target["requestId"]
        result = post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "cancel",
            {
                "requestIds": [request_id],
                "reason": reason,
            },
        )
        assert [row["requestId"] for row in result["canceled"]] == [request_id], result
        assert result["notFound"] == result["undetermined"] == [], result
        outcome = future.result(timeout=95)
        assert outcome["code"] == 3002 and reason in outcome["message"], outcome
    assert request_id not in {row["requestId"] for row in records()}
    actual = search(inputs[:1])
    assert actual["code"] == 0 and actual["topks"] == [TOPK], actual
    hits = [[{"id": int(hit["id"]), "distance": hit["distance"]} for hit in actual["data"]]]
    assert_ranked_control(hits, baseline, inputs[:1], python_search, control, descending=path in {"sparse", "bm25"})
