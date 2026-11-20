"""Cancel external-collection queries through their originating transport."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import numpy as np
import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import wait_for_records
from test_running_requests_native_sdk import canceled, capture, listed, start, terminal
from test_running_requests_rest_cross_proxy import entity_request

pytestmark = pytest.mark.tags("L1")


@pytest.mark.parametrize("kind", ["Search"])
def test_t13_native_external_search_cancel(kind, native, external_data, tmp_path):
    _, collection, vectors = external_data
    inputs = tmp_path / "external-vectors.f32"
    np.asarray(vectors[:16384], dtype="<f4").tofile(inputs)
    data = dict(Collection=collection, Vectors=str(inputs), AnnsField="embedding", NQ=min(16384, len(vectors)), Limit=1)
    call, expr = start(native, data, kind=kind)
    request_id = capture(native, data, call, expr, kind=kind)["request_id"]
    reason = "t13-native-external-" + uuid4().hex
    result = canceled(native, [request_id], reason)
    assert [row["request_id"] for row in result["canceled"]] == [request_id], result
    terminal(native, call, reason)
    assert request_id not in {row["request_id"] for row in listed(native, Collection=collection)}
    control, _ = start(native, dict(data, NQ=1), kind=kind)
    hits = native.ok("await", Call=control)["hits"]
    assert len(hits) == len(hits[0]) == 1, hits
    assert hits[0][0]["distance"] == pytest.approx(0, abs=1e-5)


@pytest.mark.parametrize("kind", ["Query"])
def test_t13_native_external_query_cancel(kind, native, external_data):
    _, collection, vectors = external_data
    data = dict(Collection=collection, Kind=kind, OutputFields=["id", "embedding"])
    expr = f"id >= -{int(uuid4().hex[:7], 16) + 1}"
    call = uuid4().hex
    native.ok("start", "a", **data, Call=call, Expr=expr, Limit=16000)
    request_id = capture(native, data, call, expr, kind=kind)["request_id"]
    reason = "t13-native-external-query-" + uuid4().hex
    result = canceled(native, [request_id], reason)
    assert [row["request_id"] for row in result["canceled"]] == [request_id], result
    terminal(native, call, reason)
    assert request_id not in {row["request_id"] for row in listed(native, Collection=collection)}

    control = uuid4().hex
    native.ok("start", "a", **data, Call=control, Expr="id == 0", Limit=1, ReturnQueryRows=True)
    result = native.ok("await", Call=control)
    assert result["count"] == len(result["rows"]) == 1, result
    row = result["rows"][0]
    assert int(row["id"]) == 0
    np.testing.assert_allclose(row["embedding"], vectors[0], rtol=1e-6, atol=1e-7)


@pytest.mark.parametrize("kind", ["Search", "Query"])
def test_t13_rest_external_cancel(kind, deployment, external_data):
    uris, token, user, _ = deployment
    a, b = uris
    _, collection, vectors = external_data
    expr = f"id >= -{int(uuid4().hex[:7], 16) + 1}"
    reason = "t13-rest-external-" + uuid4().hex

    def invoke(control=False):
        body = {"collectionName": collection, "filter": "id == 0" if control else expr}
        if kind == "Search":
            data = vectors[:1] if control else vectors[:8192]
            body.update(
                data=data.astype("float64").round(6).tolist(), annsField="embedding", outputFields=["id"], limit=1
            )
        else:
            body.update(outputFields=["id", "embedding"], limit=1 if control else 16000)
        return entity_request(uris[a], token, kind, body)

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

    assert request_id not in {item["requestId"] for item in records()}
    control = invoke(control=True)
    assert control["code"] == 0 and len(control["data"]) == 1, control
    hit = control["data"][0]
    if kind == "Search":
        assert hit["distance"] == pytest.approx(0, abs=1e-5)
    else:
        assert int(hit["id"]) == 0
        np.testing.assert_allclose(hit["embedding"], vectors[0], rtol=1e-6, atol=1e-7)
