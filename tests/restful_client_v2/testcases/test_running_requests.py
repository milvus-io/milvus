"""Single-Proxy List/Cancel REST contracts and the Search cancellation lifecycle."""

import json
from concurrent.futures import ThreadPoolExecutor
from time import monotonic, sleep
from uuid import uuid4

import numpy as np
import pytest
import requests
from utils.constant import CaseLabel

NQ = 16_384
TOPK = 10


@pytest.fixture
def rest(endpoint, token):
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    def post(path, payload, timeout=10):
        response = requests.post(
            f"{endpoint.rstrip('/')}/v2/vectordb/{path}",
            data=json.dumps(payload, separators=(",", ":")),
            headers={"Content-Type": "application/json", **headers},
            timeout=timeout,
        )
        assert response.status_code == 200, response.text
        return response.json()

    return post


def successful(post, path, payload, timeout=10):
    body = post(path, payload, timeout)
    assert body["code"] == 0, body
    return body["data"]


def single_node_id(data):
    nodes = data["nodeResults"]
    if len(nodes) > 1:
        pytest.skip("Requires a single Proxy")
    assert len(nodes) == 1, data
    node = nodes[0]
    assert node["code"] == 0 and node["message"] == "", node
    assert node["unimplemented"] is False, node
    assert type(node["nodeId"]) is int and node["nodeId"] > 0, node
    return node["nodeId"]


@pytest.mark.tags(CaseLabel.L0)
def test_list_empty_result_and_cancel_unknown_id(rest):
    missing = f"unknown_{uuid4().hex}"
    listed = successful(rest, "requests/running/list", {"collectionName": missing})
    assert listed["requests"] == [], listed
    node_id = single_node_id(listed)
    canceled = successful(rest, "requests/running/cancel", {"requestIds": [missing]})
    assert single_node_id(canceled) == node_id
    assert canceled["notFound"] == [missing], canceled
    assert canceled["canceled"] == canceled["undetermined"] == [], canceled


@pytest.mark.tags(CaseLabel.L0)
@pytest.mark.parametrize(
    ("method", "payload", "code"),
    [
        pytest.param("list", {"collectionName": {}}, 1801, id="list-collection-type"),
        pytest.param("list", {"minElapsedMs": "1"}, 1801, id="list-elapsed-type"),
        pytest.param("cancel", {}, 1802, id="cancel-missing-ids"),
        pytest.param("cancel", {"requestIds": []}, 1101, id="cancel-empty-ids"),
        pytest.param("cancel", {"requestIds": [123]}, 1801, id="cancel-id-type"),
    ],
)
def test_list_cancel_invalid_input(rest, method, payload, code):
    assert rest(f"requests/running/{method}", payload)["code"] == code


@pytest.mark.tags(CaseLabel.L0)
def test_rest_search_cancel_lifecycle(rest, request):
    collection = f"running_requests_{uuid4().hex}"
    filters = {"collectionName": collection}
    node_id = single_node_id(successful(rest, "requests/running/list", filters))
    successful(
        rest,
        "collections/create",
        {
            "collectionName": collection,
            "consistencyLevel": "Strong",
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "128"}},
                ],
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector_index", "indexType": "FLAT", "metricType": "L2"},
            ],
        },
        timeout=120,
    )
    request.addfinalizer(lambda: successful(rest, "collections/drop", filters, timeout=60))
    generator = np.random.default_rng(1113)
    for start in range(0, 100_000, 5000):
        vectors = generator.random((5000, 128), dtype=np.float32).tolist()
        inserted = successful(
            rest,
            "entities/insert",
            {**filters, "data": [{"id": start + i, "vector": v} for i, v in enumerate(vectors)]},
            timeout=60,
        )
        assert inserted["insertCount"] == 5000, inserted
    successful(rest, "collections/flush", filters, timeout=120)
    successful(rest, "collections/load", filters, timeout=120)
    deadline = monotonic() + 120
    while successful(rest, "collections/get_load_state", filters)["loadState"] != "LoadStateLoaded":
        assert monotonic() < deadline, "Collection did not load"
        sleep(0.5)

    vectors = generator.random((NQ, 128), dtype=np.float32).astype("float64").round(6).tolist()
    expr = f"id >= -{int(uuid4().hex[:7], 16)}"
    reason = f"rest-cancel-{uuid4().hex}"

    def search(data):
        return rest(
            "entities/search",
            {**filters, "data": data, "annsField": "vector", "filter": expr, "limit": TOPK},
            timeout=90,
        )

    def records():
        listed = successful(rest, "requests/running/list", filters)
        assert single_node_id(listed) == node_id
        return listed["requests"]

    with ThreadPoolExecutor(max_workers=1) as pool:
        call = pool.submit(search, vectors)
        deadline = monotonic() + 30
        while monotonic() < deadline:
            matches = [row for row in records() if row["expr"] == expr]
            if matches:
                assert len(matches) == 1, matches
                row = matches[0]
                break
            if call.done():
                pytest.fail(f"Search completed before List captured it: {call.result()}")
            sleep(0.01)
        else:
            pytest.fail("List did not capture REST Search")
        request_id = row["requestId"]
        assert isinstance(request_id, str) and request_id, row
        assert row["type"] == "Search" and row["collectionName"] == collection, row
        assert row["proxyId"] == node_id and row["dbName"] == "default" and row["cancellable"], row
        canceled = successful(rest, "requests/running/cancel", {"requestIds": [request_id], "reason": reason})
        assert single_node_id(canceled) == node_id
        assert [item["requestId"] for item in canceled["canceled"]] == [request_id], canceled
        assert canceled["notFound"] == canceled["undetermined"] == [], canceled
        outcome = call.result(timeout=95)
        assert outcome["code"] == 3002 and reason in outcome["message"], outcome
    remaining = records()
    assert all(item["requestId"] != request_id for item in remaining), remaining
    control = search(vectors[:1])
    assert control["code"] == 0 and len(control["data"]) == TOPK, control
    assert row["nq"] == NQ and row["topk"] == TOPK, row
