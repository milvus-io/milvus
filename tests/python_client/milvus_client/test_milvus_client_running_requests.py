"""Single-Proxy smoke coverage for the running-request REST API."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from time import monotonic, sleep
from uuid import uuid4

import numpy as np
import pytest
import requests
from common.common_type import CaseLabel
from pymilvus import DataType, MilvusClient, MilvusException

DIM = 128
ROWS = 100_000
NQ = 16_384
TOPK = 10


@pytest.fixture
def running_request_endpoint(host, port, token):
    uri = f"http://{host}:{port}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    def post(action, payload):
        response = requests.post(
            f"{uri}/v2/vectordb/requests/running/{action}",
            json=payload,
            headers=headers,
            timeout=10,
        )
        assert response.status_code == 200, response.text
        return response.json()

    return uri, token, post


def successful_response(post, action, payload):
    body = post(action, payload)
    assert body["code"] == 0, body
    return body["data"]


def single_node_id(result):
    nodes = result["nodeResults"]
    if len(nodes) > 1:
        pytest.skip("Requires a single Proxy")
    assert len(nodes) == 1, result
    node = nodes[0]
    assert node["code"] == 0 and node["message"] == "", node
    assert node["unimplemented"] is False, node
    assert type(node["nodeId"]) is int and node["nodeId"] > 0, node
    return node["nodeId"]


@pytest.mark.tags(CaseLabel.L0)
def test_list_and_cancel_search_on_single_proxy(running_request_endpoint):
    uri, token, post = running_request_endpoint
    collection = f"running_requests_{uuid4().hex}"
    filters = {"collectionName": collection}
    node_id = single_node_id(successful_response(post, "list", filters))

    def records():
        result = successful_response(post, "list", filters)
        assert single_node_id(result) == node_id
        return result["requests"]

    with ExitStack() as resources:
        app = MilvusClient(uri=uri, token=token, dedicated=True)
        resources.callback(app.close)
        schema = app.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=DIM)
        app.create_collection(collection, schema=schema, consistency_level="Strong", timeout=120)
        resources.callback(app.drop_collection, collection, timeout=30)
        generator = np.random.default_rng(1113)
        for start in range(0, ROWS, 5000):
            count = min(5000, ROWS - start)
            vectors = generator.random((count, DIM), dtype=np.float32).tolist()
            app.insert(
                collection,
                [{"id": start + offset, "vector": vector} for offset, vector in enumerate(vectors)],
                timeout=60,
            )
        app.flush(collection, timeout=120)
        indexes = app.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        app.create_index(collection, indexes, timeout=120)
        app.load_collection(collection, timeout=120)
        vectors = generator.random((NQ, DIM), dtype=np.float32)
        expr = f"id >= -{int(uuid4().hex[:7], 16)}"
        reason = f"single-proxy-{uuid4().hex}"

        def search(data):
            return app.search(
                collection,
                data,
                anns_field="vector",
                filter=expr,
                limit=TOPK,
                search_params={"metric_type": "L2"},
                timeout=90,
            )

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
                    call.result()
                    pytest.fail("Search completed before List captured it")
                sleep(0.02)
            else:
                pytest.fail("List did not capture the Search within 30 seconds")

            assert row["type"] == "Search" and row["collectionName"] == collection, row
            assert row["dbName"] == "default" and row["proxyId"] == node_id, row
            assert row["nq"] == NQ and row["topk"] == TOPK and row["cancellable"], row
            request_id = row["requestId"]
            assert isinstance(request_id, str) and request_id, row
            canceled = successful_response(post, "cancel", {"requestIds": [request_id], "reason": reason})
            assert single_node_id(canceled) == node_id
            assert [item["requestId"] for item in canceled["canceled"]] == [request_id], canceled
            assert canceled["notFound"] == canceled["undetermined"] == [], canceled
            with pytest.raises(MilvusException) as error:
                call.result(timeout=60)
            assert error.value.code == 3002 and reason in error.value.message

        assert all(row["requestId"] != request_id for row in records())
        control = search(vectors[:1])
        assert len(control) == 1 and len(control[0]) == TOPK


@pytest.mark.tags(CaseLabel.L0)
def test_list_empty_result_and_cancel_unknown_id(running_request_endpoint):
    _, _, post = running_request_endpoint
    missing = f"unknown_{uuid4().hex}"
    result = successful_response(post, "list", {"collectionName": missing})
    assert result["requests"] == [], result
    node_id = single_node_id(result)
    result = successful_response(post, "cancel", {"requestIds": [missing]})
    assert single_node_id(result) == node_id
    assert result["notFound"] == [missing], result
    assert result["canceled"] == result["undetermined"] == [], result


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
def test_list_cancel_invalid_input(running_request_endpoint, method, payload, code):
    _, _, post = running_request_endpoint
    body = post(method, payload)
    assert body["code"] == code, body
