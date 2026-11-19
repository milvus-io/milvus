"""Single-Proxy smoke coverage for the running-request Python SDK."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from time import monotonic, sleep
from unittest.mock import Mock
from uuid import uuid4

import numpy as np
import pytest
from common.common_type import CaseLabel
from pymilvus import DataType, MilvusClient, MilvusException
from pymilvus.exceptions import ParamError

DIM = 128
ROWS = 100_000
NQ = 16_384
TOPK = 10


@pytest.fixture
def single_proxy_client(host, port, token):
    with ExitStack() as resources:
        app = MilvusClient(uri=f"http://{host}:{port}", token=token, dedicated=True)
        resources.callback(app.close)
        listed = app.list_running_requests(collection_name=f"missing_{uuid4().hex}", timeout=10)
        node_id = single_node_id(listed)
        yield app, node_id


def single_node_id(result):
    nodes = result.node_results
    if len(nodes) > 1:
        pytest.skip("Requires a single Proxy")
    assert result.complete, result
    assert len(nodes) == 1, result
    node = nodes[0]
    assert node.answered and not node.unimplemented, node
    assert type(node.node_id) is int and node.node_id > 0, node
    return node.node_id


@pytest.mark.tags(CaseLabel.L0)
def test_list_and_cancel_search_on_single_proxy(single_proxy_client):
    app, node_id = single_proxy_client
    collection = f"running_requests_{uuid4().hex}"
    filters = {"collection_name": collection}

    def records():
        result = app.list_running_requests(timeout=10, **filters)
        assert single_node_id(result) == node_id
        return result.requests

    with ExitStack() as resources:
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
                matches = [row for row in records() if row.expr == expr]
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

            assert row.type == "Search" and row.collection_name == collection, row
            assert row.db_name == "default" and row.proxy_id == node_id, row
            assert row.nq == NQ and row.topk == TOPK and row.cancellable, row
            request_id = row.request_id
            assert isinstance(request_id, str) and request_id, row
            canceled = app.cancel_requests([request_id], reason=reason, timeout=10)
            assert len(canceled.node_results) == 1, canceled
            node = canceled.node_results[0]
            assert node.node_id == node_id and node.answered and not node.unimplemented, node
            assert [item.request_id for item in canceled.canceled] == [request_id], canceled
            assert canceled.not_found == canceled.undetermined == [], canceled
            with pytest.raises(MilvusException) as error:
                call.result(timeout=60)
            assert error.value.code == 3002 and reason in error.value.message

        assert all(row.request_id != request_id for row in records())
        control = search(vectors[:1])
        assert len(control) == 1 and len(control[0]) == TOPK


@pytest.mark.tags(CaseLabel.L0)
def test_list_empty_result_and_cancel_unknown_id(single_proxy_client):
    app, node_id = single_proxy_client
    missing = f"unknown_{uuid4().hex}"
    result = app.list_running_requests(collection_name=missing, timeout=10)
    assert single_node_id(result) == node_id
    assert result.requests == [], result
    canceled = app.cancel_requests([missing], timeout=10)
    assert len(canceled.node_results) == 1, canceled
    node = canceled.node_results[0]
    assert node.node_id == node_id and node.answered and not node.unimplemented, node
    assert canceled.not_found == [missing], canceled
    assert not canceled.canceled and not canceled.undetermined, canceled


@pytest.mark.tags(CaseLabel.L0)
def test_cancel_empty_ids_without_rpc(single_proxy_client, monkeypatch):
    app, _ = single_proxy_client
    rpc = Mock(side_effect=AssertionError("Invalid IDs reached CancelRequests RPC"))
    monkeypatch.setattr(app._get_connection()._stub, "CancelRequests", rpc)
    for ids in ([], None):
        with pytest.raises(ParamError) as error:
            app.cancel_requests(ids, timeout=10)
        assert error.value.code == 1, error.value
    rpc.assert_not_called()
