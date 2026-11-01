from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from pymilvus.client.grpc_handler import GrpcHandler
from pymilvus.client.call_context import CallContext
from pymilvus.client import ts_utils
from pymilvus.grpc_gen import milvus_pb2 as pb
from pymilvus.milvus_client.milvus_client import MilvusClient


def handler():
    h = GrpcHandler.__new__(GrpcHandler)
    h._address = "localhost:1"
    h._stub = SimpleNamespace(MultiSearch=Mock(return_value=pb.MultiSearchResponse()))
    return h


def test_same_collection_different_database_session_timestamps_and_one_rpc():
    h = handler()
    for db, ts in [("a", 1234), ("b", 5678)]:
        ts_utils.update_collection_ts("same", ts, h.server_address, db)
    args = dict(
        collection_name="same",
        anns_field="v",
        param={"metric_type": "L2"},
        limit=3,
        data=[[1.0, 2.0]],
        consistency_level="Session",
    )
    h.multi_search(
        [{**args, "db_name": "a"}, {**args, "db_name": "b"}],
        context=CallContext("default"),
        max_concurrent_searches=2,
    )
    h._stub.MultiSearch.assert_called_once()
    r = h._stub.MultiSearch.call_args.args[0]
    assert [i.search.db_name for i in r.items] == ["a", "b"]
    assert [i.search.guarantee_timestamp for i in r.items] == [1234, 5678]
    assert r.max_concurrent_searches == 2
    for i, db in enumerate(["a", "b"]):
        ordinary = h._prepare_search_request(**args, context=CallContext(db))
        ordinary.db_name = db
        assert ordinary == r.items[i].search


def test_public_client_keeps_database_state_and_input_unchanged():
    c = MilvusClient.__new__(MilvusClient)
    c._config = SimpleNamespace(db_name="default")
    h = Mock()
    h.multi_search.return_value = []
    c._get_connection = lambda: h
    items = [
        {"collection_name": "a", "data": [[1.0, 2.0]], "db_name": "other", "limit": 9},
        {"collection_name": "b", "data": [[2.0, 3.0]]},
    ]
    c.multi_search(items, max_concurrent_searches=1)
    sent = h.multi_search.call_args.args[0]
    assert [x["db_name"] for x in sent] == ["other", "default"]
    assert c._config.db_name == "default" and "db_name" not in items[1]
    assert sent[0]["limit"] == 9 and sent[1]["limit"] == 10


def test_item_transport_and_search_failures_are_returned_without_retry():
    h = handler()
    r = pb.MultiSearchResponse()
    r.items.add(id="0", grpc_code=7, grpc_message="denied")
    r.items.add(id="1").result.status.code = 100
    h._stub.MultiSearch.return_value = r
    args = dict(collection_name="a", anns_field="v", param={}, limit=1, data=[[1.0, 2.0]])
    out = h.multi_search([args, args])
    assert out[0]["error"]["grpc_code"] == 7
    assert out[1]["error"]["code"] == 100
    h._stub.MultiSearch.assert_called_once()
