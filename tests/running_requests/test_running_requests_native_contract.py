"""Native SDK wire contracts against a controlled gRPC endpoint, NOT cluster E2E.

The independent endpoint verifies requests and supplies known protobuf values;
both native SDKs must decode them without Python acting as a client adapter.
"""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import grpc
import numpy as np
import pytest
from pymilvus.grpc_gen import common_pb2 as common
from pymilvus.grpc_gen import milvus_pb2 as pb
from pymilvus.grpc_gen import milvus_pb2_grpc as rpc
from pymilvus.grpc_gen import schema_pb2 as schema
from test_running_requests_native_sdk import Native

RECORD = dict(
    request_id="7-opaque-Z9",
    proxy_id=7,
    type="Search",
    db_name="db1",
    collection_name="collection",
    user="alice",
    client_addr="10.0.0.1:43210",
    nq=16384,
    topk=10,
    expr="id > 0",
    start_time_ms=1794818315973,
    queued_ms=17,
    elapsed_ms=2345,
    state="Running",
    task_ids=[9007199254740993, 12],
    trace_id="abcdef",
    cancellable=True,
)


class Endpoint(rpc.MilvusServiceServicer):
    def __init__(self):
        self.calls = Counter()
        self.last = {}
        self.code = 0
        self.mode = "complete"

    def Connect(self, request, context):
        return pb.ConnectResponse(status=common.Status(), identifier=42)

    def ListDatabases(self, request, context):
        return pb.ListDatabasesResponse(status=common.Status(), db_names=["default"])

    def DescribeCollection(self, request, context):
        return pb.DescribeCollectionResponse(
            status=common.Status(),
            collectionID=123,
            schema=schema.CollectionSchema(
                name="collection",
                fields=[
                    schema.FieldSchema(fieldID=100, name="id", data_type=schema.Int64, is_primary_key=True),
                    schema.FieldSchema(
                        fieldID=101,
                        name="vector",
                        data_type=schema.FloatVector,
                        type_params=[common.KeyValuePair(key="dim", value="128")],
                    ),
                ],
            ),
        )

    def Search(self, request, context):
        self.calls["Search"] += 1
        return pb.SearchResults(status=common.Status(code=3002, reason="native\nsecond line: request canceled"))

    def HybridSearch(self, request, context):
        self.calls["HybridSearch"] += 1
        return pb.SearchResults(status=common.Status(code=3002, reason="native\nsecond line: request canceled"))

    def Query(self, request, context):
        self.calls["Query"] += 1
        return pb.QueryResults(status=common.Status(code=3002, reason="native\nsecond line: request canceled"))

    def nodes(self):
        result = [pb.RunningRequestNodeResult(node_id=7, status=common.Status())]
        if self.mode != "complete":
            result.append(
                pb.RunningRequestNodeResult(
                    node_id=8,
                    status=common.Status(code=10 if self.mode == "old" else 65535, reason="opaque error"),
                    unimplemented=self.mode == "old",
                )
            )
        return result

    def ListRunningRequests(self, request, context):
        self.calls["ListRunningRequests"] += 1
        self.last["list"] = request
        return pb.ListRunningRequestsResponse(
            status=common.Status(code=self.code, reason="denied" if self.code else ""),
            requests=[pb.RunningRequestInfo(**RECORD)],
            node_results=self.nodes(),
        )

    def CancelRequests(self, request, context):
        self.calls["CancelRequests"] += 1
        self.last["cancel"] = request
        return pb.CancelRequestsResponse(
            status=common.Status(code=self.code, reason="denied" if self.code else ""),
            canceled=[pb.RunningRequestInfo(**RECORD)],
            not_found=["unknown"] if self.mode != "lost" else [],
            undetermined=["unknown"] if self.mode == "lost" else [],
            node_results=self.nodes(),
        )


@pytest.fixture(params=["go", "java"])
def contract(request, tmp_path):
    endpoint = Endpoint()
    server = grpc.server(ThreadPoolExecutor(max_workers=4))
    rpc.add_MilvusServiceServicer_to_server(endpoint, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    worker = Native(request.param, tmp_path / "worker.stderr")
    try:
        worker.ok("connect", URI=f"http://127.0.0.1:{port}", DB="default", Token="test:fixture")
        yield worker, endpoint
    finally:
        worker.close()
        server.stop(0).wait()


def test_native_wire_filters_snapshot(contract):
    worker, endpoint = contract
    result = worker.ok("list", DB="db1", Collection="collection", User="alice", MinMS=1500)
    req = endpoint.last["list"]
    assert (req.db_name, req.collection_name, req.user, req.min_elapsed_ms) == ("db1", "collection", "alice", 1500)
    assert result["requests"] == [RECORD]
    assert result["nodes"][0]["answered"] and not result["nodes"][0]["unimplemented"]
    worker.ok("list")
    req = endpoint.last["list"]
    assert (req.db_name, req.collection_name, req.user, req.min_elapsed_ms) == ("", "", "", 0)


def test_native_wire_empty_ids(contract):
    worker, endpoint = contract
    for ids in ([], None):
        result = worker.call("cancel", IDs=ids)
        assert not result["ok"]
        assert result["code"] == (1101 if worker.language == "go" else "INVALID_PARAMS")
    assert endpoint.calls["CancelRequests"] == 0
    assert worker.ok("counts")["counts"].get("CancelRequests", 0) == 0


@pytest.mark.parametrize("mode", ["complete", "lost", "old"])
def test_native_wire_buckets_partial(contract, mode):
    worker, endpoint = contract
    endpoint.mode = mode
    reason = "wire\n第二行"
    result = worker.ok("cancel", IDs=[RECORD["request_id"], "unknown"], User="alice", Reason=reason)
    req = endpoint.last["cancel"]
    assert list(req.request_ids) == [RECORD["request_id"], "unknown"]
    assert req.user == "alice" and req.reason == reason
    assert result["canceled"] == [RECORD]
    assert (result["not_found"] or []) == ([] if mode == "lost" else ["unknown"])
    assert (result["undetermined"] or []) == (["unknown"] if mode == "lost" else [])
    listed = worker.ok("list")
    for response in (result, listed):
        assert response["nodes"][0]["answered"]
        if mode != "complete":
            node = response["nodes"][1]
            assert node["node_id"] == 8 and not node["answered"]
            assert node["unimplemented"] == (mode == "old")


def test_native_wire_error_mapping(contract):
    worker, endpoint = contract
    for code in (1401, 3002):
        endpoint.code = code
        for op, args, rpc_name in (
            ("list", {}, "ListRunningRequests"),
            ("cancel", {"IDs": ["opaque"]}, "CancelRequests"),
        ):
            before = endpoint.calls[rpc_name]
            result = worker.call(op, **args)
            assert not result["ok"] and result["code"] == code, result
            assert endpoint.calls[rpc_name] == before + 1


@pytest.mark.parametrize("kind", ["Search", "Query", "HybridSearch"])
def test_native_original_query_3002_no_retry(contract, tmp_path, kind):
    worker, endpoint = contract
    path = tmp_path / "vectors.f32"
    np.zeros((1, 128), dtype="<f4").tofile(path)
    worker.ok(
        "start", Call="original", Kind=kind, Collection="collection", Vectors=str(path), NQ=1, Limit=1, Expr="id > 0"
    )
    result = worker.call("await", Call="original")
    assert not result["ok"] and result["code"] == 3002, result
    assert "native\nsecond line: request canceled" in result["message"]
    assert endpoint.calls[kind] == 1
    assert worker.ok("counts")["counts"].get(kind) == 1
