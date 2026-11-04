"""Malformed request bytes and parameter strings retain input-error classification."""

import base64
import uuid
from types import SimpleNamespace
from urllib.parse import urlparse

import grpc
import pytest
import requests
from common.common_type import CaseLabel
from pymilvus import MilvusClient
from pymilvus.grpc_gen import common_pb2, milvus_pb2, milvus_pb2_grpc, schema_pb2

pytestmark = pytest.mark.tags(CaseLabel.L1)


@pytest.fixture
def parameter_error_client(request):
    uri = request.config.getoption("--uri") or (
        f"http://{request.config.getoption('--host')}:{request.config.getoption('--port')}"
    )
    token = request.config.getoption("--token")
    db_name = request.config.getoption("--db_name")
    client = MilvusClient(uri=uri, token=token, db_name=db_name)
    address = urlparse(uri)
    channel = (
        grpc.secure_channel(address.netloc, grpc.ssl_channel_credentials())
        if address.scheme == "https"
        else grpc.insecure_channel(address.netloc)
    )
    stub = milvus_pb2_grpc.MilvusServiceStub(channel)
    metadata = [("authorization", base64.b64encode(token.encode()).decode())] if token else []
    name = f"parameter_error_{uuid.uuid4().hex}"

    def call(method, message):
        return getattr(stub, method)(message, metadata=metadata, timeout=30)

    try:
        yield SimpleNamespace(client=client, call=call, name=name, db_name=db_name, uri=uri, token=token)
    finally:
        try:
            if client.has_collection(name, timeout=30):
                client.drop_collection(name, timeout=30)
        finally:
            channel.close()
            client.close()


def collection_schema(name):
    schema = schema_pb2.CollectionSchema(name=name)
    schema.fields.add(name="id", data_type=schema_pb2.Int64, is_primary_key=True)
    schema.fields.add(name="vector", data_type=schema_pb2.FloatVector).type_params.add(key="dim", value="4")
    return schema


def assert_input_error(status, *context):
    assert status.code == 1100, status
    assert status.extra_info.get("is_input_error") == "true", status
    assert not status.retriable, status
    for text in context:
        assert text in status.reason, status


@pytest.mark.parametrize("dim,cause", [("abc", "invalid syntax"), ("9223372036854775808", "value out of range")])
def test_proxy_invalid_dimension(parameter_error_client, dim, cause):
    env = parameter_error_client
    schema = collection_schema(env.name)
    schema.fields[1].type_params[0].value = dim
    status = env.call(
        "CreateCollection",
        milvus_pb2.CreateCollectionRequest(
            db_name=env.db_name, collection_name=env.name, schema=schema.SerializeToString()
        ),
    )
    assert_input_error(status, "dim", "vector", dim, cause)


@pytest.mark.parametrize("in_struct", [False, True])
def test_proxy_invalid_skip_load(parameter_error_client, in_struct):
    env = parameter_error_client
    schema = collection_schema(env.name)
    if in_struct:
        struct = schema.struct_array_fields.add(name="items")
        struct.type_params.add(key="max_capacity", value="10")
        field = struct.fields.add(name="payload", data_type=schema_pb2.Array, element_type=schema_pb2.Bool)
        field.type_params.add(key="max_capacity", value="10")
    else:
        field = schema.fields.add(name="payload", data_type=schema_pb2.Bool)
    field.type_params.add(key="field.skipLoad", value="invalid_bool")
    status = env.call(
        "CreateCollection",
        milvus_pb2.CreateCollectionRequest(
            db_name=env.db_name, collection_name=env.name, schema=schema.SerializeToString()
        ),
    )
    assert_input_error(status, "field.skipLoad", "items.payload" if in_struct else "payload", "invalid_bool")


@pytest.mark.parametrize("add_field", [False, True])
def test_proxy_malformed_schema(parameter_error_client, add_field):
    env = parameter_error_client
    if add_field:
        env.client.create_collection(env.name, dimension=4, timeout=60)
        method, request_type = "AddCollectionField", milvus_pb2.AddCollectionFieldRequest
    else:
        method, request_type = "CreateCollection", milvus_pb2.CreateCollectionRequest
    # Send raw protobuf bytes so SDK schema validation cannot hide the server path.
    status = env.call(method, request_type(db_name=env.db_name, collection_name=env.name, schema=b"\xff"))
    assert_input_error(status, "schema", env.name, "proto:")


@pytest.mark.parametrize("hybrid", [False, True])
def test_proxy_malformed_placeholder_without_nq(parameter_error_client, hybrid):
    env = parameter_error_client
    env.client.create_collection(env.name, dimension=4, timeout=60)
    search = milvus_pb2.SearchRequest(
        db_name=env.db_name,
        collection_name=env.name,
        nq=0,
        placeholder_group=b"\xff",
        search_params=[
            common_pb2.KeyValuePair(key="anns_field", value="vector"),
            common_pb2.KeyValuePair(key="topk", value="1"),
            common_pb2.KeyValuePair(key="metric_type", value="COSINE"),
        ],
    )
    if hybrid:
        response = env.call(
            "HybridSearch",
            milvus_pb2.HybridSearchRequest(
                db_name=env.db_name,
                collection_name=env.name,
                requests=[search],
                rank_params=[common_pb2.KeyValuePair(key="limit", value="1")],
            ),
        )
    else:
        response = env.call("Search", search)
    assert_input_error(response.status, "placeholder group", "proto:")


def test_proxy_quick_create_invalid_dynamic_field(parameter_error_client):
    env = parameter_error_client
    response = requests.post(
        f"{env.uri.rstrip('/')}/v2/vectordb/collections/create",
        headers={"Authorization": f"Bearer {env.token}"} if env.token else {},
        json={
            "dbName": env.db_name,
            "collectionName": env.name,
            "dimension": 4,
            "params": {"enableDynamicField": "invalid_bool"},
        },
        timeout=30,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["code"] == 1100, body
    for text in ("enableDynamicField", "invalid_bool", "invalid syntax"):
        assert text in body["message"], body
