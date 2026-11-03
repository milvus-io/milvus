"""Proxy schema selection stays coherent across TTL and collection identity changes."""

import time
import uuid

import grpc
import pytest
from common.common_type import CaseLabel
from pymilvus import DataType, MilvusClient
from pymilvus.grpc_gen import milvus_pb2, milvus_pb2_grpc


@pytest.fixture
def schema_access_client(request):
    host = request.config.getoption("--host")
    port = request.config.getoption("--port")
    client = MilvusClient(uri=f"http://{host}:{port}")
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = milvus_pb2_grpc.MilvusServiceStub(channel)
    name = f"schema_access_{uuid.uuid4().hex}"
    yield client, stub, name
    client.drop_collection(name, timeout=60)
    channel.close()
    client.close()


def snapshot(stub, name):
    response = stub.DescribeCollection(milvus_pb2.DescribeCollectionRequest(collection_name=name), timeout=60)
    assert response.status.code == 0, response.status
    return response.collectionID, response.schema.version


def create_collection(client, name):
    schema = client.create_schema(auto_id=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("value", DataType.INT64)
    schema.add_field("expiry", DataType.TIMESTAMPTZ, nullable=True)
    schema.add_field("next_expiry", DataType.TIMESTAMPTZ, nullable=True)
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
    index = client.prepare_index_params()
    index.add_index("vector", index_type="FLAT", metric_type="L2")
    client.create_collection(name, schema=schema, index_params=index, shards_num=2, timeout=60)


def row(pk, value):
    past, future = "2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z"
    return {
        "id": pk,
        "value": value,
        "vector": [float(pk), 0.0, 0.0, 0.0],
        "expiry": past if pk == 1 else future,
        "next_expiry": future if pk == 1 else past,
    }


def ids(client, name):
    return sorted(
        r["id"]
        for r in client.query(name, filter="id >= 0", output_fields=["id"], consistency_level="Strong", timeout=60)
    )


@pytest.mark.tags(CaseLabel.L1)
@pytest.mark.parametrize("dynamic", [False, True])
def test_proxy_full_schema_system_columns(schema_access_client, dynamic):
    client, stub, name = schema_access_client
    schema = client.create_schema(auto_id=False, enable_dynamic_field=dynamic)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
    index = client.prepare_index_params()
    index.add_index("vector", index_type="FLAT", metric_type="L2")
    client.create_collection(name, schema=schema, index_params=index, timeout=60)
    data = {"id": 1, "vector": [1.0, 0.0, 0.0, 0.0]}
    if dynamic:
        data.update(RowID=7, Timestamp=11)
    client.insert(name, data, timeout=60)
    client.upsert(name, data, timeout=60)
    response = stub.DescribeCollection(milvus_pb2.DescribeCollectionRequest(collection_name=name), timeout=60)
    assert {field.name for field in response.schema.fields} == {"id", "vector"}
    rows = client.query(name, filter="id == 1", output_fields=["*"], consistency_level="Strong", timeout=60)
    assert len(rows) == 1
    if dynamic:
        assert rows[0]["RowID"] == 7 and rows[0]["Timestamp"] == 11
        assert client.query(
            name, filter="RowID == 7 and Timestamp == 11", output_fields=["id"], consistency_level="Strong", timeout=60
        ) == [{"id": 1}]
        results = client.search(
            name, [[1.0, 0.0, 0.0, 0.0]], filter="RowID == 7", limit=1, consistency_level="Strong", timeout=60
        )
        assert results[0][0]["id"] == 1
        client.delete(name, filter="Timestamp == 11", timeout=60)
        assert ids(client, name) == []
    else:
        assert "RowID" not in rows[0] and "Timestamp" not in rows[0]
        for field in ("RowID", "Timestamp"):
            with pytest.raises(Exception, match="(?i)(field|schema|plan)"):
                client.query(name, filter=f"{field} > 0", output_fields=["id"], timeout=60)


@pytest.mark.tags(CaseLabel.L1)
def test_proxy_schema_ttl_snapshots_and_mutations(schema_access_client):
    client, stub, name = schema_access_client
    create_collection(client, name)
    collection_id, version = snapshot(stub, name)
    assert version == 0
    client.insert(name, [row(1, 10), row(2, 20)], timeout=60)
    assert ids(client, name) == [1, 2]

    # Warm both selectors before each ALTER. Identical updates/deletes are no-ops.
    transitions = [
        ({"collection.ttl.seconds": "3600"}, [], 1, [1, 2]),
        ({"collection.ttl.seconds": "3600"}, [], 1, [1, 2]),
        ({"collection.ttl.seconds": "7200"}, [], 2, [1, 2]),
        ({}, ["collection.ttl.seconds"], 3, [1, 2]),
        ({}, ["collection.ttl.seconds"], 3, [1, 2]),
        ({"ttl_field": "expiry"}, [], 4, [2]),
        ({"ttl_field": "expiry"}, [], 4, [2]),
        ({"ttl_field": "next_expiry"}, [], 5, [1]),
        ({}, ["ttl_field"], 6, [1, 2]),
        ({}, ["ttl_field"], 6, [1, 2]),
    ]
    for properties, deletes, expected_version, visible in transitions:
        if properties:
            client.alter_collection_properties(name, properties, timeout=60)
        else:
            client.drop_collection_properties(name, deletes, timeout=60)
        assert snapshot(stub, name) == (collection_id, expected_version)
        assert ids(client, name) == visible
        client.upsert(name, row(2, 20), timeout=60)

    # Partial update performs an internal schema-bound read; vector output
    # exercises Search's field-refill/requery path on the legacy backend.
    client.upsert(name, {"id": 2, "value": 22}, partial_update=True, timeout=60)
    result = client.search(
        name,
        data=[[2.0, 0.0, 0.0, 0.0]],
        limit=2,
        output_fields=["value", "vector"],
        consistency_level="Strong",
        timeout=60,
    )
    assert result[0][0]["id"] == 2
    assert result[0][0]["entity"]["value"] == 22
    client.delete(name, filter="value >= 20", timeout=60)
    assert ids(client, name) == [1]
    client.release_collection(name, timeout=60)
    client.delete(name, ids=[1], timeout=60)  # PK-only delete does not need load.


@pytest.mark.tags(CaseLabel.L1)
def test_proxy_schema_alias_rename_recreate(schema_access_client):
    client, stub, name = schema_access_client
    create_collection(client, name)
    old_id, _ = snapshot(stub, name)
    alias, renamed = f"{name}_alias", f"{name}_renamed"
    client.create_alias(name, alias, timeout=60)
    alias_present = True
    try:
        client.insert(alias, row(1, 10), timeout=60)
        assert ids(client, alias) == [1]
        client.rename_collection(name, renamed, timeout=60)
        assert snapshot(stub, renamed)[0] == old_id
        assert ids(client, alias) == [1]
        client.drop_alias(alias, timeout=60)
        alias_present = False
        client.drop_collection(renamed, timeout=60)
        create_collection(client, name)
        assert snapshot(stub, name)[0] != old_id
        client.insert(name, row(2, 20), timeout=60)
        assert ids(client, name) == [2]
        with pytest.raises(Exception, match="not found|not exist|can't find"):
            client.query(alias, filter="id >= 0", output_fields=["id"], timeout=60)
    finally:
        if alias_present:
            client.drop_alias(alias, timeout=60)
        client.drop_collection(renamed, timeout=60)


@pytest.mark.tags(CaseLabel.L1)
def test_proxy_schema_ttl_duration_replaces_warm_cache(schema_access_client):
    client, stub, name = schema_access_client
    create_collection(client, name)
    client.alter_collection_properties(name, {"collection.ttl.seconds": "3600"}, timeout=60)
    client.insert(name, [row(1, 10), row(2, 20)], timeout=60)
    assert ids(client, name) == [1, 2]
    # A same-version cached body used to restore the old one-hour TTL after
    # invalidation. Both old rows must now expire under the shorter snapshot.
    client.alter_collection_properties(name, {"collection.ttl.seconds": "1"}, timeout=60)
    assert snapshot(stub, name)[1] == 2
    time.sleep(2)
    assert ids(client, name) == []


@pytest.mark.tags(CaseLabel.L1)
def test_proxy_schema_add_field_after_warm_write(schema_access_client):
    client, stub, name = schema_access_client
    create_collection(client, name)
    client.insert(name, row(1, 10), timeout=60)
    assert ids(client, name) == [1]
    collection_id, version = snapshot(stub, name)
    client.add_collection_field(name, "added", DataType.INT64, nullable=True, timeout=60)
    assert snapshot(stub, name) == (collection_id, version + 1)

    # The first write after ALTER must see the field before local validation,
    # without relying on a failed Append to invalidate an old write selector.
    data = row(2, 20)
    data["added"] = 7
    client.insert(name, data, timeout=60)
    client.upsert(name, {"id": 2, "added": 9}, partial_update=True, timeout=60)
    result = client.query(
        name, filter="id == 2", output_fields=["value", "added"], consistency_level="Strong", timeout=60
    )
    assert result[0]["value"] == 20
    assert result[0]["added"] == 9


@pytest.mark.tags(CaseLabel.L1)
@pytest.mark.parametrize("first_access", ["read", "write"])
def test_proxy_schema_same_version_metadata_refresh(schema_access_client, first_access):
    client, stub, name = schema_access_client
    create_collection(client, name)
    client.insert(name, [row(1, 10), row(2, 20)], timeout=60)
    assert ids(client, name) == [1, 2]
    collection_id, version = snapshot(stub, name)

    # Run with a long SchemaClient TTL: success must come from invalidation,
    # not idle expiry. Metadata updates deliberately retain the same version.
    for description, timezone in [("first description", "Asia/Shanghai"), ("second description", "UTC")]:
        client.alter_collection_properties(
            name, {"collection.description": description, "timezone": timezone}, timeout=60
        )
        if first_access == "write":
            client.upsert(name, row(2, 22), timeout=60)
        response = stub.DescribeCollection(milvus_pb2.DescribeCollectionRequest(collection_name=name), timeout=60)
        assert response.status.code == 0, response.status
        assert (response.collectionID, response.schema.version) == (collection_id, version)
        assert response.schema.description == description
        assert {kv.key: kv.value for kv in response.schema.properties}["timezone"] == timezone
        assert ids(client, name) == [1, 2]
        if first_access == "read":
            client.upsert(name, row(2, 22), timeout=60)

    client.drop_collection_properties(name, ["timezone"], timeout=60)
    response = stub.DescribeCollection(milvus_pb2.DescribeCollectionRequest(collection_name=name), timeout=60)
    assert response.status.code == 0, response.status
    assert (response.collectionID, response.schema.version) == (collection_id, version)
    assert "timezone" not in {kv.key: kv.value for kv in response.schema.properties}
    assert response.schema.description == "second description"
