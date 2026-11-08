"""Precise state oracles shared by CDC interface contract tests."""

import time
from contextlib import contextmanager

import grpc
from pymilvus import DataType, MilvusException
from pymilvus.client.utils import check_status

from .base import TestCDCSyncBase


def eventually(check, timeout, description):
    """Retry observations, preserving the last failed assertion or RPC error."""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            check()
            return
        except (AssertionError, MilvusException, grpc.RpcError) as exc:
            last_error = exc
            time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {description}: {last_error}") from last_error


def rows_equal(client, name, expected, fields=None, **kwargs):
    if fields is None:
        fields = list(expected[0]) if expected else ["id", "value", "text"]
    actual = client.query(name, filter="id >= 0", output_fields=fields, timeout=10, **kwargs)
    # Query also returns the primary key, including an external table's virtual PK.
    actual = [{field: row[field] for field in fields} for row in actual]
    assert sorted(actual, key=lambda r: r["id"]) == sorted(expected, key=lambda r: r["id"]), actual


def logical_index(info):
    """Exclude build progress and cluster-local identities, not user parameters."""
    local = {"state", "index_state", "total_rows", "indexed_rows", "pending_index_rows", "index_id"}
    return {key: value for key, value in info.items() if key not in local}


def assert_index_sync(upstream, downstream, name, field_name, timeout):
    indexes = upstream.list_indexes(name, field_name=field_name)
    assert indexes, f"No source index on {field_name}"
    expected = {index: logical_index(upstream.describe_index(name, index)) for index in indexes}

    def check():
        for index, definition in expected.items():
            actual = downstream.describe_index(name, index)
            assert actual is not None, f"Index {index} has not reached the target"
            assert logical_index(actual) == definition, (definition, actual)
            assert actual["state"] == "Finished", actual

    eventually(check, timeout, f"index definitions for {name}.{field_name}")


def rpc(client, method, request, timeout=30):
    """Use the authenticated SDK channel for public RPCs without SDK wrappers."""
    response = getattr(client._get_connection()._stub, method)(request, timeout=timeout)
    status = getattr(response, "status", getattr(response, "alter_status", response))
    check_status(status)
    return response


class CDCContract:
    def __init__(self, upstream, downstream, timeout, request):
        self.upstream = upstream
        self.downstream = downstream
        self.timeout = timeout
        self.request = request

    def name(self, prefix, max_length=60):
        return TestCDCSyncBase.gen_unique_name(prefix, max_length=max_length)

    def wait(self, check, description):
        eventually(check, self.timeout, description)

    def schema(self):
        schema = self.upstream.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("value", DataType.INT64, nullable=True)
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=128,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params={"tokenizer": "standard"},
        )
        return schema

    def collection(self, schema=None, load=True):
        name = self.name("cdc_api")
        # Register before creation: a timeout may have committed the operation.
        self.request.addfinalizer(lambda: self.upstream.drop_collection(name))
        self.upstream.create_collection(name, schema=schema or self.schema(), consistency_level="Strong")
        indexes = self.upstream.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        self.upstream.create_index(name, indexes)
        if load:
            self.upstream.load_collection(name)
        self.sync_schema(name)
        return name

    def sync_schema(self, name):
        source = self.upstream.describe_collection(name)
        keys = ("fields", "struct_fields", "struct_array_fields", "functions", "enable_dynamic_field", "description")

        def check():
            target = self.downstream.describe_collection(name)
            for key in keys:
                assert target.get(key) == source.get(key), (key, source.get(key), target.get(key))

        self.wait(check, f"schema of {name}")
        return source

    def data(self, start=0, count=4):
        return [
            {"id": i, "vector": [float(i), 1.0, 0.0, 0.0], "value": i * 10, "text": f"canary document {i}"}
            for i in range(start, start + count)
        ]

    def assert_rows(self, name, rows, fields=None):
        for role, client in (("source", self.upstream), ("target", self.downstream)):
            self.wait(lambda: rows_equal(client, name, rows, fields), f"{role} exact rows of {name}")

    def control_barrier(self):
        """A subsequent database broadcast proves CChannel progress for local DDL."""
        name = self.name("cdc_barrier")
        self.request.addfinalizer(lambda: self.upstream.drop_database(name))
        self.upstream.create_database(name)

        def check():
            assert name in self.downstream.list_databases()

        self.wait(check, "control channel progress")

    @contextmanager
    def unchanged(self, name):
        """Verify both schemas and exact rows after a rejected request."""
        before = []
        for client in (self.upstream, self.downstream):
            before.append(
                (
                    client.describe_collection(name),
                    client.query(name, filter="id >= 0", output_fields=["id", "value", "text"], timeout=10),
                    {index: logical_index(client.describe_index(name, index)) for index in client.list_indexes(name)},
                    client.list_partitions(name),
                )
            )
        yield
        self.control_barrier()
        for client, (schema, rows, indexes, partitions) in zip((self.upstream, self.downstream), before):
            assert client.describe_collection(name) == schema
            assert client.list_partitions(name) == partitions
            assert {
                index: logical_index(client.describe_index(name, index)) for index in client.list_indexes(name)
            } == indexes
            rows_equal(client, name, rows, ["id", "value", "text"])
