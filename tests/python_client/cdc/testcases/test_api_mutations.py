"""CDC contracts for schema evolution and previously uncovered mutations."""

import pytest
from common.common_type import CaseLabel
from pymilvus import DataType, Function, FunctionType, MilvusException
from pymilvus.grpc_gen import milvus_pb2, schema_pb2

from .contract_helpers import assert_index_sync, rows_equal, rpc

pytestmark = pytest.mark.tags(CaseLabel.CDC, CaseLabel.L3)


class TestCDCAPIChanges:
    @pytest.mark.parametrize("surface", ["sdk", "schema_rpc"])
    def test_add_field_existing_and_new_rows(self, cdc_contract, surface):
        c = cdc_contract
        name = c.collection()
        rows = c.data()
        c.upstream.insert(name, rows)
        c.assert_rows(name, rows)
        if surface == "sdk":
            c.upstream.add_collection_field(name, "added", DataType.INT64, nullable=True)
        else:
            field = schema_pb2.FieldSchema(name="added", data_type=DataType.INT64, nullable=True)
            action = milvus_pb2.AlterCollectionSchemaRequest.Action(
                add_request=milvus_pb2.AlterCollectionSchemaRequest.AddRequest(
                    field_infos=[milvus_pb2.AlterCollectionSchemaRequest.FieldInfo(field_schema=field)]
                )
            )
            rpc(
                c.upstream,
                "AlterCollectionSchema",
                milvus_pb2.AlterCollectionSchemaRequest(collection_name=name, action=action),
            )
        c.sync_schema(name)
        old = [{**row, "added": None} for row in rows]
        new = [{**row, "added": row["id"] + 100} for row in c.data(10)]
        c.upstream.insert(name, new)
        c.assert_rows(name, old + new)

    @pytest.mark.parametrize("by_id", [False, True], ids=["name", "id"])
    def test_drop_field_cascades_index(self, cdc_contract, by_id):
        c = cdc_contract
        name = c.collection()
        rows = c.data()
        c.upstream.insert(name, rows)
        indexes = c.upstream.prepare_index_params()
        indexes.add_index("value", index_name="value_idx", index_type="INVERTED")
        c.upstream.create_index(name, indexes)
        assert_index_sync(c.upstream, c.downstream, name, "value", c.timeout)
        c.assert_rows(name, rows)
        description = c.upstream.describe_collection(name)
        field_id = next(f["field_id"] for f in description["fields"] if f["name"] == "value")
        kwargs = {"field_id": field_id} if by_id else {"field_name": "value"}
        c.upstream.drop_collection_field(name, **kwargs)
        c.sync_schema(name)

        def check():
            assert "value_idx" not in c.downstream.list_indexes(name)
            assert "vector" in c.downstream.list_indexes(name)

        c.wait(check, "dropped field index cleanup")
        expected = [{k: v for k, v in row.items() if k != "value"} for row in rows]
        c.assert_rows(name, expected)
        for client in (c.upstream, c.downstream):
            with pytest.raises(MilvusException, match="value.*not exist|not found.*value"):
                client.query(name, filter="value > 0", timeout=10)

    def test_add_struct_field(self, cdc_contract):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        struct = c.upstream.create_struct_field_schema()
        struct.add_field("score", DataType.INT64)
        struct.add_field("label", DataType.VARCHAR, max_length=32)
        c.upstream.add_collection_struct_field(name, "events", struct, max_capacity=4, nullable=True)
        c.sync_schema(name)
        rows = [{**row, "events": [{"score": row["id"], "label": "new"}]} for row in c.data(10)]
        c.upstream.insert(name, rows)
        c.assert_rows(name, [{**row, "events": None} for row in c.data()] + rows)

    def test_function_field_lifecycle_and_rejected_detach(self, cdc_contract):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        function = Function("bm25", FunctionType.BM25, ["text"], ["sparse"])
        field = c.upstream.create_field_schema("sparse", DataType.SPARSE_FLOAT_VECTOR)
        index = c.upstream.prepare_index_params()
        index.add_index("sparse", index_name="sparse_idx", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        c.upstream.add_function_field(name, field, function, index)
        c.sync_schema(name)
        assert_index_sync(c.upstream, c.downstream, name, "sparse", c.timeout)
        # Explicitly reach the server: the high-level drop wrapper always drops output fields.
        request = milvus_pb2.AlterCollectionSchemaRequest(
            collection_name=name,
            action=milvus_pb2.AlterCollectionSchemaRequest.Action(
                drop_request=milvus_pb2.AlterCollectionSchemaRequest.DropRequest(
                    function_name="bm25", drop_function_output_fields=False
                )
            ),
        )
        with c.unchanged(name):
            with pytest.raises(MilvusException, match="without dropping.*not supported|always removes"):
                rpc(c.upstream, "AlterCollectionSchema", request)

        def searchable():
            for client in (c.upstream, c.downstream):
                hits = client.search(
                    name, data=["canary"], anns_field="sparse", limit=10, search_params={"metric_type": "BM25"}
                )
                assert len(hits) == 1
                assert {hit["id"] for hit in hits[0]} == {row["id"] for row in c.data()}

        c.wait(searchable, "BM25 results for rows written before adding the function")
        # Schema change must permit subsequent writes on the existing loaded collection.
        c.upstream.insert(name, c.data(10))
        c.assert_rows(name, c.data() + c.data(10))
        c.upstream.drop_function_field(name, "bm25")
        c.sync_schema(name)

        def check():
            assert "sparse_idx" not in c.downstream.list_indexes(name)
            assert not c.downstream.describe_collection(name).get("functions")

        c.wait(check, "function and output index removal")
        c.assert_rows(name, c.data() + c.data(10))

    @pytest.mark.parametrize("role", ["upstream", "downstream"])
    @pytest.mark.parametrize("operation", ["add", "drop", "alter"])
    def test_legacy_function_rejected_without_changes(self, cdc_contract, role, operation):
        c = cdc_contract
        schema = c.schema()
        schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
        function = Function("bm25", FunctionType.BM25, ["text"], ["sparse"])
        schema.add_function(function)
        name = c.collection(schema, load=False)
        index = c.upstream.prepare_index_params()
        index.add_index("sparse", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        c.upstream.create_index(name, index)
        c.upstream.load_collection(name)
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        client = getattr(c, role)
        calls = {
            "add": lambda: client.add_collection_function(name, function, timeout=15),
            "drop": lambda: client.drop_collection_function(name, "bm25", timeout=15),
            "alter": lambda: client.alter_collection_function(name, "bm25", function, timeout=15),
        }
        with c.unchanged(name):
            with pytest.raises(
                MilvusException,
                match="(?i)not supported|no longer supported|does not support alter BM25|only.*TextEmbedding|not primary|secondary",
            ):
                calls[operation]()
        c.upstream.insert(name, c.data(10))
        c.assert_rows(name, c.data() + c.data(10))

    def test_truncate_preserves_metadata_and_new_writes(self, cdc_contract):
        c = cdc_contract
        name = c.collection()
        c.upstream.create_partition(name, "retained")
        alias = c.name("cdc_alias")
        c.request.addfinalizer(lambda: c.upstream.drop_alias(alias))
        c.upstream.create_alias(name, alias)
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        schema = c.sync_schema(name)
        for _ in range(2):
            c.upstream.truncate_collection(name)
            c.assert_rows(name, [], ["id", "value", "text"])
        for client in (c.upstream, c.downstream):
            assert client.describe_collection(name)["fields"] == schema["fields"]
            assert "retained" in client.list_partitions(name)
            assert client.describe_alias(alias)["collection_name"] == name
            assert "vector" in client.list_indexes(name)
        c.upstream.insert(name, c.data(100))
        c.assert_rows(name, c.data(100))

    def test_partial_update_retains_omitted_fields(self, cdc_contract):
        c = cdc_contract
        name = c.collection()
        rows = c.data()
        c.upstream.insert(name, rows)
        c.assert_rows(name, rows)
        c.upstream.upsert(name, [{"id": 0, "value": 99}, {"id": 1, "value": None}], partial_update=True)
        rows[0]["value"] = 99
        rows[1]["value"] = None
        c.assert_rows(name, rows)

    def test_alter_field_and_index_properties(self, cdc_contract):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        c.upstream.alter_collection_field(name, "text", {"max_length": 256})
        c.sync_schema(name)
        c.upstream.release_collection(name)
        c.upstream.alter_index_properties(name, "vector", {"mmap.enabled": True})
        assert_index_sync(c.upstream, c.downstream, name, "vector", c.timeout)
        assert str(c.downstream.describe_index(name, "vector")["mmap.enabled"]).lower() == "true"
        c.upstream.drop_index_properties(name, "vector", ["mmap.enabled"])
        assert_index_sync(c.upstream, c.downstream, name, "vector", c.timeout)
        assert "mmap.enabled" not in c.downstream.describe_index(name, "vector")
        c.upstream.load_collection(name)
        c.assert_rows(name, c.data())

    @pytest.mark.parametrize("operation", ["insert", "upsert", "delete", "add_field", "truncate", "create_partition"])
    def test_secondary_write_rejected_without_changes(self, cdc_contract, operation):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        calls = {
            "insert": lambda: c.downstream.insert(name, c.data(100), timeout=10),
            "upsert": lambda: c.downstream.upsert(name, c.data(100), timeout=10),
            "delete": lambda: c.downstream.delete(name, ids=[0], timeout=10),
            "add_field": lambda: c.downstream.add_collection_field(
                name, "forbidden", DataType.INT64, nullable=True, timeout=10
            ),
            "truncate": lambda: c.downstream.truncate_collection(name, timeout=10),
            "create_partition": lambda: c.downstream.create_partition(name, "forbidden", timeout=10),
        }
        with c.unchanged(name):
            with pytest.raises(MilvusException, match="(?i)not primary|replicat.*violation|secondary"):
                calls[operation]()
        assert "forbidden" not in c.downstream.list_partitions(name)
        c.upstream.insert(name, c.data(10))
        c.assert_rows(name, c.data() + c.data(10))

    def test_flush_all_on_both_roles_preserves_replication(self, cdc_contract):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        for client in (c.upstream, c.downstream):
            client.flush_all(timeout=c.timeout)
            rows_equal(client, name, c.data())
        c.upstream.insert(name, c.data(10))
        c.assert_rows(name, c.data() + c.data(10))
