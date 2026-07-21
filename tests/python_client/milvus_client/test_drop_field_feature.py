import time

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from common.external_table_common import (
    build_external_source,
    build_external_spec,
    get_minio_config,
)
from pymilvus import DataType, Function, FunctionType


def rows_by_id(rows):
    return {row["id"]: row for row in rows}


class TestMilvusClientDropFieldFeature(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_scalar_field_basic_read_write(self):
        """
        TC-L0-01: Drop normal scalar field and verify basic read/write paths.

        target: verify basic scalar drop field path
        method: drop VarChar field tag, then query/search/insert without it
        expected: tag disappears from schema; non-tag read/write works; tag references fail
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with the scalar field that will be dropped.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert sealed data containing the target field.
        rows = [{"id": i, "vec": [float(i), 0.0, 0.0, 0.0], "age": 20 + i, "tag": f"tag_{i}"} for i in range(5)]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        # Step 3: Drop the scalar field and verify it leaves the schema.
        client.drop_collection_field(collection_name, "tag")

        schema_fields = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        assert "tag" not in schema_fields
        assert "age" in schema_fields
        assert "vec" in schema_fields

        # Step 4: Verify read paths that do not reference the dropped field still work.
        query_res = client.query(
            collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["id", "age"],
        )
        query_by_id = rows_by_id(query_res)
        assert query_by_id[0]["age"] == 20
        assert query_by_id[4]["age"] == 24

        search_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["id", "age"],
        )
        assert len(search_res[0]) == 3
        assert "age" in search_res[0][0]["entity"]
        assert "tag" not in search_res[0][0]["entity"]

        # Step 5: Verify explicit references to the dropped field fail clearly.
        for kwargs in [
            {"filter": 'tag == "tag_1"', "output_fields": ["id"]},
            {"filter": "id >= 0", "output_fields": ["id", "tag"]},
        ]:
            self.query(
                client,
                collection_name,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: "field tag not exist"},
                **kwargs,
            )

        # Step 6: Insert new rows without the dropped field.
        client.insert(
            collection_name=collection_name,
            data=[{"id": 100, "vec": [1.0, 0.0, 0.0, 0.0], "age": 100}],
        )
        client.flush(collection_name)

        new_row = client.query(collection_name, filter="id == 100", output_fields=["*"])
        assert rows_by_id(new_row)[100]["age"] == 100
        assert "tag" not in rows_by_id(new_row)[100]
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_indexed_scalar_field_cascade(self):
        """
        TC-L0-02: Drop indexed scalar field and verify index cascade convergence.

        target: verify index metadata on dropped field is removed
        method: build indexes on vec and tag, drop tag, then verify tag index disappears
        expected: tag field and tag index disappear; vec index and vec search keep working
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with indexes on both the kept vector field and the dropped scalar field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(field_name="tag", index_type="INVERTED")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert enough sealed rows for the scalar index to be materialized.
        rows = [
            {
                "id": i,
                "vec": [float(i % 10), float(i % 7), float(i % 5), float(i % 3)],
                "age": 20 + (i % 100),
                "tag": f"tag_{i % 50}",
            }
            for i in range(3000)
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)

        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        assert self.wait_for_index_ready(client, collection_name, index_name="tag", timeout=120)

        indexes = client.list_indexes(collection_name)
        assert "vec" in indexes
        assert "tag" in indexes

        tag_index = client.describe_index(collection_name, index_name="tag")
        assert tag_index["field_name"] == "tag"
        assert tag_index["index_type"] == "INVERTED"

        # Step 3: Confirm the indexed field is usable before drop.
        client.load_collection(collection_name)

        pre_drop_query = client.query(
            collection_name,
            filter='tag == "tag_1"',
            output_fields=["id", "tag"],
            limit=5,
        )
        assert len(pre_drop_query) > 0
        assert all(row["tag"] == "tag_1" for row in pre_drop_query)

        # Step 4: Drop the indexed scalar field.
        client.drop_collection_field(collection_name, "tag")

        schema_fields = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        assert "tag" not in schema_fields
        assert "vec" in schema_fields
        assert "age" in schema_fields

        # Step 5: Wait for index metadata cascade and verify the vector index remains.
        for _ in range(30):
            indexes = client.list_indexes(collection_name)
            if "tag" not in indexes:
                break
            time.sleep(1)
        assert "tag" not in indexes
        assert "vec" in indexes

        vec_index = client.describe_index(collection_name, index_name="vec")
        assert vec_index["field_name"] == "vec"

        # Step 6: Verify read paths on the kept fields still work.
        search_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=5,
            output_fields=["id", "age"],
        )
        assert len(search_res[0]) == 5
        assert "age" in search_res[0][0]["entity"]
        assert "tag" not in search_res[0][0]["entity"]

        # Step 7: Verify dropped field references and dropped index lookup fail.
        self.query(
            client,
            collection_name,
            filter='tag == "tag_1"',
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field tag not exist"},
        )

        assert client.describe_index(collection_name, index_name="tag") is None
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_bm25_function_removes_output_field_and_index(self):
        """
        TC-L0-03: Drop BM25 function and verify output field/index cascade.

        target: verify dropping BM25 function removes function output field and its index
        method: create BM25 function from text to sparse, reject detach-only drop, then cascade-drop function field
        expected: function and sparse output field disappear; text and vec remain usable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a BM25 function whose output sparse vector field will be cascade-dropped.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_function(
            Function(
                name="bm25",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names="sparse",
            )
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert text rows and wait for both dense and sparse indexes.
        rows = [
            {
                "id": i,
                "text": f"milvus bm25 function document {i % 5}",
                "vec": [float(i % 10), float(i % 7), float(i % 5), float(i % 3)],
            }
            for i in range(100)
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)

        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=120)

        indexes = client.list_indexes(collection_name)
        assert "vec" in indexes
        assert "sparse" in indexes

        desc = client.describe_collection(collection_name)
        assert [func["name"] for func in desc.get("functions", [])] == ["bm25"]
        assert "sparse" in [field["name"] for field in desc["fields"]]

        # Step 3: Confirm BM25 search works before dropping the function.
        client.load_collection(collection_name)

        bm25_res = client.search(
            collection_name,
            data=["milvus function"],
            anns_field="sparse",
            limit=5,
            output_fields=["id", "text"],
        )
        assert len(bm25_res[0]) > 0
        assert "text" in bm25_res[0][0]["entity"]

        # Step 4: BM25 cannot be detached from its output field through the legacy SDK API.
        self.drop_collection_function(
            client,
            collection_name,
            "bm25",
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: (
                    "detaching a function without dropping its output field is not supported; "
                    "drop_function always removes the function together with its output field: "
                    "bm25: invalid parameter"
                ),
            },
        )

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        function_names = [func["name"] for func in desc.get("functions", [])]
        assert "bm25" in function_names
        assert "sparse" in field_names
        assert "sparse" in client.list_indexes(collection_name)

        # Step 5: Drop through the cascade SDK API and verify its output field and index disappear.
        self.drop_function_field(client, collection_name, "bm25")

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        function_names = [func["name"] for func in desc.get("functions", [])]
        assert "bm25" not in function_names
        assert "sparse" not in field_names
        assert "text" in field_names
        assert "vec" in field_names

        indexes = client.list_indexes(collection_name)
        assert "sparse" not in indexes
        assert "vec" in indexes
        assert client.describe_index(collection_name, index_name="sparse") is None

        # Step 6: Verify remaining scalar and dense vector read paths still work.
        query_res = client.query(
            collection_name,
            filter="id in [0, 1, 2]",
            output_fields=["id", "text"],
        )
        assert len(query_res) == 3
        assert all("text" in row for row in query_res)

        vec_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=5,
            output_fields=["id", "text"],
        )
        assert len(vec_res[0]) == 5
        assert "text" in vec_res[0][0]["entity"]
        assert "sparse" not in vec_res[0][0]["entity"]

        # Step 7: Verify the dropped sparse field cannot be searched.
        self.search(
            client,
            collection_name,
            data=["milvus function"],
            anns_field="sparse",
            limit=5,
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "failed to get field schema by name: fieldName(sparse) not found",
            },
        )

        # Step 8: Insert rows using the remaining fields.
        client.insert(
            collection_name=collection_name,
            data=[{"id": 1000, "text": "text after bm25 drop", "vec": [1.0, 0.0, 0.0, 0.0]}],
        )
        new_row = client.query(collection_name, filter="id == 1000", output_fields=["*"])
        assert new_row[0]["text"] == "text after bm25 drop"
        assert "sparse" not in new_row[0]
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_disable_dynamic_schema_removes_dynamic_visibility(self):
        """
        TC-L0-04: Disable dynamic schema and verify dynamic data is no longer visible.

        target: verify disabling dynamic schema hides old dynamic keys and rejects new unknown keys
        method: insert dynamic rows, disable dynamic schema, then query/search/insert around dynamic keys
        expected: dynamic keys disappear from API output; dynamic references fail; static fields still work
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a dynamic-schema collection with static fields only in the declared schema.
        schema = client.create_schema(enable_dynamic_field=True, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert rows with dynamic keys and verify they are visible before disable.
        rows = [
            {
                "id": i,
                "vec": [float(i), 0.0, 0.0, 0.0],
                "age": 20 + i,
                "dyn_tag": f"tag_{i}",
                "dyn_score": i,
            }
            for i in range(5)
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        before_desc = client.describe_collection(collection_name)
        assert before_desc["enable_dynamic_field"] is True

        before_meta = client.query(
            collection_name,
            filter="id == 1",
            output_fields=["id", "$meta"],
        )
        assert before_meta[0]["dyn_tag"] == "tag_1"
        assert before_meta[0]["dyn_score"] == 1

        before_query = client.query(
            collection_name,
            filter='dyn_tag == "tag_1"',
            output_fields=["id", "dyn_tag"],
        )
        assert len(before_query) == 1
        assert before_query[0]["dyn_tag"] == "tag_1"

        before_star = client.query(collection_name, filter="id == 1", output_fields=["*"])
        assert before_star[0]["dyn_tag"] == "tag_1"
        assert before_star[0]["dyn_score"] == 1

        # Step 3: Disable dynamic schema and verify dynamic metadata is removed from schema output.
        client.alter_collection_properties(collection_name, {"dynamicfield.enabled": False})

        after_desc = client.describe_collection(collection_name)
        assert after_desc["enable_dynamic_field"] is False
        assert "$meta" not in [field["name"] for field in after_desc["fields"]]

        # Step 4: Verify wildcard query/search no longer exposes old dynamic keys.
        after_star = client.query(
            collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["*"],
        )
        assert len(after_star) == 5
        for row in after_star:
            assert "id" in row
            assert "age" in row
            assert "vec" in row
            assert "dyn_tag" not in row
            assert "dyn_score" not in row
            assert "$meta" not in row

        search_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["*"],
        )
        assert len(search_res[0]) == 3
        assert "age" in search_res[0][0]["entity"]
        assert "dyn_tag" not in search_res[0][0]["entity"]
        assert "dyn_score" not in search_res[0][0]["entity"]
        assert "$meta" not in search_res[0][0]["entity"]

        # Step 5: Verify explicit dynamic key and $meta references fail.
        for kwargs in [
            {"filter": 'dyn_tag == "tag_1"', "output_fields": ["id"]},
            {"filter": "id >= 0", "output_fields": ["id", "dyn_tag"]},
            {"filter": "id >= 0", "output_fields": ["id", "$meta"]},
        ]:
            expected_field = "$meta" if "$meta" in kwargs["output_fields"] else "dyn_tag"
            self.query(
                client,
                collection_name,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: f"field {expected_field} not exist"},
                **kwargs,
            )

        # Step 6: Verify static-only insert works and new dynamic keys are rejected.
        client.insert(
            collection_name=collection_name,
            data=[{"id": 100, "vec": [1.0, 0.0, 0.0, 0.0], "age": 100}],
        )
        static_row = client.query(collection_name, filter="id == 100", output_fields=["*"])
        assert static_row[0]["age"] == 100
        assert "dyn_after_disable" not in static_row[0]

        self.insert(
            client,
            collection_name=collection_name,
            data=[
                {
                    "id": 101,
                    "vec": [1.0, 1.0, 0.0, 0.0],
                    "age": 101,
                    "dyn_after_disable": "new",
                }
            ],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "Attempt to insert an unexpected field `dyn_after_disable`"},
        )
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_one_vector_field_keeps_another_vector_field(self):
        """
        TC-L1-01: Drop one vector field and keep another vector field.

        target: verify multi-vector collection keeps remaining vector field searchable after dropping one vector field
        method: create dense1 and dense2, build indexes, drop dense2, then search dense1 and reject dense2/last-vector drop
        expected: dense2 disappears; dense1 search works; dense2 search fails; dropping dense1 is rejected as last vector field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with two vector fields.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("dense1", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("dense2", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="dense1", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(field_name="dense2", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert sealed rows and wait for both vector indexes.
        rows = [
            {
                "id": i,
                "dense1": [float(i % 10), float(i % 7), float(i % 5), float(i % 3)],
                "dense2": [float(i % 3), float(i % 5), float(i % 7), float(i % 10)],
                "age": 20 + (i % 100),
            }
            for i in range(3000)
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)

        assert self.wait_for_index_ready(client, collection_name, index_name="dense1", timeout=120)
        assert self.wait_for_index_ready(client, collection_name, index_name="dense2", timeout=120)

        indexes = client.list_indexes(collection_name)
        assert "dense1" in indexes
        assert "dense2" in indexes

        client.load_collection(collection_name)

        # Step 3: Confirm the vector field that will be dropped is searchable before drop.
        dense2_before_drop = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="dense2",
            limit=5,
            output_fields=["id", "age"],
        )
        assert len(dense2_before_drop[0]) == 5
        assert "age" in dense2_before_drop[0][0]["entity"]

        # Step 4: Drop one vector field and verify only that field and index disappear.
        client.drop_collection_field(collection_name, "dense2")

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        assert "dense1" in field_names
        assert "dense2" not in field_names
        assert "age" in field_names

        for _ in range(30):
            indexes = client.list_indexes(collection_name)
            if "dense2" not in indexes:
                break
            time.sleep(1)
        assert "dense1" in indexes
        assert "dense2" not in indexes
        assert client.describe_index(collection_name, index_name="dense2") is None

        # Step 5: Verify the remaining vector field still supports search.
        dense1_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="dense1",
            limit=5,
            output_fields=["id", "age"],
        )
        assert len(dense1_res[0]) == 5
        assert "age" in dense1_res[0][0]["entity"]
        assert "dense2" not in dense1_res[0][0]["entity"]

        # Step 6: Verify dropped vector search fails and the last vector field cannot be dropped.
        self.search(
            client,
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="dense2",
            limit=5,
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "failed to get field schema by name: fieldName(dense2) not found",
            },
        )

        self.drop_collection_field(
            client,
            collection_name,
            field_name="dense1",
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "cannot drop the last vector field: dense1"},
        )
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_then_add_same_name_field_does_not_reuse_old_data(self):
        """
        TC-L1-02: Drop then add same-name field and verify old data does not pollute the new field.

        target: verify same-name field after drop is a new schema field
        method: drop VarChar extra, add Int64 extra with same name, then query before and after reload
        expected: old extra values are not exposed through the new extra field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with the original VarChar extra field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("extra", DataType.VARCHAR, max_length=64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert old sealed rows with a recognizable extra value.
        old_rows = [{"id": i, "vec": [float(i), 0.0, 0.0, 0.0], "age": 20 + i, "extra": "old_value"} for i in range(5)]
        client.insert(collection_name=collection_name, data=old_rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        before_drop_desc = client.describe_collection(collection_name)
        old_extra_field = next(field for field in before_drop_desc["fields"] if field["name"] == "extra")
        old_extra_field_id = old_extra_field.get("field_id")

        old_query = client.query(collection_name, filter='extra == "old_value"', output_fields=["id", "extra"])
        assert len(old_query) == 5
        assert all(row["extra"] == "old_value" for row in old_query)

        # Step 3: Drop extra and add a same-name field with a different type.
        client.drop_collection_field(collection_name, "extra")

        after_drop_desc = client.describe_collection(collection_name)
        assert "extra" not in [field["name"] for field in after_drop_desc["fields"]]

        max_field_id_after_drop = None
        properties = after_drop_desc.get("properties", {})
        if isinstance(properties, dict) and properties.get("max_field_id") is not None:
            max_field_id_after_drop = int(properties["max_field_id"])
        elif isinstance(properties, list):
            for prop in properties:
                if prop.get("key") == "max_field_id":
                    max_field_id_after_drop = int(prop["value"])
                    break

        client.add_collection_field(
            collection_name,
            field_name="extra",
            data_type=DataType.INT64,
            nullable=True,
        )

        after_add_desc = client.describe_collection(collection_name)
        new_extra_field = next(field for field in after_add_desc["fields"] if field["name"] == "extra")
        new_extra_field_id = new_extra_field.get("field_id")

        if old_extra_field_id is not None and new_extra_field_id is not None:
            assert new_extra_field_id > old_extra_field_id
        if max_field_id_after_drop is not None and new_extra_field_id is not None:
            assert new_extra_field_id > max_field_id_after_drop

        # Step 4: Verify old rows do not expose old physical extra data through the new extra field.
        old_rows_after_add = client.query(
            collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["id", "extra"],
        )
        assert len(old_rows_after_add) == 5
        for row in old_rows_after_add:
            assert row.get("extra") != "old_value"

        # Step 5: Insert new rows using the new Int64 extra field.
        new_rows = [
            {"id": 100 + i, "vec": [float(i), 1.0, 0.0, 0.0], "age": 100 + i, "extra": 2026 + i} for i in range(5)
        ]
        client.insert(collection_name=collection_name, data=new_rows)
        client.flush(collection_name)

        new_extra_query = client.query(
            collection_name,
            filter="extra >= 2026",
            output_fields=["id", "extra"],
        )
        assert len(new_extra_query) == 5
        assert {row["id"] for row in new_extra_query} == {100, 101, 102, 103, 104}
        assert {row["extra"] for row in new_extra_query} == {2026, 2027, 2028, 2029, 2030}

        # Step 6: Verify search output does not return old extra values.
        search_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=10,
            output_fields=["id", "extra"],
        )
        assert len(search_res[0]) == 10
        for hit in search_res[0]:
            assert hit["entity"].get("extra") != "old_value"

        # Step 7: Reload and repeat the key old/new row checks.
        client.release_collection(collection_name)
        client.load_collection(collection_name)

        old_rows_after_reload = client.query(
            collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["id", "extra"],
        )
        assert len(old_rows_after_reload) == 5
        for row in old_rows_after_reload:
            assert row.get("extra") != "old_value"

        new_extra_after_reload = client.query(
            collection_name,
            filter="extra >= 2026",
            output_fields=["id", "extra"],
        )
        assert len(new_extra_after_reload) == 5
        assert {row["id"] for row in new_extra_after_reload} == {100, 101, 102, 103, 104}
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_field_loaded_collection_reload_delta_load_path(self):
        """
        TC-L1-03: Loaded collection reload / delta-load path.

        target: verify dropped field is skipped across loaded sealed data, DDL-sealed growing data, and reload
        method: insert sealed rows, load, insert growing rows, drop tag, insert post-drop rows, then verify before and after reload
        expected: all rows remain visible through kept fields; tag is not visible or usable after drop
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with a field that will be dropped while the collection is loaded.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert and flush pre-drop sealed rows.
        sealed_rows = [
            {"id": i, "vec": [float(i), 0.0, 0.0, 0.0], "age": 20 + i, "tag": "sealed_before_drop"} for i in range(5)
        ]
        client.insert(collection_name=collection_name, data=sealed_rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        sealed_check = client.query(
            collection_name,
            filter='tag == "sealed_before_drop"',
            output_fields=["id", "tag"],
        )
        assert len(sealed_check) == 5

        # Step 3: Insert pre-drop growing rows without manual flush.
        growing_rows = [
            {"id": 100 + i, "vec": [float(i), 1.0, 0.0, 0.0], "age": 100 + i, "tag": "growing_before_drop"}
            for i in range(5)
        ]
        client.insert(collection_name=collection_name, data=growing_rows)

        growing_check = client.query(
            collection_name,
            filter='tag == "growing_before_drop"',
            output_fields=["id", "tag"],
        )
        assert len(growing_check) == 5

        # Step 4: Drop tag while sealed and growing pre-drop rows both exist.
        client.drop_collection_field(collection_name, "tag")

        desc = client.describe_collection(collection_name)
        assert "tag" not in [field["name"] for field in desc["fields"]]

        # Step 5: Verify pre-drop sealed and DDL-sealed growing rows remain visible through kept fields.
        pre_drop_rows = client.query(
            collection_name,
            filter="id in [0, 1, 2, 3, 4, 100, 101, 102, 103, 104]",
            output_fields=["id", "age"],
        )
        assert {row["id"] for row in pre_drop_rows} == {0, 1, 2, 3, 4, 100, 101, 102, 103, 104}
        assert all("tag" not in row for row in pre_drop_rows)

        search_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=10,
            output_fields=["id", "age"],
        )
        assert len(search_res[0]) == 10
        assert all("tag" not in hit["entity"] for hit in search_res[0])

        # Step 6: Verify explicit references to the dropped field fail after drop.
        for kwargs in [
            {"filter": 'tag == "sealed_before_drop"', "output_fields": ["id"]},
            {"filter": "id >= 0", "output_fields": ["id", "tag"]},
        ]:
            self.query(
                client,
                collection_name,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: "field tag not exist"},
                **kwargs,
            )

        # Step 7: Insert post-drop rows using the latest schema.
        post_drop_rows = [{"id": 200 + i, "vec": [float(i), 2.0, 0.0, 0.0], "age": 200 + i} for i in range(5)]
        client.insert(collection_name=collection_name, data=post_drop_rows)
        client.flush(collection_name)

        all_rows = client.query(
            collection_name,
            filter="id in [0, 1, 2, 3, 4, 100, 101, 102, 103, 104, 200, 201, 202, 203, 204]",
            output_fields=["id", "age"],
        )
        assert {row["id"] for row in all_rows} == {
            0,
            1,
            2,
            3,
            4,
            100,
            101,
            102,
            103,
            104,
            200,
            201,
            202,
            203,
            204,
        }
        assert all("tag" not in row for row in all_rows)

        # Step 8: Release and load again to verify segment reload skips dropped field data.
        client.release_collection(collection_name)
        client.load_collection(collection_name)

        rows_after_reload = client.query(
            collection_name,
            filter="id in [0, 1, 2, 3, 4, 100, 101, 102, 103, 104, 200, 201, 202, 203, 204]",
            output_fields=["id", "age"],
        )
        assert {row["id"] for row in rows_after_reload} == {
            0,
            1,
            2,
            3,
            4,
            100,
            101,
            102,
            103,
            104,
            200,
            201,
            202,
            203,
            204,
        }
        assert all("tag" not in row for row in rows_after_reload)

        search_after_reload = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=10,
            output_fields=["id", "age"],
        )
        assert len(search_after_reload[0]) == 10
        assert all("tag" not in hit["entity"] for hit in search_after_reload[0])
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_struct_array_field_and_reject_sub_field_drop(self):
        """
        TC-L1-05: Drop StructArray field and reject sub-field drop.

        target: verify whole StructArray field can be dropped and StructArray sub-field drop is rejected
        method: reject events[name] drop first, then drop events and verify nested index cascade
        expected: failed sub-field drop does not change schema; events and nested indexes disappear after whole-field drop
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with a StructArray field and nested indexes.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=4)
        struct_schema.add_field("name", DataType.VARCHAR, max_length=128)
        struct_schema.add_field("score", DataType.INT64)
        schema.add_field(
            "events",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=4,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(field_name="events[embedding]", index_type="HNSW", metric_type="MAX_SIM_L2")
        index_params.add_index(field_name="events[name]", index_type="INVERTED")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert rows and verify the StructArray field is usable before any drop.
        rows = [
            {
                "id": i,
                "vec": [float(i), 0.0, 0.0, 0.0],
                "events": [
                    {
                        "embedding": [float(i), 0.0, 0.0, 0.0],
                        "name": f"event_{i}",
                        "score": i,
                    },
                    {
                        "embedding": [float(i), 1.0, 0.0, 0.0],
                        "name": f"event_extra_{i}",
                        "score": i + 100,
                    },
                ],
            }
            for i in range(10)
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)

        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=60)

        indexes = client.list_indexes(collection_name)
        assert "vec" in indexes
        assert "events[embedding]" in indexes
        assert "events[name]" in indexes
        assert (
            client.describe_index(collection_name, index_name="events[embedding]")["field_name"] == "events[embedding]"
        )
        assert client.describe_index(collection_name, index_name="events[name]")["field_name"] == "events[name]"

        client.load_collection(collection_name)

        pre_drop_rows = client.query(
            collection_name,
            filter='array_contains(events[name], "event_1")',
            output_fields=["id", "events"],
        )
        assert len(pre_drop_rows) == 1
        assert pre_drop_rows[0]["events"][0]["name"] == "event_1"

        # Step 3: Reject direct sub-field drop and verify schema/indexes are unchanged.
        self.drop_collection_field(
            client,
            collection_name,
            field_name="events[name]",
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field not found: events[name]: invalid parameter"},
        )

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        assert "events" in field_names
        indexes = client.list_indexes(collection_name)
        assert "events[embedding]" in indexes
        assert "events[name]" in indexes
        assert "vec" in indexes

        # Step 4: Drop the whole StructArray field.
        client.drop_collection_field(collection_name, "events")

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        assert "events" not in field_names
        assert "vec" in field_names

        # Step 5: Verify nested indexes are cascade-removed while the normal vector index remains.
        for _ in range(30):
            indexes = client.list_indexes(collection_name)
            if "events[embedding]" not in indexes and "events[name]" not in indexes:
                break
            time.sleep(1)

        assert "events[embedding]" not in indexes
        assert "events[name]" not in indexes
        assert "vec" in indexes
        assert client.describe_index(collection_name, index_name="events[embedding]") is None
        assert client.describe_index(collection_name, index_name="events[name]") is None

        # Step 6: Verify kept-field read/write paths still work and StructArray references fail.
        search_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=5,
            output_fields=["id"],
        )
        assert len(search_res[0]) == 5

        self.query(
            client,
            collection_name,
            filter='array_contains(events[name], "event_1")',
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "struct field not found: events[name]"},
        )

        client.insert(
            collection_name=collection_name,
            data=[{"id": 100, "vec": [1.0, 0.0, 0.0, 0.0]}],
        )
        new_row = client.query(collection_name, filter="id == 100", output_fields=["*"])
        assert new_row[0]["id"] == 100
        assert "events" not in new_row[0]
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_then_readd_same_struct_array_name_isolates_old_offsets(self):
        """
        target: verify a same-name Struct Array added after drop gets new field IDs and independent physical data
        method: drop an indexed vector Struct, re-add the same parent/child name with a scalar-only schema through
            an alias, then query old/new rows before and after nested index build and reload
        expected: old Struct binlogs and indexes never surface through the new field; old rows are null and new offsets
            belong only to newly inserted rows
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias = cf.gen_unique_str("struct_readd_alias")
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        old_struct_schema = client.create_struct_field_schema()
        old_struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=4)
        old_struct_schema.add_field("name", DataType.VARCHAR, max_length=64)
        schema.add_field(
            "events",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=old_struct_schema,
            max_capacity=4,
        )
        index_params = client.prepare_index_params()
        index_params.add_index("vec", index_type="HNSW", metric_type="L2", params={"M": 8, "efConstruction": 64})
        index_params.add_index("events[embedding]", index_type="HNSW", metric_type="MAX_SIM_L2")
        index_params.add_index("events[name]", index_type="INVERTED")
        client.create_collection(
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        client.create_alias(collection_name, alias)

        old_rows = [
            {
                "id": row_id,
                "vec": [float(row_id), 0.0, 0.0, 0.0],
                "events": [
                    {
                        "embedding": [float(row_id), 1.0, 0.0, 0.0],
                        "name": f"old_{row_id}",
                    }
                ],
            }
            for row_id in range(3)
        ]
        client.insert(collection_name, old_rows)
        client.flush(collection_name)
        client.load_collection(collection_name)
        before_drop = client.describe_collection(collection_name)
        old_parent = next(field for field in before_drop["fields"] if field["name"] == "events")
        old_parent_id = old_parent.get("field_id")

        client.drop_collection_field(collection_name, "events")
        for _ in range(30):
            if not any(index_name.startswith("events[") for index_name in client.list_indexes(collection_name)):
                break
            time.sleep(1)
        assert not any(index_name.startswith("events[") for index_name in client.list_indexes(collection_name))

        new_struct_schema = client.create_struct_field_schema()
        new_struct_schema.add_field("name", DataType.VARCHAR, max_length=128)
        new_struct_schema.add_field("rank", DataType.INT64)
        client.add_collection_struct_field(
            alias,
            "events",
            new_struct_schema,
            max_capacity=3,
        )
        after_add = client.describe_collection(alias)
        new_parent = next(field for field in after_add["fields"] if field["name"] == "events")
        new_parent_id = new_parent.get("field_id")
        if old_parent_id is not None and new_parent_id is not None:
            assert new_parent_id > old_parent_id

        old_after_add = client.query(
            alias,
            filter="id in [0, 1, 2]",
            output_fields=["id", "events"],
        )
        assert {row["id"] for row in old_after_add} == {0, 1, 2}
        assert all(row["events"] is None for row in old_after_add)
        assert client.query(alias, filter='array_contains(events[name], "old_1")', output_fields=["id"]) == []

        new_rows = [
            {"id": 100, "vec": [100.0, 0.0, 0.0, 0.0], "events": []},
            {
                "id": 101,
                "vec": [101.0, 0.0, 0.0, 0.0],
                "events": [{"name": "new_a", "rank": 10}, {"name": "new_b", "rank": 20}],
            },
        ]
        client.insert(alias, new_rows)
        client.flush(alias)
        nested_indexes = client.prepare_index_params()
        nested_indexes.add_index("events[name]", index_type="BITMAP")
        nested_indexes.add_index("events[rank]", index_type="STL_SORT")
        client.create_index(alias, nested_indexes)

        def assert_new_schema_data():
            old_rows_result = client.query(
                alias,
                filter="id in [0, 1, 2]",
                output_fields=["id", "events"],
            )
            assert all(row["events"] is None for row in old_rows_result)
            contains = client.query(
                alias,
                filter='array_contains(events[name], "new_b")',
                output_fields=["id", "events"],
            )
            assert [row["id"] for row in contains] == [101]
            assert contains[0]["events"] == new_rows[1]["events"]
            element_rows = client.query(
                alias,
                filter="element_filter(events, $[rank] >= 10)",
                output_fields=["id"],
                limit=10,
            )
            assert sorted((row["id"], row["offset"]) for row in element_rows) == [(101, 0), (101, 1)]

        assert_new_schema_data()
        client.release_collection(alias)
        client.load_collection(alias)
        assert_new_schema_data()
        client.drop_alias(alias)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_field_negative_constraint_matrix(self):
        """
        TC-L1-06: Drop Field negative constraint matrix.

        target: verify Proxy rejects invalid drop-field targets before schema mutation
        method: attempt to drop protected, missing, system, last-vector, and function-referenced fields
        expected: each invalid request fails with the exact validation message and schema remains unchanged
        """
        client = self._client()
        collection_name = f"{cf.gen_collection_name_by_testcase_name()}_matrix"
        last_vector_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_last_vector"

        # Step 1: Create a collection that contains most protected field categories.
        schema = client.create_schema(enable_dynamic_field=True, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("partition_tag", DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field("cluster_score", DataType.INT64, is_clustering_key=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field("age", DataType.INT64)
        schema.add_function(
            Function(
                name="bm25",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names="sparse",
            )
        )

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            consistency_level="Strong",
        )

        # Step 2: Verify each invalid field target returns its exact validation reason.
        for field_name, expected_msg in [
            ("", "Must specify exactly one valid Drop identifier (drop_field_name/drop_field_id/drop_function_name)"),
            ("missing_field", "field not found: missing_field"),
            ("id", "cannot drop primary key field: id"),
            ("partition_tag", "cannot drop partition key field: partition_tag"),
            ("cluster_score", "cannot drop clustering key field: cluster_score"),
            ("$rowid", "field not found: $rowid"),
            ("$timestamp", "field not found: $timestamp"),
            ("$namespace", "field not found: $namespace"),
            ("$meta", "field not found: $meta"),
            ("text", "field is referenced by function bm25 as input, drop function first"),
            ("sparse", "field is referenced by function bm25 as output, drop function first"),
        ]:
            self.drop_collection_field(
                client,
                collection_name,
                field_name=field_name,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: expected_msg},
            )

        # Step 3: Verify the rejected attempts did not mutate the schema.
        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        function_names = [func["name"] for func in desc.get("functions", [])]
        assert "id" in field_names
        assert "vec" in field_names
        assert "partition_tag" in field_names
        assert "cluster_score" in field_names
        assert "text" in field_names
        assert "sparse" in field_names
        assert "bm25" in function_names

        # Step 4: Create a separate collection for the last-vector constraint.
        last_vector_schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        last_vector_schema.add_field("id", DataType.INT64, is_primary=True)
        last_vector_schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        last_vector_schema.add_field("age", DataType.INT64)

        client.create_collection(
            collection_name=last_vector_collection_name,
            schema=last_vector_schema,
            consistency_level="Strong",
        )

        self.drop_collection_field(
            client,
            last_vector_collection_name,
            field_name="vec",
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "cannot drop the last vector field: vec"},
        )

        # Step 5: Verify the last-vector rejection did not mutate the schema.
        desc = client.describe_collection(last_vector_collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        assert "id" in field_names
        assert "vec" in field_names
        assert "age" in field_names
        for name in [collection_name, last_vector_collection_name]:
            self.drop_collection(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_function_negative_constraints(self):
        """
        TC-L1-07: Drop Function negative constraints.

        target: verify Drop Function rejects invalid requests and preserves schema on failure
        method: test empty name, missing function, detach-only BM25 rejection, repeated drop, and last-vector cascade rejection
        expected: errors are clear; failed drops do not partially mutate schema
        """
        client = self._client()
        collection_name = f"{cf.gen_collection_name_by_testcase_name()}_function_negative"
        last_vector_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_function_last_vector"

        # Step 1: Create a collection where cascade-dropping BM25 is allowed because vec remains.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_function(
            Function(
                name="bm25",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names="sparse",
            )
        )

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            consistency_level="Strong",
        )

        # Step 2: Reject empty and missing function names before any successful drop.
        self.drop_collection_function(
            client,
            collection_name,
            "",
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "Must specify exactly one valid Drop identifier (drop_field_name/drop_field_id/drop_function_name)",
            },
        )

        self.drop_collection_function(
            client,
            collection_name,
            "missing_function",
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "function not found: missing_function: invalid parameter"},
        )

        desc = client.describe_collection(collection_name)
        assert "bm25" in [func["name"] for func in desc.get("functions", [])]
        assert "sparse" in [field["name"] for field in desc["fields"]]

        # Step 3: Reject detach-only BM25 drop and verify schema stays unchanged.
        self.drop_collection_function(
            client,
            collection_name,
            "bm25",
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: (
                    "detaching a function without dropping its output field is not supported; "
                    "drop_function always removes the function together with its output field: "
                    "bm25: invalid parameter"
                ),
            },
        )

        desc = client.describe_collection(collection_name)
        assert "bm25" in [func["name"] for func in desc.get("functions", [])]
        assert "sparse" in [field["name"] for field in desc["fields"]]

        # Step 4: Cascade-drop BM25 once successfully, then reject repeated cascade drop.
        self.drop_function_field(client, collection_name, "bm25")

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        function_names = [func["name"] for func in desc.get("functions", [])]
        assert "bm25" not in function_names
        assert "sparse" not in field_names
        assert "text" in field_names
        assert "vec" in field_names

        self.drop_function_field(
            client,
            collection_name,
            "bm25",
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "function not found: bm25: invalid parameter"},
        )

        # Step 5: Create a collection where BM25 output sparse is the only vector field.
        last_vector_schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        last_vector_schema.add_field("id", DataType.INT64, is_primary=True)
        last_vector_schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        last_vector_schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
        last_vector_schema.add_function(
            Function(
                name="bm25",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names="sparse",
            )
        )

        client.create_collection(
            collection_name=last_vector_collection_name,
            schema=last_vector_schema,
            consistency_level="Strong",
        )

        # Step 6: Reject cascade Drop Function because it would remove the last vector field.
        self.drop_function_field(
            client,
            last_vector_collection_name,
            "bm25",
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: (
                    "cannot drop function bm25: it would leave no vector field in the collection: invalid parameter"
                ),
            },
        )

        desc = client.describe_collection(last_vector_collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        function_names = [func["name"] for func in desc.get("functions", [])]
        assert "bm25" in function_names
        assert "sparse" in field_names
        assert "text" in field_names
        for name in [collection_name, last_vector_collection_name]:
            self.drop_collection(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_minhash_function_detach_vs_cascade(self):
        """
        TC-L1-07a: Drop Function detach vs cascade semantics for MinHash.

        target: verify MinHash detach-only drop is rejected and cascade-output-field drop works
        method: drop_collection_function (detach) is rejected; drop_function_field removes function + output field/index
        expected: detach rejected (function/field unchanged); cascade removes function, output field, and output index
        """
        client = self._client()
        detach_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_detach"
        cascade_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_cascade"

        def create_minhash_collection(name):
            schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("doc", DataType.VARCHAR, max_length=1024)
            schema.add_field("mh", DataType.BINARY_VECTOR, dim=512)
            schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
            schema.add_function(
                Function(
                    name="text_to_minhash",
                    function_type=FunctionType.MINHASH,
                    input_field_names=["doc"],
                    output_field_names=["mh"],
                    params={"num_hashes": 16, "shingle_size": 3},
                )
            )

            index_params = client.prepare_index_params()
            index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
            index_params.add_index(
                field_name="mh",
                index_type="MINHASH_LSH",
                metric_type="MHJACCARD",
                params={"mh_lsh_band": 8},
            )

            client.create_collection(
                collection_name=name,
                schema=schema,
                index_params=index_params,
                consistency_level="Strong",
            )

            rows = [
                {
                    "id": i,
                    "doc": f"minhash semantic document group {i % 3}",
                    "vec": [float(i), 0.0, 0.0, 0.0],
                }
                for i in range(12)
            ]
            client.insert(collection_name=name, data=rows)
            client.flush(name)

            assert self.wait_for_index_ready(client, name, index_name="vec", timeout=120)
            assert self.wait_for_index_ready(client, name, index_name="mh", timeout=120)
            client.load_collection(name)

            search_res = client.search(
                name,
                data=[rows[0]["doc"]],
                anns_field="mh",
                search_params={"metric_type": "MHJACCARD", "params": {}},
                limit=3,
                output_fields=["id", "doc"],
            )
            assert len(search_res[0]) > 0
            assert "doc" in search_res[0][0]["entity"]

        # Step 1: Detach-only MinHash drop is rejected; the function keeps its output field.
        create_minhash_collection(detach_collection_name)
        self.drop_collection_function(
            client,
            detach_collection_name,
            "text_to_minhash",
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: (
                    "detaching a function without dropping its output field is not supported; "
                    "drop_function always removes the function together with its output field: "
                    "text_to_minhash: invalid parameter"
                ),
            },
        )

        desc = client.describe_collection(detach_collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        function_names = [func["name"] for func in desc.get("functions", [])]
        assert "text_to_minhash" in function_names
        assert "mh" in field_names
        assert "vec" in field_names
        assert "mh" in client.list_indexes(detach_collection_name)

        # Step 2: Cascade MinHash drop removes the function, output field, and output index.
        create_minhash_collection(cascade_collection_name)
        self.drop_function_field(client, cascade_collection_name, "text_to_minhash")

        desc = client.describe_collection(cascade_collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        function_names = [func["name"] for func in desc.get("functions", [])]
        assert "text_to_minhash" not in function_names
        assert "mh" not in field_names
        assert "doc" in field_names
        assert "vec" in field_names

        for _ in range(30):
            indexes = client.list_indexes(cascade_collection_name)
            if "mh" not in indexes:
                break
            time.sleep(1)
        assert "mh" not in indexes
        assert "vec" in indexes
        assert client.describe_index(cascade_collection_name, index_name="mh") is None

        client.insert(
            collection_name=cascade_collection_name,
            data=[{"id": 100, "doc": "after cascade", "vec": [1.0, 0.0, 0.0, 0.0]}],
        )
        new_row = client.query(cascade_collection_name, filter="id == 100", output_fields=["*"])
        assert new_row[0]["doc"] == "after cascade"
        assert "mh" not in new_row[0]

        for name in [detach_collection_name, cascade_collection_name]:
            self.drop_collection(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="TC-L3-01 is stability-only; normal CI/CD cannot reliably expose in-flight DDL/DML races.")
    def test_drop_field_in_flight_request_semantics(self):
        """
        TC-L3-01: In-flight request semantics around Drop Field.

        This placeholder keeps the original automation slot visible.
        The case was moved out of L1 because short CI/CD runs cannot reliably
        control or observe the in-flight DDL/DML timing window.
        """
        # Intentionally empty. Run this scenario only in long-running stability jobs.
        pass

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="TC-L1-09 is already covered by TC-L0-01 and TC-L1-02; no duplicate automation.")
    def test_drop_field_sdk_schema_cache_invalidation(self):
        """
        TC-L1-09: SDK schema cache invalidation.

        This placeholder keeps the checklist slot visible.
        The same-client post-drop describe/query/search path is covered by TC-L0-01,
        and same-name different-type cache correctness is covered by TC-L1-02.
        """
        # Intentionally empty. Add a dedicated case only if a separate PyMilvus cache bug appears.
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_field_alias_path_schema_cache(self):
        """
        TC-L1-10: Alias path schema cache invalidation.

        target: verify Drop Field cache invalidation covers both collection name and alias paths
        method: warm up alias describe/query/search, drop tag through alias, then verify both paths use the new schema
        expected: alias and collection name both hide tag; kept-field read/write paths still work
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = f"{collection_name}_alias"

        # Step 1: Create a collection with a dropped scalar field and bind an alias to it.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        client.create_alias(collection_name, alias_name)

        # Step 2: Insert data and warm up alias-side schema/read cache before drop.
        rows = [{"id": i, "vec": [float(i), 0.0, 0.0, 0.0], "age": 20 + i, "tag": f"tag_{i}"} for i in range(10)]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        alias_desc = client.describe_collection(alias_name)
        assert "tag" in [field["name"] for field in alias_desc["fields"]]

        alias_query_before_drop = client.query(
            alias_name,
            filter='tag == "tag_1"',
            output_fields=["id", "tag"],
        )
        assert len(alias_query_before_drop) == 1
        assert alias_query_before_drop[0]["tag"] == "tag_1"

        alias_search_before_drop = client.search(
            alias_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["id", "tag"],
        )
        assert len(alias_search_before_drop[0]) == 3
        assert "tag" in alias_search_before_drop[0][0]["entity"]

        # Step 3: Drop the field through the alias path.
        client.drop_collection_field(alias_name, "tag")

        # Step 4: Verify both collection name and alias describe paths observe the new schema.
        collection_desc = client.describe_collection(collection_name)
        alias_desc = client.describe_collection(alias_name)

        collection_fields = [field["name"] for field in collection_desc["fields"]]
        alias_fields = [field["name"] for field in alias_desc["fields"]]
        assert "tag" not in collection_fields
        assert "tag" not in alias_fields
        assert "age" in collection_fields
        assert "age" in alias_fields

        # Step 5: Verify kept-field query/search works through both paths.
        for name in [collection_name, alias_name]:
            query_res = client.query(
                name,
                filter="id in [0, 1, 2]",
                output_fields=["id", "age"],
            )
            assert {row["id"] for row in query_res} == {0, 1, 2}
            assert all("tag" not in row for row in query_res)

            search_res = client.search(
                name,
                data=[[0.0, 0.0, 0.0, 0.0]],
                anns_field="vec",
                limit=3,
                output_fields=["id", "age"],
            )
            assert len(search_res[0]) == 3
            assert "age" in search_res[0][0]["entity"]
            assert "tag" not in search_res[0][0]["entity"]

        # Step 6: Verify explicit dropped-field references fail through both paths.
        for name in [collection_name, alias_name]:
            self.query(
                client,
                name,
                filter='tag == "tag_1"',
                output_fields=["id"],
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: "field tag not exist"},
            )

            self.search(
                client,
                name,
                data=[[0.0, 0.0, 0.0, 0.0]],
                anns_field="vec",
                limit=3,
                output_fields=["id", "tag"],
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: "field tag not exist"},
            )

        # Step 7: Verify post-drop inserts through both paths use the new schema.
        client.insert(
            collection_name=alias_name,
            data=[{"id": 100, "vec": [1.0, 0.0, 0.0, 0.0], "age": 100}],
        )
        client.insert(
            collection_name=collection_name,
            data=[{"id": 101, "vec": [1.0, 1.0, 0.0, 0.0], "age": 101}],
        )

        inserted = client.query(
            collection_name,
            filter="id in [100, 101]",
            output_fields=["id", "age"],
        )
        assert {row["id"] for row in inserted} == {100, 101}
        assert all("tag" not in row for row in inserted)
        client.drop_alias(alias_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_vector_field_type_matrix(self):
        """
        TC-L2-11: Drop vector field type matrix.

        target: verify dropping each supported vector field type does not affect the kept vector field
        method: create one collection with vec plus all drop vector types, then drop them one by one
        expected: each dropped vector field/index/search path disappears; kept vec search remains stable after reload
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        drop_vector_fields = [
            ("drop_float_vec", DataType.FLOAT_VECTOR, "HNSW", "L2"),
            ("drop_binary_vec", DataType.BINARY_VECTOR, "BIN_FLAT", "HAMMING"),
            ("drop_float16_vec", DataType.FLOAT16_VECTOR, "HNSW", "L2"),
            ("drop_bfloat16_vec", DataType.BFLOAT16_VECTOR, "HNSW", "IP"),
            ("drop_int8_vec", DataType.INT8_VECTOR, "HNSW", "COSINE"),
            ("drop_sparse_vec", DataType.SPARSE_FLOAT_VECTOR, "SPARSE_INVERTED_INDEX", "IP"),
        ]

        # Step 1: Create one collection with a kept FloatVector field and all vector types to drop.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("age", DataType.INT64)
        for field_name, vector_type, _, _ in drop_vector_fields:
            if vector_type == DataType.SPARSE_FLOAT_VECTOR:
                schema.add_field(field_name, DataType.SPARSE_FLOAT_VECTOR)
            else:
                schema.add_field(field_name, vector_type, dim=dim)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        for field_name, _, index_type, metric_type in drop_vector_fields:
            index_params.add_index(field_name=field_name, index_type=index_type, metric_type=metric_type)

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert old rows that contain values for every vector field.
        vec_values = cf.gen_vectors(100, dim, vector_data_type=DataType.FLOAT_VECTOR)
        drop_values_by_field = {
            field_name: cf.gen_vectors(100, dim, vector_data_type=vector_type)
            for field_name, vector_type, _, _ in drop_vector_fields
        }
        rows = []
        for i in range(100):
            row = {
                "id": i,
                "vec": vec_values[i],
                "age": 20 + i,
            }
            for field_name, _, _, _ in drop_vector_fields:
                row[field_name] = drop_values_by_field[field_name][i]
            rows.append(row)

        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        indexes = client.list_indexes(collection_name)
        assert "vec" in indexes
        for field_name, _, _, _ in drop_vector_fields:
            assert field_name in indexes
            assert client.describe_index(collection_name, index_name=field_name)["field_name"] == field_name

        # Step 3: Verify the kept vector field is searchable before dropping typed vector fields.
        search_before_drop = client.search(
            collection_name,
            data=[vec_values[0]],
            anns_field="vec",
            limit=5,
            output_fields=["id", "age"],
        )
        assert len(search_before_drop[0]) == 5
        assert "age" in search_before_drop[0][0]["entity"]

        # Step 4: Drop each vector type and verify the kept vector field remains searchable.
        for field_name, _, index_type, metric_type in drop_vector_fields:
            client.drop_collection_field(collection_name, field_name)

            desc = client.describe_collection(collection_name)
            field_names = [field["name"] for field in desc["fields"]]
            assert field_name not in field_names
            assert "vec" in field_names
            assert "age" in field_names

            for _ in range(30):
                indexes = client.list_indexes(collection_name)
                if field_name not in indexes:
                    break
                time.sleep(1)
            assert field_name not in indexes
            assert "vec" in indexes
            assert client.describe_index(collection_name, index_name=field_name) is None

            self.search(
                client,
                collection_name,
                data=[drop_values_by_field[field_name][0]],
                anns_field=field_name,
                limit=5,
                output_fields=["id"],
                check_task=ct.CheckTasks.err_res,
                check_items={
                    ct.err_code: 1100,
                    ct.err_msg: f"failed to get field schema by name: fieldName({field_name}) not found",
                },
            )

            dropped_index_params = client.prepare_index_params()
            dropped_index_params.add_index(field_name=field_name, index_type=index_type, metric_type=metric_type)
            self.create_index(
                client,
                collection_name,
                dropped_index_params,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: f"cannot create index on non-exist field: {field_name}"},
            )

            kept_search = client.search(
                collection_name,
                data=[vec_values[0]],
                anns_field="vec",
                limit=5,
                output_fields=["id", "age"],
            )
            assert len(kept_search[0]) == 5
            assert "age" in kept_search[0][0]["entity"]
            assert field_name not in kept_search[0][0]["entity"]

        # Step 5: Release/load and verify old typed-vector data is still skipped.
        client.release_collection(collection_name)
        client.load_collection(collection_name)

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        for field_name, _, _, _ in drop_vector_fields:
            assert field_name not in field_names

        search_after_reload = client.search(
            collection_name,
            data=[vec_values[0]],
            anns_field="vec",
            limit=5,
            output_fields=["id", "age"],
        )
        assert len(search_after_reload[0]) == 5
        assert "age" in search_after_reload[0][0]["entity"]
        for field_name, _, _, _ in drop_vector_fields:
            assert field_name not in search_after_reload[0][0]["entity"]
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_field_output_fields_wildcard_and_explicit_reference(self):
        """
        TC-L1-12: Output fields wildcard and explicit reference after Drop Field.

        target: verify dropped field is invisible through wildcard output and explicit references fail
        method: drop tag, then query/search with ["*"], ["tag"], and ["*", "tag"]
        expected: wildcard output omits tag; explicit tag output fails instead of returning partial results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with scalar, vector, and JSON fields.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)
        schema.add_field("json_field", DataType.JSON)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert old rows with tag and JSON data, then load.
        rows = [
            {
                "id": i,
                "vec": [float(i), 0.0, 0.0, 0.0],
                "age": 20 + i,
                "tag": f"tag_{i}",
                "json_field": {"bucket": i % 2, "label": f"json_{i}"},
            }
            for i in range(10)
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        # Step 3: Verify wildcard output exposes tag before drop, then drop tag.
        before_drop = client.query(collection_name, filter="id == 1", output_fields=["*"])
        assert before_drop[0]["tag"] == "tag_1"
        assert before_drop[0]["json_field"]["label"] == "json_1"

        client.drop_collection_field(collection_name, "tag")

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        assert "tag" not in field_names
        assert "json_field" in field_names
        assert "age" in field_names

        # Step 4: query(output_fields=["*"]) must not return the dropped field.
        query_star = client.query(
            collection_name,
            filter="id in [0, 1, 2]",
            output_fields=["*"],
        )
        assert {row["id"] for row in query_star} == {0, 1, 2}
        for row in query_star:
            assert "age" in row
            assert "json_field" in row
            assert "tag" not in row

        # Step 5: search(output_fields=["*"]) must not return the dropped field.
        search_star = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["*"],
        )
        assert len(search_star[0]) == 3
        for hit in search_star[0]:
            assert "age" in hit["entity"]
            assert "json_field" in hit["entity"]
            assert "tag" not in hit["entity"]

        # Step 6: Explicit tag output in query must fail clearly, including ["*", "tag"].
        for output_fields in [["tag"], ["*", "tag"]]:
            self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=output_fields,
                limit=3,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: "field tag not exist"},
            )

        # Step 7: Explicit tag output in search must fail clearly, including ["*", "tag"].
        for output_fields in [["tag"], ["*", "tag"]]:
            self.search(
                client,
                collection_name,
                data=[[0.0, 0.0, 0.0, 0.0]],
                anns_field="vec",
                limit=3,
                output_fields=output_fields,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: "field tag not exist"},
            )
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_field_with_default_value(self):
        """
        TC-L1-13: Drop field with default value.

        target: verify default-value fields disappear completely after Drop Field
        method: create default scalar fields, verify default backfill, drop them, then verify references fail
        expected: dropped default fields are not returned by wildcard output and new writes cannot carry them
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        default_fields = [
            ("default_int", DataType.INT64, {"default_value": 42}, 42, "default_int == 42"),
            ("default_float", DataType.FLOAT, {"default_value": 1.5}, 1.5, "default_float == 1.5"),
            ("default_bool", DataType.BOOL, {"default_value": True}, True, "default_bool == true"),
            (
                "default_varchar",
                DataType.VARCHAR,
                {"max_length": 64, "default_value": "default"},
                "default",
                'default_varchar == "default"',
            ),
        ]

        # Step 1: Create a collection with supported scalar default-value fields.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        for field_name, field_type, field_kwargs, _, _ in default_fields:
            schema.add_field(field_name, field_type, **field_kwargs)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert rows that omit default fields and verify default backfill before drop.
        rows = [{"id": i, "vec": [float(i), 0.0, 0.0, 0.0], "age": 20 + i} for i in range(10)]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        before_drop = client.query(collection_name, filter="id == 1", output_fields=["*"])[0]
        for field_name, _, _, expected_value, _ in default_fields:
            assert before_drop[field_name] == expected_value

        # Step 3: Drop every default-value field.
        for field_name, _, _, _, _ in default_fields:
            client.drop_collection_field(collection_name, field_name)

        # Step 4: Verify dropped fields are no longer visible in schema.
        field_names = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        for field_name, _, _, _, _ in default_fields:
            assert field_name not in field_names
        assert "id" in field_names
        assert "vec" in field_names
        assert "age" in field_names

        # Step 5: Wildcard query/search output must not return dropped default fields.
        query_star = client.query(collection_name, filter="id in [0, 1, 2]", output_fields=["*"])
        for row in query_star:
            assert "age" in row
            for field_name, _, _, _, _ in default_fields:
                assert field_name not in row

        search_star = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["*"],
        )
        assert len(search_star[0]) == 3
        for hit in search_star[0]:
            assert "age" in hit["entity"]
            for field_name, _, _, _, _ in default_fields:
                assert field_name not in hit["entity"]

        # Step 6: Explicit output/filter references to dropped default fields must fail.
        for field_name, _, _, _, filter_expr in default_fields:
            self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=[field_name],
                limit=1,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: f"field {field_name} not exist"},
            )

            self.query(
                client,
                collection_name,
                filter=filter_expr,
                output_fields=["id"],
                limit=1,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: f"field {field_name} not exist"},
            )

        # Step 7: New insert without dropped fields succeeds; insert carrying a dropped field fails.
        client.insert(
            collection_name=collection_name,
            data=[{"id": 100, "vec": [1.0, 0.0, 0.0, 0.0], "age": 100}],
        )
        client.flush(collection_name)

        new_row = client.query(collection_name, filter="id == 100", output_fields=["*"])[0]
        assert new_row["age"] == 100
        for field_name, _, _, _, _ in default_fields:
            assert field_name not in new_row

        self.insert(
            client,
            collection_name=collection_name,
            data=[{"id": 101, "vec": [1.0, 0.0, 0.0, 0.0], "age": 101, "default_int": 42}],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "Attempt to insert an unexpected field `default_int`"},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_then_add_same_name_analyzer_field(self):
        """
        TC-L1-14: Drop analyzer field and re-add same-name field with different analyzer params.

        target: verify old analyzer tokens/index data do not pollute a re-added same-name analyzer field
        method: drop text_content with standard analyzer, re-add text_content with length filter, then query text_match
        expected: old long-token rows are not matched; new analyzer semantics only apply to post-add rows
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        old_token = "legacytoken"
        new_token = "new"
        old_analyzer_params = {"tokenizer": "standard"}
        # The new analyzer drops tokens longer than 4 chars. This makes legacytoken searchable before drop
        # but unsearchable after re-add, so any post-readd match would indicate old analyzer/index leakage.
        new_analyzer_params = {
            "tokenizer": "standard",
            "filter": [
                {
                    "type": "length",
                    "max": 4,
                }
            ],
        }

        # Step 1: Create text_content with the old analyzer and an inverted index.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field(
            "text_content",
            DataType.VARCHAR,
            max_length=1024,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=old_analyzer_params,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(field_name="text_content", index_type="INVERTED")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert old rows and verify the old analyzer can match the long token.
        old_rows = [
            {
                "id": i,
                "vec": [float(i), 0.0, 0.0, 0.0],
                "age": 20 + i,
                "text_content": f"{old_token} old document {i}",
            }
            for i in range(5)
        ]
        client.insert(collection_name=collection_name, data=old_rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="text_content", timeout=60)

        before_drop_desc = client.describe_collection(collection_name)
        old_text_field = next(field for field in before_drop_desc["fields"] if field["name"] == "text_content")
        old_text_field_id = old_text_field.get("field_id")

        old_match = client.query(
            collection_name,
            filter=f"text_match(text_content, '{old_token}')",
            output_fields=["id", "text_content"],
        )
        assert {row["id"] for row in old_match} == {0, 1, 2, 3, 4}
        assert all(old_token in row["text_content"] for row in old_match)

        # Step 3: Drop text_content and wait until its index metadata is removed.
        client.drop_collection_field(collection_name, "text_content")

        after_drop_desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in after_drop_desc["fields"]]
        assert "text_content" not in field_names

        # Record the field-id high watermark when the SDK exposes it. This is a semi-whitebox guard
        # against reusing the old dropped field id; text_match checks below are the primary validation.
        max_field_id_after_drop = None
        properties = after_drop_desc.get("properties", {})
        if isinstance(properties, dict) and properties.get("max_field_id") is not None:
            max_field_id_after_drop = int(properties["max_field_id"])
        elif isinstance(properties, list):
            for prop in properties:
                if prop.get("key") == "max_field_id":
                    max_field_id_after_drop = int(prop["value"])
                    break

        for _ in range(30):
            indexes = client.list_indexes(collection_name)
            if "text_content" not in indexes:
                break
            time.sleep(1)
        assert "text_content" not in indexes
        assert client.describe_index(collection_name, index_name="text_content") is None

        self.query(
            client,
            collection_name,
            filter=f"text_match(text_content, '{old_token}')",
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field text_content not exist"},
        )

        # Step 4: Re-add same-name text_content with a new analyzer that drops tokens longer than 4 chars.
        client.add_collection_field(
            collection_name,
            field_name="text_content",
            data_type=DataType.VARCHAR,
            nullable=True,
            max_length=1024,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=new_analyzer_params,
        )

        after_add_desc = client.describe_collection(collection_name)
        new_text_field = next(field for field in after_add_desc["fields"] if field["name"] == "text_content")
        new_text_field_id = new_text_field.get("field_id")

        if old_text_field_id is not None and new_text_field_id is not None:
            assert new_text_field_id > old_text_field_id
        if max_field_id_after_drop is not None and new_text_field_id is not None:
            assert new_text_field_id > max_field_id_after_drop

        new_index_params = client.prepare_index_params()
        new_index_params.add_index(field_name="text_content", index_type="INVERTED")
        client.create_index(collection_name, new_index_params)
        assert self.wait_for_index_ready(client, collection_name, index_name="text_content", timeout=60)

        # Step 5: Insert post-add rows and verify only new analyzer semantics are visible.
        new_rows = [
            {
                "id": 100,
                "vec": [1.0, 0.0, 0.0, 0.0],
                "age": 100,
                "text_content": f"{new_token} {old_token}",
            },
            {
                "id": 101,
                "vec": [2.0, 0.0, 0.0, 0.0],
                "age": 101,
                "text_content": "tiny new word",
            },
        ]
        client.insert(collection_name=collection_name, data=new_rows)
        client.flush(collection_name)

        new_token_match = client.query(
            collection_name,
            filter=f"text_match(text_content, '{new_token}')",
            output_fields=["id", "text_content"],
        )
        assert {row["id"] for row in new_token_match} == {100, 101}

        old_token_after_readd = client.query(
            collection_name,
            filter=f"text_match(text_content, '{old_token}')",
            output_fields=["id", "text_content"],
        )
        assert len(old_token_after_readd) == 0

        old_rows_after_readd = client.query(
            collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["id", "text_content"],
        )
        assert len(old_rows_after_readd) == 5
        for row in old_rows_after_readd:
            assert row.get("text_content") is None or old_token not in row.get("text_content", "")

        # Step 6: Reload and repeat the key old/new analyzer checks.
        client.release_collection(collection_name)
        client.load_collection(collection_name)

        new_token_after_reload = client.query(
            collection_name,
            filter=f"text_match(text_content, '{new_token}')",
            output_fields=["id", "text_content"],
        )
        assert {row["id"] for row in new_token_after_reload} == {100, 101}

        old_token_after_reload = client.query(
            collection_name,
            filter=f"text_match(text_content, '{old_token}')",
            output_fields=["id", "text_content"],
        )
        assert len(old_token_after_reload) == 0

        search_res = client.search(
            collection_name,
            data=[[1.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["id", "age", "text_content"],
        )
        assert len(search_res[0]) == 3
        for hit in search_res[0]:
            if hit["entity"]["id"] < 100:
                assert hit["entity"].get("text_content") is None

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_field_upsert_and_partial_update_paths(self):
        """
        TC-L1-15: Upsert and partial update after Drop Field.

        target: verify update paths do not revive dropped field data after schema evolution
        method: drop score, then run upsert and upsert(partial_update=True) with and without score
        expected: kept-field updates work; dropped score is rejected without dynamic schema and becomes dynamic data with it
        """
        client = self._client()
        static_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_static"
        dynamic_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_dynamic"

        # Step 1: Create a non-dynamic collection with score as a normal scalar field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("score", DataType.INT64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=static_collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        rows = [
            {"id": 0, "vec": [0.0, 0.0, 0.0, 0.0], "age": 20, "score": 100},
            {"id": 1, "vec": [1.0, 0.0, 0.0, 0.0], "age": 21, "score": 101},
            {"id": 2, "vec": [2.0, 0.0, 0.0, 0.0], "age": 22, "score": 102},
        ]
        client.insert(collection_name=static_collection_name, data=rows)
        client.flush(static_collection_name)
        client.load_collection(static_collection_name)

        before_drop = client.query(static_collection_name, filter="id in [0, 1, 2]", output_fields=["*"])
        assert {row["score"] for row in before_drop} == {100, 101, 102}

        # Step 2: Drop score. From this point on, score must no longer be a schema field.
        client.drop_collection_field(static_collection_name, "score")
        field_names = [field["name"] for field in client.describe_collection(static_collection_name)["fields"]]
        assert "score" not in field_names

        # Verify full upsert can replace an old row using only kept fields.
        # This must not bring the old score value back through wildcard output.
        client.upsert(
            collection_name=static_collection_name,
            data=[{"id": 0, "vec": [0.5, 0.0, 0.0, 0.0], "age": 200}],
        )
        row0 = client.query(static_collection_name, filter="id == 0", output_fields=["*"])[0]
        assert row0["age"] == 200
        assert "score" not in row0

        # Verify partial_update updates only the provided kept field and preserves other kept fields.
        # The dropped score field must still stay invisible.
        client.upsert(
            collection_name=static_collection_name,
            data=[{"id": 1, "age": 201}],
            partial_update=True,
        )
        row1 = client.query(static_collection_name, filter="id == 1", output_fields=["*"])[0]
        assert row1["age"] == 201
        assert row1["vec"] == [1.0, 0.0, 0.0, 0.0]
        assert "score" not in row1

        # Step 3: Non-dynamic collection rejects dropped score on both update paths.
        for kwargs in [
            {
                "data": [{"id": 0, "vec": [0.6, 0.0, 0.0, 0.0], "age": 300, "score": 999}],
            },
            {
                "data": [{"id": 1, "score": 998}],
                "partial_update": True,
            },
        ]:
            self.upsert(
                client,
                collection_name=static_collection_name,
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1, ct.err_msg: "Attempt to insert an unexpected field `score`"},
                **kwargs,
            )

        search_res = client.search(
            static_collection_name,
            data=[[0.5, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["id", "age"],
        )
        assert len(search_res[0]) == 3
        assert "score" not in search_res[0][0]["entity"]

        # Step 4: Repeat with dynamic schema enabled to distinguish dynamic keys from revived schema fields.
        dynamic_schema = client.create_schema(enable_dynamic_field=True, auto_id=False)
        dynamic_schema.add_field("id", DataType.INT64, is_primary=True)
        dynamic_schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        dynamic_schema.add_field("age", DataType.INT64)
        dynamic_schema.add_field("score", DataType.INT64)

        dynamic_index_params = client.prepare_index_params()
        dynamic_index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=dynamic_collection_name,
            schema=dynamic_schema,
            index_params=dynamic_index_params,
            consistency_level="Strong",
        )

        client.insert(collection_name=dynamic_collection_name, data=rows)
        client.flush(dynamic_collection_name)
        client.load_collection(dynamic_collection_name)

        dynamic_before_drop = client.query(dynamic_collection_name, filter="id in [0, 1, 2]", output_fields=["*"])
        assert {row["score"] for row in dynamic_before_drop} == {100, 101, 102}

        client.drop_collection_field(dynamic_collection_name, "score")
        dynamic_field_names = [field["name"] for field in client.describe_collection(dynamic_collection_name)["fields"]]
        assert "score" not in dynamic_field_names

        # Verify kept-field full upsert works and does not expose the old schema score value.
        client.upsert(
            collection_name=dynamic_collection_name,
            data=[{"id": 0, "vec": [0.5, 0.0, 0.0, 0.0], "age": 200}],
        )
        dynamic_row0 = client.query(dynamic_collection_name, filter="id == 0", output_fields=["*"])[0]
        assert dynamic_row0["age"] == 200
        assert "score" not in dynamic_row0

        # Verify kept-field partial_update preserves the vector and still does not expose old score.
        client.upsert(
            collection_name=dynamic_collection_name,
            data=[{"id": 1, "age": 201}],
            partial_update=True,
        )
        dynamic_row1 = client.query(dynamic_collection_name, filter="id == 1", output_fields=["*"])[0]
        assert dynamic_row1["age"] == 201
        assert dynamic_row1["vec"] == [1.0, 0.0, 0.0, 0.0]
        assert "score" not in dynamic_row1

        # id=2 is never rewritten with score after drop; this sentinel proves old score=102 does not revive.
        dynamic_row2 = client.query(dynamic_collection_name, filter="id == 2", output_fields=["*"])[0]
        assert dynamic_row2["age"] == 22
        assert "score" not in dynamic_row2

        # Step 5: With dynamic schema enabled, post-drop score is accepted only as newly written dynamic data.
        client.upsert(
            collection_name=dynamic_collection_name,
            data=[{"id": 0, "vec": [0.6, 0.0, 0.0, 0.0], "age": 300, "score": 999}],
        )
        dynamic_score_row0 = client.query(dynamic_collection_name, filter="id == 0", output_fields=["*"])[0]
        assert dynamic_score_row0["age"] == 300
        assert dynamic_score_row0["score"] == 999
        assert dynamic_score_row0["score"] != 100

        client.upsert(
            collection_name=dynamic_collection_name,
            data=[{"id": 1, "score": 998}],
            partial_update=True,
        )
        dynamic_score_row1 = client.query(dynamic_collection_name, filter="id == 1", output_fields=["*"])[0]
        assert dynamic_score_row1["age"] == 201
        assert dynamic_score_row1["score"] == 998
        assert dynamic_score_row1["score"] != 101

        dynamic_row2_after_dynamic_writes = client.query(
            dynamic_collection_name,
            filter="id == 2",
            output_fields=["*"],
        )[0]
        assert "score" not in dynamic_row2_after_dynamic_writes

        # Step 6: The schema still has no score field; returned score values above are dynamic keys after drop.
        dynamic_field_names = [field["name"] for field in client.describe_collection(dynamic_collection_name)["fields"]]
        assert "score" not in dynamic_field_names

        self.drop_collection(client, static_collection_name)
        self.drop_collection(client, dynamic_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_field_query_and_search_iterator_paths(self):
        """
        TC-L2-16: Query/search iterator after Drop Field.

        target: verify iterator paths use the post-drop schema without losing iterator snapshot isolation
        method: drop tag while query_iterator is open, insert a new row, then continue iterator and run post-drop iterators
        expected: existing iterator omits tag after drop and does not see the new row; tag references fail clearly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection and insert multiple batches so iterator pagination is exercised.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        rows = []
        for batch_id in range(3):
            batch_rows = [
                {
                    "id": batch_id * 10 + i,
                    "vec": [float(batch_id * 10 + i), 0.0, 0.0, 0.0],
                    "age": 20 + batch_id * 10 + i,
                    "tag": f"tag_{batch_id}_{i}",
                }
                for i in range(10)
            ]
            client.insert(collection_name=collection_name, data=batch_rows)
            rows.extend(batch_rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        # Step 2: Start query_iterator before drop and consume one batch under the old schema.
        query_iterator = client.query_iterator(
            collection_name=collection_name,
            batch_size=5,
            limit=100,
            filter="id >= 0",
            output_fields=["*"],
        )
        first_batch = query_iterator.next()
        assert len(first_batch) == 5
        first_batch_ids = {row["id"] for row in first_batch}
        assert len(first_batch_ids) == len(first_batch)
        assert first_batch_ids.issubset({row["id"] for row in rows})
        for row in first_batch:
            assert "tag" in row

        # Step 3: Drop tag and insert a new row after the iterator snapshot was established.
        client.drop_collection_field(collection_name, "tag")
        field_names = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        assert "tag" not in field_names

        client.insert(
            collection_name=collection_name,
            data=[{"id": 999, "vec": [999.0, 0.0, 0.0, 0.0], "age": 999}],
        )
        client.flush(collection_name)

        # A fresh query sees the new row, so the iterator check below proves snapshot isolation.
        new_row = client.query(
            collection_name=collection_name,
            filter="id == 999",
            output_fields=["*"],
        )
        assert len(new_row) == 1
        assert new_row[0]["id"] == 999
        assert "tag" not in new_row[0]

        # Step 4: Continue the old query_iterator. It uses the new schema, but not the new data snapshot.
        query_rows_after_drop = []
        while True:
            batch = query_iterator.next()
            if len(batch) == 0:
                query_iterator.close()
                break
            query_rows_after_drop.extend(batch)

        remaining_ids = {row["id"] for row in query_rows_after_drop}
        expected_remaining_ids = {row["id"] for row in rows} - first_batch_ids
        assert len(query_rows_after_drop) == len(expected_remaining_ids)
        assert len(remaining_ids) == len(query_rows_after_drop)
        assert remaining_ids == expected_remaining_ids
        assert 999 not in remaining_ids
        for row in query_rows_after_drop:
            assert "age" in row
            assert "tag" not in row

        # Step 5: search_iterator created after drop must use the new schema for every hit.
        search_hits = []
        search_iterator = client.search_iterator(
            collection_name=collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            batch_size=5,
            limit=12,
            output_fields=["*"],
        )
        while True:
            batch = search_iterator.next()
            if not batch:
                search_iterator.close()
                break
            search_hits.extend(batch)

        assert len(search_hits) == 12
        for hit in search_hits:
            assert "age" in hit["entity"]
            assert "tag" not in hit["entity"]

        # Step 6: iterator requests that explicitly output the dropped field must fail.
        query_output_iterator = client.query_iterator(
            collection_name=collection_name,
            batch_size=3,
            limit=3,
            filter="id >= 0",
            output_fields=["tag"],
        )
        with pytest.raises(Exception) as query_output_exc:
            query_output_iterator.next()
        query_output_iterator.close()
        assert "field tag not exist" in str(query_output_exc.value).lower(), str(query_output_exc.value)

        with pytest.raises(Exception) as search_output_exc:
            client.search_iterator(
                collection_name=collection_name,
                data=[[0.0, 0.0, 0.0, 0.0]],
                anns_field="vec",
                batch_size=3,
                limit=3,
                output_fields=["tag"],
            )
        assert "field tag not exist" in str(search_output_exc.value).lower(), str(search_output_exc.value)

        # Step 7: iterator filters that explicitly reference the dropped field must also fail.
        with pytest.raises(Exception) as query_filter_exc:
            client.query_iterator(
                collection_name=collection_name,
                batch_size=3,
                limit=3,
                filter='tag == "tag_0_0"',
                output_fields=["id"],
            )
        assert "field tag not exist" in str(query_filter_exc.value).lower(), str(query_filter_exc.value)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_field_runtime_query_reference_paths(self):
        """
        TC-L1-17: Drop field referenced by runtime query options.

        target: verify runtime query options fail clearly after their referenced fields are dropped
        method: drop fields referenced by decay reranker, group_by_field, and order_by_fields
        expected: Drop Field succeeds; post-drop runtime references return field-not-found errors
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create one collection with independent fields for each runtime reference path.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("rank_score", DataType.INT64)
        schema.add_field("group_field", DataType.VARCHAR, max_length=64)
        schema.add_field("order_field", DataType.FLOAT)
        schema.add_field("age", DataType.INT64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        rows = [
            {
                "id": i,
                "vec": [float(i), 0.0, 0.0, 0.0],
                "rank_score": i * 10,
                "group_field": f"group_{i % 3}",
                "order_field": float(30 - i),
                "age": 20 + i,
            }
            for i in range(12)
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        decay_ranker = Function(
            name="rank_score_decay",
            input_field_names=["rank_score"],
            function_type=FunctionType.RERANK,
            params={"reranker": "decay", "function": "gauss", "origin": 0, "offset": 0, "decay": 0.5, "scale": 100},
        )

        # Step 2: Verify the three runtime reference paths work before Drop Field.
        rerank_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            ranker=decay_ranker,
            limit=5,
            output_fields=["id", "rank_score"],
        )
        assert len(rerank_res[0]) == 5
        assert "rank_score" in rerank_res[0][0]["entity"]

        group_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            group_by_field="group_field",
            limit=3,
            output_fields=["id", "group_field"],
        )
        assert len(group_res[0]) == 3
        group_values = [hit["entity"]["group_field"] for hit in group_res[0]]
        assert len(group_values) == len(set(group_values))

        order_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=5,
            output_fields=["id", "order_field"],
            order_by_fields=[{"field": "order_field", "order": "asc"}],
        )
        assert len(order_res[0]) == 5
        order_values = [hit["entity"]["order_field"] for hit in order_res[0]]
        assert order_values == sorted(order_values)

        # Step 3: Drop the reranker input. Drop Field is allowed because this is a request-time dependency.
        client.drop_collection_field(collection_name, "rank_score")
        field_names = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        assert "rank_score" not in field_names

        self.search(
            client,
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            ranker=decay_ranker,
            limit=5,
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "input field rank_score not found in collection schema"},
        )

        # Step 4: Drop the group-by field. Post-drop group_by_field must fail clearly.
        client.drop_collection_field(collection_name, "group_field")
        field_names = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        assert "group_field" not in field_names

        self.search(
            client,
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            group_by_field="group_field",
            limit=3,
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "groupBy field not found in schema: field not found[field=group_field]",
            },
        )

        # Step 5: Drop the order-by field. Post-drop order_by_fields must fail clearly.
        client.drop_collection_field(collection_name, "order_field")
        field_names = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        assert "order_field" not in field_names

        self.search(
            client,
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=5,
            output_fields=["id"],
            order_by_fields=[{"field": "order_field", "order": "asc"}],
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "order_by field 'order_field' does not exist in collection schema: invalid parameter",
            },
        )

        # Step 6: Kept fields remain queryable after all runtime dependency fields are dropped.
        kept_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["id", "age"],
        )
        assert len(kept_res[0]) == 3
        assert "age" in kept_res[0][0]["entity"]
        assert "rank_score" not in kept_res[0][0]["entity"]
        assert "group_field" not in kept_res[0][0]["entity"]
        assert "order_field" not in kept_res[0][0]["entity"]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_timestamptz_field(self):
        """
        TC-L1-18: Drop TIMESTAMPTZ field.

        target: verify Drop Field works for a normal TIMESTAMPTZ field
        method: query/search by TIMESTAMPTZ before drop, then drop it and retry references
        expected: TIMESTAMPTZ field is removed; post-drop references fail; other fields still work
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with a normal TIMESTAMPTZ field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("event_time", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field("age", DataType.INT64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        rows = [
            {"id": 0, "vec": [0.0, 0.0, 0.0, 0.0], "event_time": "2025-01-01T00:00:00Z", "age": 20},
            {"id": 1, "vec": [1.0, 0.0, 0.0, 0.0], "event_time": "2025-01-02T00:00:00Z", "age": 21},
            {"id": 2, "vec": [2.0, 0.0, 0.0, 0.0], "event_time": "2025-01-03T00:00:00Z", "age": 22},
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        # Step 2: Verify TIMESTAMPTZ filter and output work before Drop Field.
        time_filter = "event_time >= ISO '2025-01-02T00:00:00Z'"
        query_res = client.query(
            collection_name=collection_name,
            filter=time_filter,
            output_fields=["id", "event_time"],
        )
        assert {row["id"] for row in query_res} == {1, 2}
        for row in query_res:
            assert "event_time" in row

        search_res = client.search(
            collection_name=collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            filter=time_filter,
            limit=2,
            output_fields=["id", "event_time"],
        )
        assert len(search_res[0]) == 2
        assert {hit["entity"]["id"] for hit in search_res[0]} == {1, 2}
        for hit in search_res[0]:
            assert "event_time" in hit["entity"]

        # Step 3: Drop the TIMESTAMPTZ field and verify schema convergence.
        client.drop_collection_field(collection_name, "event_time")
        field_names = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        assert "event_time" not in field_names
        assert "age" in field_names
        assert "vec" in field_names

        # Step 4: Explicit post-drop references to TIMESTAMPTZ output must fail.
        self.query(
            client,
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["event_time"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field event_time not exist"},
        )

        self.search(
            client,
            collection_name=collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=2,
            output_fields=["event_time"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field event_time not exist"},
        )

        # Step 5: Explicit post-drop TIMESTAMPTZ filters must fail.
        # Use a field-existence predicate after drop; ISO literals require the field type to exist during parsing.
        post_drop_time_filter = "event_time is not null"
        self.query(
            client,
            collection_name=collection_name,
            filter=post_drop_time_filter,
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field event_time not exist"},
        )

        self.search(
            client,
            collection_name=collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            filter=post_drop_time_filter,
            limit=2,
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field event_time not exist"},
        )

        # Step 6: Query/search paths that do not reference the dropped field still work.
        post_drop_query = client.query(
            collection_name=collection_name,
            filter="id in [0, 1, 2]",
            output_fields=["*"],
        )
        assert {row["id"] for row in post_drop_query} == {0, 1, 2}
        for row in post_drop_query:
            assert "age" in row
            assert "event_time" not in row

        post_drop_search = client.search(
            collection_name=collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["*"],
        )
        assert len(post_drop_search[0]) == 3
        for hit in post_drop_search[0]:
            assert "age" in hit["entity"]
            assert "event_time" not in hit["entity"]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_active_ttl_field_rejected_until_unbound(self):
        """
        TC-L1-18a: Reject Drop active entity TTL field.

        target: verify an active ttl_field cannot be dropped until the TTL binding is removed
        method: create ttl TIMESTAMPTZ with properties.ttl_field, reject drop, remove property, then drop
        expected: active drop does not mutate schema/properties; unbound TIMESTAMPTZ field can be dropped
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create an entity-level TTL collection whose active TTL field is `ttl`.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            properties={"ttl_field": "ttl", "timezone": "UTC"},
            consistency_level="Strong",
        )

        client.insert(
            collection_name=collection_name,
            data=[
                {
                    "id": 1,
                    "ttl": "2099-01-01T00:00:00Z",
                    "vec": [0.0, 0.0, 0.0, 0.0],
                    "age": 10,
                }
            ],
        )
        client.flush(collection_name)
        client.load_collection(collection_name)

        before_query = client.query(collection_name, filter="id == 1", output_fields=["id", "ttl", "age"])
        assert len(before_query) == 1
        assert before_query[0]["age"] == 10

        before_desc = client.describe_collection(collection_name)
        assert before_desc.get("properties", {}).get("ttl_field") == "ttl"
        assert "ttl" in [field["name"] for field in before_desc["fields"]]

        # Step 2: Reject dropping the active TTL field and verify no metadata is partially mutated.
        self.drop_collection_field(
            client,
            collection_name,
            field_name="ttl",
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: (
                    "cannot drop field ttl because it is referenced by collection property ttl_field, "
                    "drop the property first"
                ),
            },
        )

        after_reject_desc = client.describe_collection(collection_name)
        assert after_reject_desc.get("properties", {}).get("ttl_field") == "ttl"
        assert "ttl" in [field["name"] for field in after_reject_desc["fields"]]

        still_visible = client.query(collection_name, filter="id == 1", output_fields=["id", "ttl", "age"])
        assert len(still_visible) == 1
        assert still_visible[0]["age"] == 10

        self.add_collection_field(
            client,
            collection_name,
            field_name="ttl",
            data_type=DataType.INT64,
            nullable=True,
            default_value=0,
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "duplicated field name ttl",
            },
        )

        # Step 3: Explicitly remove the TTL binding; the same TIMESTAMPTZ field is now a normal droppable field.
        client.drop_collection_properties(collection_name, property_keys=["ttl_field"])
        unbound_desc = client.describe_collection(collection_name)
        assert "ttl_field" not in unbound_desc.get("properties", {})
        assert "ttl" in [field["name"] for field in unbound_desc["fields"]]

        client.drop_collection_field(collection_name, "ttl")

        after_drop_desc = client.describe_collection(collection_name)
        assert "ttl_field" not in after_drop_desc.get("properties", {})
        assert "ttl" not in [field["name"] for field in after_drop_desc["fields"]]

        after_drop_query = client.query(collection_name, filter="id == 1", output_fields=["id", "age"])
        assert len(after_drop_query) == 1
        assert after_drop_query[0]["age"] == 10
        assert "ttl" not in after_drop_query[0]

        search_res = client.search(
            collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=1,
            output_fields=["id", "age"],
        )
        assert len(search_res[0]) == 1
        assert search_res[0][0]["entity"]["id"] == 1
        assert "ttl" not in search_res[0][0]["entity"]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_loaded_field_after_partial_load(self):
        """
        TC-L2-18b: Drop a loaded field after partial load.

        target: verify partial-load metadata does not keep a dropped field reference
        method: load selected fields including drop_me, drop it, reload, then add same-name different-type field
        expected: stale load_fields metadata does not break reload/search/query or bind to the new field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with two scalar fields:
        # keep is the control field; drop_me is the field recorded in load_fields and then dropped.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("keep", DataType.VARCHAR, max_length=64)
        schema.add_field("drop_me", DataType.VARCHAR, max_length=64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert sealed rows so partial load and reload both read persisted segment data.
        rows = [
            {
                "id": i,
                "vec": [float(i), 0.0, 0.0, 0.0],
                "keep": f"keep_{i}",
                "drop_me": f"old_drop_{i}",
            }
            for i in range(5)
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)

        # Step 3: Partial-load the collection with drop_me explicitly included in the load field list.
        client.load_collection(collection_name, load_fields=["id", "vec", "keep", "drop_me"])

        # Verify the old load_fields list is effective before Drop Field: both keep and drop_me are readable.
        before_drop = client.query(
            collection_name=collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["id", "keep", "drop_me"],
        )
        assert len(before_drop) == 5
        for row in before_drop:
            assert row["keep"] == f"keep_{row['id']}"
            assert row["drop_me"] == f"old_drop_{row['id']}"

        # Step 4: Drop the loaded field and verify the schema no longer exposes it.
        client.drop_collection_field(collection_name, "drop_me")

        # Verify schema metadata has removed the dropped field while preserving the remaining fields.
        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        assert "drop_me" not in field_names
        assert "keep" in field_names
        assert "vec" in field_names

        # Step 5: Verify operations that use only remaining loaded fields still work after the drop.
        post_drop_query = client.query(
            collection_name=collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["id", "keep"],
        )
        assert len(post_drop_query) == 5
        assert {row["keep"] for row in post_drop_query} == {f"keep_{i}" for i in range(5)}

        post_drop_search = client.search(
            collection_name=collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["id", "keep"],
        )
        assert len(post_drop_search[0]) == 3
        assert "keep" in post_drop_search[0][0]["entity"]
        assert "drop_me" not in post_drop_search[0][0]["entity"]

        # Step 6: Verify explicit references to drop_me fail even though old load_fields contained it.
        self.query(
            client,
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["drop_me"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field drop_me not exist"},
        )

        self.query(
            client,
            collection_name=collection_name,
            filter='drop_me == "old_drop_1"',
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field drop_me not exist"},
        )

        # Step 7: Release and reload without explicit load_fields.
        # This validates QueryCoord/QueryNode do not require the stale old field ID from the previous partial load.
        client.release_collection(collection_name)
        client.load_collection(collection_name)

        # Verify wildcard query/search follows the latest schema after reload and does not expose drop_me.
        after_reload = client.query(
            collection_name=collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["*"],
        )
        assert len(after_reload) == 5
        for row in after_reload:
            assert "keep" in row
            assert "drop_me" not in row

        after_reload_search = client.search(
            collection_name=collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["*"],
        )
        assert len(after_reload_search[0]) == 3
        assert "keep" in after_reload_search[0][0]["entity"]
        assert "drop_me" not in after_reload_search[0][0]["entity"]

        # Step 8: Re-add the same field name with a different type.
        # This verifies stale load_fields metadata is not rebound to the new same-name field.
        client.add_collection_field(
            collection_name,
            field_name="drop_me",
            data_type=DataType.INT64,
            nullable=True,
        )
        client.insert(
            collection_name=collection_name,
            data=[{"id": 100, "vec": [1.0, 0.0, 0.0, 0.0], "keep": "keep_new", "drop_me": 9000}],
        )
        client.flush(collection_name)

        client.release_collection(collection_name)
        client.load_collection(collection_name)

        # Verify the new Int64 drop_me field is queryable with new data.
        new_field_query = client.query(
            collection_name=collection_name,
            filter="drop_me >= 9000",
            output_fields=["id", "drop_me"],
        )
        assert len(new_field_query) == 1
        assert new_field_query[0]["id"] == 100
        assert new_field_query[0]["drop_me"] == 9000

        # Verify old rows do not expose the old VarChar drop_me values through the new field.
        old_rows_after_readd = client.query(
            collection_name=collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["id", "drop_me"],
        )
        assert len(old_rows_after_readd) == 5
        for row in old_rows_after_readd:
            assert row.get("drop_me") != f"old_drop_{row['id']}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_field_then_truncate_then_write(self):
        """
        TC-L1-19: Truncate then write after Drop Field.

        target: verify truncate keeps the latest schema after a field is dropped
        method: drop tag, truncate old data, insert new rows without tag, then validate reads and rejected tag references
        expected: truncate clears data only; it does not restore the dropped field or accept old-schema writes
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a collection with a scalar field that will be dropped before truncate.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert old rows with tag and confirm the old schema path works before Drop Field.
        old_rows = [{"id": i, "vec": [float(i), 0.0, 0.0, 0.0], "age": 20 + i, "tag": f"old_tag_{i}"} for i in range(5)]
        client.insert(collection_name=collection_name, data=old_rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        before_drop = client.query(
            collection_name=collection_name,
            filter='tag == "old_tag_1"',
            output_fields=["id", "tag"],
        )
        assert len(before_drop) == 1
        assert before_drop[0]["tag"] == "old_tag_1"

        # Step 3: Drop tag and verify the schema has moved to the new shape before truncate.
        client.drop_collection_field(collection_name, "tag")

        field_names = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        assert "tag" not in field_names
        assert "age" in field_names
        assert "vec" in field_names

        # Step 4: Truncate after Drop Field; this should clear rows without restoring the dropped field.
        self.truncate_collection(client, collection_name)

        count_after_truncate = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["count(*)"],
        )
        assert count_after_truncate[0]["count(*)"] == 0

        # Step 5: Insert new rows using only the latest schema.
        new_rows = [{"id": 100 + i, "vec": [float(i), 1.0, 0.0, 0.0], "age": 100 + i} for i in range(3)]
        client.insert(collection_name=collection_name, data=new_rows)
        client.flush(collection_name)

        # Step 6: Wildcard query/search should return only fields from the latest schema.
        query_res = client.query(
            collection_name=collection_name,
            filter="id in [100, 101, 102]",
            output_fields=["*"],
        )
        assert len(query_res) == 3
        for row in query_res:
            assert "age" in row
            assert "tag" not in row

        search_res = client.search(
            collection_name=collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=3,
            output_fields=["*"],
        )
        assert len(search_res[0]) == 3
        for hit in search_res[0]:
            assert "age" in hit["entity"]
            assert "tag" not in hit["entity"]

        # Step 7: Old-schema writes and explicit tag reads must still fail after truncate.
        self.insert(
            client,
            collection_name=collection_name,
            data=[{"id": 200, "vec": [2.0, 0.0, 0.0, 0.0], "age": 200, "tag": "old_schema"}],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "Attempt to insert an unexpected field `tag`"},
        )

        self.query(
            client,
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["tag"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field tag not exist"},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="TODO: external collection Drop Field coverage will be validated with refresh scenarios")
    def test_external_collection_drop_field_rejected(self, request):
        """
        TC-L1-20: Reject Drop Field on external collection.

        target: verify external collection schema cannot be mutated by Drop Field
        method: create an external collection, try to drop a mapped external field, then describe schema
        expected: Drop Field is rejected; external schema mapping remains unchanged
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        cfg = get_minio_config(
            minio_host=request.config.getoption("--minio_host"),
            minio_bucket=request.config.getoption("--minio_bucket"),
        )
        external_source = build_external_source(cfg, f"drop-field-external/{collection_name}")

        # Step 1: Create an external collection with field-to-external-column mappings.
        schema = client.create_schema(
            enable_dynamic_field=False,
            auto_id=False,
            external_source=external_source,
            external_spec=build_external_spec(cfg),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("value", DataType.FLOAT, external_field="value")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=4, external_field="embedding")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            consistency_level="Strong",
        )

        # Step 2: Verify the field that will be dropped is part of the external schema mapping.
        before_desc = client.describe_collection(collection_name)
        before_fields = {field["name"]: field for field in before_desc["fields"]}
        assert "value" in before_fields
        assert before_fields["value"].get("external_field") == "value"
        assert before_desc.get("external_source") == external_source

        # Step 3: Drop Field should be rejected for external collections.
        self.drop_collection_field(
            client,
            collection_name,
            field_name="value",
            check_task=ct.CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "alter collection schema operation is not supported for external collection",
            },
        )

        # Step 4: Verify the failed Drop Field did not mutate schema or external field mapping.
        after_desc = client.describe_collection(collection_name)
        after_fields = {field["name"]: field for field in after_desc["fields"]}
        assert "value" in after_fields
        assert after_fields["value"].get("external_field") == "value"
        assert after_desc.get("external_source") == external_source
        assert after_desc.get("external_spec") == before_desc.get("external_spec")

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_scalar_field_type_matrix(self):
        """
        TC-L2-21: Drop scalar field type matrix.

        target: verify scalar and complex fields disappear consistently after Drop Field
        method: create one collection with all target scalar types, then drop each field and verify output/filter/read paths
        expected: dropped field is invisible; explicit references fail; remaining fields work before and after reload
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        cases = [
            ("flag", DataType.BOOL, {}, True, "flag == true"),
            ("i8", DataType.INT8, {}, 8, "i8 == 8"),
            ("i16", DataType.INT16, {}, 16, "i16 == 16"),
            ("i32", DataType.INT32, {}, 32, "i32 == 32"),
            ("i64", DataType.INT64, {}, 64, "i64 == 64"),
            ("score_f", DataType.FLOAT, {}, 1.5, "score_f > 1.0"),
            ("score_d", DataType.DOUBLE, {}, 2.5, "score_d > 2.0"),
            ("tag", DataType.VARCHAR, {"max_length": 64}, "drop_tag", 'tag == "drop_tag"'),
            ("payload", DataType.JSON, {}, {"k": "v", "n": 1}, 'payload["k"] == "v"'),
            (
                "arr",
                DataType.ARRAY,
                {"element_type": DataType.INT64, "max_capacity": 8},
                [1, 2, 3],
                "array_contains(arr, 2)",
            ),
            ("event_time", DataType.TIMESTAMPTZ, {"nullable": True}, "2025-01-02T00:00:00Z", "event_time is not null"),
        ]

        # Step 1: Create one collection that contains every scalar/complex field covered by the matrix.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("keep", DataType.INT64)
        for field_name, data_type, field_kwargs, _value, _filter_expr in cases:
            schema.add_field(field_name, data_type, **field_kwargs)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert sealed rows where every target field has a recognizable value.
        rows = []
        for row_id in [0, 1]:
            row = {
                "id": row_id,
                "vec": [float(row_id), 0.0, 0.0, 0.0],
                "keep": 100 + row_id,
            }
            for field_name, _data_type, _field_kwargs, value, _filter_expr in cases:
                row[field_name] = value
            rows.append(row)
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        dropped_fields = []
        for field_name, _data_type, _field_kwargs, _value, filter_expr in cases:
            # Step 3: Verify the target field is usable before it is dropped.
            before_query = client.query(
                collection_name=collection_name,
                filter=filter_expr,
                output_fields=["id", field_name],
            )
            assert len(before_query) > 0
            for row in before_query:
                assert field_name in row

            # Step 4: Drop the target scalar field and verify schema convergence.
            client.drop_collection_field(collection_name, field_name)
            dropped_fields.append(field_name)

            field_names = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
            assert field_name not in field_names
            assert "keep" in field_names
            assert "vec" in field_names

            # Step 5: Wildcard query should not return the newly dropped field.
            after_query = client.query(
                collection_name=collection_name,
                filter="id in [0, 1]",
                output_fields=["*"],
            )
            assert len(after_query) == 2
            for row in after_query:
                assert "keep" in row
                assert field_name not in row

            # Step 6: Explicit output/filter references must fail with field-not-exist errors.
            self.query(
                client,
                collection_name=collection_name,
                filter="id >= 0",
                output_fields=[field_name],
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: f"field {field_name} not exist"},
            )

            self.query(
                client,
                collection_name=collection_name,
                filter=filter_expr,
                output_fields=["id"],
                check_task=ct.CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: f"field {field_name} not exist"},
            )

        # Step 7: Search wildcard should follow the final schema after all scalar fields are dropped.
        after_all_drops_search = client.search(
            collection_name=collection_name,
            data=[[0.0, 0.0, 0.0, 0.0]],
            anns_field="vec",
            limit=2,
            output_fields=["*"],
        )
        assert len(after_all_drops_search[0]) == 2
        for hit in after_all_drops_search[0]:
            assert "keep" in hit["entity"]
            for field_name in dropped_fields:
                assert field_name not in hit["entity"]

        # Step 8: Reload and repeat the final visibility checks.
        client.release_collection(collection_name)
        client.load_collection(collection_name)

        after_reload = client.query(
            collection_name=collection_name,
            filter="id in [0, 1]",
            output_fields=["*"],
        )
        assert len(after_reload) == 2
        for row in after_reload:
            assert "keep" in row
            for field_name in dropped_fields:
                assert field_name not in row

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_disable_dynamic_schema_reload_reenable_and_index_cascade(self):
        """
        TC-L1-22: Disable dynamic schema reload, re-enable, and index cascade.

        target: verify dynamic metadata is not resurrected after disable/reload/re-enable
        method: create dynamic rows and dynamic index, disable dynamic schema, reload, re-enable, then insert new dynamic rows
        expected: old dynamic keys/index disappear; reload does not expose old $meta; only new dynamic rows are visible after re-enable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dynamic_index_name = "$meta/dyn_obj/score"

        # Step 1: Create one dynamic-schema collection with a dynamic object used by a JSON path index.
        schema = client.create_schema(enable_dynamic_field=True, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert old rows with dynamic data and verify the dynamic key is visible.
        old_rows = [
            {
                "id": i,
                "vec": [float(i), 0.0, 0.0, 0.0],
                "age": 20 + i,
                "dyn_obj": {"score": i, "tag": f"old_{i}"},
            }
            for i in range(5)
        ]
        client.insert(collection_name=collection_name, data=old_rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        before_disable = client.query(
            collection_name=collection_name,
            filter='dyn_obj["score"] == 1',
            output_fields=["id", "dyn_obj"],
        )
        assert len(before_disable) == 1
        assert before_disable[0]["dyn_obj"]["tag"] == "old_1"

        # Step 3: Create a dynamic JSON path index so disable can verify index cascade cleanup.
        dynamic_index_params = client.prepare_index_params()
        dynamic_index_params.add_index(
            field_name="dyn_obj",
            index_type="INVERTED",
            params={"json_cast_type": "DOUBLE", "json_path": 'dyn_obj["score"]'},
        )
        client.create_index(collection_name, dynamic_index_params)
        assert dynamic_index_name in client.list_indexes(collection_name)

        # Step 4: Disable dynamic schema and verify dynamic metadata leaves the public schema.
        client.alter_collection_properties(collection_name, {"dynamicfield.enabled": False})

        after_disable_desc = client.describe_collection(collection_name)
        assert after_disable_desc["enable_dynamic_field"] is False
        assert "$meta" not in [field["name"] for field in after_disable_desc["fields"]]

        # Step 5: Verify the dynamic index is removed by the disable operation.
        deadline = time.time() + 30
        while dynamic_index_name in client.list_indexes(collection_name) and time.time() < deadline:
            time.sleep(1)
        assert dynamic_index_name not in client.list_indexes(collection_name)

        # Step 6: Reload and verify old dynamic data is still hidden from wildcard output.
        client.release_collection(collection_name)
        client.load_collection(collection_name)

        after_reload = client.query(
            collection_name=collection_name,
            filter="id in [0, 1, 2, 3, 4]",
            output_fields=["*"],
        )
        assert len(after_reload) == 5
        for row in after_reload:
            assert "id" in row
            assert "age" in row
            assert "vec" in row
            assert "dyn_obj" not in row
            assert "$meta" not in row

        # Step 7: Dynamic references must fail while dynamic schema is disabled.
        self.query(
            client,
            collection_name=collection_name,
            filter='dyn_obj["score"] == 1',
            output_fields=["id"],
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field dyn_obj not exist"},
        )

        # Step 8: Re-enable dynamic schema and insert new rows with a new dynamic key.
        client.alter_collection_properties(collection_name, {"dynamicfield.enabled": True})

        after_enable_desc = client.describe_collection(collection_name)
        assert after_enable_desc["enable_dynamic_field"] is True

        client.insert(
            collection_name=collection_name,
            data=[
                {
                    "id": 100,
                    "vec": [1.0, 0.0, 0.0, 0.0],
                    "age": 100,
                    "dyn_after_enable": "new_value",
                }
            ],
        )
        client.flush(collection_name)

        # Step 9: Verify old dynamic rows stay hidden, while new dynamic rows are visible.
        after_enable_rows = client.query(
            collection_name=collection_name,
            filter="id in [1, 100]",
            output_fields=["*"],
        )
        rows = rows_by_id(after_enable_rows)
        assert "dyn_obj" not in rows[1]
        assert rows[100]["dyn_after_enable"] == "new_value"

        old_dynamic_filter = client.query(
            collection_name=collection_name,
            filter='dyn_obj["score"] == 1',
            output_fields=["id"],
        )
        assert old_dynamic_filter == []

        new_dynamic_filter = client.query(
            collection_name=collection_name,
            filter='dyn_after_enable == "new_value"',
            output_fields=["id", "dyn_after_enable"],
        )
        assert len(new_dynamic_filter) == 1
        assert new_dynamic_filter[0]["id"] == 100
        assert new_dynamic_filter[0]["dyn_after_enable"] == "new_value"

        # Step 10: Reload once more to verify re-enabled dynamic data remains scoped to new rows only.
        client.release_collection(collection_name)
        client.load_collection(collection_name)

        after_final_reload = client.query(
            collection_name=collection_name,
            filter="id in [1, 100]",
            output_fields=["*"],
        )
        rows = rows_by_id(after_final_reload)
        assert "dyn_obj" not in rows[1]
        assert rows[100]["dyn_after_enable"] == "new_value"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_expression_references_dropped_field(self):
        """
        TC-L1-24: Delete expression references dropped field.

        target: verify delete uses the latest schema after Drop Field
        method: drop score, delete by kept field successfully, then reject delete by dropped score before and after reload
        expected: dropped-field delete fails without deleting data; kept-field delete still works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dynamic_collection_name = f"{collection_name}_dyn"

        # Step 1: Create a normal collection with score as the field to drop.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("age", DataType.INT64)
        schema.add_field("score", DataType.INT64)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        rows = [{"id": i, "vec": [float(i), 0.0, 0.0, 0.0], "age": 20 + i, "score": i * 10} for i in range(6)]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        client.load_collection(collection_name)

        # Step 2: Drop score and verify it leaves the schema.
        client.drop_collection_field(collection_name, "score")

        fields = [field["name"] for field in client.describe_collection(collection_name)["fields"]]
        assert "score" not in fields
        assert "age" in fields

        # Step 3: Delete by a kept primary-key expression should still work.
        delete_res = client.delete(collection_name=collection_name, filter="id in [0, 1]")
        assert delete_res["delete_count"] == 2

        deleted_rows = client.query(
            collection_name=collection_name,
            filter="id in [0, 1]",
            output_fields=["id"],
        )
        assert deleted_rows == []

        remaining_before_bad_delete = client.query(
            collection_name=collection_name,
            filter="id in [2, 3, 4, 5]",
            output_fields=["id", "age"],
        )
        assert {row["id"] for row in remaining_before_bad_delete} == {2, 3, 4, 5}

        # Step 4: Delete by dropped score must fail and must not delete any remaining rows.
        self.delete(
            client,
            collection_name=collection_name,
            filter="score > 10",
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field score not exist"},
        )

        remaining_after_bad_delete = client.query(
            collection_name=collection_name,
            filter="id in [2, 3, 4, 5]",
            output_fields=["id", "age"],
        )
        assert {row["id"] for row in remaining_after_bad_delete} == {2, 3, 4, 5}

        # Step 5: Reload and verify dropped-field delete is still rejected.
        client.release_collection(collection_name)
        client.load_collection(collection_name)

        self.delete(
            client,
            collection_name=collection_name,
            filter="score > 10",
            check_task=ct.CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "field score not exist"},
        )

        remaining_after_reload_bad_delete = client.query(
            collection_name=collection_name,
            filter="id in [2, 3, 4, 5]",
            output_fields=["id", "age"],
        )
        assert {row["id"] for row in remaining_after_reload_bad_delete} == {2, 3, 4, 5}

        # Step 6: Create a dynamic collection to verify same-name dynamic key is not treated as the old dropped field.
        dynamic_schema = client.create_schema(enable_dynamic_field=True, auto_id=False)
        dynamic_schema.add_field("id", DataType.INT64, is_primary=True)
        dynamic_schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        dynamic_schema.add_field("score", DataType.INT64)

        dynamic_index_params = client.prepare_index_params()
        dynamic_index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=dynamic_collection_name,
            schema=dynamic_schema,
            index_params=dynamic_index_params,
            consistency_level="Strong",
        )

        dynamic_rows = [
            {"id": 10, "vec": [0.0, 0.0, 0.0, 0.0], "score": 10},
            {"id": 11, "vec": [1.0, 0.0, 0.0, 0.0], "score": 20},
        ]
        client.insert(collection_name=dynamic_collection_name, data=dynamic_rows)
        client.flush(dynamic_collection_name)
        client.load_collection(dynamic_collection_name)

        client.drop_collection_field(dynamic_collection_name, "score")

        # Step 7: After score is dropped, newly inserted score is dynamic data and delete(score > ...) must target only it.
        client.insert(
            collection_name=dynamic_collection_name,
            data=[{"id": 12, "vec": [2.0, 0.0, 0.0, 0.0], "score": 30}],
        )
        client.flush(dynamic_collection_name)

        dynamic_delete_res = client.delete(dynamic_collection_name, filter="score > 25")
        assert dynamic_delete_res["delete_count"] == 1

        old_rows = client.query(
            collection_name=dynamic_collection_name,
            filter="id in [10, 11]",
            output_fields=["*"],
        )
        assert {row["id"] for row in old_rows} == {10, 11}
        assert all("score" not in row for row in old_rows)

        new_row = client.query(
            collection_name=dynamic_collection_name,
            filter="id == 12",
            output_fields=["id"],
        )
        assert new_row == []

        for name in [collection_name, dynamic_collection_name]:
            self.drop_collection(client, name)
