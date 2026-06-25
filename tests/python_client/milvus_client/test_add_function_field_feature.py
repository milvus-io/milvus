import json
import time

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import AnnSearchRequest, DataType, FieldSchema, Function, FunctionType, RRFRanker, WeightedRanker
from utils.util_log import test_log as log


class TestMilvusClientAddFunctionFieldFeature(TestMilvusClientV2Base):
    def wait_for_search_hit(
        self,
        client,
        collection_name,
        data,
        anns_field,
        expected_id,
        label,
        search_params=None,
        output_fields=None,
        limit=5,
        timeout=30,
        interval=1,
        filter=None,
    ):
        poll_start = time.time()
        deadline = poll_start + timeout
        last_error = None
        attempts = 0

        while time.time() < deadline:
            attempts += 1
            try:
                search_kwargs = {
                    "collection_name": collection_name,
                    "data": data,
                    "anns_field": anns_field,
                    "limit": limit,
                    "output_fields": output_fields or ["id"],
                }
                if search_params is not None:
                    search_kwargs["search_params"] = search_params
                if filter is not None:
                    search_kwargs["filter"] = filter

                search_res = client.search(**search_kwargs)
                hit_ids = [hit["id"] for hit in search_res[0]]
                if expected_id in hit_ids:
                    ready_msg = (
                        f"{label} search ready after {time.time() - poll_start:.2f}s, "
                        f"attempts={attempts}, hit_ids={hit_ids}"
                    )
                    log.info(ready_msg)
                    return search_res
                last_error = f"{label} search returned ids={hit_ids}"
            except Exception as exc:
                last_error = str(exc)

            time.sleep(interval)

        raise AssertionError(f"{label} search was not ready within {timeout}s, attempts={attempts}: {last_error}")

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_bm25_function_field_main_path(self):
        """
        TC-01: BM25 add_function_field main path.

        target: verify old sealed rows, new growing rows, and new sealed rows are searchable via added BM25 field
        method: add BM25 function field, create sparse index, search old data, insert new data, then flush and search again
        expected: schema has new field/function; old sealed, new growing, and new sealed rows are searchable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 0: Create a collection with analyzer-enabled text input field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 0: Insert old rows and flush them to sealed segment before add_function_field.
        old_rows = [
            {"id": 0, "text": "alpha milvus old sealed document", "vec": [0.0, 0.0, 0.0, 0.0]},
            {"id": 1, "text": "beta milvus old sealed document", "vec": [1.0, 0.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=old_rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        # Step 1: Execute add_function_field.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        # Step 2: Verify the new output field and function are visible in collection schema.
        desc = client.describe_collection(collection_name)
        assert "sparse" in [field["name"] for field in desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in desc.get("functions", [])]

        # Step 3: Explicitly create sparse index for the added BM25 output field.
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=180)

        # Step 4: Load collection after sparse index is ready.
        client.load_collection(collection_name)

        # Step 5 and Step 6: Search old sealed rows and wait until backfilled BM25 output is readable.
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["alpha"],
            anns_field="sparse",
            expected_id=0,
            label="BM25 old sealed",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        # Step 7: Insert new rows after add_function_field without manually providing sparse values.
        new_rows = [
            {"id": 100, "text": "gamma milvus new growing document", "vec": [0.0, 1.0, 0.0, 0.0]},
            {"id": 101, "text": "delta milvus new sealed document", "vec": [0.0, 0.0, 1.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=new_rows)

        # Step 8: Search add-after growing rows before flush.
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["gamma"],
            anns_field="sparse",
            expected_id=100,
            label="BM25 new growing",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        # Step 9: Flush add-after rows and verify they are still searchable as sealed data.
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=180)

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["delta"],
            anns_field="sparse",
            expected_id=101,
            label="BM25 new sealed",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        # Step 10: Release and load collection, then verify the added BM25 field still works after reload.
        self.release_collection(client, collection_name)
        client.load_collection(collection_name)
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["alpha"],
            anns_field="sparse",
            expected_id=0,
            label="BM25 after release/load",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        desc = client.describe_collection(collection_name)
        assert "sparse" in [field["name"] for field in desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in desc.get("functions", [])]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_bm25_function_field_no_auto_index(self):
        """
        TC-02: add_function_field does not auto-create index.

        target: verify schema evolution and index management boundary
        method: add BM25 function field, search before index, then create index and search again
        expected: add_function_field does not create index implicitly; BM25 search works after explicit index/load
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 0: Create a base collection with analyzer-enabled text field and dense vector index.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        sealed_rows = [
            {"id": 0, "text": "alpha milvus old sealed document", "vec": [0.0, 0.0, 0.0, 0.0]},
            {"id": 1, "text": "beta milvus old sealed document", "vec": [1.0, 0.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=sealed_rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        # Step 1: Execute add_function_field.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        desc = client.describe_collection(collection_name)
        assert "sparse" in [field["name"] for field in desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in desc.get("functions", [])]

        # Expected: add_function_field should not create sparse index implicitly.
        assert client.describe_index(collection_name, index_name="sparse") is None

        # Step 2: Search on anns_field="sparse" before creating sparse index.
        # Expected: no crash; field-not-loaded, no-index, empty result, or equivalent behavior is acceptable.
        search_res, is_succ = self.search(
            client,
            collection_name,
            data=["alpha"],
            anns_field="sparse",
            limit=5,
            output_fields=["id"],
            check_task=CheckTasks.check_nothing,
        )
        if is_succ:
            assert len(search_res[0]) == 0
        else:
            err = str(search_res).lower()
            assert any(token in err for token in ["sparse", "index", "load", "loaded", "not exist", "not found"]), err

        # Step 3: Create sparse index and load collection.
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=180)
        client.load_collection(collection_name)

        # Step 4: Search again and wait until BM25 search becomes ready.
        # Expected: BM25 search succeeds after explicit index creation and load.
        search_timeout = 30
        poll_interval = 1
        poll_start = time.time()
        deadline = poll_start + search_timeout
        last_error = None
        attempts = 0
        while time.time() < deadline:
            attempts += 1
            try:
                search_res = client.search(
                    collection_name,
                    data=["alpha"],
                    anns_field="sparse",
                    limit=5,
                    output_fields=["id", "text"],
                    search_params={"metric_type": "BM25"},
                )
                hit_ids = [hit["id"] for hit in search_res[0]]
                if 0 in hit_ids:
                    ready_msg = (
                        f"BM25 search ready after {time.time() - poll_start:.2f}s, "
                        f"attempts={attempts}, hit_ids={hit_ids}"
                    )
                    log.info(ready_msg)
                    break
                last_error = f"BM25 search returned ids={hit_ids}"
            except Exception as exc:
                last_error = str(exc)
            time.sleep(poll_interval)
        else:
            raise AssertionError(
                f"BM25 search was not ready within {search_timeout}s, attempts={attempts}: {last_error}"
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_bm25_function_field_pre_add_growing_data(self):
        """
        TC-03: Add function field after pre-add growing data exists.

        target: verify growing rows inserted before add_function_field are not lost at DDL boundary
        method: insert old sealed rows, insert old growing rows without flush, add BM25 field, then search all batches
        expected: old sealed, pre-add growing, and post-add growing rows are searchable via added BM25 field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create collection and load it before writing old data.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        # Step 2: Insert the first batch and flush it to sealed segment before add_function_field.
        sealed_rows = [
            {"id": 0, "text": "alpha pre add sealed document", "vec": [0.0, 0.0, 0.0, 0.0]},
            {"id": 1, "text": "beta pre add sealed document", "vec": [1.0, 0.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=sealed_rows)
        client.flush(collection_name)

        # Step 3: Insert the second batch before add_function_field, but do not flush manually.
        growing_rows = [
            {"id": 10, "text": "theta pre add growing document", "vec": [0.0, 1.0, 0.0, 0.0]},
            {"id": 11, "text": "iota pre add growing document", "vec": [0.0, 0.0, 1.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=growing_rows)

        # Step 4: Execute add_function_field while pre-add growing rows exist.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        desc = client.describe_collection(collection_name)
        assert "sparse" in [field["name"] for field in desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in desc.get("functions", [])]

        # Step 5: Create sparse index and load collection.
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=30)
        client.load_collection(collection_name)

        # Step 6: Search old sealed rows and verify backfilled BM25 output is readable.
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["alpha"],
            anns_field="sparse",
            expected_id=0,
            label="BM25 pre-add sealed",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        # Step 6: Search pre-add growing rows that should be handled at schema change boundary.
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["theta"],
            anns_field="sparse",
            expected_id=10,
            label="BM25 pre-add growing",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        # Step 7: Insert post-add rows and verify new growing data is searchable.
        post_add_rows = [
            {"id": 100, "text": "gamma post add growing document", "vec": [1.0, 1.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=post_add_rows)

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["gamma"],
            anns_field="sparse",
            expected_id=100,
            label="BM25 post-add growing",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_schema_metadata(self):
        """
        TC-05: Schema version, field ID, and function ID metadata.

        target: verify add_function_field updates schema metadata consistently
        method: describe schema before/after add_function_field and validate version/id/function mapping
        expected: schema_version increases; new field/function IDs are allocated; max_field_id is updated
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create base collection and record schema metadata before add_function_field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        before_desc = client.describe_collection(collection_name)
        before_schema_version = before_desc["schema_version"]
        before_field_ids = {field["name"]: field["field_id"] for field in before_desc["fields"]}
        before_function_ids = {func["id"] for func in before_desc.get("functions", [])}
        before_max_field_id = max(before_field_ids.values())

        assert "sparse" not in before_field_ids
        assert "bm25_fn" not in [func["name"] for func in before_desc.get("functions", [])]
        assert int(before_desc["properties"]["max_field_id"]) == before_max_field_id

        # Step 2: Execute add_function_field.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        # Step 3: Describe collection again and verify metadata has been updated.
        after_desc = client.describe_collection(collection_name)
        after_fields = {field["name"]: field for field in after_desc["fields"]}
        after_functions = {func["name"]: func for func in after_desc.get("functions", [])}

        # Step 4: Verify schema_version increases and new field/function are visible.
        assert after_desc["schema_version"] > before_schema_version
        assert "sparse" in after_fields
        assert "bm25_fn" in after_functions

        # Step 5: Verify new field ID is allocated after historical field IDs.
        sparse_field_id = after_fields["sparse"]["field_id"]
        assert sparse_field_id > before_max_field_id
        assert int(after_desc["properties"]["max_field_id"]) == sparse_field_id
        assert after_fields["sparse"].get("is_function_output") is True

        # Step 6: Verify function ID and input/output field mapping are stable.
        bm25_func = after_functions["bm25_fn"]
        assert bm25_func["id"] not in before_function_ids
        assert bm25_func["input_field_names"] == ["text"]
        assert bm25_func["output_field_names"] == ["sparse"]
        assert bm25_func["input_field_ids"] == [before_field_ids["text"]]
        assert bm25_func["output_field_ids"] == [sparse_field_id]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_invalid_params(self):
        """
        TC-06: add_function_field negative parameter matrix for BM25.

        target: verify invalid BM25 add_function_field requests are rejected without partial schema mutation
        method: send invalid field/function combinations and compare schema before/after each failure
        expected: each invalid request fails clearly; schema_version, fields, functions, and max_field_id stay unchanged
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 0: Create a base collection with valid and invalid candidate input fields.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("plain_text", DataType.VARCHAR, max_length=1024)
        schema.add_field("int_text", DataType.INT64)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        before_desc = client.describe_collection(collection_name)
        before_schema_version = before_desc["schema_version"]
        before_field_names = [field["name"] for field in before_desc["fields"]]
        before_function_names = [func["name"] for func in before_desc.get("functions", [])]
        before_max_field_id = before_desc["properties"]["max_field_id"]

        # Step 1: Invalid add_function_field requests from TC-06 BM25 matrix.
        invalid_cases = [
            (
                "input field does not exist",
                FieldSchema(name="sparse_missing_input", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="bm25_missing_input",
                    function_type=FunctionType.BM25,
                    input_field_names=["missing_text"],
                    output_field_names=["sparse_missing_input"],
                ),
                {ct.err_code: 1100, ct.err_msg: "function input field not found: missing_text"},
            ),
            (
                "output field is not the newly added field",
                FieldSchema(name="sparse_orphan", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="bm25_output_not_new",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["sparse_missing_output"],
                ),
                {
                    ct.err_code: 1100,
                    ct.err_msg: 'function output field "sparse_missing_output" must be the newly-added field "sparse_orphan"',
                },
            ),
            (
                "output field name already exists",
                FieldSchema(name="vec", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="bm25_existing_output",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["vec"],
                ),
                {ct.err_code: 1100, ct.err_msg: "duplicated field name vec"},
            ),
            (
                "function name is empty",
                FieldSchema(name="sparse_empty_function_name", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["sparse_empty_function_name"],
                ),
                {ct.err_code: 1101, ct.err_msg: "function name cannot be empty"},
            ),
            (
                "input field is not analyzer-enabled",
                FieldSchema(name="sparse_no_analyzer", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="bm25_no_analyzer",
                    function_type=FunctionType.BM25,
                    input_field_names=["plain_text"],
                    output_field_names=["sparse_no_analyzer"],
                ),
                {ct.err_code: 1100, ct.err_msg: "BM25 function input field must set enable_analyzer to true"},
            ),
            (
                "input field is not VARCHAR",
                FieldSchema(name="sparse_int_input", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="bm25_int_input",
                    function_type=FunctionType.BM25,
                    input_field_names=["int_text"],
                    output_field_names=["sparse_int_input"],
                ),
                {
                    ct.err_code: 1100,
                    ct.err_msg: "BM25 function input field must be a VARCHAR/TEXT field, got 1 field with type Int64",
                },
            ),
            (
                "BM25 output type is not sparse vector",
                FieldSchema(name="dense_output", dtype=DataType.FLOAT_VECTOR, dim=4),
                Function(
                    name="bm25_dense_output",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["dense_output"],
                ),
                {
                    ct.err_code: 1,
                    ct.err_msg: "add_function_field requires SPARSE_FLOAT_VECTOR output field for 1, got 101",
                },
            ),
            (
                "MINHASH output type is not binary vector",
                FieldSchema(name="minhash_sparse_output", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="minhash_sparse_output",
                    function_type=FunctionType.MINHASH,
                    input_field_names=["text"],
                    output_field_names=["minhash_sparse_output"],
                    params={"num_hashes": 16, "shingle_size": 3},
                ),
                {
                    ct.err_code: 1,
                    ct.err_msg: "add_function_field requires BINARY_VECTOR output field for 4, got 104",
                },
            ),
            (
                "MINHASH output type is float vector",
                FieldSchema(name="minhash_float_output", dtype=DataType.FLOAT_VECTOR, dim=4),
                Function(
                    name="minhash_float_output",
                    function_type=FunctionType.MINHASH,
                    input_field_names=["text"],
                    output_field_names=["minhash_float_output"],
                    params={"num_hashes": 16, "shingle_size": 3},
                ),
                {
                    ct.err_code: 1,
                    ct.err_msg: "add_function_field requires BINARY_VECTOR output field for 4, got 101",
                },
            ),
            (
                "BM25 output field is nullable",
                FieldSchema(name="sparse_nullable", dtype=DataType.SPARSE_FLOAT_VECTOR, nullable=True),
                Function(
                    name="bm25_nullable_output",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["sparse_nullable"],
                ),
                {
                    ct.err_code: 1100,
                    ct.err_msg: "function output field cannot be nullable: function bm25_nullable_output, field sparse_nullable",
                },
            ),
            (
                "function input is empty",
                FieldSchema(name="sparse_empty_input", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="bm25_empty_input",
                    function_type=FunctionType.BM25,
                    input_field_names=[],
                    output_field_names=["sparse_empty_input"],
                ),
                {
                    ct.err_code: 1100,
                    ct.err_msg: "BM25 function should have exactly one input field and exactly one output field",
                },
            ),
            (
                "function output is empty",
                FieldSchema(name="sparse_empty_output", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="bm25_empty_output",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=[],
                ),
                {
                    ct.err_code: 1100,
                    ct.err_msg: "BM25 function should have exactly one input field and exactly one output field",
                },
            ),
            (
                "function has multiple outputs",
                FieldSchema(name="sparse_multi_a", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="bm25_multi_output",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["sparse_multi_a", "sparse_multi_b"],
                ),
                {
                    ct.err_code: 1100,
                    ct.err_msg: "BM25 function should have exactly one input field and exactly one output field",
                },
            ),
            (
                "BM25 params are not empty",
                FieldSchema(name="sparse_with_params", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="bm25_with_params",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["sparse_with_params"],
                    params={"invalid_param": "value"},
                ),
                {ct.err_code: 1100, ct.err_msg: "BM25 function accepts no params"},
            ),
        ]

        for case_name, field_schema, function, error in invalid_cases:
            # Step 2: Each invalid request must fail clearly.
            self.add_function_field(
                client,
                collection_name,
                field_schema,
                function,
                check_task=CheckTasks.err_res,
                check_items=error,
            )

            # Step 3: Failure must not partially mutate schema.
            after_desc = client.describe_collection(collection_name)
            assert after_desc["schema_version"] == before_schema_version, case_name
            assert [field["name"] for field in after_desc["fields"]] == before_field_names, case_name
            assert [func["name"] for func in after_desc.get("functions", [])] == before_function_names, case_name
            assert after_desc["properties"]["max_field_id"] == before_max_field_id, case_name

        # Step 4: Non-existent collection should fail and not affect the original collection.
        sparse_field = FieldSchema(name="sparse_missing_collection", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_missing_collection",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse_missing_collection"],
        )
        self.add_function_field(
            client,
            f"{collection_name}_missing",
            sparse_field,
            bm25_function,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 100, ct.err_msg: "can't find collection"},
        )

        desc = client.describe_collection(collection_name)
        assert desc["schema_version"] == before_schema_version
        assert [field["name"] for field in desc["fields"]] == before_field_names
        assert [func["name"] for func in desc.get("functions", [])] == before_function_names

        # Step 5: Function name duplicate is validated on a collection that already has a function.
        duplicate_name_collection = cf.gen_collection_name_by_testcase_name()
        duplicate_schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        duplicate_schema.add_field("id", DataType.INT64, is_primary=True)
        duplicate_schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        duplicate_schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        duplicate_schema.add_field("base_sparse", DataType.SPARSE_FLOAT_VECTOR)
        duplicate_schema.add_function(
            Function(
                name="bm25_fn",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names=["base_sparse"],
            )
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(field_name="base_sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_collection(
            collection_name=duplicate_name_collection,
            schema=duplicate_schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        duplicate_before_desc = client.describe_collection(duplicate_name_collection)
        self.add_function_field(
            client,
            duplicate_name_collection,
            FieldSchema(name="sparse_duplicate_function", dtype=DataType.SPARSE_FLOAT_VECTOR),
            Function(
                name="bm25_fn",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names=["sparse_duplicate_function"],
            ),
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "duplicate function name: bm25_fn"},
        )

        duplicate_after_desc = client.describe_collection(duplicate_name_collection)
        assert duplicate_after_desc["schema_version"] == duplicate_before_desc["schema_version"]
        assert [field["name"] for field in duplicate_after_desc["fields"]] == [
            field["name"] for field in duplicate_before_desc["fields"]
        ]
        assert [func["name"] for func in duplicate_after_desc.get("functions", [])] == [
            func["name"] for func in duplicate_before_desc.get("functions", [])
        ]

        # Step 6: A valid request still succeeds after all rejected requests.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        desc = client.describe_collection(collection_name)
        assert "sparse" in [field["name"] for field in desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in desc.get("functions", [])]

        self.drop_collection(client, duplicate_name_collection)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_reject_manual_output_writes(self):
        """
        TC-07: Users cannot manually write BM25 function output field.

        target: verify BM25 function output field is system-generated and cannot be user-written
        method: add BM25 function field, reject insert/upsert/partial_update with sparse, then verify normal writes work
        expected: manual sparse writes fail; normal insert/upsert without sparse are searchable through generated BM25 output
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create a base collection, then add BM25 function output field through add_function_field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=180)
        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        error = {
            ct.err_code: 1,
            ct.err_msg: "Attempt to insert an unexpected function output field `sparse` to collection",
        }

        # Step 2: insert rows with explicit sparse output field should be rejected.
        self.insert(
            client,
            collection_name,
            data=[
                {
                    "id": 1,
                    "text": "alpha manual sparse insert document",
                    "vec": [0.0, 0.0, 0.0, 0.0],
                    "sparse": {1: 100.0},
                }
            ],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        # Step 3: upsert rows with explicit sparse output field should be rejected.
        self.upsert(
            client,
            collection_name,
            data=[
                {
                    "id": 2,
                    "text": "beta manual sparse upsert document",
                    "vec": [1.0, 0.0, 0.0, 0.0],
                    "sparse": {2: 100.0},
                }
            ],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        # Step 4: normal insert without sparse should let the system generate BM25 output.
        client.insert(
            collection_name=collection_name,
            data=[
                {"id": 10, "text": "gamma normal insert document", "vec": [0.0, 1.0, 0.0, 0.0]},
            ],
        )
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["gamma"],
            anns_field="sparse",
            expected_id=10,
            label="BM25 normal insert without sparse",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        # Step 5: normal upsert without sparse should also generate BM25 output.
        client.upsert(
            collection_name=collection_name,
            data=[
                {"id": 20, "text": "delta normal upsert document", "vec": [0.0, 0.0, 1.0, 0.0]},
            ],
        )
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["delta"],
            anns_field="sparse",
            expected_id=20,
            label="BM25 normal upsert without sparse",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        # Step 6: partial_update that explicitly updates sparse should be rejected.
        self.upsert(
            client,
            collection_name,
            data=[{"id": 10, "sparse": {3: 100.0}}],
            partial_update=True,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        # Step 7: failed manual sparse partial_update must not pollute the existing generated BM25 output.
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["gamma"],
            anns_field="sparse",
            expected_id=10,
            label="BM25 after rejected sparse partial update",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_backfill_nullable_input(self):
        """
        TC-08: Historical nullable BM25 input backfill.

        target: verify add_function_field backfill handles historical null/empty text input
        method: insert sealed rows with text=None, empty string, and normal text before add; add BM25 field and search
        expected: non-null historical and post-add rows are searchable; null/empty rows do not crash or match normal queries
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create collection with nullable analyzer-enabled BM25 input field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, nullable=True, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 1: Insert add-before rows with normal, null, and empty-string text interleaved.
        historical_rows = [
            {"id": 0, "text": "alpha nullable control", "vec": [0.0, 0.0, 0.0, 0.0]},
            {"id": 1, "text": None, "vec": [1.0, 0.0, 0.0, 0.0]},
            {"id": 2, "text": "beta nullable baseline", "vec": [0.0, 1.0, 0.0, 0.0]},
            {"id": 3, "text": "", "vec": [0.0, 0.0, 1.0, 0.0]},
            {"id": 4, "text": None, "vec": [0.0, 0.0, 0.0, 1.0]},
            {"id": 5, "text": "theta nullable tail", "vec": [1.0, 1.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=historical_rows)

        # Step 2: Flush to make nullable input rows historical sealed data before add_function_field.
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        # Step 3: Execute add_function_field.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        # Step 4: Create sparse BM25 index and reload collection.
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=30)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=30)
        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        # Step 5 and Step 6: Wait for BM25 readiness and verify non-null historical rows are searchable.
        for query_text, expected_id, label in [
            ("alpha", 0, "BM25 nullable historical alpha"),
            ("beta", 2, "BM25 nullable historical beta"),
            ("theta", 5, "BM25 nullable historical tail"),
        ]:
            self.wait_for_search_hit(
                client,
                collection_name,
                data=[query_text],
                anns_field="sparse",
                expected_id=expected_id,
                label=label,
                search_params={"metric_type": "BM25"},
                output_fields=["id", "text"],
            )

        # Step 7: Null and empty historical rows remain queryable by PK and do not match normal BM25 query.
        nullable_rows = client.query(
            collection_name,
            filter="id in [1, 3, 4]",
            output_fields=["id", "text"],
        )
        nullable_by_id = {row["id"]: row for row in nullable_rows}
        assert nullable_by_id[1]["text"] is None
        assert nullable_by_id[3]["text"] == ""
        assert nullable_by_id[4]["text"] is None

        search_res = client.search(
            collection_name,
            data=["alpha"],
            anns_field="sparse",
            limit=10,
            output_fields=["id", "text"],
            search_params={"metric_type": "BM25"},
        )
        hit_ids = [hit["id"] for hit in search_res[0]]
        assert 0 in hit_ids
        assert 1 not in hit_ids
        assert 3 not in hit_ids
        assert 4 not in hit_ids

        # Step 8: Insert post-add rows with normal, null, and empty-string text interleaved.
        post_add_rows = [
            {"id": 10, "text": "gamma nullable growing", "vec": [1.0, 0.0, 1.0, 0.0]},
            {"id": 11, "text": None, "vec": [0.0, 1.0, 1.0, 0.0]},
            {"id": 12, "text": "delta nullable sealed", "vec": [1.0, 0.0, 0.0, 1.0]},
            {"id": 13, "text": "", "vec": [0.0, 1.0, 0.0, 1.0]},
            {"id": 14, "text": None, "vec": [1.0, 1.0, 1.0, 0.0]},
            {"id": 15, "text": "omega nullable tail", "vec": [0.0, 1.0, 1.0, 1.0]},
        ]
        client.insert(collection_name=collection_name, data=post_add_rows)

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["gamma"],
            anns_field="sparse",
            expected_id=10,
            label="BM25 nullable post-add growing",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=30)

        for query_text, expected_id, label in [
            ("delta", 12, "BM25 nullable post-add sealed"),
            ("omega", 15, "BM25 nullable post-add tail"),
        ]:
            self.wait_for_search_hit(
                client,
                collection_name,
                data=[query_text],
                anns_field="sparse",
                expected_id=expected_id,
                label=label,
                search_params={"metric_type": "BM25"},
                output_fields=["id", "text"],
            )

        search_res = client.search(
            collection_name,
            data=["gamma"],
            anns_field="sparse",
            limit=10,
            output_fields=["id", "text"],
            search_params={"metric_type": "BM25"},
        )
        hit_ids = [hit["id"] for hit in search_res[0]]
        assert 10 in hit_ids
        assert 11 not in hit_ids
        assert 13 not in hit_ids
        assert 14 not in hit_ids

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_bm25_function_field_multi_round_insert_flush_search(self):
        """
        TC-09: Continue writing after add_function_field with multiple flush rounds.

        target: verify post-add writes keep generating BM25 function output across growing and sealed states
        method: add BM25 field, then insert multiple batches; search each batch before and after flush with id filter
        expected: every post-add batch is searchable while growing and remains searchable after flush/index catch-up
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 0: Create collection and prepare one historical sealed row before add_function_field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        historical_rows = [
            {"id": 0, "text": "alpha historical sealed document", "vec": [0.0, 0.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=historical_rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        # Step 1: Complete add/index/load for the added BM25 function output field.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=180)
        client.load_collection(collection_name)

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["alpha"],
            anns_field="sparse",
            expected_id=0,
            label="BM25 multi-round historical",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        # Step 2 and Step 3: Insert multiple post-add batches; verify growing and sealed search for each batch.
        post_add_batches = [
            (
                1,
                [
                    {"id": 101, "text": "roundone cardinal growing document", "vec": [1.0, 0.0, 0.0, 0.0]},
                    {"id": 102, "text": "roundone meridian growing document", "vec": [0.0, 1.0, 0.0, 0.0]},
                    {"id": 103, "text": "roundone horizon growing document", "vec": [0.0, 0.0, 1.0, 0.0]},
                ],
                "cardinal",
                101,
            ),
            (
                2,
                [
                    {"id": 201, "text": "roundtwo lantern growing document", "vec": [0.0, 0.0, 0.0, 1.0]},
                    {"id": 202, "text": "roundtwo anchor growing document", "vec": [1.0, 1.0, 0.0, 0.0]},
                    {"id": 203, "text": "roundtwo cobalt growing document", "vec": [1.0, 0.0, 1.0, 0.0]},
                ],
                "lantern",
                201,
            ),
            (
                3,
                [
                    {"id": 301, "text": "roundthree violet growing document", "vec": [0.0, 1.0, 1.0, 0.0]},
                    {"id": 302, "text": "roundthree ember growing document", "vec": [1.0, 0.0, 0.0, 1.0]},
                    {"id": 303, "text": "roundthree silver growing document", "vec": [0.0, 1.0, 0.0, 1.0]},
                ],
                "violet",
                301,
            ),
        ]

        for round_no, rows, query_text, expected_id in post_add_batches:
            batch_ids = [row["id"] for row in rows]
            batch_filter = f"id in {batch_ids}"

            client.insert(collection_name=collection_name, data=rows)

            # Step 3: Search the current batch while it is still growing.
            self.wait_for_search_hit(
                client,
                collection_name,
                data=[query_text],
                anns_field="sparse",
                expected_id=expected_id,
                label=f"BM25 round {round_no} growing",
                search_params={"metric_type": "BM25"},
                output_fields=["id", "text"],
                filter=batch_filter,
            )

            client.flush(collection_name)
            assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=60)

            # Step 3 and Step 4: Search the same batch after flush with id filter.
            self.wait_for_search_hit(
                client,
                collection_name,
                data=[query_text],
                anns_field="sparse",
                expected_id=expected_id,
                label=f"BM25 round {round_no} sealed",
                search_params={"metric_type": "BM25"},
                output_fields=["id", "text"],
                filter=batch_filter,
            )

        # Step 4: Verify every batch remains visible by PK after all flush rounds.
        all_ids = [row["id"] for _, rows, _, _ in post_add_batches for row in rows]
        queried_rows = client.query(
            collection_name,
            filter=f"id in {all_ids}",
            output_fields=["id", "text"],
        )
        assert sorted(row["id"] for row in queried_rows) == sorted(all_ids)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_drop_function_field_combo(self):
        """
        TC-10: Add Function Field with Drop Function / Drop Field combination.

        target: verify add_function_field and drop_function_field dependency boundaries
        method: add BM25 field, reject direct input/output drop, cascade drop function field, then re-add
        expected: direct field drops fail; cascade removes function/output/index; re-add uses a new field ID and current text
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 0: Create base collection and insert one sealed row before add_function_field.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        client.insert(
            collection_name=collection_name,
            data=[
                {
                    "id": 0,
                    "text": "firstonly original document before drop function",
                    "vec": [0.0, 0.0, 0.0, 0.0],
                }
            ],
        )
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        # Step 1: add_function_field adds bm25_fn and sparse.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        desc = client.describe_collection(collection_name)
        fields = {field["name"]: field for field in desc["fields"]}
        functions = {func["name"]: func for func in desc.get("functions", [])}
        assert "sparse" in fields
        assert "bm25_fn" in functions
        old_sparse_field_id = fields["sparse"]["field_id"]

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=60)
        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["firstonly"],
            anns_field="sparse",
            expected_id=0,
            label="BM25 before drop function field",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        # Step 2: Direct Drop Field on function input field must be rejected.
        self.drop_collection_field(
            client,
            collection_name,
            field_name="text",
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "field is referenced by function bm25_fn as input, drop function first",
            },
        )

        # Step 3: Direct Drop Field on function output field must be rejected.
        self.drop_collection_field(
            client,
            collection_name,
            field_name="sparse",
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "field is referenced by function bm25_fn as output, drop function first",
            },
        )

        desc = client.describe_collection(collection_name)
        assert "text" in [field["name"] for field in desc["fields"]]
        assert "sparse" in [field["name"] for field in desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in desc.get("functions", [])]

        # Step 4: Drop Function deletes bm25_fn and cascades its output field/index.
        self.drop_function_field(client, collection_name, "bm25_fn")

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        function_names = [func["name"] for func in desc.get("functions", [])]
        assert "bm25_fn" not in function_names
        assert "sparse" not in field_names
        assert "text" in field_names
        assert "vec" in field_names
        assert "sparse" not in client.list_indexes(collection_name)
        assert client.describe_index(collection_name, index_name="sparse") is None

        # Step 5: Insert a new row after drop to verify the next add uses the re-created function output.
        client.insert(
            collection_name=collection_name,
            data=[
                {
                    "id": 10,
                    "text": "secondonly inserted document after drop function",
                    "vec": [1.0, 0.0, 0.0, 0.0],
                }
            ],
        )
        client.flush(collection_name)

        queried_rows = client.query(
            collection_name,
            filter="id == 10",
            output_fields=["id", "text"],
        )
        assert queried_rows[0]["text"] == "secondonly inserted document after drop function"

        # Step 6: Re-add the same function/output names and verify the field ID is not reused.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        desc = client.describe_collection(collection_name)
        fields = {field["name"]: field for field in desc["fields"]}
        functions = {func["name"]: func for func in desc.get("functions", [])}
        assert "sparse" in fields
        assert "bm25_fn" in functions
        assert fields["sparse"]["field_id"] > old_sparse_field_id

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=60)
        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        # Step 6: New function output should reflect current text, not stale pre-drop output.
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["secondonly"],
            anns_field="sparse",
            expected_id=10,
            label="BM25 after re-add uses inserted text",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            filter="id == 10",
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_bm25_function_field_alias_path_schema_cache(self):
        """
        TC-12: Alias path.

        target: verify add_function_field schema cache convergence covers both collection name and alias paths
        method: add BM25 field by collection name then use alias; add BM25 field by alias then use collection name
        expected: alias and collection name paths both observe the new schema and can search generated BM25 output
        """
        client = self._client()

        # Step 1: Create collection alias.
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = f"{collection_name}_alias"

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        client.create_alias(collection_name, alias_name)

        client.insert(
            collection_name=collection_name,
            data=[
                {"id": 0, "text": "alpha alias collection name add", "vec": [0.0, 0.0, 0.0, 0.0]},
            ],
        )
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)

        alias_desc = client.describe_collection(alias_name)
        assert "sparse" not in [field["name"] for field in alias_desc["fields"]]
        assert "bm25_fn" not in [func["name"] for func in alias_desc.get("functions", [])]

        # Step 2: Execute add_function_field through collection name.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        # Step 3: Verify alias describe/create_index/load/search observes the new schema.
        alias_desc = client.describe_collection(alias_name)
        assert "sparse" in [field["name"] for field in alias_desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in alias_desc.get("functions", [])]

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(alias_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, alias_name, index_name="sparse", timeout=60)
        client.load_collection(alias_name)

        self.wait_for_search_hit(
            client,
            alias_name,
            data=["alpha"],
            anns_field="sparse",
            expected_id=0,
            label="BM25 alias path after collection-name add",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        client.drop_alias(alias_name)
        self.drop_collection(client, collection_name)

        # Step 4: Reverse path: add_function_field through alias, then use collection name to index/load/search.
        reverse_collection_name = cf.gen_collection_name_by_testcase_name()
        reverse_alias_name = f"{reverse_collection_name}_alias"

        reverse_schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        reverse_schema.add_field("id", DataType.INT64, is_primary=True)
        reverse_schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        reverse_schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=reverse_collection_name,
            schema=reverse_schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        client.create_alias(reverse_collection_name, reverse_alias_name)

        client.insert(
            collection_name=reverse_collection_name,
            data=[
                {"id": 10, "text": "beta alias add collection name search", "vec": [1.0, 0.0, 0.0, 0.0]},
            ],
        )
        client.flush(reverse_collection_name)
        assert self.wait_for_index_ready(client, reverse_collection_name, index_name="vec", timeout=120)

        reverse_alias_desc = client.describe_collection(reverse_alias_name)
        assert "sparse" not in [field["name"] for field in reverse_alias_desc["fields"]]
        assert "bm25_fn" not in [func["name"] for func in reverse_alias_desc.get("functions", [])]

        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(reverse_alias_name, sparse_field, bm25_function)

        reverse_desc = client.describe_collection(reverse_collection_name)
        assert "sparse" in [field["name"] for field in reverse_desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in reverse_desc.get("functions", [])]

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(reverse_collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, reverse_collection_name, index_name="sparse", timeout=60)
        client.load_collection(reverse_collection_name)

        self.wait_for_search_hit(
            client,
            reverse_collection_name,
            data=["beta"],
            anns_field="sparse",
            expected_id=10,
            label="BM25 collection path after alias add",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        client.drop_alias(reverse_alias_name)
        self.drop_collection(client, reverse_collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_after_delete_upsert_history(self):
        """
        TC-13: Delete / Upsert before add_function_field.

        target: verify add_function_field backfill uses the latest visible row state
        method: delete one historical row and upsert another before add_function_field, then search the added BM25 field
        expected: deleted row is not searchable; upserted row uses updated text; old text does not pollute BM25 output
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create collection, insert historical rows, and flush them to sealed segments.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        historical_rows = [
            {"id": 1, "text": "deletedonly alpha should be deleted", "vec": [1.0, 0.0, 0.0, 0.0]},
            {"id": 2, "text": "oldonly beta old text", "vec": [0.0, 1.0, 0.0, 0.0]},
            {"id": 3, "text": "keepgamma gamma keep text", "vec": [0.0, 0.0, 1.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=historical_rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)

        # Step 2: Delete id=1 and upsert id=2 before add_function_field.
        client.delete(collection_name=collection_name, filter="id == 1")
        client.upsert(
            collection_name=collection_name,
            data=[
                {"id": 2, "text": "updatedonly beta updated text", "vec": [0.0, 1.0, 1.0, 0.0]},
            ],
        )

        # Step 3: Flush to persist delete delta and upserted latest row state.
        client.flush(collection_name)

        visible_rows = client.query(
            collection_name,
            filter="id in [1, 2, 3]",
            output_fields=["id", "text"],
        )
        visible_by_id = {row["id"]: row for row in visible_rows}
        assert 1 not in visible_by_id
        assert visible_by_id[2]["text"] == "updatedonly beta updated text"
        assert visible_by_id[3]["text"] == "keepgamma gamma keep text"

        # Step 4: Execute add_function_field after delete/upsert state is persisted.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        # Step 5: Create sparse BM25 index and load the added function output field.
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=60)
        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        # Step 6: Wait for user-visible BM25 search readiness on the latest upserted and kept rows.
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["updatedonly"],
            anns_field="sparse",
            expected_id=2,
            label="BM25 after pre-add upsert uses updated text",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            filter="id == 2",
        )
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["keepgamma"],
            anns_field="sparse",
            expected_id=3,
            label="BM25 after pre-add delete/upsert keeps unchanged row",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            filter="id == 3",
        )

        # Step 7: Deleted historical text must not be backfilled into searchable BM25 output.
        deleted_search = client.search(
            collection_name,
            data=["deletedonly"],
            anns_field="sparse",
            limit=5,
            output_fields=["id", "text"],
            search_params={"metric_type": "BM25"},
            filter="id == 1",
        )
        assert len(deleted_search[0]) == 0

        # Step 8 and Step 9: Upserted row should match updated text, not stale old text.
        stale_search = client.search(
            collection_name,
            data=["oldonly"],
            anns_field="sparse",
            limit=5,
            output_fields=["id", "text"],
            search_params={"metric_type": "BM25"},
            filter="id == 2",
        )
        assert len(stale_search[0]) == 0

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_on_loaded_collection_no_reload(self):
        """
        TC-14: Pre-loaded collection add_function_field does not require extra load.

        target: verify add_function_field on a loaded collection keeps the new BM25 field usable without explicit reload
        method: load collection before add_function_field, create sparse index, then search without calling load_collection again
        expected: collection remains loaded and BM25 search works without user-triggered reload
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create collection, insert historical text rows, and flush.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        historical_rows = [
            {"id": 0, "text": "preloaded alpha document", "vec": [0.0, 0.0, 0.0, 0.0]},
            {"id": 1, "text": "preloaded beta document", "vec": [1.0, 0.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=historical_rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)

        # Step 2: Load collection before add_function_field and verify load state.
        client.load_collection(collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert str(load_state["state"]) == "Loaded"

        # Step 3: Execute add_function_field while collection is already loaded.
        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        desc = client.describe_collection(collection_name)
        assert "sparse" in [field["name"] for field in desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in desc.get("functions", [])]

        # Step 4: Create sparse BM25 index for the added output field.
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=60)

        # Step 5: Do not call load_collection/release_collection here.
        # Step 6 and Step 7: Search directly through the added BM25 field.
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["alpha"],
            anns_field="sparse",
            expected_id=0,
            label="BM25 pre-loaded collection without reload",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            timeout=30,
        )

        # Step 8: Verify collection load state remains loaded after schema evolution and search.
        load_state = self.get_load_state(client, collection_name)[0]
        assert str(load_state["state"]) == "Loaded"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_function_field_matches_existing_bm25_and_minhash_output(self):
        """
        TC-15: BM25 / MinHash old-vs-added function output equivalence.

        target: verify added function output matches an existing function output with the same input and params
        method: create base BM25/MinHash function output at collection creation, add another output later, compare search results
        expected: base and added output fields return the same topK ids, and distances match when SDK exposes them
        """
        client = self._client()

        # BM25 sub-scenario.
        bm25_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_bm25"

        # Step 1: Create collection with initial BM25 function output sparse_base.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("sparse_base", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(
            Function(
                name="bm25_base_fn",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names=["sparse_base"],
            )
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(field_name="sparse_base", index_type="AUTOINDEX", metric_type="BM25")

        client.create_collection(
            collection_name=bm25_collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert distinguishable text rows and flush.
        bm25_rows = [
            {"id": 0, "text": "alpha equivalence base document", "vec": [0.0, 0.0, 0.0, 0.0]},
            {"id": 1, "text": "alpha equivalence nearby document", "vec": [1.0, 0.0, 0.0, 0.0]},
            {"id": 2, "text": "beta equivalence base document", "vec": [0.0, 1.0, 0.0, 0.0]},
            {"id": 3, "text": "gamma unrelated document", "vec": [0.0, 0.0, 1.0, 0.0]},
        ]
        client.insert(collection_name=bm25_collection_name, data=bm25_rows)
        client.flush(bm25_collection_name)
        assert self.wait_for_index_ready(client, bm25_collection_name, index_name="vec", timeout=120)
        assert self.wait_for_index_ready(client, bm25_collection_name, index_name="sparse_base", timeout=120)

        # Step 3: Load base output and record base BM25 search results.
        client.load_collection(bm25_collection_name)
        bm25_queries = ["alpha", "beta"]
        bm25_base_results = {}
        for query_text in bm25_queries:
            search_res = client.search(
                bm25_collection_name,
                data=[query_text],
                anns_field="sparse_base",
                limit=3,
                output_fields=["id", "text"],
                search_params={"metric_type": "BM25"},
            )
            bm25_base_results[query_text] = search_res[0]
            assert len(search_res[0]) > 0

        # Step 5: Add BM25 function output sparse_added with the same input and params.
        sparse_added = FieldSchema(name="sparse_added", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_added_function = Function(
            name="bm25_added_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse_added"],
        )
        client.add_function_field(bm25_collection_name, sparse_added, bm25_added_function)

        # Step 6 and Step 7: Create index for added output and wait for backfill/search convergence.
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse_added", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(bm25_collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, bm25_collection_name, index_name="sparse_added", timeout=60)

        # Step 8 and Step 9: Search added output and compare topK id order and distances when present.
        for query_text in bm25_queries:
            base_res = bm25_base_results[query_text]
            base_ids = [hit["id"] for hit in base_res]
            added_res = []
            added_ids = []
            poll_start = time.time()
            while time.time() - poll_start < 30:
                added_res = client.search(
                    bm25_collection_name,
                    data=[query_text],
                    anns_field="sparse_added",
                    limit=3,
                    output_fields=["id", "text"],
                    search_params={"metric_type": "BM25"},
                )[0]
                added_ids = [hit["id"] for hit in added_res]
                if added_ids == base_ids:
                    break
                time.sleep(1)
            else:
                raise AssertionError(
                    f"BM25 added output did not match base output for {query_text}: "
                    f"base_ids={base_ids}, added_ids={added_ids}"
                )

            for base_hit, added_hit in zip(base_res, added_res):
                if "distance" in base_hit and "distance" in added_hit:
                    assert abs(base_hit["distance"] - added_hit["distance"]) < 1e-6

        # Step 10: Release/load and repeat BM25 comparison.
        self.release_collection(client, bm25_collection_name)
        client.load_collection(bm25_collection_name)

        for query_text in bm25_queries:
            base_res = client.search(
                bm25_collection_name,
                data=[query_text],
                anns_field="sparse_base",
                limit=3,
                output_fields=["id", "text"],
                search_params={"metric_type": "BM25"},
            )[0]
            added_res = client.search(
                bm25_collection_name,
                data=[query_text],
                anns_field="sparse_added",
                limit=3,
                output_fields=["id", "text"],
                search_params={"metric_type": "BM25"},
            )[0]
            assert [hit["id"] for hit in added_res] == [hit["id"] for hit in base_res]

        self.drop_collection(client, bm25_collection_name)

        # MinHash sub-scenario.
        minhash_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_minhash"
        minhash_num_hashes = 16
        minhash_dim = minhash_num_hashes * 32
        minhash_params = {"num_hashes": minhash_num_hashes, "shingle_size": 3}

        # Step 1: Create collection with initial MinHash function output mh_base.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("doc", DataType.VARCHAR, max_length=1024)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("mh_base", DataType.BINARY_VECTOR, dim=minhash_dim)
        schema.add_function(
            Function(
                name="minhash_base_fn",
                function_type=FunctionType.MINHASH,
                input_field_names=["doc"],
                output_field_names=["mh_base"],
                params=minhash_params,
            )
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(
            field_name="mh_base",
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )

        client.create_collection(
            collection_name=minhash_collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # Step 2: Insert distinguishable MinHash text rows and flush.
        minhash_rows = [
            {"id": 10, "doc": "minhash alpha semantic document group zero", "vec": [0.0, 0.0, 0.0, 0.0]},
            {"id": 11, "doc": "minhash alpha semantic document group one", "vec": [1.0, 0.0, 0.0, 0.0]},
            {"id": 12, "doc": "minhash beta semantic document group two", "vec": [0.0, 1.0, 0.0, 0.0]},
            {"id": 13, "doc": "totally unrelated minhash text sample", "vec": [0.0, 0.0, 1.0, 0.0]},
        ]
        client.insert(collection_name=minhash_collection_name, data=minhash_rows)
        client.flush(minhash_collection_name)
        assert self.wait_for_index_ready(client, minhash_collection_name, index_name="vec", timeout=120)
        assert self.wait_for_index_ready(client, minhash_collection_name, index_name="mh_base", timeout=120)

        # Step 3 and Step 4: Load base output and record base MinHash search results.
        client.load_collection(minhash_collection_name)
        minhash_queries = [
            "minhash alpha semantic document group zero",
            "minhash beta semantic document group two",
        ]
        minhash_base_results = {}
        for query_text in minhash_queries:
            search_res = client.search(
                minhash_collection_name,
                data=[query_text],
                anns_field="mh_base",
                limit=3,
                output_fields=["id", "doc"],
                search_params={"metric_type": "MHJACCARD", "params": {}},
            )
            minhash_base_results[query_text] = search_res[0]
            assert len(search_res[0]) > 0

        # Step 5: Add MinHash function output mh_added with the same input and params.
        mh_added = FieldSchema(name="mh_added", dtype=DataType.BINARY_VECTOR, dim=minhash_dim)
        minhash_added_function = Function(
            name="minhash_added_fn",
            function_type=FunctionType.MINHASH,
            input_field_names=["doc"],
            output_field_names=["mh_added"],
            params=minhash_params,
        )
        client.add_function_field(minhash_collection_name, mh_added, minhash_added_function)

        # Step 6 and Step 7: Create index for added MinHash output and wait for backfill/search convergence.
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="mh_added",
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        client.create_index(minhash_collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, minhash_collection_name, index_name="mh_added", timeout=60)

        # Step 8 and Step 9: Search added output and compare topK id order and distances when present.
        for query_text in minhash_queries:
            base_res = minhash_base_results[query_text]
            base_ids = [hit["id"] for hit in base_res]
            added_res = []
            added_ids = []
            poll_start = time.time()
            while time.time() - poll_start < 30:
                added_res = client.search(
                    minhash_collection_name,
                    data=[query_text],
                    anns_field="mh_added",
                    limit=3,
                    output_fields=["id", "doc"],
                    search_params={"metric_type": "MHJACCARD", "params": {}},
                )[0]
                added_ids = [hit["id"] for hit in added_res]
                if added_ids == base_ids:
                    break
                time.sleep(1)
            else:
                raise AssertionError(
                    f"MinHash added output did not match base output for {query_text}: "
                    f"base_ids={base_ids}, added_ids={added_ids}"
                )

            for base_hit, added_hit in zip(base_res, added_res):
                if "distance" in base_hit and "distance" in added_hit:
                    assert abs(base_hit["distance"] - added_hit["distance"]) < 1e-6

        # Step 10: Release/load and repeat MinHash comparison.
        self.release_collection(client, minhash_collection_name)
        client.load_collection(minhash_collection_name)

        for query_text in minhash_queries:
            base_res = client.search(
                minhash_collection_name,
                data=[query_text],
                anns_field="mh_base",
                limit=3,
                output_fields=["id", "doc"],
                search_params={"metric_type": "MHJACCARD", "params": {}},
            )[0]
            added_res = client.search(
                minhash_collection_name,
                data=[query_text],
                anns_field="mh_added",
                limit=3,
                output_fields=["id", "doc"],
                search_params={"metric_type": "MHJACCARD", "params": {}},
            )[0]
            assert [hit["id"] for hit in added_res] == [hit["id"] for hit in base_res]

        self.drop_collection(client, minhash_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_multiple_bm25_function_fields_sequentially(self):
        """
        TC-19: Multiple BM25 functions are added sequentially.

        target: verify multiple add_function_field operations can coexist on one collection
        method: add title->sparse_title first, then body->sparse_body, create indexes and search each field
        expected: function/field ids do not conflict, and BM25 search on each output field does not mix data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create collection with two analyzer-enabled source fields.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("title", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("body", DataType.VARCHAR, max_length=2048, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

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
                "id": 0,
                "title": "titletoken alpha headline",
                "body": "bodytoken apple paragraph",
                "vec": [0.0, 0.0, 0.0, 0.0],
            },
            {
                "id": 1,
                "title": "titletoken beta headline",
                "body": "bodytoken banana paragraph",
                "vec": [1.0, 0.0, 0.0, 0.0],
            },
            {
                "id": 2,
                "title": "other gamma headline",
                "body": "bodytoken cherry paragraph",
                "vec": [0.0, 1.0, 0.0, 0.0],
            },
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        # Step 2: Add title -> sparse_title.
        sparse_title = FieldSchema(name="sparse_title", dtype=DataType.SPARSE_FLOAT_VECTOR)
        title_function = Function(
            name="bm25_title_fn",
            function_type=FunctionType.BM25,
            input_field_names=["title"],
            output_field_names=["sparse_title"],
        )
        client.add_function_field(collection_name, sparse_title, title_function)

        # Step 3: Add body -> sparse_body.
        sparse_body = FieldSchema(name="sparse_body", dtype=DataType.SPARSE_FLOAT_VECTOR)
        body_function = Function(
            name="bm25_body_fn",
            function_type=FunctionType.BM25,
            input_field_names=["body"],
            output_field_names=["sparse_body"],
        )
        client.add_function_field(collection_name, sparse_body, body_function)

        desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in desc["fields"]]
        function_names = [func["name"] for func in desc.get("functions", [])]
        assert "sparse_title" in field_names
        assert "sparse_body" in field_names
        assert "bm25_title_fn" in function_names
        assert "bm25_body_fn" in function_names

        # Step 4: Create indexes for both added BM25 output fields.
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse_title", index_type="AUTOINDEX", metric_type="BM25")
        index_params.add_index(field_name="sparse_body", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse_title", timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse_body", timeout=60)

        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        # Step 5: Search each anns_field and verify the outputs do not mix source fields.
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["alpha"],
            anns_field="sparse_title",
            expected_id=0,
            label="BM25 multiple functions title field",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "title", "body"],
            filter="id == 0",
        )
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["banana"],
            anns_field="sparse_body",
            expected_id=1,
            label="BM25 multiple functions body field",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "title", "body"],
            filter="id == 1",
        )

        title_term_on_body = client.search(
            collection_name,
            data=["alpha"],
            anns_field="sparse_body",
            limit=5,
            output_fields=["id", "title", "body"],
            search_params={"metric_type": "BM25"},
            filter="id == 0",
        )
        assert len(title_term_on_body[0]) == 0

        body_term_on_title = client.search(
            collection_name,
            data=["banana"],
            anns_field="sparse_title",
            limit=5,
            output_fields=["id", "title", "body"],
            search_params={"metric_type": "BM25"},
            filter="id == 1",
        )
        assert len(body_term_on_title[0]) == 0

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_respects_analyzer_params(self):
        """
        TC-20: Analyzer parameter basic matrix.

        target: verify BM25 added by add_function_field uses the source field analyzer params
        method: add BM25 fields on identical text fields with different analyzer filters, then compare search behavior
        expected: analyzer params remain unchanged and BM25 output follows each source analyzer
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        standard_analyzer_params = {"tokenizer": "standard"}
        filtered_analyzer_params = {
            "tokenizer": "standard",
            "filter": [
                {
                    "type": "length",
                    "max": 4,
                }
            ],
        }

        def analyzer_params_from_desc(desc, field_name):
            field = next(field for field in desc["fields"] if field["name"] == field_name)
            return json.loads(field["params"]["analyzer_params"])

        # Step 1: Create two text fields with identical data but different analyzer params.
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(
            "standard_text",
            DataType.VARCHAR,
            max_length=1024,
            enable_analyzer=True,
            analyzer_params=standard_analyzer_params,
        )
        schema.add_field(
            "filtered_text",
            DataType.VARCHAR,
            max_length=1024,
            enable_analyzer=True,
            analyzer_params=filtered_analyzer_params,
        )
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        before_add_desc = client.describe_collection(collection_name)
        assert analyzer_params_from_desc(before_add_desc, "standard_text") == standard_analyzer_params
        assert analyzer_params_from_desc(before_add_desc, "filtered_text") == filtered_analyzer_params

        rows = [
            {
                "id": 0,
                "standard_text": "longtoken tiny",
                "filtered_text": "longtoken tiny",
                "vec": [0.0, 0.0, 0.0, 0.0],
            },
            {
                "id": 1,
                "standard_text": "another mediumword mini",
                "filtered_text": "another mediumword mini",
                "vec": [1.0, 0.0, 0.0, 0.0],
            },
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        # Step 2: Add BM25 function fields for both source fields.
        sparse_standard = FieldSchema(name="sparse_standard", dtype=DataType.SPARSE_FLOAT_VECTOR)
        standard_function = Function(
            name="bm25_standard_fn",
            function_type=FunctionType.BM25,
            input_field_names=["standard_text"],
            output_field_names=["sparse_standard"],
        )
        client.add_function_field(collection_name, sparse_standard, standard_function)

        sparse_filtered = FieldSchema(name="sparse_filtered", dtype=DataType.SPARSE_FLOAT_VECTOR)
        filtered_function = Function(
            name="bm25_filtered_fn",
            function_type=FunctionType.BM25,
            input_field_names=["filtered_text"],
            output_field_names=["sparse_filtered"],
        )
        client.add_function_field(collection_name, sparse_filtered, filtered_function)

        after_add_desc = client.describe_collection(collection_name)
        field_names = [field["name"] for field in after_add_desc["fields"]]
        function_names = [func["name"] for func in after_add_desc.get("functions", [])]
        assert "sparse_standard" in field_names
        assert "sparse_filtered" in field_names
        assert "bm25_standard_fn" in function_names
        assert "bm25_filtered_fn" in function_names
        assert analyzer_params_from_desc(after_add_desc, "standard_text") == standard_analyzer_params
        assert analyzer_params_from_desc(after_add_desc, "filtered_text") == filtered_analyzer_params

        # Step 3: Create indexes and verify analyzer-specific BM25 behavior.
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse_standard", index_type="AUTOINDEX", metric_type="BM25")
        index_params.add_index(field_name="sparse_filtered", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse_standard", timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse_filtered", timeout=60)

        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["longtoken"],
            anns_field="sparse_standard",
            expected_id=0,
            label="BM25 standard analyzer keeps long token",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "standard_text", "filtered_text"],
            filter="id == 0",
        )
        filtered_long_token = client.search(
            collection_name,
            data=["longtoken"],
            anns_field="sparse_filtered",
            limit=5,
            output_fields=["id", "standard_text", "filtered_text"],
            search_params={"metric_type": "BM25"},
            filter="id == 0",
        )
        assert len(filtered_long_token[0]) == 0

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["tiny"],
            anns_field="sparse_standard",
            expected_id=0,
            label="BM25 standard analyzer keeps short token",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "standard_text", "filtered_text"],
            filter="id == 0",
        )
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["tiny"],
            anns_field="sparse_filtered",
            expected_id=0,
            label="BM25 filtered analyzer keeps short token",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "standard_text", "filtered_text"],
            filter="id == 0",
        )

        final_desc = client.describe_collection(collection_name)
        assert analyzer_params_from_desc(final_desc, "standard_text") == standard_analyzer_params
        assert analyzer_params_from_desc(final_desc, "filtered_text") == filtered_analyzer_params

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_added_bm25_function_field_in_hybrid_search(self):
        """
        TC-21: Added BM25 function field participates in hybrid search.

        target: verify add_function_field BM25 output can be used as one hybrid search request
        method: combine dense AnnSearchRequest and added BM25 AnnSearchRequest with WeightedRanker and RRFRanker
        expected: old sealed, post-add growing, and post-flush/reload rows are returned by hybrid search
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        def assert_hybrid_hit(query_vector, query_text, expected_id, dense_expected_id, ranker, label):
            dense_only = client.search(
                collection_name,
                data=[query_vector],
                anns_field="vec",
                limit=1,
                output_fields=["id"],
                search_params={"metric_type": "L2"},
            )
            dense_only_ids = [hit["id"] for hit in dense_only[0]]
            assert dense_only_ids == [dense_expected_id], (
                f"{label} dense-only expected {dense_expected_id}, got {dense_only_ids}"
            )
            assert expected_id not in dense_only_ids, f"{label} dense-only unexpectedly hit {expected_id}"

            bm25_only = client.search(
                collection_name,
                data=[query_text],
                anns_field="sparse",
                limit=1,
                output_fields=["id", "text"],
                search_params={"metric_type": "BM25"},
            )
            bm25_only_ids = [hit["id"] for hit in bm25_only[0]]
            assert bm25_only_ids == [expected_id], f"{label} BM25-only expected {expected_id}, got {bm25_only_ids}"

            dense_req = AnnSearchRequest(
                data=[query_vector],
                anns_field="vec",
                param={"metric_type": "L2", "params": {}},
                limit=1,
            )
            bm25_req = AnnSearchRequest(
                data=[query_text],
                anns_field="sparse",
                param={"metric_type": "BM25", "params": {}},
                limit=1,
            )
            res = client.hybrid_search(
                collection_name,
                reqs=[dense_req, bm25_req],
                ranker=ranker,
                limit=2,
                output_fields=["id", "text"],
            )
            hit_ids = [hit["id"] for hit in res[0]]
            assert expected_id in hit_ids, f"{label} expected BM25 id {expected_id}, got {hit_ids}"
            assert dense_expected_id in hit_ids, f"{label} expected dense id {dense_expected_id}, got {hit_ids}"
            return res

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        old_rows = [
            {"id": 0, "text": "hybridold lexical anchor", "vec": [100.0, 0.0, 0.0, 0.0]},
            {"id": 1, "text": "dense old distractor", "vec": [0.0, 0.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=old_rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_hybrid_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=120)

        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        assert_hybrid_hit(
            [0.0, 0.0, 0.0, 0.0],
            "hybridold",
            0,
            1,
            WeightedRanker(0.5, 0.5),
            "weighted old sealed hybrid",
        )
        assert_hybrid_hit(
            [0.0, 0.0, 0.0, 0.0],
            "hybridold",
            0,
            1,
            RRFRanker(),
            "rrf old sealed hybrid",
        )

        new_rows = [
            {"id": 100, "text": "hybridnew lexical anchor", "vec": [200.0, 0.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=new_rows)

        assert_hybrid_hit(
            [0.0, 0.0, 0.0, 0.0],
            "hybridnew",
            100,
            1,
            WeightedRanker(0.5, 0.5),
            "weighted new growing hybrid",
        )

        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=120)
        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        assert_hybrid_hit(
            [0.0, 0.0, 0.0, 0.0],
            "hybridnew",
            100,
            1,
            WeightedRanker(0.5, 0.5),
            "weighted new sealed hybrid after reload",
        )
        assert_hybrid_hit(
            [0.0, 0.0, 0.0, 0.0],
            "hybridnew",
            100,
            1,
            RRFRanker(),
            "rrf new sealed hybrid after reload",
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_output_fields_query_semantics(self):
        """
        TC-22: output_fields and query semantics for added BM25 function output.

        target: verify ordinary fields remain retrievable and raw BM25 output is not retrievable
        method: add BM25 function field, search normally, query with "*", then request sparse raw output explicitly
        expected: ordinary fields are returned; raw sparse output requests fail with clear error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        rows = [
            {"id": 0, "text": "tc22 visible lexical document", "vec": [0.0, 0.0, 0.0, 0.0]},
            {"id": 1, "text": "tc22 unrelated document", "vec": [1.0, 0.0, 0.0, 0.0]},
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_output_fields_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=120)

        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["visible"],
            anns_field="sparse",
            expected_id=0,
            label="BM25 output fields baseline search",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
        )

        query_star = client.query(
            collection_name,
            filter="id == 0",
            output_fields=["*"],
        )
        assert len(query_star) == 1
        assert query_star[0]["id"] == 0
        assert query_star[0]["text"] == "tc22 visible lexical document"
        assert "vec" in query_star[0]
        assert "sparse" not in query_star[0]

        raw_sparse_error = {
            ct.err_code: 1100,
            ct.err_msg: "not allowed to retrieve raw data of field sparse: invalid parameter",
        }

        self.query(
            client,
            collection_name,
            filter="id == 0",
            output_fields=["sparse"],
            check_task=CheckTasks.err_res,
            check_items=raw_sparse_error,
        )

        self.query(
            client,
            collection_name,
            filter="id == 0",
            output_fields=["id", "text", "sparse"],
            check_task=CheckTasks.err_res,
            check_items=raw_sparse_error,
        )

        self.search(
            client,
            collection_name,
            data=["visible"],
            anns_field="sparse",
            limit=5,
            output_fields=["id", "text", "sparse"],
            search_params={"metric_type": "BM25"},
            check_task=CheckTasks.err_res,
            check_items=raw_sparse_error,
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_bm25_function_field_with_multi_analyzer_by_field(self):
        """
        TC-24: Multi-analyzer / by_field BM25 input.

        target: verify add_function_field keeps the input field multi-analyzer by_field behavior
        method: add BM25 output on a VARCHAR field with multi_analyzer_params and search by analyzer_name
        expected: rows are tokenized by the analyzer selected from the language field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        multi_analyzer_params = {
            "by_field": "language",
            "analyzers": {
                "en": {"type": "english"},
                "default": {
                    "tokenizer": "standard",
                    "filter": [{"type": "length", "max": 4}],
                },
            },
            "alias": {"eng": "en"},
        }

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("language", DataType.VARCHAR, max_length=16)
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=1024,
            enable_analyzer=True,
            multi_analyzer_params=multi_analyzer_params,
        )
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

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
                "id": 0,
                "language": "en",
                "text": "running tc24englishanchor",
                "vec": [0.0, 0.0, 0.0, 0.0],
            },
            {
                "id": 1,
                "language": "eng",
                "text": "running tc24aliasanchor",
                "vec": [1.0, 0.0, 0.0, 0.0],
            },
            {
                "id": 2,
                "language": "unknown",
                "text": "tiny tc24defaultlonganchor",
                "vec": [2.0, 0.0, 0.0, 0.0],
            },
        ]
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        before_add_desc = client.describe_collection(collection_name)
        text_field_before = next(field for field in before_add_desc["fields"] if field["name"] == "text")
        assert json.loads(text_field_before["params"]["multi_analyzer_params"]) == multi_analyzer_params

        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_multi_analyzer_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        after_add_desc = client.describe_collection(collection_name)
        text_field_after = next(field for field in after_add_desc["fields"] if field["name"] == "text")
        assert json.loads(text_field_after["params"]["multi_analyzer_params"]) == multi_analyzer_params

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=120)

        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["running"],
            anns_field="sparse",
            expected_id=0,
            label="multi-analyzer english by_field",
            search_params={"metric_type": "BM25", "analyzer_name": "en"},
            output_fields=["id", "language", "text"],
            filter='language == "en"',
        )
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["running"],
            anns_field="sparse",
            expected_id=1,
            label="multi-analyzer alias by_field",
            search_params={"metric_type": "BM25", "analyzer_name": "en"},
            output_fields=["id", "language", "text"],
            filter='language == "eng"',
        )
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["tiny"],
            anns_field="sparse",
            expected_id=2,
            label="multi-analyzer default by_field short token",
            search_params={"metric_type": "BM25", "analyzer_name": "default"},
            output_fields=["id", "language", "text"],
            filter='language == "unknown"',
        )

        filtered_long_res = client.search(
            collection_name=collection_name,
            data=["tc24defaultlonganchor"],
            anns_field="sparse",
            limit=5,
            output_fields=["id", "language", "text"],
            search_params={"metric_type": "BM25", "analyzer_name": "default"},
            filter='language == "unknown"',
        )
        assert [hit["id"] for hit in filtered_long_res[0]] == []

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_function_field_function_type_support_matrix(self):
        """
        TC-26: FunctionType support matrix.

        target: verify add_function_field has a clear support contract for each FunctionType
        method: add supported BM25/MINHASH function fields, then reject unsupported TEXTEMBEDDING/RERANK/UNKNOWN
        expected: supported types mutate schema; unsupported types fail clearly without partial schema mutation
        """
        client = self._client()

        # Step 1: BM25 is accepted by add_function_field and appears in schema.
        bm25_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_bm25"
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        client.create_collection(
            collection_name=bm25_collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        bm25_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_type_matrix_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(bm25_collection_name, bm25_field, bm25_function)

        bm25_desc = client.describe_collection(bm25_collection_name)
        assert "sparse" in [field["name"] for field in bm25_desc["fields"]]
        assert ["bm25_type_matrix_fn"] == [func["name"] for func in bm25_desc.get("functions", [])]
        self.drop_collection(client, bm25_collection_name)

        # Step 2: MINHASH is accepted by add_function_field and appears in schema.
        minhash_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_minhash"
        minhash_num_hashes = 16
        minhash_dim = minhash_num_hashes * 32
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("doc", DataType.VARCHAR, max_length=1024)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        client.create_collection(
            collection_name=minhash_collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        minhash_field = FieldSchema(name="mh", dtype=DataType.BINARY_VECTOR, dim=minhash_dim)
        minhash_function = Function(
            name="minhash_type_matrix_fn",
            function_type=FunctionType.MINHASH,
            input_field_names=["doc"],
            output_field_names=["mh"],
            params={"num_hashes": minhash_num_hashes, "shingle_size": 3},
        )
        client.add_function_field(minhash_collection_name, minhash_field, minhash_function)

        minhash_desc = client.describe_collection(minhash_collection_name)
        assert "mh" in [field["name"] for field in minhash_desc["fields"]]
        assert ["minhash_type_matrix_fn"] == [func["name"] for func in minhash_desc.get("functions", [])]
        self.drop_collection(client, minhash_collection_name)

        # Step 3: Unsupported FunctionType values are rejected before any schema mutation.
        unsupported_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_unsupported"
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        client.create_collection(
            collection_name=unsupported_collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        before_desc = client.describe_collection(unsupported_collection_name)
        before_schema_version = before_desc["schema_version"]
        before_field_names = [field["name"] for field in before_desc["fields"]]
        before_function_names = [func["name"] for func in before_desc.get("functions", [])]
        before_max_field_id = before_desc["properties"]["max_field_id"]

        unsupported_cases = [
            (
                "TEXTEMBEDDING",
                FieldSchema(name="te_vec", dtype=DataType.FLOAT_VECTOR, dim=4),
                Function(
                    name="te_type_matrix_fn",
                    function_type=FunctionType.TEXTEMBEDDING,
                    input_field_names=["text"],
                    output_field_names=["te_vec"],
                    params={"provider": "tei", "model_name": "BAAI/bge-small-en-v1.5"},
                ),
                2,
            ),
            (
                "RERANK",
                FieldSchema(name="rerank_out", dtype=DataType.FLOAT_VECTOR, dim=4),
                Function(
                    name="rerank_type_matrix_fn",
                    function_type=FunctionType.RERANK,
                    input_field_names=["text"],
                    output_field_names=["rerank_out"],
                ),
                3,
            ),
            (
                "UNKNOWN",
                FieldSchema(name="unknown_out", dtype=DataType.SPARSE_FLOAT_VECTOR),
                Function(
                    name="unknown_type_matrix_fn",
                    function_type=FunctionType.UNKNOWN,
                    input_field_names=["text"],
                    output_field_names=["unknown_out"],
                ),
                0,
            ),
        ]

        for case_name, field_schema, function, function_type_value in unsupported_cases:
            self.add_function_field(
                client,
                unsupported_collection_name,
                field_schema,
                function,
                check_task=CheckTasks.err_res,
                check_items={
                    ct.err_code: 1,
                    ct.err_msg: (
                        "add_function_field only supports FunctionType.BM25 with SPARSE_FLOAT_VECTOR "
                        "or FunctionType.MINHASH with BINARY_VECTOR for now, got "
                        f"{function_type_value}"
                    ),
                },
            )

            after_desc = client.describe_collection(unsupported_collection_name)
            assert after_desc["schema_version"] == before_schema_version, case_name
            assert [field["name"] for field in after_desc["fields"]] == before_field_names, case_name
            assert [func["name"] for func in after_desc.get("functions", [])] == before_function_names, case_name
            assert after_desc["properties"]["max_field_id"] == before_max_field_id, case_name

        self.drop_collection(client, unsupported_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_minhash_function_field_params_and_backfill(self):
        """
        TC-28: MinHash parameter boundary and backfill convergence.

        target: verify dynamic MinHash add_function_field with non-default params and clear parameter failures
        method: add MinHash output with num_hashes=32/shingle_size=5, search old/new rows, then verify invalid params fail
        expected: legal params are searchable across flush/reload; invalid params fail without partial schema mutation
        """
        client = self._client()

        def assert_minhash_hit(collection_name, query_text, expected_id, label, timeout=60):
            poll_start = time.time()
            last_hit_ids = []
            while time.time() - poll_start < timeout:
                search_res = client.search(
                    collection_name,
                    data=[query_text],
                    anns_field="mh",
                    limit=5,
                    output_fields=["id", "doc"],
                    search_params={"metric_type": "MHJACCARD", "params": {}},
                )
                last_hit_ids = [hit["id"] for hit in search_res[0]]
                if expected_id in last_hit_ids:
                    return search_res
                time.sleep(1)
            raise AssertionError(f"{label} expected id {expected_id}, got {last_hit_ids}")

        # Positive path: num_hashes=32, shingle_size=5, mh_lsh_band=8.
        collection_name = cf.gen_collection_name_by_testcase_name()
        num_hashes = 32
        dim = num_hashes * 32

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("doc", DataType.VARCHAR, max_length=2048)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        old_rows = [
            {
                "id": 0,
                "doc": "tc28 alpha cluster document historical zero shared phrase",
                "vec": [0.0, 0.0, 0.0, 0.0],
            },
            {
                "id": 1,
                "doc": "tc28 alpha cluster document historical one shared phrase",
                "vec": [0.1, 0.0, 0.0, 0.0],
            },
            {
                "id": 2,
                "doc": "tc28 beta cluster document historical zero separate phrase",
                "vec": [1.0, 0.0, 0.0, 0.0],
            },
            {
                "id": 3,
                "doc": "tc28 gamma unrelated historical baseline text",
                "vec": [2.0, 0.0, 0.0, 0.0],
            },
        ]
        client.insert(collection_name=collection_name, data=old_rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=120)
        client.load_collection(collection_name)

        mh_field = FieldSchema(name="mh", dtype=DataType.BINARY_VECTOR, dim=dim)
        mh_function = Function(
            name="minhash_params_backfill_fn",
            function_type=FunctionType.MINHASH,
            input_field_names=["doc"],
            output_field_names=["mh"],
            params={"num_hashes": num_hashes, "shingle_size": 5},
        )
        client.add_function_field(collection_name, mh_field, mh_function)

        desc = client.describe_collection(collection_name)
        assert "mh" in [field["name"] for field in desc["fields"]]
        assert "minhash_params_backfill_fn" in [func["name"] for func in desc.get("functions", [])]

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="mh",
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        client.create_index(collection_name, index_params, timeout=60)
        assert self.wait_for_index_ready(client, collection_name, index_name="mh", timeout=120)

        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        assert_minhash_hit(
            collection_name,
            "tc28 alpha cluster document historical zero shared phrase",
            expected_id=0,
            label="MinHash old sealed backfill",
        )

        new_rows = [
            {
                "id": 100,
                "doc": "tc28 alpha cluster document growing new shared phrase",
                "vec": [0.2, 0.0, 0.0, 0.0],
            },
            {
                "id": 101,
                "doc": "tc28 beta cluster document growing new separate phrase",
                "vec": [1.1, 0.0, 0.0, 0.0],
            },
        ]
        client.insert(collection_name=collection_name, data=new_rows)

        assert_minhash_hit(
            collection_name,
            "tc28 alpha cluster document growing new shared phrase",
            expected_id=100,
            label="MinHash post-add growing",
        )

        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="mh", timeout=120)

        self.release_collection(client, collection_name)
        client.load_collection(collection_name)

        assert_minhash_hit(
            collection_name,
            "tc28 beta cluster document growing new separate phrase",
            expected_id=101,
            label="MinHash post-add sealed after reload",
        )

        self.drop_collection(client, collection_name)

        # Negative path: output dim must equal num_hashes * 32.
        mismatch_collection_name = f"{cf.gen_collection_name_by_testcase_name()}_dim_mismatch"
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("doc", DataType.VARCHAR, max_length=1024)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        client.create_collection(
            collection_name=mismatch_collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        before_desc = client.describe_collection(mismatch_collection_name)
        mismatch_field = FieldSchema(name="mh", dtype=DataType.BINARY_VECTOR, dim=256)
        mismatch_function = Function(
            name="minhash_dim_mismatch_fn",
            function_type=FunctionType.MINHASH,
            input_field_names=["doc"],
            output_field_names=["mh"],
            params={"num_hashes": 16, "shingle_size": 3},
        )
        self.add_function_field(
            client,
            mismatch_collection_name,
            mismatch_field,
            mismatch_function,
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: (
                    "minhash function output field 'mh' dim 256 does not match expected dim 512 "
                    "(numHashes 16 * one minhash signature size of 32bit): invalid parameter"
                ),
            },
        )
        after_desc = client.describe_collection(mismatch_collection_name)
        assert [field["name"] for field in after_desc["fields"]] == [field["name"] for field in before_desc["fields"]]
        assert after_desc.get("functions", []) == before_desc.get("functions", [])

        # Negative path: shingle_size must be positive.
        bad_shingle_field = FieldSchema(name="mh_bad_shingle", dtype=DataType.BINARY_VECTOR, dim=512)
        bad_shingle_function = Function(
            name="minhash_bad_shingle_fn",
            function_type=FunctionType.MINHASH,
            input_field_names=["doc"],
            output_field_names=["mh_bad_shingle"],
            params={"num_hashes": 16, "shingle_size": 0},
        )
        self.add_function_field(
            client,
            mismatch_collection_name,
            bad_shingle_field,
            bad_shingle_function,
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "param shingle_size:0 must be positive: invalid parameter",
            },
        )
        final_desc = client.describe_collection(mismatch_collection_name)
        assert [field["name"] for field in final_desc["fields"]] == [field["name"] for field in before_desc["fields"]]
        assert final_desc.get("functions", []) == before_desc.get("functions", [])

        self.drop_collection(client, mismatch_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_bm25_function_field_large_segment_true_index_build(self):
        """
        TC-36: Large sealed segment triggers real sparse index build.

        target: verify added BM25 output field can build sparse index on a segment above index build threshold
        method: insert 3000 sealed rows, add BM25 function field, create sparse index, then search old sealed data
        expected: sparse index finishes for all 3000 rows and old sealed rows are searchable after load/reload
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        row_count = 3000
        batch_size = 1000

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        for start in range(0, row_count, batch_size):
            rows = []
            for i in range(start, min(start + batch_size, row_count)):
                rows.append(
                    {
                        "id": i,
                        "text": f"tc36 large sealed true index token_{i} document",
                        "vec": [
                            float(i % 17) / 17.0,
                            float(i % 23) / 23.0,
                            float(i % 29) / 29.0,
                            float(i % 31) / 31.0,
                        ],
                    }
                )
            client.insert(collection_name=collection_name, data=rows)

        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=180)
        client.load_collection(collection_name)

        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        sparse_index = client.prepare_index_params()
        sparse_index.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, sparse_index, timeout=180)

        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=240)
        sparse_index_info = client.describe_index(collection_name, index_name="sparse")
        assert sparse_index_info["total_rows"] >= row_count
        assert sparse_index_info["indexed_rows"] >= row_count
        assert sparse_index_info["pending_index_rows"] == 0

        client.load_collection(collection_name)
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["token_2048"],
            anns_field="sparse",
            expected_id=2048,
            label="BM25 large sealed true index",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            timeout=60,
        )

        self.release_collection(client, collection_name)
        client.load_collection(collection_name)
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["token_2999"],
            anns_field="sparse",
            expected_id=2999,
            label="BM25 large sealed true index after release/load",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            timeout=60,
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_bm25_function_field_snapshot_restore(self):
        """
        TC-29: Snapshot restore.

        target: verify snapshot restore preserves added BM25 function field schema and search semantics
        method: add BM25 function field, create snapshot, restore to a new collection, then verify schema/index/search
        expected: restored collection has added field/function and BM25 search works for old and post-add sealed rows
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        restored_collection_name = f"{collection_name}_restored"
        snapshot_name = cf.gen_unique_str("tc29_snapshot")
        old_row_count = 1500
        batch_size = 500

        def make_vec(seed):
            return [
                float(seed % 17) / 17.0,
                float(seed % 23) / 23.0,
                float(seed % 29) / 29.0,
                float(seed % 31) / 31.0,
            ]

        def wait_for_restore_complete(job_id, timeout=180):
            start_time = time.time()
            while time.time() - start_time < timeout:
                state, _ = self.get_restore_snapshot_state(client, job_id)
                if state.state == "RestoreSnapshotCompleted":
                    return state
                if state.state == "RestoreSnapshotFailed":
                    raise AssertionError(f"restore snapshot failed: {state.reason}")
                time.sleep(1)
            raise AssertionError(f"restore snapshot job {job_id} did not complete within {timeout}s")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=1024, enable_analyzer=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        for start in range(0, old_row_count, batch_size):
            rows = []
            for i in range(start, min(start + batch_size, old_row_count)):
                rows.append(
                    {
                        "id": i,
                        "text": f"tc29snapold{i} historical snapshot restore document",
                        "vec": make_vec(i),
                    }
                )
            client.insert(collection_name=collection_name, data=rows)

        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="vec", timeout=180)
        client.load_collection(collection_name)

        sparse_field = FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name="bm25_fn",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse"],
        )
        client.add_function_field(collection_name, sparse_field, bm25_function)

        sparse_index = client.prepare_index_params()
        sparse_index.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
        client.create_index(collection_name, sparse_index, timeout=180)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=240)

        post_add_rows = [
            {"id": 2000, "text": "tc29snappost2000 post add sealed snapshot document", "vec": make_vec(2000)},
            {"id": 2001, "text": "tc29snappost2001 post add sealed snapshot document", "vec": make_vec(2001)},
        ]
        client.insert(collection_name=collection_name, data=post_add_rows)
        client.flush(collection_name)
        assert self.wait_for_index_ready(client, collection_name, index_name="sparse", timeout=180)

        self.wait_for_search_hit(
            client,
            collection_name,
            data=["tc29snapold1200"],
            anns_field="sparse",
            expected_id=1200,
            label="BM25 snapshot source old sealed",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            timeout=60,
        )
        self.wait_for_search_hit(
            client,
            collection_name,
            data=["tc29snappost2000"],
            anns_field="sparse",
            expected_id=2000,
            label="BM25 snapshot source post-add sealed",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            timeout=60,
        )

        self.create_snapshot(
            client,
            snapshot_name,
            collection_name,
            description="TC-29 add function field snapshot restore",
        )

        job_id, _ = self.restore_snapshot(client, snapshot_name, collection_name, restored_collection_name)
        assert job_id > 0
        wait_for_restore_complete(job_id)

        restored_desc = client.describe_collection(restored_collection_name)
        assert "sparse" in [field["name"] for field in restored_desc["fields"]]
        assert "bm25_fn" in [func["name"] for func in restored_desc.get("functions", [])]

        restored_indexes = set(client.list_indexes(restored_collection_name))
        if "vec" not in restored_indexes:
            dense_index = client.prepare_index_params()
            dense_index.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
            client.create_index(restored_collection_name, dense_index, timeout=180)
        assert self.wait_for_index_ready(client, restored_collection_name, index_name="vec", timeout=180)

        if "sparse" not in restored_indexes:
            restored_sparse_index = client.prepare_index_params()
            restored_sparse_index.add_index(field_name="sparse", index_type="AUTOINDEX", metric_type="BM25")
            client.create_index(restored_collection_name, restored_sparse_index, timeout=180)
        assert self.wait_for_index_ready(client, restored_collection_name, index_name="sparse", timeout=240)

        client.load_collection(restored_collection_name)
        self.wait_for_search_hit(
            client,
            restored_collection_name,
            data=["tc29snapold1200"],
            anns_field="sparse",
            expected_id=1200,
            label="BM25 snapshot restored old sealed",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            timeout=60,
        )
        self.wait_for_search_hit(
            client,
            restored_collection_name,
            data=["tc29snappost2000"],
            anns_field="sparse",
            expected_id=2000,
            label="BM25 snapshot restored post-add sealed",
            search_params={"metric_type": "BM25"},
            output_fields=["id", "text"],
            timeout=60,
        )

        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)
        self.drop_collection(client, collection_name)
