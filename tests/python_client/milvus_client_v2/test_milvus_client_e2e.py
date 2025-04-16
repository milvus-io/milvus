import pytest
import numpy as np
import time
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from utils.util_pymilvus import *
from base.client_v2_base import TestMilvusClientV2Base
from pymilvus import DataType, FieldSchema, CollectionSchema

# Test parameters
default_dim = ct.default_dim
default_nb = ct.default_nb
default_nq = ct.default_nq
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name


class TestMilvusClientE2E(TestMilvusClientV2Base):
    """ Test case of end-to-end interface """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("flush_enable", [True, False])
    @pytest.mark.parametrize("scalar_index_enable", [True, False])
    def test_milvus_client_e2e_default(self, flush_enable, scalar_index_enable):
        """
        target: test high level api: client.create_collection, insert, search, query
        method: create connection, collection, insert and search with:
               1. flush enabled/disabled
               2. scalar index enabled/disabled
        expected: search/query successfully
        """
        client = self._client()
        
        # 1. Create collection with custom schema
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        # Primary key and vector field
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=default_dim)
        # Boolean type
        schema.add_field("bool_field", DataType.BOOL, nullable=True)
        # Integer types
        schema.add_field("int8_field", DataType.INT8, nullable=True)
        schema.add_field("int16_field", DataType.INT16, nullable=True)
        schema.add_field("int32_field", DataType.INT32, nullable=True)
        schema.add_field("int64_field", DataType.INT64, nullable=True)
        # Float types
        schema.add_field("float_field", DataType.FLOAT, nullable=True)
        schema.add_field("double_field", DataType.DOUBLE, nullable=True)
        # String type
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=65535, nullable=True)
        # JSON type
        schema.add_field("json_field", DataType.JSON, nullable=True)
        # Array type
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12, nullable=True)

        # Create collection
        self.create_collection(client, collection_name, schema=schema)

        # 2. Insert data with null values for nullable fields
        num_inserts = 5  # insert data for 5 times
        total_rows = []
        for batch in range(num_inserts):
            vectors = cf.gen_vectors(default_nb, default_dim)
            rows = []
            start_id = batch * default_nb  # ensure id is not duplicated

            for i in range(default_nb):
                row = {
                    "id": start_id + i,  # ensure id is not duplicated
                    "embeddings": list(vectors[i])
                }

                # Add nullable fields with null values for every 5th record
                if i % 5 == 0:
                    row.update({
                        "bool_field": None,
                        "int8_field": None,
                        "int16_field": None,
                        "int32_field": None,
                        "int64_field": None,
                        "float_field": None,
                        "double_field": None,
                        "varchar_field": None,
                        "json_field": None,
                        "array_field": None
                    })
                else:
                    row.update({
                        "bool_field": i % 2 == 0,
                        "int8_field": i % 128,
                        "int16_field": i % 32768,
                        "int32_field": i,
                        "int64_field": i,
                        "float_field": float(i),
                        "double_field": float(i) * 1.0,
                        "varchar_field": f"varchar_{start_id + i}",
                        "json_field": {"id": start_id + i, "value": f"json_{start_id + i}"},
                        "array_field": [i, i + 1, i + 2]
                    })
                rows.append(row)
                total_rows.append(row)

            t0 = time.time()
            self.insert(client, collection_name, rows)
            t1 = time.time()
            time.sleep(0.5)
            log.info(f"Insert batch {batch + 1}: {default_nb} entities cost {t1 - t0:.4f} seconds")

        log.info(f"Total inserted {num_inserts * default_nb} entities")

        if flush_enable:
            self.flush(client, collection_name)
            log.info("Flush enabled: executing flush operation")
        else:
            log.info("Flush disabled: skipping flush operation")

        # Create index parameters
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("embeddings", metric_type="COSINE")

        # Add autoindex for scalar fields if enabled
        if scalar_index_enable:
            index_params.add_index(field_name="int8_field", index_type="AUTOINDEX")
            index_params.add_index(field_name="int16_field", index_type="AUTOINDEX")
            index_params.add_index(field_name="int32_field", index_type="AUTOINDEX")
            index_params.add_index(field_name="int64_field", index_type="AUTOINDEX")
            index_params.add_index(field_name="float_field", index_type="AUTOINDEX")
            index_params.add_index(field_name="double_field", index_type="AUTOINDEX")
            index_params.add_index(field_name="varchar_field", index_type="AUTOINDEX")
            index_params.add_index(field_name="array_field", index_type="AUTOINDEX")

        # 3. create index
        self.create_index(client, collection_name, index_params)

        # Verify scalar indexes are created if enabled
        indexes = self.list_indexes(client, collection_name)[0]
        log.info(f"Created indexes: {indexes}")
        expected_scalar_indexes = ["int8_field", "int16_field", "int32_field", "int64_field",
                                   "float_field", "double_field", "varchar_field", "array_field"]
        if scalar_index_enable:
            for field in expected_scalar_indexes:
                assert field in indexes, f"Scalar index not created for field: {field}"
        else:
            for field in expected_scalar_indexes:
                assert field not in indexes, f"Scalar index should not be created for field: {field}"

        # 4. Load collection
        t0 = time.time()
        self.load_collection(client, collection_name)
        t1 = time.time()
        log.info(f"Load collection cost {t1 - t0:.4f} seconds")
        
        # 4. Search
        t0 = time.time()
        vectors_to_search = cf.gen_vectors(1, default_dim)
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 100}}
        search_res, _ = self.search(
            client, 
            collection_name,
            vectors_to_search,
            anns_field="embeddings",
            search_params=search_params,
            limit=default_limit,
            output_fields=['*'],
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                "nq": len(vectors_to_search),
                "limit": default_limit
            }
        )
        t1 = time.time()
        log.info(f"Search cost {t1 - t0:.4f} seconds")
        
        # 5. Query with filters on each scalar field
        t0 = time.time()

        # Query on boolean field
        bool_filter = "bool_field == true"
        bool_expected = [r for r in total_rows if r["bool_field"] is not None and r["bool_field"]]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=bool_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": bool_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on int8 field
        int8_filter = "int8_field < 50"
        int8_expected = [r for r in total_rows if r["int8_field"] is not None and r["int8_field"] < 50]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=int8_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": int8_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on int16 field
        int16_filter = "int16_field < 1000"
        int16_expected = [r for r in total_rows if r["int16_field"] is not None and r["int16_field"] < 1000]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=int16_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": int16_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on int32 field
        int32_filter = "int32_field in [1,2,3,4,5]"
        int32_expected = [r for r in total_rows if r["int32_field"] is not None and r["int32_field"] in [1,2,3,4,5]]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=int32_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": int32_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on int64 field
        int64_filter = "int64_field >= 10"
        int64_expected = [r for r in total_rows if r["int64_field"] is not None and r["int64_field"] >= 10]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=int64_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": int64_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on float field
        float_filter = "float_field > 5.0"
        float_expected = [r for r in total_rows if r["float_field"] is not None and r["float_field"] > 5.0]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=float_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": float_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on double field
        double_filter = "3.0 <=double_field <= 7.0"
        double_expected = [r for r in total_rows if r["double_field"] is not None and 3.0 <= r["double_field"] <= 7.0]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=double_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": double_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on varchar field
        varchar_filter = "varchar_field like \"varchar_1%\""
        varchar_expected = [r for r in total_rows if r["varchar_field"] is not None and r["varchar_field"].startswith("varchar_1")]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=varchar_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": varchar_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on varchar null values
        varchar_null_filter = "varchar_field is null"
        varchar_null_expected = [r for r in total_rows if r["varchar_field"] is None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=varchar_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": varchar_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on json field null values
        json_null_filter = "json_field is null"
        json_null_expected = [r for r in total_rows if r["json_field"] is None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=json_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": json_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on array field null values
        array_null_filter = "array_field is null"
        array_null_expected = [r for r in total_rows if r["array_field"] is None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=array_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": array_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on multiple nullable fields
        multi_null_filter = "varchar_field is null and json_field is null and array_field is null"
        multi_null_expected = [r for r in total_rows if r["varchar_field"] is None and r["json_field"] is None and r["array_field"] is None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=multi_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": multi_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on mix of null and non-null conditions
        mix_filter = "varchar_field is null and json_field is not null"
        mix_expected = [r for r in total_rows if r["varchar_field"] is None and r["json_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=mix_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": mix_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Query on is not null conditions for each scalar field
        # Int8 field is not null
        int8_not_null_filter = "int8_field is not null"
        int8_not_null_expected = [r for r in total_rows if r["int8_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=int8_not_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": int8_not_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Int16 field is not null
        int16_not_null_filter = "int16_field is not null"
        int16_not_null_expected = [r for r in total_rows if r["int16_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=int16_not_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": int16_not_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Float field is not null
        float_not_null_filter = "float_field is not null"
        float_not_null_expected = [r for r in total_rows if r["float_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=float_not_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": float_not_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Double field is not null
        double_not_null_filter = "double_field is not null"
        double_not_null_expected = [r for r in total_rows if r["double_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=double_not_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": double_not_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Varchar field is not null
        varchar_not_null_filter = "varchar_field is not null"
        varchar_not_null_expected = [r for r in total_rows if r["varchar_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=varchar_not_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": varchar_not_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # JSON field is not null
        json_not_null_filter = "json_field is not null"
        json_not_null_expected = [r for r in total_rows if r["json_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=json_not_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": json_not_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Array field is not null
        array_not_null_filter = "array_field is not null"
        array_not_null_expected = [r for r in total_rows if r["array_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=array_not_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": array_not_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Multiple fields is not null
        multi_not_null_filter = "varchar_field is not null and json_field is not null and array_field is not null"
        multi_not_null_expected = [r for r in total_rows if r["varchar_field"] is not None and r["json_field"] is not None and r["array_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=multi_not_null_filter,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": multi_not_null_expected,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Complex mixed conditions with is null, is not null, and comparison operators
        # Test case 1: int field is null AND float field > value AND varchar field is not null
        complex_mix_filter1 = "int32_field is null and float_field > 10.0 and varchar_field is not null"
        complex_mix_expected1 = [r for r in total_rows if r["int32_field"] is None and
                               r["float_field"] is not None and r["float_field"] > 10.0 and
                               r["varchar_field"] is not None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=complex_mix_filter1,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": complex_mix_expected1,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Test case 2: varchar field is not null AND int field between values AND float field is null
        complex_mix_filter2 = "varchar_field is not null and 5 <= int64_field <= 15 and float_field is null"
        complex_mix_expected2 = [r for r in total_rows if r["varchar_field"] is not None and
                               r["int64_field"] is not None and 5 <= r["int64_field"] <= 15 and
                               r["float_field"] is None]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=complex_mix_filter2,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": complex_mix_expected2,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        # Test case 3: Multiple fields with mixed null/not null conditions and range comparisons
        complex_mix_filter3 = "int8_field is not null and int8_field < 50 and double_field is null and varchar_field is not null and varchar_field like \"varchar_2%\""
        complex_mix_expected3 = [r for r in total_rows if r["int8_field"] is not None and r["int8_field"] < 50 and
                               r["double_field"] is None and
                               r["varchar_field"] is not None and r["varchar_field"].startswith("varchar_2")]
        query_res, _ = self.query(
            client,
            collection_name,
            filter=complex_mix_filter3,
            output_fields=['*'],
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_res": complex_mix_expected3,
                "with_vec": True,
                "primary_field": "id"
            }
        )

        t1 = time.time()
        log.info(f"Query on all scalar fields cost {t1 - t0:.4f} seconds")

        # 6. Delete data
        t0 = time.time()
        self.delete(client, collection_name, filter=default_search_exp)
        t1 = time.time()
        log.info(f"Delete cost {t1 - t0:.4f} seconds")

        # 7. Verify deletion
        query_res, _ = self.query(
            client,
            collection_name,
            filter=default_search_exp,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_res": []}
        )
        
        # 8. Cleanup
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)