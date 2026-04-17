import math
import time

import pytest
from pymilvus import DataType

from base.client_v2_base import TestMilvusClientV2Base
from check import param_check as pc
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
from utils.util_pymilvus import *

# Test parameters
default_nb = ct.default_nb
default_limit = ct.default_limit
default_search_exp = "id >= 0"


class TestMilvusClientE2E(TestMilvusClientV2Base):
    """Test case of end-to-end interface"""

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("flush_enable", [True, False])
    @pytest.mark.parametrize("scalar_index_enable", [True, False])
    def test_milvus_client_e2e_default(self, flush_enable, scalar_index_enable):
        """
        target: test full E2E lifecycle with all nullable scalar types and nullable vector
        method: 1. create collection with nullable fields (bool, int8/16/32/64, float, double, varchar, json, array, vector)
                2. insert 6000 rows (2 batches × 3000) with ~20% nulls
                3. create vector index + optional scalar indexes
                4. search with COSINE metric, verify distance ordering and no NaN (nullable vector)
                5. query with filters on each scalar type: null/not-null/comparison/range/like/in
                6. delete all data, verify search and query return empty
        expected: all search/query results match locally computed expected data;
                  no NaN distances from nullable vector; deletion fully effective
        """
        client = self._client()
        dim = 8
        vector_type = DataType.FLOAT_VECTOR

        # 1. Create collection with custom schema
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        # Primary key and vector field
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", vector_type, dim=dim, nullable=True)
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
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=12, nullable=True)

        # Create collection
        self.create_collection(client, collection_name, schema=schema)

        # 2. Insert data with null values for nullable fields
        num_inserts = 2  # 2 batches to cover sealed + growing scenarios
        total_rows = []
        for i in range(num_inserts):
            data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=i * default_nb)
            self.insert(client, collection_name, data)
            total_rows.extend(data)
        log.info(f"Total inserted {num_inserts * default_nb} entities")

        if flush_enable:
            self.flush(client, collection_name)
            log.info("Flush enabled: executing flush operation")

        # Create index parameters
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("vector", metric_type="COSINE")

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
        expected_scalar_indexes = [
            "int8_field",
            "int16_field",
            "int32_field",
            "int64_field",
            "float_field",
            "double_field",
            "varchar_field",
            "array_field",
        ]
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

        # 5. Search
        t0 = time.time()
        vectors_to_search = cf.gen_vectors(1, dim, vector_data_type=vector_type)
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 100}}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=default_limit,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": len(vectors_to_search),
                "pk_name": "id",
                "limit": default_limit,
                "metric": "COSINE",
            },
        )
        # Verify no NaN distances (nullable vector leak detection)
        for hits in search_res:
            for hit in hits:
                assert not math.isnan(hit["distance"]), f"NaN distance found in search result, pk={hit['id']}"
        t1 = time.time()
        log.info(f"Search cost {t1 - t0:.4f} seconds")

        # 6. Query with filters on each scalar field
        t0 = time.time()
        # Data-driven query cases: (filter_string, predicate_lambda, with_vec, description)
        query_cases = [
            # Boolean field (with_vec=False: skip nullable vector comparison in check)
            (
                "bool_field == true",
                lambda r: r["bool_field"] is not None and r["bool_field"] is True,
                False,
                "bool true",
            ),
            # Int8: null or < 10
            (
                "int8_field is null || int8_field < 10",
                lambda r: r["int8_field"] is None or r["int8_field"] < 10,
                True,
                "int8 null or < 10",
            ),
            # Int16: range [100, 200)
            (
                "100 <= int16_field < 200",
                lambda r: r["int16_field"] is not None and 100 <= r["int16_field"] < 200,
                True,
                "int16 range [100, 200)",
            ),
            # Int32: in set
            (
                "int32_field in [1,2,5,6]",
                lambda r: r["int32_field"] is not None and r["int32_field"] in [1, 2, 5, 6],
                True,
                "int32 in [1,2,5,6]",
            ),
            # Int64: range [4678, 5050)
            (
                "int64_field >= 4678 and int64_field < 5050",
                lambda r: r["int64_field"] is not None and r["int64_field"] >= 4678 and r["int64_field"] < 5050,
                True,
                "int64 range [4678, 5050)",
            ),
            # Float: (0.5, 0.7]
            (
                "float_field > 0.5 and float_field <= 0.7",
                lambda r: r["float_field"] is not None and r["float_field"] > 0.5 and r["float_field"] <= 0.7,
                True,
                "float (0.5, 0.7]",
            ),
            # Double: [0.5, 0.7]
            (
                "0.5 <=double_field <= 0.7",
                lambda r: r["double_field"] is not None and 0.5 <= r["double_field"] <= 0.7,
                True,
                "double [0.5, 0.7]",
            ),
            # Varchar: like prefix
            (
                'varchar_field like "varchar_1%"',
                lambda r: r["varchar_field"] is not None and r["varchar_field"].startswith("varchar_1"),
                True,
                "varchar like varchar_1%",
            ),
            # Varchar: is null
            ("varchar_field is null", lambda r: r["varchar_field"] is None, True, "varchar is null"),
            # JSON: is null
            ("json_field is null", lambda r: r["json_field"] is None, True, "json is null"),
            # Array: is null
            ("array_field is null", lambda r: r["array_field"] is None, True, "array is null"),
            # Multiple fields all null
            (
                "varchar_field is null and json_field is null and array_field is null",
                lambda r: r["varchar_field"] is None and r["json_field"] is None and r["array_field"] is None,
                True,
                "multi fields all null",
            ),
            # Mix: varchar null and json not null
            (
                "varchar_field is null and json_field is not null",
                lambda r: r["varchar_field"] is None and r["json_field"] is not None,
                True,
                "varchar null and json not null",
            ),
            # Int8: not null and > 100
            (
                "int8_field is not null and int8_field > 100",
                lambda r: r["int8_field"] is not None and r["int8_field"] > 100,
                True,
                "int8 not null and > 100",
            ),
            # Int16: not null and < 100
            (
                "int16_field is not null and int16_field < 100",
                lambda r: r["int16_field"] is not None and r["int16_field"] < 100,
                True,
                "int16 not null and < 100",
            ),
            # Float: not null and (0.5, 0.7]
            (
                "float_field is not null and float_field > 0.5 and float_field <= 0.7",
                lambda r: r["float_field"] is not None and r["float_field"] > 0.5 and r["float_field"] <= 0.7,
                True,
                "float not null and (0.5, 0.7]",
            ),
            # Double: not null and <= 0.2
            (
                "double_field is not null and double_field <= 0.2",
                lambda r: r["double_field"] is not None and r["double_field"] <= 0.2,
                True,
                "double not null and <= 0.2",
            ),
            # Varchar: not null
            ("varchar_field is not null", lambda r: r["varchar_field"] is not None, True, "varchar not null"),
            # JSON: not null and count < 15
            (
                "json_field is not null and json_field['count'] < 15",
                lambda r: r["json_field"] is not None and r["json_field"]["count"] < 15,
                True,
                "json not null and count < 15",
            ),
            # Array: not null and first element < 100
            (
                "array_field is not null and array_field[0] < 100",
                lambda r: r["array_field"] is not None and r["array_field"][0] < 100,
                True,
                "array not null and [0] < 100",
            ),
            # Multiple fields all not null
            (
                "varchar_field is not null and json_field is not null and array_field is not null",
                lambda r: r["varchar_field"] is not None
                and r["json_field"] is not None
                and r["array_field"] is not None,
                True,
                "multi fields all not null",
            ),
            # Complex: int32 null, float > 0.7, varchar not null
            (
                "int32_field is null and float_field > 0.7 and varchar_field is not null",
                lambda r: (
                    r["int32_field"] is None
                    and r["float_field"] is not None
                    and r["float_field"] > 0.7
                    and r["varchar_field"] is not None
                ),
                True,
                "int32 null and float > 0.7 and varchar not null",
            ),
            # Complex: varchar not null, int64 in [5, 15], float null
            (
                "varchar_field is not null and 5 <= int64_field <= 15 and float_field is null",
                lambda r: (
                    r["varchar_field"] is not None
                    and r["int64_field"] is not None
                    and 5 <= r["int64_field"] <= 15
                    and r["float_field"] is None
                ),
                True,
                "varchar not null and int64 [5,15] and float null",
            ),
            # Complex: int8 not null < 15, double null, varchar not null like varchar_2%
            (
                "int8_field is not null and int8_field < 15 and double_field is null and "
                'varchar_field is not null and varchar_field like "varchar_2%"',
                lambda r: (
                    r["int8_field"] is not None
                    and r["int8_field"] < 15
                    and r["double_field"] is None
                    and r["varchar_field"] is not None
                    and r["varchar_field"].startswith("varchar_2")
                ),
                True,
                "int8 < 15 and double null and varchar like varchar_2%",
            ),
        ]

        for filter_str, predicate, with_vec, desc in query_cases:
            expected = [r for r in total_rows if predicate(r)]
            log.info(f"query {desc}: filter={filter_str}, expected={len(expected)}")
            self.query(
                client,
                collection_name,
                filter=filter_str,
                output_fields=["*"],
                check_task=CheckTasks.check_query_results,
                check_items={"exp_res": expected, "with_vec": with_vec, "vector_type": vector_type, "pk_name": "id"},
            )

        t1 = time.time()
        log.info(f"Query on all scalar fields cost {t1 - t0:.4f} seconds")

        # 7. Delete data
        t0 = time.time()
        self.delete(client, collection_name, filter=default_search_exp)
        t1 = time.time()
        log.info(f"Delete cost {t1 - t0:.4f} seconds")

        # 8. Verify deletion via query
        self.query(
            client,
            collection_name,
            filter=default_search_exp,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_res": []},
        )

        # 9. Verify deletion via search — should return 0 results
        self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": len(vectors_to_search),
                "pk_name": "id",
                "limit": 0,
                "metric": "COSINE",
            },
        )

        # 10. Cleanup
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("flush_enable", [True, False])
    def test_milvus_client_data_consistent(self, flush_enable):
        """
        target: verify data consistency between inserted data and query_iterator results
        method: 1. create collection with nullable scalar fields + array fields
                2. insert 6000 rows (2 batches × 3000) with ~20% nulls
                3. create COSINE index, load, search with metric verification
                4. use query_iterator to retrieve all rows
                5. compare query_iterator results with original inserted data (epsilon-aware)
        expected: query_iterator results exactly match inserted data (order-independent, float-epsilon-tolerant)
        """
        client = self._client()
        dim = 28
        vector_type = DataType.FLOAT_VECTOR

        # 1. Create collection with custom schema
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        # Primary key and vector field
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", vector_type, dim=dim)
        # Boolean type
        schema.add_field("bool_field", DataType.BOOL, nullable=True)
        # Integer types
        schema.add_field("int16_field", DataType.INT16, nullable=True)
        schema.add_field("int32_field", DataType.INT32, nullable=True)
        schema.add_field("int64_field", DataType.INT64, nullable=True)
        # Float types
        schema.add_field("float_field", DataType.FLOAT, nullable=True)
        schema.add_field("double_field", DataType.DOUBLE, nullable=True)
        # String type
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=200, nullable=True)
        # JSON type
        schema.add_field("json_field", DataType.JSON, nullable=True)
        # Array float type
        schema.add_field(
            "array_float_field", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=15, nullable=True
        )
        # Array varchar type
        schema.add_field(
            "array_varchar_field",
            DataType.ARRAY,
            element_type=DataType.VARCHAR,
            max_capacity=15,
            max_length=100,
            nullable=True,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema)

        # 2. Insert data with null values for nullable fields
        num_inserts = 2  # 2 batches to cover sealed + growing scenarios
        total_rows = []
        for i in range(num_inserts):
            data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=i * default_nb)
            self.insert(client, collection_name, data)
            total_rows.extend(data)
        log.info(f"Total inserted {num_inserts * default_nb} entities")

        if flush_enable:
            self.flush(client, collection_name)
            log.info("Flush enabled: executing flush operation")

        # Create index parameters
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("vector", metric_type="COSINE")
        # 3. create index
        self.create_index(client, collection_name, index_params)

        # 4. Load collection
        self.load_collection(client, collection_name)

        # 5. Search
        vectors_to_search = cf.gen_vectors(1, dim, vector_data_type=vector_type)
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 100}}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=default_limit,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": len(vectors_to_search),
                "pk_name": "id",
                "limit": default_limit,
                "metric": "COSINE",
            },
        )

        # use query iterator to get all the data and compare with the inserted original data
        query_total_rows = []
        query_iterator = self.query_iterator(client, collection_name, output_fields=["*"])[0]
        while True:
            res = query_iterator.next()
            if len(res) == 0:
                log.info("search iteration finished, close")
                query_iterator.close()
                break
            query_total_rows.extend(res)

        # 6. Query with filters on each scalar field
        t1 = time.time()
        compare_res = pc.compare_lists_with_epsilon_ignore_dict_order(a=query_total_rows, b=total_rows)
        assert compare_res, "query result is not consistent with the inserted original data"
        t2 = time.time()
        log.info(f"Query results compare costs {t2 - t1:.4f} seconds")

        # 7. Cleanup
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)
