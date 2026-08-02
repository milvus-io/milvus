import numpy as np
import pytest
from pymilvus import DataType
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base

default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params


class TestSearchNoneDefaultIndependent(TestMilvusClientV2Base):
    """Independent tests for nullable field and default-value search scenarios.
    Each test creates its own collection because nullable/default-value configs
    and vector_data_type vary per test case.
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [False, True])
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_search_normal_none_data(self, auto_id, is_flush, enable_dynamic_field, vector_data_type):
        """
        target: verify search works correctly with nullable float field at various null ratios
        method: 1. create collection with nullable float field
                2. insert data with null_data_percent nulls
                3. search with filter "int64 >= 0" and output nullable field
                4. check nq, limit, IDs, output_fields, distance order via check_task
        expected: search returns correct results with distances in COSINE order
        """
        nq = 200
        dim = ct.default_dim
        null_data_percent = 0.5
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(
            nb=default_nb,
            dim=dim,
            auto_id=auto_id,
            with_json=True,
            vector_data_type=vector_data_type,
            nullable_fields={ct.default_float_field_name: null_data_percent},
        )
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        vectors = cf.gen_vectors(nq, dim, vector_data_type)
        self.search(
            client,
            collection_name,
            data=vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=default_search_exp,
            output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": nq,
                "ids": insert_ids,
                "pk_name": ct.default_int64_field_name,
                "limit": default_limit,
                "metric": "COSINE",
                "output_fields": [ct.default_int64_field_name, ct.default_float_field_name],
            },
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("varchar_scalar_index", ["TRIE", "INVERTED", "BITMAP"])
    @pytest.mark.parametrize("numeric_scalar_index", ["STL_SORT", "INVERTED"])
    def test_search_after_none_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index):
        """
        target: verify search works with nullable fields across all scalar types and different index types
        method: 1. create collection with all scalar data types, all nullable at given ratio
                2. create HNSW vector index + scalar indexes (varchar/numeric/bool)
                3. search with filter and output nullable fields
                4. check nq, limit, IDs, output_fields via check_task
        expected: search returns correct results with distances in COSINE order
        """
        null_data_percent = 0.5
        nullable_fields = {
            ct.default_int32_field_name: null_data_percent,
            ct.default_int16_field_name: null_data_percent,
            ct.default_int8_field_name: null_data_percent,
            ct.default_bool_field_name: null_data_percent,
            ct.default_float_field_name: null_data_percent,
            ct.default_double_field_name: null_data_percent,
            ct.default_string_field_name: null_data_percent,
        }
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        default_schema = cf.gen_collection_schema_all_datatype(
            auto_id=False, dim=default_dim, enable_dynamic_field=False, nullable_fields=nullable_fields
        )
        self.create_collection(client, collection_name, schema=default_schema)
        data = cf.gen_default_rows_data_all_data_type(nb=3000, dim=default_dim)
        for field_key, percent in nullable_fields.items():
            null_number = int(3000 * percent)
            for row in data[-null_number:]:
                if field_key in row:
                    row[field_key] = None
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        idx = self.prepare_index_params(client)[0]
        idx.add_index(
            field_name=ct.default_float_vec_field_name,
            index_type="HNSW",
            metric_type="COSINE",
            params=cf.get_index_params_params("HNSW"),
        )
        self.create_index(client, collection_name, index_params=idx)
        scalar_idx = self.prepare_index_params(client)[0]
        scalar_idx.add_index(field_name=ct.default_string_field_name, index_type=varchar_scalar_index)
        self.create_index(client, collection_name, index_params=scalar_idx)
        for scalar_field in [
            ct.default_int64_field_name,
            ct.default_int32_field_name,
            ct.default_int16_field_name,
            ct.default_int8_field_name,
            ct.default_float_field_name,
        ]:
            scalar_idx2 = self.prepare_index_params(client)[0]
            scalar_idx2.add_index(field_name=scalar_field, index_type=numeric_scalar_index)
            self.create_index(client, collection_name, index_params=scalar_idx2)
        bool_idx = self.prepare_index_params(client)[0]
        bool_idx.add_index(field_name=ct.default_bool_field_name, index_type="INVERTED")
        self.create_index(client, collection_name, index_params=bool_idx)
        self.load_collection(client, collection_name)
        search_params = {}
        limit = ct.default_limit
        vectors = cf.gen_vectors(default_nq, default_dim)
        self.search(
            client,
            collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params=search_params,
            limit=limit,
            filter=default_search_exp,
            output_fields=[ct.default_string_field_name, ct.default_float_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "ids": insert_ids,
                "limit": limit,
                "metric": "COSINE",
                "pk_name": ct.default_int64_field_name,
                "output_fields": [ct.default_string_field_name, ct.default_float_field_name],
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [32, 128])
    @pytest.mark.parametrize("auto_id", [False, True])
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_search_default_value_with_insert(self, dim, auto_id, is_flush, enable_dynamic_field, vector_data_type):
        """
        target: verify search works on collection with default_value field when data IS inserted with the field
        method: 1. create collection with float field having default_value=10.0
                2. insert data (float field included in rows, so default NOT triggered)
                3. search and verify nq, limit, IDs, output_fields, distance order
        expected: search returns correct results with distances in COSINE order
        """
        nq = 200
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, default_value=np.float32(10.0))
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(
            nb=default_nb, dim=dim, auto_id=auto_id, with_json=True, vector_data_type=vector_data_type
        )
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        vectors = cf.gen_vectors(nq, dim, vector_data_type)
        self.search(
            client,
            collection_name,
            data=vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=default_search_exp,
            output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": nq,
                "ids": insert_ids,
                "pk_name": ct.default_int64_field_name,
                "limit": default_limit,
                "metric": "COSINE",
                "output_fields": [ct.default_int64_field_name, ct.default_float_field_name],
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_search_default_value_without_insert(self, enable_dynamic_field):
        """
        target: verify search returns empty results on collection with default_value but no data
        method: 1. create collection with nullable float field + default_value=10.0
                2. do NOT insert any data
                3. search and verify limit=0 (empty collection)
        expected: search returns 0 results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True, default_value=np.float32(10.0))
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search(
            client,
            collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=default_search_exp,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "pk_name": ct.default_int64_field_name,
                "limit": 0,
            },
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("varchar_scalar_index", ["TRIE", "INVERTED", "BITMAP"])
    @pytest.mark.parametrize("numeric_scalar_index", ["STL_SORT", "INVERTED"])
    def test_search_after_default_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index):
        """
        target: verify search works with default_value fields across all scalar types and different index types
        method: 1. create collection with all scalar types having default values
                2. create HNSW vector index + scalar indexes
                3. search with filter and output all scalar fields
                4. check nq, limit, IDs, output_fields via check_task
        expected: search returns correct results with distances in L2 order
        """
        default_value_fields = {
            ct.default_int32_field_name: np.int32(1),
            ct.default_int16_field_name: np.int32(2),
            ct.default_int8_field_name: np.int32(3),
            ct.default_bool_field_name: True,
            ct.default_float_field_name: np.float32(10.0),
            ct.default_double_field_name: 10.0,
            ct.default_string_field_name: "1",
        }
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        default_schema = cf.gen_collection_schema_all_datatype(
            auto_id=False, dim=default_dim, enable_dynamic_field=False, default_value_fields=default_value_fields
        )
        self.create_collection(client, collection_name, schema=default_schema)
        data = cf.gen_default_rows_data_all_data_type(nb=5000, dim=default_dim)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        idx = self.prepare_index_params(client)[0]
        idx.add_index(
            field_name=ct.default_float_vec_field_name,
            index_type="HNSW",
            metric_type="L2",
            params=cf.get_index_params_params("HNSW"),
        )
        self.create_index(client, collection_name, index_params=idx)
        scalar_idx = self.prepare_index_params(client)[0]
        scalar_idx.add_index(field_name=ct.default_string_field_name, index_type=varchar_scalar_index)
        self.create_index(client, collection_name, index_params=scalar_idx)
        for scalar_field in [
            ct.default_int64_field_name,
            ct.default_int32_field_name,
            ct.default_int16_field_name,
            ct.default_int8_field_name,
            ct.default_float_field_name,
        ]:
            scalar_idx2 = self.prepare_index_params(client)[0]
            scalar_idx2.add_index(field_name=scalar_field, index_type=numeric_scalar_index)
            self.create_index(client, collection_name, index_params=scalar_idx2)
        if numeric_scalar_index != "STL_SORT":
            bool_idx = self.prepare_index_params(client)[0]
            bool_idx.add_index(field_name=ct.default_bool_field_name, index_type=numeric_scalar_index)
            self.create_index(client, collection_name, index_params=bool_idx)
        self.load_collection(client, collection_name)
        search_params = {}
        limit = ct.default_limit
        vectors = cf.gen_vectors(default_nq, default_dim)
        output_fields = [
            ct.default_int64_field_name,
            ct.default_int32_field_name,
            ct.default_int16_field_name,
            ct.default_int8_field_name,
            ct.default_bool_field_name,
            ct.default_float_field_name,
            ct.default_double_field_name,
            ct.default_string_field_name,
        ]
        self.search(
            client,
            collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params=search_params,
            limit=limit,
            filter=default_search_exp,
            output_fields=output_fields,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "ids": insert_ids,
                "pk_name": ct.default_int64_field_name,
                "limit": limit,
                "metric": "L2",
                "output_fields": output_fields,
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [32, 128])
    @pytest.mark.parametrize("auto_id", [False, True])
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_search_both_default_value_non_data(self, dim, auto_id, is_flush, enable_dynamic_field, vector_data_type):
        """
        target: verify search works when nullable+default_value float field is inserted with all None values
        method: 1. create collection with float field: nullable=True + default_value=10.0
                2. insert data with null_data_percent=1 (all float values are None → default applies)
                3. search and verify results
        expected: search returns correct results with distances in COSINE order
        """
        nq = 200
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True, default_value=np.float32(10.0))
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        # null_data_percent=1 means all float field values are None
        data = cf.gen_default_rows_data(
            nb=default_nb,
            dim=dim,
            auto_id=auto_id,
            with_json=True,
            vector_data_type=vector_data_type,
            nullable_fields={ct.default_float_field_name: 1},
        )
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        vectors = cf.gen_vectors(nq, dim, vector_data_type)
        self.search(
            client,
            collection_name,
            data=vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=default_search_exp,
            output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": nq,
                "ids": insert_ids,
                "pk_name": ct.default_int64_field_name,
                "limit": default_limit,
                "metric": "COSINE",
                "output_fields": [ct.default_int64_field_name, ct.default_float_field_name],
            },
        )
        # Verify that all returned float values equal the default_value (10.0)
        # since all inserted values were None
        res = self.search(
            client,
            collection_name,
            data=vectors[:1],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=default_search_exp,
            output_fields=[ct.default_float_field_name],
        )[0]
        for hit in res[0]:
            assert hit.get(ct.default_float_field_name) == 10.0, (
                f"Expected default_value 10.0 but got {hit.get(ct.default_float_field_name)}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_collection_with_non_default_data_after_release_load(self):
        """
        target: verify search works after release+load on collection with nullable varchar and default float
        method: 1. create collection with default_value float + nullable varchar
                2. insert, flush, index, load → release → load again
                3. search and verify results
        expected: search returns correct results after re-load, distances in COSINE order
        """
        nq = 200
        nb = ct.default_nb
        dim = 64
        auto_id = True
        null_data_percent = 0.5
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, default_value=np.float32(10.0))
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, nullable=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(
            nb=nb,
            dim=dim,
            auto_id=auto_id,
            with_json=True,
            nullable_fields={ct.default_string_field_name: null_data_percent},
        )
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # release and reload
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        log.info("test_search_collection_with_non_default_data_after_release_load: searching after load")
        vectors = cf.gen_vectors(nq, dim)
        self.search(
            client,
            collection_name,
            data=vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=default_search_exp,
            output_fields=[ct.default_float_field_name, ct.default_string_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": nq,
                "ids": insert_ids,
                "pk_name": ct.default_int64_field_name,
                "limit": default_limit,
                "metric": "COSINE",
                "output_fields": [ct.default_float_field_name, ct.default_string_field_name],
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("varchar_scalar_index", ["TRIE", "INVERTED", "BITMAP"])
    @pytest.mark.parametrize("numeric_scalar_index", ["STL_SORT", "INVERTED"])
    def test_search_after_different_index_with_params_none_default_data(
        self, varchar_scalar_index, numeric_scalar_index
    ):
        """
        target: verify search works with nullable varchar + default_value float across different scalar indexes
        method: 1. create collection with nullable varchar + default_value float
                2. create various scalar indexes (TRIE/INVERTED/BITMAP for varchar, STL_SORT/INVERTED for numeric)
                3. search and verify results
        expected: search returns correct results with distances in COSINE order
        """
        null_data_percent = 0.5
        nullable_fields = {ct.default_string_field_name: null_data_percent}
        default_value_fields = {ct.default_float_field_name: np.float32(10.0)}
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        default_schema = cf.gen_collection_schema_all_datatype(
            auto_id=False,
            dim=default_dim,
            enable_dynamic_field=False,
            nullable_fields=nullable_fields,
            default_value_fields=default_value_fields,
        )
        self.create_collection(client, collection_name, schema=default_schema)
        data = cf.gen_default_rows_data_all_data_type(nb=3000, dim=default_dim)
        for field_key, percent in nullable_fields.items():
            null_number = int(3000 * percent)
            for row in data[-null_number:]:
                if field_key in row:
                    row[field_key] = None
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        idx = self.prepare_index_params(client)[0]
        idx.add_index(
            field_name=ct.default_float_vec_field_name,
            index_type="HNSW",
            metric_type="COSINE",
            params=cf.get_index_params_params("HNSW"),
        )
        self.create_index(client, collection_name, index_params=idx)
        scalar_idx = self.prepare_index_params(client)[0]
        scalar_idx.add_index(field_name=ct.default_string_field_name, index_type=varchar_scalar_index)
        self.create_index(client, collection_name, index_params=scalar_idx)
        scalar_idx2 = self.prepare_index_params(client)[0]
        scalar_idx2.add_index(field_name=ct.default_float_field_name, index_type=numeric_scalar_index)
        self.create_index(client, collection_name, index_params=scalar_idx2)
        self.load_collection(client, collection_name)
        limit = ct.default_limit
        search_params = {}
        vectors = cf.gen_vectors(default_nq, default_dim)
        self.search(
            client,
            collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params=search_params,
            limit=limit,
            filter=default_search_exp,
            output_fields=[ct.default_string_field_name, ct.default_float_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "ids": insert_ids,
                "limit": limit,
                "metric": "COSINE",
                "pk_name": ct.default_int64_field_name,
                "output_fields": [ct.default_string_field_name, ct.default_float_field_name],
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("batch_size", [200, 600])
    def test_search_iterator_with_none_data(self, batch_size):
        """
        target: verify search iterator works on collection with nullable varchar field
        method: 1. create collection with nullable varchar, insert data
                2. run search iterator with L2 metric
                3. check batch_size via check_search_iterator
        expected: iterator returns batches of correct size with unique PKs
        """
        dim = 64
        null_data_percent = 0.5
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, nullable=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(
            nb=default_nb, dim=dim, with_json=True, nullable_fields={ct.default_string_field_name: null_data_percent}
        )
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        search_params = {"metric_type": "L2"}
        vectors = cf.gen_vectors(1, dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search_iterator(
            client,
            collection_name,
            data=vectors[:1],
            batch_size=batch_size,
            anns_field=ct.default_float_vec_field_name,
            search_params=search_params,
            check_task=CheckTasks.check_search_iterator,
            check_items={"batch_size": batch_size},
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_search_none_data_partial_load(self, is_flush, enable_dynamic_field):
        """
        target: verify search works after partial load on collection with nullable float field
        method: 1. create collection with nullable float, insert, load
                2. release, then partial load (only PK + vector + float if not dynamic)
                3. search and verify results
        expected: search returns correct results with distances in COSINE order
        """
        null_data_percent = 0.5
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(
            nb=default_nb,
            dim=default_dim,
            with_json=True,
            nullable_fields={ct.default_float_field_name: null_data_percent},
        )
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # release and partial load
        self.release_collection(client, collection_name)
        loaded_fields = [ct.default_int64_field_name, ct.default_float_vec_field_name]
        if not enable_dynamic_field:
            loaded_fields.append(ct.default_float_field_name)
        self.load_collection(client, collection_name, load_fields=loaded_fields)
        vectors = cf.gen_vectors(default_nq, default_dim)
        output_fields = [ct.default_int64_field_name, ct.default_float_field_name]
        self.search(
            client,
            collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=default_search_exp,
            output_fields=output_fields,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "ids": insert_ids,
                "pk_name": ct.default_int64_field_name,
                "limit": default_limit,
                "metric": "COSINE",
                "output_fields": output_fields,
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #37547")
    @pytest.mark.parametrize("is_flush", [False, True])
    def test_search_none_data_expr_cache(self, is_flush):
        """
        target: verify expression cache invalidation when collection is recreated with different nullable field type
        method: 1. create collection with nullable FLOAT field, search with "float == 0"
                2. drop collection
                3. recreate same name with float field as VARCHAR (nullable), insert None
                4. search with same expr "float == 0" → should error (VarChar vs Int64)
        expected: first search succeeds with limit=1; second search returns type mismatch error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(
            nb=default_nb, dim=default_dim, with_json=True, nullable_fields={ct.default_float_field_name: 0.5}
        )
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
        search_exp = f"{ct.default_float_field_name} == 0"
        output_fields = [ct.default_int64_field_name, ct.default_float_field_name]
        self.search(
            client,
            collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=search_exp,
            output_fields=output_fields,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "ids": insert_ids,
                "limit": 1,
                "metric": "COSINE",
                "pk_name": ct.default_int64_field_name,
                "output_fields": output_fields,
            },
        )
        # drop and recreate with varchar type for float field
        self.drop_collection(client, collection_name)
        schema2 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema2.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema2.add_field(ct.default_float_field_name, DataType.VARCHAR, max_length=65535, nullable=True)
        schema2.add_field(ct.default_json_field_name, DataType.JSON)
        schema2.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema2)
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema2)
        for row in rows:
            row[ct.default_float_field_name] = None
        self.insert(client, collection_name, data=rows)
        idx2 = self.prepare_index_params(client)[0]
        idx2.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx2)
        self.load_collection(client, collection_name)
        self.flush(client, collection_name)
        self.search(
            client,
            collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=search_exp,
            output_fields=output_fields,
            check_task=CheckTasks.err_res,
            check_items={
                "err_code": 1100,
                "err_msg": "failed to create query plan: cannot parse expression: float == 0, "
                "error: comparisons between VarChar and Int64 are not supported: "
                "invalid parameter",
            },
        )
