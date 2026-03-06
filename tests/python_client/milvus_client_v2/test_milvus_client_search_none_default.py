import numpy as np
import random
import pytest
import pandas as pd
from pymilvus import DataType
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base

prefix = "search_collection"
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name
field_name = ct.default_float_vec_field_name


class TestSearchNoneDefaultIndependent(TestMilvusClientV2Base):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("dim", [32, 128])
    @pytest.mark.parametrize("auto_id", [False, True])
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_normal_none_data(self, dim, auto_id, is_flush, enable_dynamic_field, vector_data_type,
                                     null_data_percent):
        """
        target: test search normal case with none data inserted
        method: create connection, collection with nullable fields, insert data including none, and search
        expected: 1. search successfully with limit(topK)
        """
        nq = 200
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(nb=default_nb, dim=dim, auto_id=auto_id, with_json=True,
                                        vector_data_type=vector_data_type,
                                        nullable_fields={ct.default_float_field_name: null_data_percent})
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. generate search data
        vectors = cf.gen_vectors(nq, dim, vector_data_type)
        # 3. search after insert
        self.search(client, collection_name,
                    data=vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    output_fields=[default_int64_field_name,
                                   default_float_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": nq,
                                 "ids": insert_ids,
                                 "pk_name": ct.default_int64_field_name,
                                 "limit": default_limit,
                                 "output_fields": [default_int64_field_name,
                                                   default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("varchar_scalar_index", ["TRIE", "INVERTED", "BITMAP"])
    @pytest.mark.parametrize("numeric_scalar_index", ["STL_SORT", "INVERTED"])
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_after_none_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index,
                                                       null_data_percent):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        nullable_fields = {ct.default_int32_field_name: null_data_percent,
                           ct.default_int16_field_name: null_data_percent,
                           ct.default_int8_field_name: null_data_percent,
                           ct.default_bool_field_name: null_data_percent,
                           ct.default_float_field_name: null_data_percent,
                           ct.default_double_field_name: null_data_percent,
                           ct.default_string_field_name: null_data_percent}
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        default_schema = cf.gen_collection_schema_all_datatype(auto_id=False, dim=default_dim,
                                                               enable_dynamic_field=False,
                                                               nullable_fields=nullable_fields)
        self.create_collection(client, collection_name, schema=default_schema)
        # generate and insert data with nullable fields
        data = cf.gen_default_rows_data_all_data_type(nb=5000, dim=default_dim)
        # apply nullable fields
        for field_key, percent in nullable_fields.items():
            null_number = int(5000 * percent)
            for row in data[-null_number:]:
                if field_key in row:
                    row[field_key] = None
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        # 2. create index on vector field and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW",
                      metric_type="COSINE", params=cf.get_index_params_params("HNSW"))
        self.create_index(client, collection_name, index_params=idx)
        # 3. create index on scalar field with None data
        scalar_idx = self.prepare_index_params(client)[0]
        scalar_idx.add_index(field_name=ct.default_string_field_name, index_type=varchar_scalar_index)
        self.create_index(client, collection_name, index_params=scalar_idx)
        # 4. create index on scalar field with default data
        for scalar_field in [ct.default_int64_field_name, ct.default_int32_field_name,
                             ct.default_int16_field_name, ct.default_int8_field_name,
                             ct.default_float_field_name]:
            scalar_idx2 = self.prepare_index_params(client)[0]
            scalar_idx2.add_index(field_name=scalar_field, index_type=numeric_scalar_index)
            self.create_index(client, collection_name, index_params=scalar_idx2)
        bool_idx = self.prepare_index_params(client)[0]
        bool_idx.add_index(field_name=ct.default_bool_field_name, index_type="INVERTED")
        self.create_index(client, collection_name, index_params=bool_idx)
        self.load_collection(client, collection_name)
        # 5. search
        search_params = {}
        limit = ct.default_limit
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=search_params,
                    limit=limit,
                    filter=default_search_exp,
                    output_fields=[ct.default_string_field_name, ct.default_float_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "ids": insert_ids,
                                 "limit": limit,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": [ct.default_string_field_name,
                                                   ct.default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("dim", [32, 128])
    @pytest.mark.parametrize("auto_id", [False, True])
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_search_default_value_with_insert(self, dim, auto_id, is_flush, enable_dynamic_field, vector_data_type):
        """
        target: test search normal case with default value set
        method: create connection, collection with default value set, insert and search
        expected: 1. search successfully with limit(topK)
        """
        nq = 200
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, default_value=np.float32(10.0))
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(nb=default_nb, dim=dim, auto_id=auto_id, with_json=True,
                                        vector_data_type=vector_data_type)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. generate search data
        vectors = cf.gen_vectors(nq, dim, vector_data_type)
        # 3. search after insert
        self.search(client, collection_name,
                    data=vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    output_fields=[default_int64_field_name,
                                   default_float_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": nq,
                                 "ids": insert_ids,
                                 "pk_name": ct.default_int64_field_name,
                                 "limit": default_limit,
                                 "output_fields": [default_int64_field_name,
                                                   default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_search_default_value_without_insert(self, enable_dynamic_field):
        """
        target: test search normal case with default value set
        method: create connection, collection with default value set, no insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize without data
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True,
                         default_value=np.float32(10.0))
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. generate search data
        vectors = cf.gen_vectors(default_nq, default_dim, vector_data_type=DataType.FLOAT_VECTOR)
        # 3. search after insert
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "pk_name": ct.default_int64_field_name,
                                 "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("varchar_scalar_index", ["TRIE", "INVERTED", "BITMAP"])
    @pytest.mark.parametrize("numeric_scalar_index", ["STL_SORT", "INVERTED"])
    def test_search_after_default_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        default_value_fields = {ct.default_int32_field_name: np.int32(1),
                                ct.default_int16_field_name: np.int32(2),
                                ct.default_int8_field_name: np.int32(3),
                                ct.default_bool_field_name: True,
                                ct.default_float_field_name: np.float32(10.0),
                                ct.default_double_field_name: 10.0,
                                ct.default_string_field_name: "1"}
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        default_schema = cf.gen_collection_schema_all_datatype(auto_id=False, dim=default_dim,
                                                               enable_dynamic_field=False,
                                                               default_value_fields=default_value_fields)
        self.create_collection(client, collection_name, schema=default_schema)
        # generate and insert data
        data = cf.gen_default_rows_data_all_data_type(nb=5000, dim=default_dim)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        # 2. create index on vector field and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW",
                      metric_type="L2", params=cf.get_index_params_params("HNSW"))
        self.create_index(client, collection_name, index_params=idx)
        # 3. create index on scalar field with None data
        scalar_idx = self.prepare_index_params(client)[0]
        scalar_idx.add_index(field_name=ct.default_string_field_name, index_type=varchar_scalar_index)
        self.create_index(client, collection_name, index_params=scalar_idx)
        # 4. create index on scalar field with default data
        for scalar_field in [ct.default_int64_field_name, ct.default_int32_field_name,
                             ct.default_int16_field_name, ct.default_int8_field_name,
                             ct.default_float_field_name]:
            scalar_idx2 = self.prepare_index_params(client)[0]
            scalar_idx2.add_index(field_name=scalar_field, index_type=numeric_scalar_index)
            self.create_index(client, collection_name, index_params=scalar_idx2)
        if numeric_scalar_index != "STL_SORT":
            bool_idx = self.prepare_index_params(client)[0]
            bool_idx.add_index(field_name=ct.default_bool_field_name, index_type=numeric_scalar_index)
            self.create_index(client, collection_name, index_params=bool_idx)
        self.load_collection(client, collection_name)
        # 5. search
        search_params = {}
        limit = ct.default_limit
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        output_fields = [ct.default_int64_field_name, ct.default_int32_field_name,
                         ct.default_int16_field_name, ct.default_int8_field_name,
                         ct.default_bool_field_name, ct.default_float_field_name,
                         ct.default_double_field_name, ct.default_string_field_name]
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=search_params,
                    limit=limit,
                    filter=default_search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "ids": insert_ids,
                                 "pk_name": ct.default_int64_field_name,
                                 "limit": limit,
                                 "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [32, 128])
    @pytest.mark.parametrize("auto_id", [False, True])
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_search_both_default_value_non_data(self, dim, auto_id, is_flush, enable_dynamic_field,
                                                vector_data_type):
        """
        target: test search normal case with default value set
        method: create connection, collection with default value set, insert and search
        expected: 1. search successfully with limit(topK)
        """
        nq = 200
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True,
                         default_value=np.float32(10.0))
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        # null_data_percent=1 means all float field values are None
        data = cf.gen_default_rows_data(nb=default_nb, dim=dim, auto_id=auto_id, with_json=True,
                                        vector_data_type=vector_data_type,
                                        nullable_fields={ct.default_float_field_name: 1})
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. generate search data
        vectors = cf.gen_vectors(nq, dim, vector_data_type)
        # 3. search after insert
        self.search(client, collection_name,
                    data=vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    output_fields=[default_int64_field_name,
                                   default_float_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": nq,
                                 "ids": insert_ids,
                                 "pk_name": ct.default_int64_field_name,
                                 "limit": default_limit,
                                 "output_fields": [default_int64_field_name,
                                                   default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_collection_with_non_default_data_after_release_load(self, null_data_percent):
        """
        target: search the pre-released collection after load
        method: 1. create collection
                2. release collection
                3. load collection
                4. search the pre-released collection
        expected: search successfully
        """
        # 1. initialize without data
        nq = 200
        nb = 2000
        dim = 64
        auto_id = True
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, default_value=np.float32(10.0))
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, nullable=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(nb=nb, dim=dim, auto_id=auto_id, with_json=True,
                                        nullable_fields={ct.default_string_field_name: null_data_percent})
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. release collection
        self.release_collection(client, collection_name)
        # 3. Search the pre-released collection after load
        self.load_collection(client, collection_name)
        log.info("test_search_collection_with_non_default_data_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        self.search(client, collection_name,
                    data=vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    output_fields=[ct.default_float_field_name, ct.default_string_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": nq,
                                 "ids": insert_ids,
                                 "pk_name": ct.default_int64_field_name,
                                 "limit": default_limit,
                                 "output_fields": [ct.default_float_field_name,
                                                   ct.default_string_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("varchar_scalar_index", ["TRIE", "INVERTED", "BITMAP"])
    @pytest.mark.parametrize("numeric_scalar_index", ["STL_SORT", "INVERTED"])
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_after_different_index_with_params_none_default_data(self, varchar_scalar_index,
                                                                        numeric_scalar_index,
                                                                        null_data_percent):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        nullable_fields = {ct.default_string_field_name: null_data_percent}
        default_value_fields = {ct.default_float_field_name: np.float32(10.0)}
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        default_schema = cf.gen_collection_schema_all_datatype(auto_id=False, dim=default_dim,
                                                               enable_dynamic_field=False,
                                                               nullable_fields=nullable_fields,
                                                               default_value_fields=default_value_fields)
        self.create_collection(client, collection_name, schema=default_schema)
        # generate and insert data with nullable fields
        data = cf.gen_default_rows_data_all_data_type(nb=5000, dim=default_dim)
        # apply nullable fields
        for field_key, percent in nullable_fields.items():
            null_number = int(5000 * percent)
            for row in data[-null_number:]:
                if field_key in row:
                    row[field_key] = None
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        # 2. create index on vector field and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW",
                      metric_type="COSINE", params=cf.get_index_params_params("HNSW"))
        self.create_index(client, collection_name, index_params=idx)
        # 3. create index on scalar field with None data
        scalar_idx = self.prepare_index_params(client)[0]
        scalar_idx.add_index(field_name=ct.default_string_field_name, index_type=varchar_scalar_index)
        self.create_index(client, collection_name, index_params=scalar_idx)
        # 4. create index on scalar field with default data
        scalar_idx2 = self.prepare_index_params(client)[0]
        scalar_idx2.add_index(field_name=ct.default_float_field_name, index_type=numeric_scalar_index)
        self.create_index(client, collection_name, index_params=scalar_idx2)
        self.load_collection(client, collection_name)
        # 5. search
        limit = ct.default_limit
        search_params = {}
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=search_params,
                    limit=limit,
                    filter=default_search_exp,
                    output_fields=[ct.default_string_field_name, ct.default_float_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "ids": insert_ids,
                                 "limit": limit,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": [ct.default_string_field_name,
                                                   ct.default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("batch_size", [200, 600])
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_iterator_with_none_data(self, batch_size, null_data_percent):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
        """
        # 1. initialize with data
        dim = 64
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, nullable=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(nb=default_nb, dim=dim, with_json=True,
                                        nullable_fields={ct.default_string_field_name: null_data_percent})
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. search iterator
        search_params = {"metric_type": "L2"}
        vectors = cf.gen_vectors(1, dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search_iterator(client, collection_name, data=vectors[:1],
                             batch_size=batch_size,
                             anns_field=field_name,
                             search_params=search_params,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={"batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_none_data_partial_load(self, is_flush, enable_dynamic_field, null_data_percent):
        """
        target: test search normal case with none data inserted
        method: create connection, collection with nullable fields, insert data including none, and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(nb=default_nb, dim=default_dim, with_json=True,
                                        nullable_fields={ct.default_float_field_name: null_data_percent})
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. release and partial load again
        self.release_collection(client, collection_name)
        loaded_fields = [default_int64_field_name, ct.default_float_vec_field_name]
        if not enable_dynamic_field:
            loaded_fields.append(default_float_field_name)
        self.load_collection(client, collection_name, load_fields=loaded_fields)
        # 3. generate search data
        vectors = cf.gen_vectors(default_nq, default_dim)
        # 4. search after partial load field with None data
        output_fields = [default_int64_field_name, default_float_field_name]
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "ids": insert_ids,
                                 "pk_name": ct.default_int64_field_name,
                                 "limit": default_limit,
                                 "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #37547")
    @pytest.mark.parametrize("is_flush", [False, True])
    def test_search_none_data_expr_cache(self, is_flush):
        """
        target: test search case with none data to test expr cache
        method: 1. create collection with double datatype as nullable field
                2. search with expr "nullableFid == 0"
                3. drop this collection
                4. create collection with same collection name and same field name but modify the type of nullable field
                   as varchar datatype
                5. search with expr "nullableFid == 0" again
        expected: 1. search successfully with limit(topK) for the first collection
                  2. report error for the second collection with the same name
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(nb=default_nb, dim=default_dim, with_json=True,
                                        nullable_fields={ct.default_float_field_name: 0.5})
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. generate search data
        vectors = cf.gen_vectors(default_nq, default_dim)
        # 3. search with expr "nullableFid == 0"
        search_exp = f"{ct.default_float_field_name} == 0"
        output_fields = [default_int64_field_name, default_float_field_name]
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "ids": insert_ids,
                                 "limit": 1,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": output_fields})
        # 4. drop collection
        self.drop_collection(client, collection_name)
        # 5. create the same collection name with same field name but varchar field type
        schema2 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema2.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema2.add_field(ct.default_float_field_name, DataType.VARCHAR, max_length=65535, nullable=True)
        schema2.add_field(ct.default_json_field_name, DataType.JSON)
        schema2.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema2)
        # insert data
        int64_values = [i for i in range(default_nb)]
        json_values = [{"number": i, "string": str(i), "bool": bool(i),
                        "list": [j for j in range(i, i + ct.default_json_list_length)]} for i in range(default_nb)]
        float_vec_values = cf.gen_vectors(default_nb, default_dim)
        rows = []
        for i in range(default_nb):
            rows.append({
                ct.default_int64_field_name: int64_values[i],
                ct.default_float_field_name: None,
                ct.default_json_field_name: json_values[i],
                ct.default_float_vec_field_name: float_vec_values[i]
            })
        self.insert(client, collection_name, data=rows)
        idx2 = self.prepare_index_params(client)[0]
        idx2.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx2)
        self.load_collection(client, collection_name)
        self.flush(client, collection_name)
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": "failed to create query plan: cannot parse expression: float == 0, "
                                            "error: comparisons between VarChar and Int64 are not supported: "
                                            "invalid parameter"})
