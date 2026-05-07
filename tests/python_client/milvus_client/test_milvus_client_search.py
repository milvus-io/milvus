# ruff: noqa: E712,E731,F401,F403,F405,F541,F841,I001,UP031,UP032,W291,W292,W293
# fmt: off
import math
import time
import os
import json
import requests
import random
import numpy as np

import pytest
from faker import Faker

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import * # noqa
from common.constants import * # noqa
from pymilvus import DataType, Function, FunctionType, AnnSearchRequest

fake = Faker()

prefix = "client_search"
partition_prefix = "client_partition"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_dynamic_field_name = "field_new"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name

# v2-migrated TestSearchInvalid{Shared,Independent} classes create collections with
# ct.default_int64_field_name as the PK field, so their filter expression must match
# that field name (distinct from v1's default_search_exp = 'id >= 0').
v2_invalid_search_exp = f"{ct.default_int64_field_name} >= 0"

# Module-level search vectors used across TestSearchInvalid{Shared,Independent} tests
# (migrated from test_milvus_client_search_invalid.py where these were
# referenced bare without `self.` — keep as module-level to preserve byte-identical bodies).
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]

# Shared collection for TestMilvusClientSearchInvalidRerankerShared — unique suffix avoids
# conflicts across parallel workers; dim=5 matches the original per-test schema.
RERANKER_INVALID_SHARED_COLLECTION = "test_reranker_invalid_shared_" + cf.gen_unique_str("_")
RERANKER_INVALID_DIM = 5

# Shared collections for TestMilvusClientSearchDecayRerankShared — two collections
# provisioned to preserve flushed vs. growing segment coverage from Test #1's is_flush
# parametrization.
DECAY_RERANK_SHARED_COLLECTION_GROWING = "test_decay_rerank_shared_growing_" + cf.gen_unique_str("_")
DECAY_RERANK_SHARED_COLLECTION_FLUSHED = "test_decay_rerank_shared_flushed_" + cf.gen_unique_str("_")
DECAY_RERANK_SHARED_DIM = 5


class TestMilvusClientSearchInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_search_invalid_collection_name_string(self, invalid_collection_name):
        """
        target: test search with invalid collection name
        method: create connection, collection, insert and search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        # collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        # self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default][collection={invalid_collection_name}]"}
        self.search(client, invalid_collection_name, vectors_to_search, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        # self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2587")
    @pytest.mark.parametrize("invalid_collection_name", [1])
    def test_milvus_client_search_invalid_collection_name_non_string(self, invalid_collection_name):
        """
        target: test search with invalid collection name
        method: create connection, collection, insert and search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default][collection={invalid_collection_name}]"}
        self.search(client, invalid_collection_name, vectors_to_search, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

        # self.drop_collection(client, collection_name)

        # self.drop_collection(client, collection_name)

        # self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2588")
    @pytest.mark.parametrize("invalid_search_params", [1, "1"])
    def test_milvus_client_search_invalid_search_params(self, invalid_search_params):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 1,
                 ct.err_msg: f"`search_params` value {invalid_search_params} is illegal"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    search_params=invalid_search_params,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

        # self.drop_collection(client, collection_name)

        # self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="pymilvus issue 1554")
    def test_milvus_client_collection_invalid_primary_field(self):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        error = {ct.err_code: 1, ct.err_msg: "Param id_type must be int or string"}
        self.create_collection(client, collection_name, default_dim, id_type="invalid",
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_string_auto_id(self):
        """
        target: test high level api: client.create_collection
        method: create collection with auto id on string primary key without mx length
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        error = {ct.err_code: 65535, ct.err_msg: f"type param(max_length) should be specified for the "
                                                 f"field({default_primary_key_field_name}) of collection {collection_name}"}
        self.create_collection(client, collection_name, default_dim, id_type="string", auto_id=True,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_same_collection_different_params(self):
        """
        target: test high level api: client.create_collection
        method: create
        expected: 1. Successfully to create collection with same params
                  2. Report errors for creating collection with same name and different params
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. create collection with same params
        self.create_collection(client, collection_name, default_dim)
        # 3. create collection with same name and different params
        error = {ct.err_code: 1, ct.err_msg: f"create duplicate collection with different parameters, "
                                             f"collection: {collection_name}"}
        self.create_collection(client, collection_name, default_dim + 1,
                               check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_invalid_metric_type(self):
        """
        target: test high level api: client.create_collection
        method: create collection with auto id on string primary key
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        error = {ct.err_code: 1100,
                 ct.err_msg: "float vector index does not support metric type: invalid: "
                             "invalid parameter[expected=valid index params][actual=invalid index params]"}
        self.create_collection(client, collection_name, default_dim, metric_type="invalid",
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/29880")
    def test_milvus_client_search_not_consistent_metric_type(self, metric_type):
        """
        target: test search with inconsistent metric type (default is IP) with that of index
        method: create connection, collection, insert and search with not consistent metric type
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        search_params = {"metric_type": metric_type}
        error = {ct.err_code: 1100,
                 ct.err_msg: f"metric type not match: invalid parameter[expected=IP][actual={metric_type}]"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    search_params=search_params,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_null_expr_vector_field(self):
        """
        target: test search with null expression on vector field
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        null_expr_ops = ["is null", "IS NULL", "is not null", "IS NOT NULL"]
        vectors_to_search = rng.random((1, dim))
        for null_expr_op in null_expr_ops:
            null_expr = default_vector_field_name + " " + null_expr_op
            error = {ct.err_code: 1100,
                    ct.err_msg: "IsNull/IsNotNull operations are not supported on vector fields"}
            self.search(client, collection_name, vectors_to_search,
                        filter=null_expr,
                        check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_null_expr_not_exist_field(self):
        """
        target: test search with null expression on vector field
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        not_exist_field_name = "not_exist_field"
        null_expr_ops = ["is null", "IS NULL", "is not null", "IS NOT NULL"]
        for null_expr_op in null_expr_ops:
            null_expr = not_exist_field_name + " " + null_expr_op
            error = {ct.err_code: 1100,
                    ct.err_msg: f"failed to create query plan: cannot parse expression: "
                                f"{null_expr}, error: field {not_exist_field_name} not exist: invalid parameter"}
            self.search(client, collection_name, vectors_to_search,
                        filter=null_expr,
                        check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_search_null_expr_json_key(self, nullable):
        """
        target: test search with null expression on each key of json
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(nullable_field_name, DataType.JSON, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, dim)
        null_expr_ops = ["is null", "IS NULL", "is not null", "IS NOT NULL"]
        for null_expr_op in null_expr_ops:
            if nullable:
                rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                        nullable_field_name: {'a': None}} for i in range(default_nb)]
                null_expr = nullable_field_name + "['a']" + " " + null_expr_op
            else:
                rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                        nullable_field_name: {'a': 1, 'b': None}} for i in range(default_nb)]
                null_expr = nullable_field_name + "['b']" + " " + null_expr_op
            self.insert(client, collection_name, rows)
            # 3. search
            self.search(client, collection_name, [vectors[0]],
                        filter=null_expr)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_search_null_expr_array_element(self, nullable):
        """
        target: test search with null expression on each key of json
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(nullable_field_name, DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, dim)
        null_expr_ops = ["is null", "IS NULL", "is not null", "IS NOT NULL"]
        for null_expr_op in null_expr_ops:
            if nullable:
                rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                        nullable_field_name: None} for i in range(default_nb)]
            else:
                rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                        nullable_field_name: [1, 2, 3]} for i in range(default_nb)]
            self.insert(client, collection_name, rows)
            # 3. search
            null_expr = nullable_field_name + "[0]" + " " + null_expr_op
            error = {ct.err_code: 65535,
                    ct.err_msg: "unsupported data type: ARRAY"}
            self.search(client, collection_name, [vectors[0]],
                        filter=null_expr,
                        check_task=CheckTasks.err_res, check_items=error)

        self.drop_collection(client, collection_name)


@pytest.mark.xdist_group("TestSearchInvalidShared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchInvalidShared(TestMilvusClientV2Base):
    """Test search with invalid parameters using shared collection.
    Schema: int64(PK), float, varchar(65535), json, float_vector(128), dynamic=False
    Data: 3000 rows
    Index: COSINE on float_vector
    """

    shared_alias = "TestSearchInvalidShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchInvalidShared" + cf.gen_unique_str("search_invalid")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        data = cf.gen_row_data_by_schema(nb=3000, schema=schema)
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_missing(self):
        """
        target: test search with incomplete parameters
        method: search with incomplete parameters
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_missing: Searching collection %s "
                 "with missing parameters" % self.collection_name)
        self.search(client, self.collection_name,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "Either ids or data must be provided"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_vectors", ct.get_invalid_vectors)
    def test_search_param_invalid_vectors(self, invalid_vectors):
        """
        target: test search with invalid parameter values
        method: search with invalid data
        expected: raise exception and report the error
        """
        if invalid_vectors in [[" "], ['a']]:
            pytest.skip("['a'] and [' '] is valid now")
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_invalid_vectors: searching with "
                 "invalid vectors: {}".format(invalid_vectors))
        if invalid_vectors is None:
            err_msg = "Either ids or data must be provided"
        else:
            err_msg = "`search_data` value {} is illegal".format(invalid_vectors)
        self.search(client, self.collection_name,
                    data=invalid_vectors, anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_dim(self):
        """
        target: test search with invalid parameter values
        method: search with invalid dim
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_invalid_dim: searching with invalid dim")
        wrong_dim = 129
        wrong_vectors = [[random.random() for _ in range(wrong_dim)] for _ in range(default_nq)]
        self.search(client, self.collection_name,
                    data=wrong_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": 'vector dimension mismatch'})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_field_name", ct.invalid_resource_names)
    def test_search_param_invalid_field(self, invalid_field_name):
        """
        target: test search with invalid parameter type
        method: search with invalid field type
        expected: raise exception and report the error
        """
        if invalid_field_name in [None, ""]:
            pytest.skip("None is legal")
        client = self._client(alias=self.shared_alias)
        error = {"err_code": 999, "err_msg": f"failed to create query plan: failed to get field schema by name"}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=invalid_field_name,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_metric", ct.get_invalid_metric_type)
    def test_search_param_invalid_metric_type(self, invalid_metric):
        """
        target: test search with invalid parameter values
        method: search with invalid metric type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_invalid_metric_type: searching with invalid metric_type")
        search_params = {"metric_type": invalid_metric, "params": {"nprobe": 10}}
        if isinstance(invalid_metric, dict):
            self.search(client, self.collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=search_params, limit=default_limit,
                        filter=v2_invalid_search_exp,
                        check_task=CheckTasks.err_res,
                        check_items={"err_code": 1,
                                     "err_msg": "Dict key must be str"})
        else:
            self.search(client, self.collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=search_params, limit=default_limit,
                        filter=v2_invalid_search_exp,
                        check_task=CheckTasks.err_res,
                        check_items={"err_code": 65535,
                                     "err_msg": "metric type not match"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_metric_type_not_match(self):
        """
        target: test search with invalid parameter values
        method: search with invalid metric type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_metric_type_not_match: searching with not matched metric_type")
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "metric type not match: invalid parameter"
                                            "[expected=COSINE][actual=L2]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_limit", [p for p in ct.get_invalid_ints
                                                if not (isinstance(p, int) and p >= 0)])
    def test_search_param_invalid_limit_type(self, invalid_limit):
        """
        target: test search with invalid limit type
        method: search with invalid limit type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_invalid_limit_type: searching with "
                 "invalid limit: %s" % invalid_limit)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=invalid_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "`limit` value %s is illegal" % invalid_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [0, 16385])
    def test_search_param_invalid_limit_value(self, limit):
        """
        target: test search with invalid limit value
        method: search with invalid limit: 0 and maximum
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        err_msg = f"topk [{limit}] is invalid, it should be in range [1, 16384]"
        if limit == 0:
            err_msg = "`limit` value 0 is illegal"
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_search_expr", ["'non_existing_field'==2", 1])
    def test_search_param_invalid_expr_type(self, invalid_search_expr):
        """
        target: test search with invalid parameter type
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        error = {"err_code": 999, "err_msg": "failed to create query plan: cannot parse expression"}
        if invalid_search_expr == 1:
            error = {"err_code": 1, "err_msg": "'int' object has no attribute 'lower'"}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=invalid_search_expr,
                    check_task=CheckTasks.err_res,
                    check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_expr_value", ["string", 1.2, None, [1, 2, 3]])
    def test_search_param_invalid_expr_value(self, invalid_expr_value):
        """
        target: test search with invalid parameter values
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        invalid_search_expr = f"{ct.default_int64_field_name}=={invalid_expr_value}"
        log.info("test_search_param_invalid_expr_value: searching with "
                 "invalid expr: %s" % invalid_search_expr)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=invalid_search_expr,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999,
                                 "err_msg": "failed to create query plan: cannot parse expression: %s"
                                            % invalid_search_expr})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", [f"{ct.default_int64_field_name} like 33",
                                               f"{ct.default_float_field_name} LIKE 33"])
    def test_search_with_expression_invalid_like(self, expression):
        """
        target: test search int64 and float with like
        method: test search int64 and float with like
        expected: searched failed
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        search_vectors = [[random.random() for _ in range(default_dim)]
                          for _ in range(default_nq)]
        self.search(client, self.collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "failed to create query plan: cannot parse "
                                            "expression: %s" % expression})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_partitions", [[None], [1, 2]])
    def test_search_partitions_invalid_type(self, invalid_partitions):
        """
        target: test search invalid partition
        method: search with invalid partition type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        err_msg = "`partition_name_array` value {} is illegal".format(invalid_partitions)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    partition_names=invalid_partitions,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999,
                                 "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_partitions", [["non_existing"], [ct.default_partition_name, "non_existing"]])
    def test_search_partitions_non_existing(self, invalid_partitions):
        """
        target: test search invalid partition
        method: search with invalid partition type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        err_msg = "partition name non_existing not found"
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    partition_names=invalid_partitions,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_output_fields", [[None], [1, 2], ct.default_int64_field_name])
    def test_search_with_output_fields_invalid_type(self, invalid_output_fields):
        """
        target: test search with output fields
        method: search with invalid output_field
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        err_msg = f"`output_fields` value {invalid_output_fields} is illegal"
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    output_fields=invalid_output_fields,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 999,
                                 ct.err_msg: err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("non_existing_output_fields",
                             [["non_existing"], [ct.default_int64_field_name, "non_existing"]])
    def test_search_with_output_fields_non_existing(self, non_existing_output_fields):
        """
        target: test search with output fields
        method: search with invalid output_field
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        err_msg = f"field non_existing not exist"
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    output_fields=non_existing_output_fields,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 999,
                                 ct.err_msg: err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("output_fields", [[default_search_field], ["*"]])
    def test_search_output_field_vector(self, output_fields):
        """
        target: test search with vector as output field
        method: search with one vector output_field or
                wildcard for vector
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_output_field_vector: Searching collection %s" %
                 self.collection_name)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq, "limit": default_limit,
                                 "metric": "COSINE", "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", [["*%"], ["**"], ["*", "@"]])
    def test_search_output_field_invalid_wildcard(self, output_fields):
        """
        target: test search with invalid output wildcard
        method: search with invalid output_field wildcard
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_output_field_invalid_wildcard: Searching collection %s" %
                 self.collection_name)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": f"field {output_fields[-1]} not exist"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_guarantee_time",
                             [p for p in ct.get_invalid_ints
                              if p != 9999999999 and p is not None])
    def test_search_param_invalid_guarantee_timestamp(self, invalid_guarantee_time):
        """
        target: test search with invalid guarantee timestamp
        method: search with invalid guarantee timestamp
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info(
            "test_search_param_invalid_guarantee_timestamp: searching with invalid guarantee timestamp")
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    guarantee_timestamp=invalid_guarantee_time,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "`guarantee_timestamp` value %s is illegal"
                                            % invalid_guarantee_time})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("round_decimal", [7, -2, 999, 1.0, None, [1], "string", {}])
    def test_search_invalid_round_decimal(self, round_decimal):
        """
        target: test search with invalid round decimal
        method: search with invalid round decimal
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_invalid_round_decimal: Searching collection %s" %
                 self.collection_name)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    round_decimal=round_decimal,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": f"`round_decimal` value {round_decimal} is illegal"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [16385])
    def test_search_with_invalid_nq(self, nq):
        """
        target: test search with invalid nq
        method: search with invalid nq
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = [[random.random() for _ in range(default_dim)]
                          for _ in range(nq)]
        self.search(client, self.collection_name,
                    data=search_vectors[:nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "nq (number of search vector per search "
                                            "request) should be in range [1, 16384]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_radius", [[0.1], "str"])
    def test_range_search_invalid_radius(self, invalid_radius):
        """
        target: test range search with invalid radius
        method: range search with invalid radius
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_range_search_invalid_radius: Range searching collection %s" %
                 self.collection_name)
        range_search_params = {"metric_type": "COSINE",
                               "params": {"radius": invalid_radius, "range_filter": 0}}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=range_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": "type must be number"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr", [
        f"{ct.default_int64_field_name} / 0 > 0",
        f"{ct.default_int64_field_name} / 0 == 1",
        f"{ct.default_float_field_name} / 0 == 1.0",
        f"{ct.default_int64_field_name} % 0 == 1",
        f"{ct.default_int64_field_name} % 0 != 0",
        f"{ct.default_json_field_name}['number'] / 0 > 0",
        f"{ct.default_json_field_name}['number'] % 0 == 1",
    ])
    def test_search_filter_division_by_zero(self, expr):
        """
        target: test search with division/modulo by zero in filter expression (issue #47285)
        method: search with filter containing division or modulo by zero on int64/float/json fields
        expected: raise error with 'by zero' message instead of crashing server (SIGFPE)
        """
        client = self._client(alias=self.shared_alias)
        log.info(f"test_search_filter_division_by_zero: searching with expr: {expr}")
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expr,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": "by zero"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr", [
        f"{ct.default_int64_field_name} / 0 > 0",
        f"{ct.default_int64_field_name} % 0 == 1",
    ])
    def test_query_filter_division_by_zero(self, expr):
        """
        target: test query with division/modulo by zero in filter expression (issue #47285)
        method: query with filter containing division or modulo by zero
        expected: raise error with 'by zero' message instead of crashing server (SIGFPE)
        """
        client = self._client(alias=self.shared_alias)
        log.info(f"test_query_filter_division_by_zero: querying with expr: {expr}")
        self.query(client, self.collection_name,
                   filter=expr,
                   check_task=CheckTasks.err_res,
                   check_items={"err_code": 999, "err_msg": "by zero"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr,expr_params", [
        (f"{ct.default_int64_field_name} / {{d}} > 0", {"d": 0}),
        (f"{ct.default_int64_field_name} % {{d}} == 1", {"d": 0}),
        (f"{ct.default_float_field_name} / {{d}} == 1.0", {"d": 0}),
    ])
    def test_search_filter_division_by_zero_with_expr_params(self, expr, expr_params):
        """
        target: test search with division/modulo by zero via expr_params (issue #47285)
        method: search with parameterized filter where divisor is zero
        expected: raise error with 'by zero' message instead of crashing server (SIGFPE)
        """
        client = self._client(alias=self.shared_alias)
        log.info(f"test_search_filter_division_by_zero_with_expr_params: "
                 f"searching with expr: {expr}, params: {expr_params}")
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expr, filter_params=expr_params,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": "by zero"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr", [
        f"{ct.default_int64_field_name} / 2 >= 0",
        f"{ct.default_int64_field_name} % 3 == 1",
        f"{ct.default_float_field_name} / 2.0 < 1000",
    ])
    def test_search_filter_division_by_nonzero(self, expr):
        """
        target: test search with valid division/modulo expressions still works (issue #47285)
        method: search with filter containing division or modulo by non-zero values
        expected: search succeeds without error
        """
        client = self._client(alias=self.shared_alias)
        log.info(f"test_search_filter_division_by_nonzero: searching with expr: {expr}")
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expr,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq, "limit": default_limit,
                                 "metric": "COSINE", "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_range_filter", [[0.1], "str"])
    def test_range_search_invalid_range_filter(self, invalid_range_filter):
        """
        target: test range search with invalid range_filter
        method: range search with invalid range_filter
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_range_search_invalid_range_filter: Range searching collection %s" %
                 self.collection_name)
        range_search_params = {"metric_type": "COSINE",
                               "params": {"radius": 1, "range_filter": invalid_range_filter}}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=range_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": "type must be number"})


class TestSearchInvalidIndependent(TestMilvusClientV2Base):
    """ Test case of search interface """

    def _create_standard_schema(self, client, dim=default_dim):
        """Create a standard schema: int64(PK), float, varchar(65535), json, float_vector(dim)."""
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        return schema

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_no_connection(self):
        """
        target: test search without connection
        method: create and delete connection, then search
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. close client connection
        log.info("test_search_no_connection: closing client connection")
        client.close()
        log.info("test_search_no_connection: closed client connection")

        # 3. search without connection
        log.info("test_search_no_connection: searching without connection")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "should create connection first"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_no_collection(self):
        """
        target: test the scenario which search the non-exist collection
        method: 1. create collection
                2. drop collection
                3. search the dropped collection
        expected: raise exception and report the error
        """
        # 1. initialize without data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # 2. Drop collection
        self.drop_collection(client, collection_name)

        # 3. Search without collection
        log.info("test_search_no_collection: Searching without collection ")
        self.search(client, collection_name,
                    data=vectors, anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "collection not found"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:8])
    def test_search_invalid_params_type(self, index):
        """
        target: test search with invalid search params
        method: test search with invalid params type
        expected: raise exception and report the error
        """
        if index == "FLAT":
            pytest.skip("skip in FLAT index")
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index and load
        params = cf.get_index_params_params(index)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type=index, metric_type="L2", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search
        invalid_search_params = cf.gen_invalid_search_params_type()
        for invalid_search_param in invalid_search_params:
            if index == invalid_search_param["index_type"]:
                search_params = {"metric_type": "L2",
                                 "params": invalid_search_param["search_params"]}
                log.info("search_params: {}".format(search_params))
                self.search(client, collection_name,
                            data=vectors[:default_nq], anns_field=default_search_field,
                            search_params=search_params, limit=default_limit,
                            filter=v2_invalid_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999,
                                         "err_msg": "fail to search on QueryNode"})

    @pytest.mark.skip("not support now")
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("search_k", [-10, -1, 0, 10, 125])
    def test_search_param_invalid_annoy_index(self, search_k):
        """
        target: test search with invalid search params matched with annoy index
        method: search with invalid param search_k out of [top_k, infinity)
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=3000, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create annoy index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="ANNOY", metric_type="L2", params={"n_trees": 512})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search
        annoy_search_param = {"index_type": "ANNOY",
                              "search_params": {"search_k": search_k}}
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=annoy_search_param, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "Search params check failed"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_field_compare_expressions())
    def test_search_with_expression_join_two_fields(self, expression):
        """
        target: test search with expressions linking two fields such as 'and'
        method: create a collection and search with different conjunction
        expected: raise exception and report the error
        """
        # 1. create a collection
        nb = 1
        dim = 2
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_1", DataType.INT64, is_primary=True)
        schema.add_field("int64_2", DataType.INT64)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data
        data = [{"int64_1": i, "int64_2": i,
                 ct.default_float_vec_field_name: [random.random() for _ in range(dim)]}
                for i in range(nb)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 3. create index, load, and search with expression
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        log.info("test_search_with_expression: searching with expression: %s" % expression)
        expression = expression.replace("&&", "and").replace("||", "or")
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=nb,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999,
                                 "err_msg": "failed to create query plan: "
                                            "cannot parse expression: %s" % expression})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_expr_bool_value", [1.2, 10, "string"])
    def test_search_param_invalid_expr_bool(self, invalid_expr_bool_value):
        """
        target: test search with invalid parameter values
        method: search with invalid bool search expressions
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search with invalid bool expr
        invalid_search_expr_bool = f"{ct.default_bool_field_name} == {invalid_expr_bool_value}"
        log.info("test_search_param_invalid_expr_bool: searching with "
                 "invalid expr: %s" % invalid_search_expr_bool)
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=invalid_search_expr_bool,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "failed to create query plan"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_invalid_bool(self):
        """
        target: test search invalid bool
        method: test search invalid bool
        expected: searched failed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        expressions = [ct.default_bool_field_name, "true", "false"]
        for expression in expressions:
            log.debug(f"search with expression: {expression}")
            self.search(client, collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=default_search_params, limit=default_limit,
                        filter=expression,
                        check_task=CheckTasks.err_res,
                        check_items={"err_code": 1100,
                                     "err_msg": "failed to create query plan: predicate is not a "
                                                "boolean expression: %s, data type: Bool" % expression})
        expression = f"!{ct.default_bool_field_name}"
        log.debug(f"search with expression: {expression}")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": f"cannot parse expression: !{ct.default_bool_field_name}, "
                                            "error: not op can only be applied on boolean expression"})
        expression = f"{ct.default_int64_field_name} > 0 and {ct.default_bool_field_name}"
        log.debug(f"search with expression: {expression}")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": f"cannot parse expression: {ct.default_int64_field_name} > 0 and {ct.default_bool_field_name}, "
                                            "error: 'and' can only be used between boolean expressions"})
        expression = f"{ct.default_int64_field_name} > 0 or false"
        log.debug(f"search with expression: {expression}")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": f"cannot parse expression: {ct.default_int64_field_name} > 0 or false, "
                                            "error: 'or' can only be used between boolean expressions"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_invalid_array_one(self):
        """
        target: test search with invalid array expressions
        method: test search with invalid array expressions:
                the order of array > the length of array
        expected: searched successfully with correct limit(topK)
        """
        # 1. create a collection
        nb = ct.default_nb
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_int32_array_field_name, DataType.ARRAY,
                         element_type=DataType.INT32, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_float_array_field_name, DataType.ARRAY,
                         element_type=DataType.FLOAT, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_string_array_field_name, DataType.ARRAY,
                         element_type=DataType.VARCHAR, max_capacity=ct.default_max_capacity,
                         max_length=100, nullable=True)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(schema=schema)
        data[1][ct.default_int32_array_field_name] = [1]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search (subscript > max_capacity)
        expression = "int32_array[101] > 0"
        res, _ = self.search(client, collection_name,
                             data=vectors[:default_nq], anns_field=default_search_field,
                             search_params=default_search_params, limit=nb,
                             filter=expression)
        assert len(res[0]) == 0

        # 3. search (max_capacity > subscript > actual length of array)
        expression = "int32_array[51] > 0"
        res, _ = self.search(client, collection_name,
                             data=vectors[:default_nq], anns_field=default_search_field,
                             search_params=default_search_params, limit=default_limit,
                             filter=expression)
        assert len(res[0]) == default_limit

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_invalid_array_two(self):
        """
        target: test search with invalid array expressions
        method: test search with invalid array expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. create a collection
        nb = ct.default_nb
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_int32_array_field_name, DataType.ARRAY,
                         element_type=DataType.INT32, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_float_array_field_name, DataType.ARRAY,
                         element_type=DataType.FLOAT, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_string_array_field_name, DataType.ARRAY,
                         element_type=DataType.VARCHAR, max_capacity=ct.default_max_capacity,
                         max_length=100, nullable=True)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search
        expression = "int32_array[0] - 1 < 1"
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=nb,
                    filter=expression)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_release_collection(self):
        """
        target: test the scenario which search the released collection
        method: 1. create collection
                2. release collection
                3. search the released collection
        expected: raise exception and report the error
        """
        # 1. initialize without data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. release collection
        self.release_collection(client, collection_name)

        # 3. Search the released collection
        log.info("test_search_release_collection: Searching without collection ")
        self.search(client, collection_name,
                    data=vectors, anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_release_partition(self):
        """
        target: test the scenario which search the released collection
        method: 1. create collection
                2. release partition
                3. search the released partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition and insert data
        par_name = "search_partition_0"
        self.create_partition(client, collection_name, partition_name=par_name)
        data = cf.gen_row_data_by_schema(nb=10, schema=schema)
        self.insert(client, collection_name, data=data, partition_name=par_name)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT")
        self.create_index(client, collection_name, index_params=idx)
        self.load_partitions(client, collection_name, partition_names=[par_name])

        # 2. release partition
        self.release_partitions(client, collection_name, partition_names=[par_name])

        # 3. Search the released partition
        log.info("test_search_release_partition: Searching the released partition")
        limit = 10
        self.search(client, collection_name,
                    data=vectors, anns_field=default_search_field,
                    search_params=default_search_params, limit=limit,
                    filter=v2_invalid_search_exp,
                    partition_names=[par_name],
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_search_with_empty_collection(self, vector_data_type):
        """
        target: test search with empty connection
        method: 1. search the empty collection before load
                2. search the empty collection after load
                3. search collection with data inserted but not load again
        expected: 1. raise exception if not loaded
                  2. return topk=0  if loaded
                  3. return topk successfully
        """
        # 1. initialize without data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, vector_data_type, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        # 2. search collection without data before load
        log.info("test_search_with_empty_collection: Searching empty collection %s"
                 % collection_name)
        err_msg = "collection not loaded"
        search_vectors = cf.gen_vectors(default_nq, default_dim, vector_data_type)
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 101,
                                 "err_msg": err_msg})

        # 3. search collection without data after load
        if vector_data_type == DataType.INT8_VECTOR:
            idx = self.prepare_index_params(client)[0]
            idx.add_index(field_name=ct.default_float_vec_field_name,
                          index_type="HNSW", metric_type="L2")
            self.create_index(client, collection_name, index_params=idx)
        else:
            idx = self.prepare_index_params(client)[0]
            idx.add_index(field_name=ct.default_float_vec_field_name,
                          index_type="FLAT", metric_type="COSINE")
            self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

        # 4. search with data inserted but not load again
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": insert_ids,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_empty_collection_with_partition(self):
        """
        target: test search with empty collection
        method: 1. collection an empty collection with partitions
                2. load
                3. search
        expected: return 0 result
        """
        # 1. initialize without data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        par_name = "search_partition_0"
        self.create_partition(client, collection_name, partition_name=par_name)

        # 2. search collection without data after load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

        # 3. search a partition without data after load
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    partition_names=[par_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_partition_deleted(self):
        """
        target: test search deleted partition
        method: 1. create a collection with partitions
                2. delete a partition
                3. search the deleted partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition and insert data
        deleted_par_name = "search_partition_0"
        self.create_partition(client, collection_name, partition_name=deleted_par_name)
        data = cf.gen_row_data_by_schema(nb=1000, schema=schema)
        self.insert(client, collection_name, data=data, partition_name=deleted_par_name)
        self.flush(client, collection_name)

        # 2. delete partition
        log.info("test_search_partition_deleted: deleting a partition")
        self.drop_partition(client, collection_name, partition_name=deleted_par_name)
        log.info("test_search_partition_deleted: deleted a partition")

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search after delete partitions
        log.info("test_search_partition_deleted: searching deleted partition")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    partition_names=[deleted_par_name],
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "partition name search_partition_0 not found"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partition_not_existed(self):
        """
        target: test search not existed partition
        method: search with not existed partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="IVF_FLAT", metric_type="L2", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)

        # 3. search the non exist partition
        partition_name = "search_non_exist"
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    partition_names=[partition_name],
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "partition name %s not found" % partition_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("reorder_k", [100])
    def test_search_scann_with_invalid_reorder_k(self, reorder_k):
        """
        target: test search with invalid nq
        method: search with invalid nq
        expected: raise exception and report the error
        """
        # initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="SCANN", metric_type="L2", params={"nlist": 1024})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search
        search_params = {"metric_type": "L2", "params": {"nprobe": 10, "reorder_k": reorder_k}}
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=search_params, limit=reorder_k + 1,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "reorder_k(100) should be larger than k(101)"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_binary(self):
        """
        target: test search within binary data (invalid parameter)
        method: search with wrong metric type
        expected: raise exception and report the error
        """
        # 1. initialize with binary data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name,
                      index_type="BIN_IVF_FLAT", metric_type="JACCARD", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search with exception
        _, search_binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        wrong_search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        # err_code 65535: BIN_IVF_FLAT now returns metric type mismatch at search time
        # (previously returned 1100 during index/collection migration)
        self.search(client, collection_name,
                    data=search_binary_vectors[:default_nq],
                    anns_field=ct.default_binary_vec_field_name,
                    search_params=wrong_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "metric type not match"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_flat_with_L2(self):
        """
        target: search binary collection using FlAT with L2
        method: search binary collection using FLAT with L2
        expected: raise exception and report error
        """
        # 1. initialize with binary data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, metric_type="JACCARD")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search and assert
        _, search_binary_vectors = cf.gen_binary_vectors(2, default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        self.search(client, collection_name,
                    data=search_binary_vectors[:default_nq],
                    anns_field=ct.default_binary_vec_field_name,
                    search_params=search_params, limit=default_limit,
                    filter=f"{ct.default_int64_field_name} >= 0",
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "metric type not match: invalid "
                                            "parameter[expected=JACCARD][actual=L2]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", ["int63", ""])
    @pytest.mark.parametrize("enable_dynamic", [True, False])
    def test_search_with_output_fields_not_exist(self, output_fields, enable_dynamic):
        """
        target: test search with output fields
        method: search with non-exist output_field
        expected: raise exception for non-dynamic or empty string fields
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search
        log.info("test_search_with_output_fields_not_exist: Searching collection %s" %
                 collection_name)
        if enable_dynamic and output_fields == "int63":
            # dynamic field enabled: non-existent field name returns success (treated as dynamic field)
            self.search(client, collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=default_search_params, limit=default_limit,
                        filter=v2_invalid_search_exp,
                        output_fields=[output_fields])
        elif output_fields == "":
            # empty string output field
            self.search(client, collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=default_search_params, limit=default_limit,
                        filter=v2_invalid_search_exp,
                        output_fields=[output_fields],
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 1,
                                     ct.err_msg: "is illegal"})
        else:
            # non-dynamic: non-existent field raises error
            self.search(client, collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=default_search_params, limit=default_limit,
                        filter=v2_invalid_search_exp,
                        output_fields=[output_fields],
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 65535,
                                     ct.err_msg: "field int63 not exist"})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("index", ct.all_index_types[-2:])
    def test_search_output_field_vector_after_gpu_index(self, index):
        """
        target: test search with vector as output field
        method: 1. create a collection and insert data
                2. create an index which doesn't output vectors
                3. load and search
        expected: raise exception and report the error
        """
        # 1. create a collection and insert data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create an index which doesn't output vectors
        params = cf.get_index_params_params(index)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type=index, metric_type="L2", params=params)
        self.create_index(client, collection_name, index_params=idx)

        # 3. load and search
        self.load_collection(client, collection_name)
        search_params = cf.get_search_params_params(index)
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params={"params": search_params}, limit=default_limit,
                    output_fields=[default_search_field],
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "not supported"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ignore_growing", [1.2, "string", [True]])
    def test_search_invalid_ignore_growing_param(self, ignore_growing):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. search with param ignore_growing invalid
        expected: raise exception
        """
        # 1. create a collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=10, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. insert data again
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=100)
        self.insert(client, collection_name, data=data)

        # 3. search with param ignore_growing=invalid
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}, "ignore_growing": ignore_growing}
        vector = [[random.random() for _ in range(default_dim)] for _ in range(1)]
        self.search(client, collection_name,
                    data=vector[:default_nq], anns_field=default_search_field,
                    search_params=search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999,
                                 "err_msg": "parse ignore growing field failed"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_invalid_radius_range_filter_L2(self):
        """
        target: test range search with invalid radius and range_filter for L2
        method: range search with radius smaller than range_filter
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=100, schema=schema)
        self.insert(client, collection_name, data=data)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params=idx)
        # 3. load
        self.load_collection(client, collection_name)

        # 4. range search
        log.info("test_range_search_invalid_radius_range_filter_L2: Range searching collection %s" %
                 collection_name)
        range_search_params = {"metric_type": "L2", "params": {"nprobe": 10, "radius": 1, "range_filter": 10}}
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=range_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "must be less than radius"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_invalid_radius_range_filter_IP(self):
        """
        target: test range search with invalid radius and range_filter for IP
        method: range search with radius larger than range_filter
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=100, schema=schema)
        self.insert(client, collection_name, data=data)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="FLAT", metric_type="IP")
        self.create_index(client, collection_name, index_params=idx)
        # 3. load
        self.load_collection(client, collection_name)

        # 4. range search
        log.info("test_range_search_invalid_radius_range_filter_IP: Range searching collection %s" %
                 collection_name)
        range_search_params = {"metric_type": "IP",
                               "params": {"nprobe": 10, "radius": 10, "range_filter": 1}}
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=range_search_params, limit=default_limit,
                    filter=v2_invalid_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "must be greater than radius"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_dynamic_compare_two_fields(self):
        """
        target: test search compare with two fields for dynamic collection
        method: 1.create collection , insert data, enable dynamic function
                2.search with two fields comparisons
        expected: Raise exception
        """
        # create collection, insert data, enable dynamic field
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        data = [{ct.default_string_field_name: str(i),
                 ct.default_float_vec_field_name: [random.random() for _ in range(default_dim)]}
                for i in range(1)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # create indexes
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="IVF_SQ8", metric_type="COSINE", params={"nlist": 64})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search with two fields comparison
        expr = f'{ct.default_float_field_name} >= {ct.default_int64_field_name}'
        search_vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expr,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": "error: two column comparison with JSON type is not supported"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_ef_less_than_limit(self):
        """
        target: test the scenario which search with ef less than limit
        method: 1. create collection
                2. search with ef less than limit
        expected: raise exception and report the error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="HNSW", metric_type="L2",
                      params={"M": 8, "efConstruction": 256})
        self.create_index(client, collection_name, index_params=idx)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_params = {"metric_type": "L2", "params": {"ef": 10}}
        self.search(client, collection_name,
                    data=vectors, anns_field=ct.default_float_vec_field_name,
                    search_params=search_params, limit=100,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "query failed: N6milvus21ExecOperatorExceptionE :Operator::GetOutput failed"})


class TestMilvusClientSearchValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("new_field_data_type", [DataType.INT64, DataType.INT8, DataType.INT16, DataType.INT32,
                                                     DataType.FLOAT, DataType.DOUBLE, DataType.BOOL, DataType.VARCHAR,
                                                     DataType.ARRAY, DataType.JSON])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_query_default(self, new_field_data_type, is_flush):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 5. add field
        if new_field_data_type == DataType.ARRAY:
            self.add_collection_field(client, collection_name, field_name="field_new", data_type=new_field_data_type,
                                      element_type=DataType.INT64, max_capacity=12, max_length=64, nullable=True)
        else:
            self.add_collection_field(client, collection_name, field_name="field_new", data_type=new_field_data_type,
                                      nullable=True, max_length=100)
        if is_flush:
            self.flush(client, collection_name)
        # 6. check the old search is not impacted after add field
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 7. check the old query is not impacted after add field
        for row in rows:
            row["field_new"] = None
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 8. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is not null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        # 9. query filtered with the new field
        self.query(client, collection_name, filter="field_new is null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.query(client, collection_name, filter="field_new is not null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: [],
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("new_field_data_type", [DataType.INT64, DataType.INT8, DataType.INT16, DataType.INT32])
    @pytest.mark.parametrize("is_flush", [True])
    @pytest.mark.skip(reason="issue #42629")
    def test_milvus_client_search_query_add_new_field_with_default_value_int(self, new_field_data_type, is_flush):
        """
        target: test search with add field using default value
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 5. add field
        if new_field_data_type == DataType.INT8:
            field_type = np.int8
        elif new_field_data_type == DataType.INT16:
            field_type = np.int16
        elif new_field_data_type == DataType.INT32:
            field_type = np.int32
        elif new_field_data_type == DataType.INT64:
            field_type = np.int64
        else:
            raise Exception(f"Unsupported type {new_field_data_type}")

        default_value = field_type(1)

        self.add_collection_field(client, collection_name, field_name="field_new", data_type=new_field_data_type,
                                  nullable=True, default_value=default_value)

        if is_flush:
            self.flush(client, collection_name)
        time.sleep(5)
        # 6. check the old search is not impacted after add field
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 7. check the old query is not impacted after add field
        for row in rows:
            row["field_new"] = field_type(1)

        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 8. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new == 1",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        # 9. query filtered with the new field
        self.query(client, collection_name, filter="field_new == 1",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.query(client, collection_name, filter="field_new is null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: [],
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("new_field_data_type", [DataType.FLOAT, DataType.DOUBLE])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("use_numpy_float", [True, False])
    def test_milvus_client_search_query_add_new_field_with_default_value_float(self, new_field_data_type, is_flush, use_numpy_float):
        """
        target: test search with add field using default value
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        # Skip use_numpy_float parameter when new_field_data_type is DOUBLE
        if new_field_data_type == DataType.DOUBLE and not use_numpy_float:
            pytest.skip("DOUBLE type doesn't need to consider use_numpy_float parameter")
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 5. add field
        default_value = 1.0
        if new_field_data_type == DataType.FLOAT:
            if use_numpy_float:
                default_value = np.float32(1.0)
        elif new_field_data_type == DataType.DOUBLE:
            default_value = np.float64(1.0)
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=new_field_data_type,
                                  nullable=True, default_value=default_value)
        if is_flush:
            self.flush(client, collection_name)
        # 6. check the old search is not impacted after add field
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 7. check the old query is not impacted after add field
        for row in rows:
            row["field_new"] = default_value
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 8. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new == 1",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        # 9. query filtered with the new field
        self.query(client, collection_name, filter="field_new == 1",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.query(client, collection_name, filter="field_new is null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: [],
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("new_field_data_type", [DataType.BOOL])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_query_add_new_field_with_default_value_bool(self, new_field_data_type, is_flush):
        """
        target: test search with add field using default value
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 5. add field
        default_value = True
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=new_field_data_type,
                                  nullable=True, default_value=default_value)
        if is_flush:
            self.flush(client, collection_name)
        # 6. check the old search is not impacted after add field
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 7. check the old query is not impacted after add field
        for row in rows:
            row["field_new"] = default_value
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 8. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new == True",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        # 9. query filtered with the new field
        self.query(client, collection_name, filter="field_new == True",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.query(client, collection_name, filter="field_new is null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: [],
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("new_field_data_type", [DataType.VARCHAR])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_query_add_new_field_with_default_value_varchar(self, new_field_data_type, is_flush):
        """
        target: test search with add field using default value
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 5. add field
        default_value = "1"
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=new_field_data_type,
                                  nullable=True, max_length=100, default_value=default_value)
        if is_flush:
            self.flush(client, collection_name)
        # 6. check the old search is not impacted after add field
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 7. check the old query is not impacted after add field
        for row in rows:
            row["field_new"] = default_value
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 8. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new >='0'",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        # 9. query filtered with the new field
        self.query(client, collection_name, filter="field_new >='0'",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.query(client, collection_name, filter="field_new is null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: [],
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("new_field_data_type", [DataType.JSON])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_query_add_new_field_with_default_value_json(self, new_field_data_type, is_flush):
        """
        target: test search with add field using default value
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 5. add field
        default_value = None
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=new_field_data_type,
                                  nullable=True, max_length=100, default_value=default_value)
        if is_flush:
            self.flush(client, collection_name)
        # 6. check the old search is not impacted after add field
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 7. check the old query is not impacted after add field
        for row in rows:
            row["field_new"] = default_value
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 8. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is not null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        # 9. query filtered with the new field
        self.query(client, collection_name, filter="field_new is null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.query(client, collection_name, filter="field_new is not null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: [],
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("new_field_data_type", [DataType.ARRAY])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_query_add_new_field_with_default_value_array(self, new_field_data_type, is_flush):
        """
        target: test search with add field using default value
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 5. add field
        default_value = None
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=new_field_data_type,
                                  nullable=True, element_type=DataType.INT64, max_capacity=12, max_length=100,
                                  default_value=default_value)
        if is_flush:
            self.flush(client, collection_name)
        # 6. check the old search is not impacted after add field
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 7. check the old query is not impacted after add field
        for row in rows:
            row["field_new"] = default_value
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 8. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is not null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        # 9. query filtered with the new field
        self.query(client, collection_name, filter="field_new is null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.query(client, collection_name, filter="field_new is not null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: [],
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)


    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_search_query_self_creation_default(self, nullable):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field("nullable_field", DataType.INT64, nullable=True, default_value=10)
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i), "nullable_field": None, "array_field": None} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_rename_search_query_default(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        old_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, old_name, default_dim, consistency_level="Strong")
        collections = self.list_collections(client)[0]
        assert old_name in collections
        c_info = self.describe_collection(client, old_name,
                                          check_task=CheckTasks.check_describe_collection_property,
                                          check_items={"collection_name": old_name,
                                                       "dim": default_dim,
                                                       "consistency_level": 0})[0]

        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=c_info)
        self.insert(client, old_name, rows)
        self.flush(client, old_name)
        self.wait_for_index_ready(client, collection_name=old_name, index_name='vector')

        vectors_to_search = cf.gen_vectors(ct.default_nq, default_dim)
        insert_ids = [item.get('id') for item in rows]
        old_search_res = self.search(client, old_name, vectors_to_search,
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"enable_milvus_client_api": True,
                                                  "nq": ct.default_nq,
                                                  "ids": insert_ids,
                                                  "pk_name": "id",
                                                  "limit": default_limit})[0]
        old_query_res = self.query(client, old_name, filter=default_search_exp,
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: rows,
                                                "with_vec": True})[0]

        new_name = old_name + "new"
        self.rename_collection(client, old_name, new_name)
        self.describe_collection(client, new_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": new_name,
                                              "dim": default_dim})

        # search again after rename collection
        new_search_res = self.search(client, new_name, vectors_to_search,
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"enable_milvus_client_api": True,
                                                  "nq": ct.default_nq,
                                                  "ids": insert_ids,
                                                  "pk_name": "id",
                                                  "limit": default_limit})[0]
        new_query_res = self.query(client, new_name, filter=default_search_exp,
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: rows,
                                                "with_vec": True})[0]
        assert old_search_res[0].ids == new_search_res[0].ids
        assert old_query_res == new_query_res

        rows = cf.gen_row_data_by_schema(nb=200, schema=c_info, start=default_nb)
        error = {ct.err_code: 0, ct.err_msg: "collection not found"}
        self.insert(client, old_name, rows,
                    check_task=CheckTasks.err_res,
                    check_items=error)
        self.insert(client, new_name, rows)
        new_ids = [item.get('id') for item in rows]
        insert_ids.extend(new_ids)
        self.search(client, new_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "ids": insert_ids,
                                 "pk_name": "id",
                                 "limit": default_limit})

        self.release_collection(client, new_name)
        self.drop_collection(client, new_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_array_insert_search(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
            default_float_field_name: i * 1.0,
            default_int32_array_field_name: [i, i + 1, i + 2],
            default_string_array_field_name: [str(i), str(i + 1), str(i + 2)]
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 25110")
    def test_milvus_client_search_query_string(self):
        """
        target: test search (high level api) for string primary key
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_different_metric_types_not_specifying_in_search_params(self, metric_type, auto_id):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, metric_type=metric_type, auto_id=auto_id,
                               consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        if auto_id:
            for row in rows:
                row.pop(default_primary_key_field_name)
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        # search_params = {"metric_type": metric_type}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    output_fields=[default_primary_key_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("pymilvus issue #1866")
    def test_milvus_client_search_different_metric_types_specifying_in_search_params(self, metric_type, auto_id):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, metric_type=metric_type, auto_id=auto_id,
                               consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        if auto_id:
            for row in rows:
                row.pop(default_primary_key_field_name)
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        search_params = {"metric_type": metric_type}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    search_params=search_params,
                    output_fields=[default_primary_key_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_delete_with_ids(self):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)[0]
        # 3. delete
        delete_num = 3
        self.delete(client, collection_name, ids=[i for i in range(delete_num)])
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        self.search(client, collection_name, vectors_to_search, limit=default_nb,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        # 5. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[delete_num:],
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_delete_after_add_field(self):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=DataType.INT64,
                                  nullable=True, max_length=100)
        for row in rows:
            row["field_new"] = None
        # 3. delete
        delete_num = 3
        self.delete(client, collection_name, ids=[i for i in range(delete_num)])
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        self.search(client, collection_name, vectors_to_search, limit=default_nb,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        # 5. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[delete_num:],
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 6. insert to the new added field
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i), "field_new": i} for i in
                range(delete_num)]
        self.insert(client, collection_name, rows)
        # 7. flush
        self.flush(client, collection_name)
        limit = default_nb
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search, limit=default_nb,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        # 8. delete
        self.delete(client, collection_name, filter=f"field_new >=0 and field_new <={delete_num}")
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        self.search(client, collection_name, vectors_to_search, limit=default_nb,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_delete_with_filters(self):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)[0]
        # 3. delete
        delete_num = 3
        self.delete(client, collection_name, filter=f"id < {delete_num}")
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        self.search(client, collection_name, vectors_to_search, limit=default_nb,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        # 5. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[delete_num:],
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_client_search_with_iterative_filter(self):
        """
        target: test search with iterative filter
        method: create connection, collection, insert, search with iterative filter
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        dim = 32
        pk_field_name = 'id'
        vector_field_name = 'embeddings'
        str_field_name = 'title'
        json_field_name = 'json_field'
        max_length = 16
        schema.add_field(pk_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(str_field_name, DataType.VARCHAR, max_length=max_length)
        schema.add_field(json_field_name, DataType.JSON)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name, metric_type="COSINE",
                               index_type="IVF_FLAT", params={"nlist": 128})
        index_params.add_index(field_name=str_field_name)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            pk_field_name: i,
            vector_field_name: list(rng.random((1, dim))[0]),
            str_field_name: cf.gen_str_by_length(max_length),
            json_field_name: {"number": i}
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=DataType.VARCHAR,
                                  nullable=True, max_length=100)
        # 3. search
        search_vector = list(rng.random((1, dim))[0])
        search_params = {'hints': "iterative_filter",
                         'params': cf.get_search_params_params('IVF_FLAT')}
        self.search(client, collection_name, data=[search_vector], filter='id >= 10',
                    search_params=search_params, limit=default_limit)
        not_supported_hints = "not_supported_hints"
        error = {ct.err_code: 0,
                 ct.err_msg: f"Create Plan by expr failed:  => hints: {not_supported_hints} not supported"}
        search_params = {'hints': not_supported_hints,
                         'params': cf.get_search_params_params('IVF_FLAT')}
        self.search(client, collection_name, data=[search_vector], filter='id >= 10',
                    search_params=search_params, check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_client_search_with_expr_float_vector(self):
        """
        target: test search using float vector field as filter
        method: create connection, collection, insert, search with float vector field as filter
        expected: raise error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        dim = 5
        pk_field_name = 'id'
        vector_field_name = 'embeddings'
        str_field_name = 'title'
        json_field_name = 'json_field'
        max_length = 16
        schema.add_field(pk_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(str_field_name, DataType.VARCHAR, max_length=max_length)
        schema.add_field(json_field_name, DataType.JSON)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name, metric_type="COSINE",
                               index_type="IVF_FLAT", params={"nlist": 128})
        index_params.add_index(field_name=str_field_name)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            pk_field_name: i,
            vector_field_name: list(rng.random((1, dim))[0]),
            str_field_name: cf.gen_str_by_length(max_length),
            json_field_name: {"number": i}
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # 3. search
        search_vector = list(rng.random((1, dim))[0])
        raw_vector = [random.random() for _ in range(dim)]
        vectors = np.array(raw_vector, dtype=np.float32)
        error = {ct.err_code: 1100,
                 ct.err_msg: "failed to create query plan: cannot parse expression"}
        self.search(client, collection_name, data=[search_vector], filter=f"{vector_field_name} == {raw_vector}",
                    search_params=default_search_params, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.search(client, collection_name, data=[search_vector], filter=f"{vector_field_name} == {vectors}",
                    search_params=default_search_params, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)


class TestMilvusClientSearchNullExpr(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr(self, nullable, null_expr_op):
        """
        target: test search with null expression on int64 fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.INT64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=DataType.JSON,
                                  nullable=True, max_length=100)
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        insert_ids = [str(i) for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is null",
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter="field_new is not null",
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_search_null_expr_int8(self, nullable):
        """
        target: test search with null expression on int8 fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.INT8, nullable=nullable)
        # schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
        #                  max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.int8(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        null_expr_ops = ["is null", "IS NULL", "is not null", "IS NOT NULL"]
        for null_expr_op in null_expr_ops:
            null_expr = nullable_field_name + " " + null_expr_op
            insert_ids = [str(i) for i in range(default_nb)]
            if nullable:
                if "not" in null_expr or "NOT" in null_expr:
                    insert_ids = []
                    limit = 0

                else:
                    limit = default_limit
            else:
                if "not" in null_expr or "NOT" in null_expr:
                    limit = default_limit
                else:
                    insert_ids = []
                    limit = 0
            self.search(client, collection_name, vectors_to_search,
                        filter=null_expr,
                        consistency_level="Strong",
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                    "nq": len(vectors_to_search),
                                    "ids": insert_ids,
                                    "pk_name": default_primary_key_field_name,
                                    "limit": limit})

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_search_null_expr_int16(self, nullable):
        """
        target: test search with null expression on int16 fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.INT16, nullable=nullable)
        # schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
        #                  max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.int16(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        null_expr_ops = ["is null", "IS NULL", "is not null", "IS NOT NULL"]
        for null_expr_op in null_expr_ops:
            insert_ids = [str(i) for i in range(default_nb)]
            null_expr = nullable_field_name + " " + null_expr_op
            if nullable:
                if "not" in null_expr or "NOT" in null_expr:
                    insert_ids = []
                    limit = 0

                else:
                    limit = default_limit
            else:
                if "not" in null_expr or "NOT" in null_expr:
                    limit = default_limit
                else:
                    insert_ids = []
                    limit = 0
            self.search(client, collection_name, vectors_to_search,
                        filter=null_expr,
                        consistency_level="Strong",
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                    "nq": len(vectors_to_search),
                                    "ids": insert_ids,
                                    "pk_name": default_primary_key_field_name,
                                    "limit": limit})
        self.drop_collection(client, collection_name)

############################################################
# Needs to modify to remove parameterize and use for loop ##
############################################################
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_int32(self, nullable, null_expr_op):
        """
        target: test search with null expression on int32 fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.INT32, nullable=nullable)
        # schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
        #                  max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.int32(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_float(self, nullable, null_expr_op):
        """
        target: test search with null expression on float fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.FLOAT, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": i * 1.0} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_double(self, nullable, null_expr_op):
        """
        target: test search with null expression on double fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.DOUBLE, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.double(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_bool(self, nullable, null_expr_op):
        """
        target: test search with null expression on bool fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.BOOL, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.bool_(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_varchar(self, nullable, null_expr_op):
        """
        target: test search with null expression on varchar fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.VARCHAR, nullable=nullable, max_length=128)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("json_flat_index", [True, False])
    def test_milvus_client_search_null_expr_json(self, nullable, json_flat_index):
        """
        target: test search with null expression on json fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.JSON, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        if json_flat_index:
            index_params.add_index(field_name=nullable_field_name, index_name="json_index", index_type="INVERTED",
                                params={"json_cast_type": "json",
                                        "json_path": f"{nullable_field_name}['a']['b']"})
            index_params.add_index(field_name=nullable_field_name, index_name="json_index_1", index_type="INVERTED",
                                params={"json_cast_type": "json",
                                        "json_path": f"{nullable_field_name}['a']['c']"})
        else:
            index_params.add_index(field_name=nullable_field_name, index_name="json_index", index_type="INVERTED",
                                params={"json_cast_type": "double",
                                        "json_path": f"{nullable_field_name}['a']['b']"})
            index_params.add_index(field_name=nullable_field_name, index_name="json_index_1", index_type="INVERTED",
                                params={"json_cast_type": "varchar",
                                        "json_path": f"{nullable_field_name}['a']['c']"})
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), nullable_field_name: None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), nullable_field_name: {'a': {'b': i, 'c': None}}} for i in
                    range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        null_expr_ops = ["is null", "IS NULL", "is not null", "IS NOT NULL"]
        for null_expr_op in null_expr_ops:
            insert_ids = [str(i) for i in range(default_nb)]
            null_expr = nullable_field_name + " " + null_expr_op
            if nullable:
                if "not" in null_expr or "NOT" in null_expr:
                    insert_ids = []
                    limit = 0

                else:
                    limit = default_limit
            else:
                if "not" in null_expr or "NOT" in null_expr:
                    limit = default_limit
                else:
                    insert_ids = []
                    limit = 0
            self.search(client, collection_name, vectors_to_search,
                        filter=null_expr,
                        output_fields=[nullable_field_name],
                        consistency_level="Strong",
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                    "nq": len(vectors_to_search),
                                    "ids": insert_ids,
                                    "pk_name": default_primary_key_field_name,
                                    "limit": limit})

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_search_null_expr_json_after_flush(self, nullable):
        """
        target: test search with null expression on json fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.JSON, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), nullable_field_name: None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), nullable_field_name: {'a': {'b': i, 'c': None}}} for i in
                    range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. flush
        self.flush(client, collection_name)
        # 4. create vector and json index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(field_name=nullable_field_name, index_name="json_index", index_type="INVERTED",
                               params={"json_cast_type": "DOUBLE",
                                       "json_path": f"{nullable_field_name}['a']['b']"})
        index_params.add_index(field_name=nullable_field_name, index_name="json_index_1", index_type="INVERTED",
                               params={"json_cast_type": "double",
                                       "json_path": f"{nullable_field_name}['a']['c']"})
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 5. search
        vectors_to_search = rng.random((1, dim))
        null_expr_ops = ["is null", "IS NULL", "is not null", "IS NOT NULL"]
        for null_expr_op in null_expr_ops:
            insert_ids = [str(i) for i in range(default_nb)]
            null_expr = nullable_field_name + " " + null_expr_op
            if nullable:
                if "not" in null_expr or "NOT" in null_expr:
                    insert_ids = []
                    limit = 0

                else:
                    limit = default_limit
            else:
                if "not" in null_expr or "NOT" in null_expr:
                    limit = default_limit
                else:
                    insert_ids = []
                    limit = 0
            self.search(client, collection_name, vectors_to_search,
                        filter=null_expr,
                        output_fields=[nullable_field_name],
                        consistency_level="Strong",
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                    "nq": len(vectors_to_search),
                                    "ids": insert_ids,
                                    "pk_name": default_primary_key_field_name,
                                    "limit": limit})

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    @pytest.mark.parametrize("is_scalar_index", [True, False])
    @pytest.mark.parametrize("scalar_index_type", ["AUTOINDEX", "INVERTED", "BITMAP"])
    def test_milvus_client_search_null_expr_array(self, nullable, is_flush, is_release,
                                                  is_scalar_index, scalar_index_type):
        """
        target: test search with null expression on array fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        if is_scalar_index:
            index_params.add_index(nullable_field_name, index_type=scalar_index_type)
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": [1, 2]} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        if is_flush:
            self.flush(client, collection_name)
        if is_release:
            self.release_collection(client, collection_name)
            self.drop_index(client, collection_name, default_vector_field_name)
            self.drop_index(client, collection_name, nullable_field_name)
            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(default_vector_field_name, metric_type="COSINE")
            if is_scalar_index:
                index_params.add_index(nullable_field_name, index_type=scalar_index_type)
            self.create_index(client, collection_name, index_params)
            self.load_collection(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        null_expr_ops = ["is null", "IS NULL", "is not null", "IS NOT NULL"]
        for null_expr_op in null_expr_ops:
            insert_ids = [str(i) for i in range(default_nb)]
            null_expr = nullable_field_name + " " + null_expr_op
            if nullable:
                if "not" in null_expr or "NOT" in null_expr:
                    insert_ids = []
                    limit = 0
                else:
                    limit = default_limit
            else:
                if "not" in null_expr or "NOT" in null_expr:
                    limit = default_limit
                else:
                    insert_ids = []
                    limit = 0
            self.search(client, collection_name, vectors_to_search,
                        filter=null_expr,
                        output_fields=[nullable_field_name],
                        consistency_level="Strong",
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                    "nq": len(vectors_to_search),
                                    "ids": insert_ids,
                                    "pk_name": default_primary_key_field_name,
                                    "limit": limit})

        self.drop_collection(client, collection_name)


_json_path_index_params = [
    ("INVERTED", "BOOL"),
    ("INVERTED", "DOUBLE"),
    ("INVERTED", "VARCHAR"),
    ("INVERTED", "JSON"),
    ("STL_SORT", "DOUBLE"),
    ("STL_SORT", "VARCHAR"),
    ("BITMAP", "BOOL"),
    ("BITMAP", "VARCHAR"),
]


class TestMilvusClientSearchJsonPathIndex(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=_json_path_index_params, ids=[f"{t[0]}_{t[1]}" for t in _json_path_index_params])
    def json_index_params(self, request):
        yield request.param

    @pytest.fixture(scope="function")
    def supported_varchar_scalar_index(self, json_index_params):
        yield json_index_params[0]

    @pytest.fixture(scope="function")
    def supported_json_cast_type(self, json_index_params):
        yield json_index_params[1]

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_json_path_index_default(self, enable_dynamic_field, supported_json_cast_type,
                                                          supported_varchar_scalar_index, is_flush):
        """
        target: test search after the json path index created
        method: Search after creating json path index
        Step: 1. create schema
              2. prepare index_params with the required vector index params
              3. create collection with the above schema and index params
              4. insert
              5. flush if specified
              6. prepare json path index params
              7. create json path index using the above index params created in step 6
              8. create the same json path index again
              9. search with expressions related with the json paths
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 60, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i, "c": i}}} for i in
                range(default_nb)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: i} for i in
                range(default_nb, default_nb + 10)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {}} for i in
                range(default_nb + 10, default_nb + 20)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [1, 2, 3]}} for i in
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': 1}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 40, default_nb + 50)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': 1}} for i in
                range(default_nb + 50, default_nb + 60)]
        self.insert(client, collection_name, rows)
        if is_flush:
            self.flush(client, collection_name)
        # 2. prepare index params
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="FLAT", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_name=index_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '1',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '2',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '3',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '4',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        # 3. create index
        self.create_index(client, collection_name, index_params)
        # 4. create same json index twice
        self.create_index(client, collection_name, index_params)
        # 5. search without filter
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb + 60)]
        self.search(client, collection_name, vectors_to_search,
                    output_fields=[json_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 6. search with filter on json without output_fields
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        insert_ids = [default_nb / 2]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name} == {default_nb + 5}"
        insert_ids = [default_nb + 5]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name}['a'][0] == 1"
        insert_ids = [i for i in range(default_nb + 20, default_nb + 30)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'][0]['b'] == 1"
        insert_ids = [i for i in range(default_nb + 30, default_nb + 40)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'] == 1"
        insert_ids = [i for i in range(default_nb + 50, default_nb + 60)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_search_json_path_index_default_index_name(self, enable_dynamic_field,
                                                                     supported_json_cast_type,
                                                                     supported_varchar_scalar_index):
        """
        target: test json path index without specifying the index_name parameter
        method: create json path index without specifying the index_name parameter
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, is_primary=True, auto_id=False,
                         max_length=128)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        # 4. create index
        self.create_index(client, collection_name, index_params)
        # 5. search with filter on json with output_fields
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        vectors_to_search = [vectors[0]]
        insert_ids = [str(int(default_nb / 2))]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[json_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #40636")
    def test_milvus_client_search_json_path_index_on_non_json_field(self, supported_json_cast_type,
                                                                    supported_varchar_scalar_index):
        """
        target: test json path index on non-json field
        method: create json path index on int64 field
        expected: successfully with original inverted index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=default_primary_key_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{default_string_field_name}['a']['b']"})
        # 3. create index
        index_name = default_string_field_name
        self.create_index(client, collection_name, index_params)
        self.describe_index(client, collection_name, index_name,
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                # "json_cast_type": supported_json_cast_type, # issue 40426
                                "json_path": f"{default_string_field_name}['a']['b']",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": default_string_field_name,
                                "index_name": index_name})
        self.flush(client, collection_name)
        # 5. search with filter on json with output_fields
        expr = f"{default_primary_key_field_name} >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[default_string_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_search_diff_index_same_field_diff_index_name_diff_index_params(self, enable_dynamic_field,
                                                                                          supported_json_cast_type,
                                                                                          supported_varchar_scalar_index):
        """
        target: test search after different json path index with different default index name at the same time
        method: Search after different json path index with different default index name at the same index_params object
        expected: Search successfully
        """
        if enable_dynamic_field:
            pytest.skip('need to fix the field name when enabling dynamic field')
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        self.load_collection(client, collection_name)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        self.create_index(client, collection_name, index_params)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        self.create_index(client, collection_name, index_params)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        self.create_index(client, collection_name, index_params)
        # 4. release and load collection to make sure new index is loaded
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        # 5. search with filter on json with output_fields
        expr = f"{json_field_name}['a']['b'] >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[default_string_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    def test_milvus_client_json_search_index_same_json_path_diff_field(self, enable_dynamic_field,
                                                                       supported_json_cast_type,
                                                                       supported_varchar_scalar_index, is_flush,
                                                                       is_release):
        """
        target: test search after creating same json path for different field
        method: Search after creating same json path for different field
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
            schema.add_field(json_field_name + "1", DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {'b': i}},
                 json_field_name + "1": {'a': {'b': i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 3. release and drop index if specified
        if is_release:
            self.release_collection(client, collection_name)
            self.drop_index(client, collection_name, default_vector_field_name)
        # 4. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        self.create_index(client, collection_name, index_params)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name + "1",
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}1['a']['b']"})
        # 5. create index with json path index
        self.create_index(client, collection_name, index_params)
        if is_release:
            self.load_collection(client, collection_name)
        # 6. search with filter on json with output_fields on each json field
        expr = f"{json_field_name}['a']['b'] >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[json_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}1['a']['b'] >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[json_field_name + "1"],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_json_path_index_before_load(self, enable_dynamic_field, supported_json_cast_type,
                                                              supported_varchar_scalar_index, is_flush):
        """
        target: test search after creating json path index before load
        method: Search after creating json path index before load
        Step: 1. create schema
              2. prepare index_params with vector index params
              3. create collection with the above schema and index params
              4. release collection
              5. insert
              6. flush if specified
              7. prepare json path index params
              8. create index
              9. load collection
              10. search
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. release collection
        self.release_collection(client, collection_name)
        # 3. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 50, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in
                range(default_nb)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: i} for i in
                range(default_nb, default_nb + 10)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {}} for i in
                range(default_nb + 10, default_nb + 20)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [1, 2, 3]}} for i in
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': 1}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 40, default_nb + 50)]
        self.insert(client, collection_name, rows)
        # 4. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 5. prepare index params
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_name=index_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '1',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '2',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '3',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '4',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        # 5. create index
        self.create_index(client, collection_name, index_params)
        # 6. load collection
        self.load_collection(client, collection_name)
        # 7. search with filter on json without output_fields
        vectors_to_search = [vectors[0]]
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        insert_ids = [default_nb / 2]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name} == {default_nb + 5}"
        insert_ids = [default_nb + 5]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name}['a'][0] == 1"
        insert_ids = [i for i in range(default_nb + 20, default_nb + 30)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'][0]['b'] == 1"
        insert_ids = [i for i in range(default_nb + 30, default_nb + 40)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_json_path_index_after_release_load(self, enable_dynamic_field,
                                                                     supported_json_cast_type,
                                                                     supported_varchar_scalar_index, is_flush):
        """
        target: test search after creating json path index after release and load
        method: Search after creating json path index after release and load
        Step: 1. create schema
              2. prepare index_params with vector index params
              3. create collection with the above schema and index params
              4. insert
              5. flush if specified
              6. prepare json path index params
              7. create index
              8. release collection
              9. create index again
              10. load collection
              11. search with expressions related with the json paths
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 50, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in
                range(default_nb)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: i} for i in
                range(default_nb, default_nb + 10)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {}} for i in
                range(default_nb + 10, default_nb + 20)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [1, 2, 3]}} for i in
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': 1}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 40, default_nb + 50)]
        self.insert(client, collection_name, rows)
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 4. prepare index params
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_name=index_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '1',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '2',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '3',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '4',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        # 5. create json index
        self.create_index(client, collection_name, index_params)
        # 6. release collection
        self.release_collection(client, collection_name)
        # 7. create json index again
        self.create_index(client, collection_name, index_params)
        # 8. load collection
        self.load_collection(client, collection_name)
        # 9. search with filter on json without output_fields
        vectors_to_search = [vectors[0]]
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        insert_ids = [default_nb / 2]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name} == {default_nb + 5}"
        insert_ids = [default_nb + 5]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name}['a'][0] == 1"
        insert_ids = [i for i in range(default_nb + 20, default_nb + 30)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'][0]['b'] == 1"
        insert_ids = [i for i in range(default_nb + 30, default_nb + 40)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})


class TestMilvusClientSearchDecayRerank(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[DataType.INT8, DataType.INT16, DataType.INT32,
                                              DataType.FLOAT, DataType.DOUBLE])
    def rerank_fields(self, request):
        tags = request.config.getoption("--tags", default=['L0', 'L1', 'L2'], skip=True)
        if CaseLabel.L2 not in tags:
            if request.param not in [DataType.INT8, DataType.FLOAT]:
                pytest.skip(f"skip rerank field type {request.param}")
        yield request.param

    @pytest.fixture(scope="function", params=["STL_SORT", "INVERTED", "AUTOINDEX", ""])
    def scalar_index(self, request):
        tags = request.config.getoption("--tags", default=['L0', 'L1', 'L2'], skip=True)
        if CaseLabel.L2 not in tags:
            if request.param not in ["INVERTED", ""]:
                pytest.skip(f"skip scalar index type {request.param}")
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("function", ["gauss", "linear", "exp"])
    @pytest.mark.parametrize("scale", [100, 10000, 100.0])
    @pytest.mark.parametrize("origin", [-1, 0, 200, 2000])
    @pytest.mark.parametrize("offset", [0, 10, 1.2, 2000])
    @pytest.mark.parametrize("decay", [0.5])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_with_reranker(self, function, scale, origin, offset, decay, is_flush):
        """
        target: test search with reranker
        method: create connection, collection, insert and search
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        if is_flush:
            self.flush(client, collection_name)
        # 3. search
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": function,
                "origin": origin,
                "offset": offset,
                "decay": decay,
                "scale": scale
            }
        )
        vectors_to_search = rng.random((1, dim))
        # search without output_fields
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        # search with output_fields
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    output_fields=[ct.default_reranker_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        # range search
        params = {"radius": 0, "range_filter": 1}
        self.search(client, collection_name, vectors_to_search, search_params=params, ranker=my_rerank_fn,
                    output_fields=[ct.default_reranker_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("function", ["gauss", "linear", "exp"])
    def test_milvus_client_search_with_reranker_default_offset_decay(self, function):
        """
        target: test search with reranker with default offset(0) and decay(0.5) value
        method: create connection, collection, insert and search
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": function,
                "origin": 0,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=9 and {ct.default_reranker_field_name}<=4",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=0 and {ct.default_reranker_field_name}<=10",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_reranker_default_value_field(self):
        """
        target: test search with reranker with default offset(0) and decay(0.5) value
        method: create connection, collection, insert and search
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=False, default_value=0)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=9 and {ct.default_reranker_field_name}<=4",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0}
                    )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_clustering", [True, False])
    def test_milvus_client_search_with_reranker_partition_key_field(self, enable_dynamic_field, is_clustering):
        """
        target: test search with reranker with partition key field
        method: create connection, collection, insert and search
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=False, is_partition_key=True,
                         is_clustering_key=is_clustering)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. compact
        self.compact(client, collection_name, is_clustering=is_clustering)
        # 4. search
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=9 and {ct.default_reranker_field_name}<=4",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=0 and {ct.default_reranker_field_name}<=10",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_with_reranker_all_supported_datatype_field(self, rerank_fields):
        """
        target: test search with reranker with partition key field
        method: create connection, collection, insert and search
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, rerank_fields)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = []
        for i in range(default_nb):
            if rerank_fields == DataType.INT8:
                value = np.int8(i)
            elif rerank_fields == DataType.INT16:
                value = np.int16(i)
            elif rerank_fields == DataType.INT32:
                value = np.int32(i)
            elif rerank_fields == DataType.FLOAT:
                value = np.float32(i)
            elif rerank_fields == DataType.DOUBLE:
                value = np.float64(i)
            single_row = {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
                          ct.default_reranker_field_name: value}
            rows.append(single_row)
        self.insert(client, collection_name, rows)
        # 3. compact
        self.compact(client, collection_name)
        # 4. flush
        self.flush(client, collection_name)
        # 5. search
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=9 and {ct.default_reranker_field_name}<=4",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=0 and {ct.default_reranker_field_name}<=10",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mmap", [True, False])
    def test_milvus_client_search_with_reranker_scalar_index(self, rerank_fields, scalar_index, mmap):
        """
        Test search functionality with reranker using scalar index in Milvus client.

        This test verifies the search operation works correctly when using a reranker with different scalar index types.
        It covers various scenarios including:
        - Different data types for rerank fields (INT8, INT16, INT32, FLOAT, DOUBLE)
        - Different index types (STL_SORT, INVERTED, AUTOINDEX, "")
        - Memory-mapped and non-memory-mapped configurations

        The test performs the following steps:
        1. Creates a collection with specified schema and index parameters
        2. Inserts test data with appropriate data types
        3. Builds indexes on both vector and scalar fields
        4. Executes search operations with reranking function
        5. Validates search results with different filter conditions
        6. Cleans up by releasing collection and dropping indexes

        Note: This is an L1 (basic functionality) test case.
        target: test search with reranker with scalar index
        method: create connection, collection, insert and search
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, rerank_fields, mmap_enabled=mmap)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, index_type='HNSW', metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = []
        for i in range(default_nb):
            if rerank_fields == DataType.INT8:
                value = np.int8(i)
            elif rerank_fields == DataType.INT16:
                value = np.int16(i)
            elif rerank_fields == DataType.INT32:
                value = np.int32(i)
            elif rerank_fields == DataType.INT64:
                value = i
            elif rerank_fields == DataType.FLOAT:
                value = np.float32(i)
            elif rerank_fields == DataType.DOUBLE:
                value = np.float64(i)
            single_row = {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
                          ct.default_reranker_field_name: value}
            rows.append(single_row)
        self.insert(client, collection_name, rows)
        # flush
        self.flush(client, collection_name)
        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_reranker_field_name, index_type=scalar_index, params={})
        # 3. create index
        self.create_index(client, collection_name, index_params)
        # 4. compact
        self.compact(client, collection_name)
        self.wait_for_index_ready(client, collection_name, index_name=ct.default_reranker_field_name)
        self.wait_for_index_ready(client, collection_name, index_name=default_vector_field_name)

        # 5. search
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=9 and {ct.default_reranker_field_name}<=4",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=0 and {ct.default_reranker_field_name}<=10",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        # 5. release collection
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, ct.default_reranker_field_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 6. create index
        params = {"metric_type": "COSINE"}
        if scalar_index != "STL_SORT":
            params['mmap.enabled'] = mmap
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_reranker_field_name, index_type=scalar_index, params=params)
        index_params.add_index(field_name=default_vector_field_name, index_type='HNSW', params=params)
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=ct.default_reranker_field_name)
        self.wait_for_index_ready(client, collection_name, index_name=default_vector_field_name)
        self.load_collection(client, collection_name)
        # vectors_to_search = rng.random((1, dim))
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=9 and {ct.default_reranker_field_name}<=4",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    filter=f"{ct.default_reranker_field_name}>=0 and {ct.default_reranker_field_name}<=10",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )

    @staticmethod
    def _gauss_decay(origin, scale, decay, offset, distance):
        adj = max(0, abs(distance - origin) - offset)
        sigma_sq = scale ** 2 / math.log(decay)
        return math.exp(adj ** 2 / sigma_sq)

    @staticmethod
    def _exp_decay(origin, scale, decay, offset, distance):
        adj = max(0, abs(distance - origin) - offset)
        lam = math.log(decay) / scale
        return math.exp(lam * adj)

    @staticmethod
    def _linear_decay(origin, scale, decay, offset, distance):
        adj = max(0, abs(distance - origin) - offset)
        slope = (1 - decay) / scale
        return max(decay, 1 - slope * adj)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("function", ["gauss", "linear", "exp"])
    @pytest.mark.parametrize("decay", [0.1, 0.5, 0.9])
    def test_milvus_client_search_reranker_decay_score_ordering(self, function, decay):
        """
        target: verify decay reranker produces scores ordered by distance from origin
        method: insert rows with identical vectors and varying reranker_field values,
                search with decay reranker, check score ordering matches distance ordering
        expected: results ordered by distance from origin (closer = higher score), all scores > 0
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.FLOAT, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert rows with identical vectors but different reranker_field values
        fixed_vector = [0.5] * dim
        field_values = [0, 10, 50, 100, 200, 500]
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: fixed_vector,
                 ct.default_reranker_field_name: np.float32(field_values[i])}
                for i in range(len(field_values))]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search with decay reranker
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": function,
                "origin": 0,
                "offset": 0,
                "decay": decay,
                "scale": 100
            }
        )
        vectors_to_search = [fixed_vector]
        res = self.search(client, collection_name, vectors_to_search, limit=len(field_values),
                          ranker=my_rerank_fn,
                          output_fields=[ct.default_reranker_field_name])[0]
        # 4. verify score ordering: closer to origin should have higher score
        results = res[0]
        assert len(results) == len(field_values), \
            f"Expected {len(field_values)} results, got {len(results)}"
        scores = [r["distance"] for r in results]
        reranker_values = [r[ct.default_reranker_field_name] for r in results]
        log.info(f"function={function}, decay={decay}, scores={scores}, reranker_values={reranker_values}")
        # All scores must be positive
        for i, score in enumerate(scores):
            assert score > 0, f"Score at position {i} should be > 0, got {score}"
        # Scores must be in descending order (higher score first)
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], \
                f"Scores not in descending order: scores[{i}]={scores[i]} < scores[{i + 1}]={scores[i + 1]}"
        # Distance from origin must be in ascending order (closer first)
        distances = [abs(v) for v in reranker_values]
        for i in range(len(distances) - 1):
            assert distances[i] <= distances[i + 1], \
                f"Distances not in ascending order: dist[{i}]={distances[i]} > dist[{i + 1}]={distances[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("function", ["gauss", "linear", "exp"])
    def test_milvus_client_search_reranker_decay_score_ratio(self, function):
        """
        target: verify decay reranker produces mathematically correct score ratios
        method: insert rows with identical vectors at known distances, search with decay reranker,
                compare actual score ratios against Python-computed expected ratios
        expected: score ratios match expected decay function ratios within tolerance
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.FLOAT, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert rows with identical vectors at specific distances
        fixed_vector = [0.5] * dim
        origin = 0
        scale = 100
        decay_param = 0.5
        offset = 0
        field_values = [0, 25, 50, 75, 100]
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: fixed_vector,
                 ct.default_reranker_field_name: np.float32(field_values[i])}
                for i in range(len(field_values))]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search with decay reranker
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": function,
                "origin": origin,
                "offset": offset,
                "decay": decay_param,
                "scale": scale
            }
        )
        vectors_to_search = [fixed_vector]
        res = self.search(client, collection_name, vectors_to_search, limit=len(field_values),
                          ranker=my_rerank_fn,
                          output_fields=[ct.default_reranker_field_name])[0]
        # 4. build mapping from reranker_field value to actual score
        results = res[0]
        assert len(results) == len(field_values), \
            f"Expected {len(field_values)} results, got {len(results)}"
        actual_scores = {}
        for r in results:
            actual_scores[r[ct.default_reranker_field_name]] = r["distance"]
        # 5. compute expected decay scores using Python formulas
        decay_funcs = {"gauss": self._gauss_decay, "linear": self._linear_decay, "exp": self._exp_decay}
        decay_fn = decay_funcs[function]
        expected_scores = {}
        for v in field_values:
            expected_scores[v] = decay_fn(origin, scale, decay_param, offset, v)
        log.info(f"function={function}, actual_scores={actual_scores}, expected_scores={expected_scores}")
        # 6. verify score ratios match expected ratios
        # Use distance=0 as reference point (decay score = 1.0, so actual score = base_score)
        ref_value = 0
        ref_actual = actual_scores[ref_value]
        ref_expected = expected_scores[ref_value]
        epsilon = 0.01
        for v in field_values:
            if v == ref_value:
                continue
            actual_ratio = actual_scores[v] / ref_actual
            expected_ratio = expected_scores[v] / ref_expected
            log.info(f"  distance={v}: actual_ratio={actual_ratio:.6f}, expected_ratio={expected_ratio:.6f}")
            assert abs(actual_ratio - expected_ratio) < epsilon, \
                f"Score ratio mismatch for distance={v}: actual_ratio={actual_ratio:.6f}, " \
                f"expected_ratio={expected_ratio:.6f}, diff={abs(actual_ratio - expected_ratio):.6f}"
        # 7. additionally verify that score at distance=scale equals decay * score at origin
        if scale in actual_scores:
            actual_decay_at_scale = actual_scores[scale] / actual_scores[ref_value]
            assert abs(actual_decay_at_scale - decay_param) < epsilon, \
                f"At distance=scale, expected decay≈{decay_param}, got {actual_decay_at_scale:.6f}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_decay_offset_effect(self):
        """
        target: verify decay reranker offset parameter works correctly
        method: insert rows with identical vectors at various distances, search with decay reranker
                using offset=10, verify items within offset zone have equal scores and items beyond
                have decreasing scores
        expected: items at distance <= offset have same score (decay=1.0), items beyond offset have
                  strictly decreasing scores
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.FLOAT, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert rows with identical vectors
        fixed_vector = [0.5] * dim
        field_values = [0, 5, 10, 15, 50, 100]
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: fixed_vector,
                 ct.default_reranker_field_name: np.float32(field_values[i])}
                for i in range(len(field_values))]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search with decay reranker using offset=10
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": 10,
                "decay": 0.5,
                "scale": 100
            }
        )
        vectors_to_search = [fixed_vector]
        res = self.search(client, collection_name, vectors_to_search, limit=len(field_values),
                          ranker=my_rerank_fn,
                          output_fields=[ct.default_reranker_field_name])[0]
        # 4. build mapping from reranker_field value to actual score
        results = res[0]
        assert len(results) == len(field_values), \
            f"Expected {len(field_values)} results, got {len(results)}"
        score_map = {}
        for r in results:
            score_map[r[ct.default_reranker_field_name]] = r["distance"]
        log.info(f"offset_test score_map={score_map}")
        # 5. verify items within offset zone (distance <= 10) have the same score
        within_offset = [0, 5, 10]
        epsilon = 1e-4
        ref_score = score_map[within_offset[0]]
        for v in within_offset:
            assert abs(score_map[v] - ref_score) < epsilon, \
                f"Items within offset should have equal scores: score({v})={score_map[v]}, " \
                f"score({within_offset[0]})={ref_score}"
        # 6. verify items beyond offset have strictly decreasing scores
        beyond_offset = [15, 50, 100]
        # Items within offset should have higher score than items beyond offset
        for v in beyond_offset:
            assert score_map[v] < ref_score, \
                f"Score beyond offset should be < offset zone score: score({v})={score_map[v]}, ref={ref_score}"
        # Items beyond offset should be in strictly decreasing order by distance
        for i in range(len(beyond_offset) - 1):
            assert score_map[beyond_offset[i]] > score_map[beyond_offset[i + 1]], \
                f"Scores beyond offset not decreasing: score({beyond_offset[i]})={score_map[beyond_offset[i]]} " \
                f"<= score({beyond_offset[i + 1]})={score_map[beyond_offset[i + 1]]}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("function", ["gauss", "linear", "exp"])
    def test_milvus_client_search_reranker_decay_nullable_field(self, function):
        """
        target: verify decay reranker works with nullable input field
        method: create collection with nullable reranker field, insert rows with some null values,
                search with decay reranker
        expected: search successfully, results include both null and non-null rows
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection with nullable reranker field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.FLOAT, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert rows: some with values, some with None
        rng = np.random.default_rng(seed=19530)
        rows = []
        for i in range(default_nb):
            row = {default_primary_key_field_name: i,
                   default_vector_field_name: list(rng.random((1, dim))[0])}
            if i % 5 == 0:
                row[ct.default_reranker_field_name] = None
            else:
                row[ct.default_reranker_field_name] = np.float32(i)
            rows.append(row)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search with decay reranker
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": function,
                "origin": 0,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        # search with output_fields
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    output_fields=[ct.default_reranker_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_decay_nullable_field_score_ordering(self):
        """
        target: verify decay reranker produces correct score ordering with nullable field,
                null values should be ranked last
        method: insert rows with identical vectors, some with null reranker_field values,
                search with decay reranker, verify non-null rows are ranked before null rows
        expected: non-null rows ranked by distance from origin, null rows ranked last
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection with nullable reranker field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.FLOAT, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert rows with identical vectors: mix of non-null and null values
        fixed_vector = [0.5] * dim
        # ids 0-3: non-null values at known distances from origin
        # ids 4-5: null values
        rows = [
            {default_primary_key_field_name: 0, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: np.float32(0)},
            {default_primary_key_field_name: 1, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: np.float32(50)},
            {default_primary_key_field_name: 2, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: np.float32(100)},
            {default_primary_key_field_name: 3, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: np.float32(200)},
            {default_primary_key_field_name: 4, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: None},
            {default_primary_key_field_name: 5, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: None},
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search with decay reranker
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        vectors_to_search = [fixed_vector]
        res = self.search(client, collection_name, vectors_to_search, limit=len(rows),
                          ranker=my_rerank_fn,
                          output_fields=[ct.default_reranker_field_name])[0]
        results = res[0]
        log.info(f"nullable decay results: {results}")
        # 4. verify: non-null rows should have positive scores and be ordered by distance
        non_null_results = [r for r in results if r.get(ct.default_reranker_field_name) is not None]
        null_results = [r for r in results if r.get(ct.default_reranker_field_name) is None]
        # non-null scores should be positive and in descending order
        non_null_scores = [r["distance"] for r in non_null_results]
        for i, score in enumerate(non_null_scores):
            assert score > 0, f"Non-null score at position {i} should be > 0, got {score}"
        for i in range(len(non_null_scores) - 1):
            assert non_null_scores[i] >= non_null_scores[i + 1], \
                f"Non-null scores not in descending order: {non_null_scores[i]} < {non_null_scores[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_decay_nullable_field_null_score_last(self):
        """
        target: verify decay reranker with nullable input field produces null scores
                for null-field rows and always ranks them last
        method: insert rows with identical vectors, some with null reranker_field values,
                search with decay reranker, verify null-score rows appear at the end
        expected: non-null rows ranked first with positive descending scores,
                  null rows ranked last with null distance
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection with nullable reranker field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.FLOAT, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert rows: ids 0-3 non-null, ids 4-6 null
        fixed_vector = [0.5] * dim
        rows = [
            {default_primary_key_field_name: 0, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: np.float32(0)},
            {default_primary_key_field_name: 1, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: np.float32(50)},
            {default_primary_key_field_name: 2, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: np.float32(100)},
            {default_primary_key_field_name: 3, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: np.float32(200)},
            {default_primary_key_field_name: 4, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: None},
            {default_primary_key_field_name: 5, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: None},
            {default_primary_key_field_name: 6, default_vector_field_name: fixed_vector,
             ct.default_reranker_field_name: None},
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search with decay reranker
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        vectors_to_search = [fixed_vector]
        res = self.search(client, collection_name, vectors_to_search, limit=len(rows),
                          ranker=my_rerank_fn,
                          output_fields=[ct.default_reranker_field_name])[0]
        results = res[0]
        log.info(f"nullable decay null-score-last results: {results}")
        # 4. verify: null-field rows should be ranked last with null distance
        n_total = len(results)
        n_null = 3  # ids 4, 5, 6
        n_non_null = n_total - n_null
        # first n_non_null results should have non-null scores, positive and descending
        for i in range(n_non_null):
            score = results[i]["distance"]
            assert score is not None, f"Expected non-null score at position {i}, got None"
            assert score > 0, f"Non-null score at position {i} should be > 0, got {score}"
            field_val = results[i].get(ct.default_reranker_field_name)
            assert field_val is not None, f"Expected non-null field at position {i}"
        non_null_scores = [results[i]["distance"] for i in range(n_non_null)]
        for i in range(len(non_null_scores) - 1):
            assert non_null_scores[i] >= non_null_scores[i + 1], \
                f"Non-null scores not in descending order at {i}: {non_null_scores[i]} < {non_null_scores[i + 1]}"
        # last n_null results should have null field values and null distance
        for i in range(n_non_null, n_total):
            field_val = results[i].get(ct.default_reranker_field_name)
            assert field_val is None, \
                f"Expected null field at position {i}, got {field_val}"
            score = results[i]["distance"]
            assert score is None or score == 0, \
                f"Expected null/zero score for null-field row at position {i}, got {score}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_reranker_decay_nullable_all_types(self, rerank_fields):
        """
        target: verify decay reranker works with nullable fields of all supported numeric types
        method: create collection with nullable reranker field of various types,
                insert rows with some null values, search with decay reranker
        expected: search successfully with all supported numeric types
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection with nullable reranker field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, rerank_fields, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert rows with some null values
        rng = np.random.default_rng(seed=19530)
        rows = []
        for i in range(default_nb):
            row = {default_primary_key_field_name: i,
                   default_vector_field_name: list(rng.random((1, dim))[0])}
            if i % 5 == 0:
                row[ct.default_reranker_field_name] = None
            else:
                if rerank_fields == DataType.INT8:
                    row[ct.default_reranker_field_name] = np.int8(i % 127)
                elif rerank_fields == DataType.INT16:
                    row[ct.default_reranker_field_name] = np.int16(i)
                elif rerank_fields == DataType.INT32:
                    row[ct.default_reranker_field_name] = np.int32(i)
                elif rerank_fields == DataType.FLOAT:
                    row[ct.default_reranker_field_name] = np.float32(i)
                elif rerank_fields == DataType.DOUBLE:
                    row[ct.default_reranker_field_name] = np.float64(i)
            rows.append(row)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search with decay reranker
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    output_fields=[ct.default_reranker_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit}
                    )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_decay_nullable_all_null(self):
        """
        target: verify decay reranker handles the case where all reranker field values are null
        method: create collection with nullable reranker field, insert rows with all null values,
                search with decay reranker
        expected: search successfully, all results have null reranker field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection with nullable reranker field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.FLOAT, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert rows with all null reranker field values
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: None}
                for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search with decay reranker
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        res = self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                          output_fields=[ct.default_reranker_field_name],
                          check_task=CheckTasks.check_search_results,
                          check_items={"enable_milvus_client_api": True,
                                       "nq": len(vectors_to_search),
                                       "pk_name": default_primary_key_field_name,
                                       "limit": default_limit}
                          )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_decay_rerank_l2_metric_no_norm_score(self):
        """
        target: verify decay reranker with L2 metric and norm_score=false ranks
                results by "smaller distance = better match" semantics. The
                decay factor is then multiplied as a [0, 1] weight.
        method: 1. create collection with FLAT index + L2 metric + INT64 ts field
                2. insert rows with vectors at progressively larger L2 distances
                   from a fixed query, all with ts=origin so decay factor=1.0
                3. search with decay reranker, no norm_score
        expected: row with smallest L2 distance ranks first.
        Without the fix, raw L2 × decay is sorted DESC, putting the WORST L2
        match first (i.e., the entire ordering is reversed).
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # 1. create collection with L2 metric + FLAT index for exact distances
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64,
                         is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("ts", DataType.INT64, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, index_type="FLAT",
                               metric_type="L2")
        self.create_collection(client, collection_name, dimension=dim,
                               schema=schema, index_params=index_params)
        # 2. insert deterministic data:
        #    row i has vector [0.1*i] * dim
        #    all rows have ts = 1000 (decay origin) → decay factor = 1.0
        nrows = 5
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: [0.1 * i] * dim,
                 "ts": 1000}
                for i in range(nrows)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search with query vector matching row 0 exactly → row 0 has L2=0
        query_vector = [[0.0] * dim]
        decay_fn = Function(
            name="decay_l2",
            input_field_names=["ts"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 1000,
                "scale": 100,
                "decay": 0.5,
                # norm_score not set → defaults to false
            }
        )
        res = self.search(client, collection_name, query_vector,
                          limit=nrows, ranker=decay_fn,
                          output_fields=[default_primary_key_field_name])[0]
        results = res[0]
        ids = [r[default_primary_key_field_name] for r in results]
        scores = [r["distance"] for r in results]
        log.info(f"decay+L2 no_norm result ids={ids} scores={scores}")
        assert len(results) == nrows, \
            f"expected {nrows} results, got {len(results)}: ids={ids}"
        # Row 0 has L2=0 (perfect match) and decay=1.0 → must rank first.
        # Without the fix the ordering is exactly reversed (row 4 first).
        assert ids[0] == 0, \
            f"row 0 (perfect L2 match) must rank first; got order {ids} with scores {scores}"
        # Scores must be non-increasing — decay rerank always produces
        # "larger = better" output regardless of metric direction.
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], \
                f"decay scores must be DESC; got scores[{i}]={scores[i]} < scores[{i + 1}]={scores[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_decay_rerank_timestamptz_field_rejected(self):
        """
        target: verify decay reranker rejects TIMESTAMPTZ input fields with a
                clear runtime error. Legacy decay code listed Timestamptz in
                its type-dispatch switch but the converter and GetNumericValue
                paths never supported it end-to-end, so this PR preserves
                legacy actual behavior — Timestamptz remains unsupported.
        method: create collection with a TIMESTAMPTZ field, attempt search
                with decay reranker using that field as input
        expected: error reporting unsupported field type Timestamptz.
        Note: in the proxy search pipeline, chain.FromSearchResultData
        (Arrow converter) runs *before* BuildRerankChain, so the user-visible
        error comes from the converter's "unsupported field type" branch
        rather than from chain validateInputField. Both layers reject
        Timestamptz; the converter just fires first end-to-end.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection with a TIMESTAMPTZ field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64,
                         is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("event_time", DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim,
                               schema=schema, index_params=index_params)
        # 2. insert (any data — chain converter rejects before any rerank
        #    logic actually runs)
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: list(rng.random((1, dim))[0]),
                 "event_time": "2025-01-01T00:00:00"}
                for i in range(10)]
        self.insert(client, collection_name, rows)
        # 3. search with decay reranker using the TIMESTAMPTZ field as input
        decay_fn = Function(
            name="decay_tstz",
            input_field_names=["event_time"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "scale": 100,
                "decay": 0.5,
            }
        )
        vectors_to_search = rng.random((1, dim))
        error = {ct.err_code: 65535,
                 ct.err_msg: "unsupported field type: Timestamptz"}
        self.search(client, collection_name, vectors_to_search, ranker=decay_fn,
                    check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientSearchModelRerank(TestMilvusClientV2Base):

    @pytest.fixture(scope="function")
    def setup_collection(self):
        """Setup collection for model rerank testing"""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dense_metric_type = "COSINE"

        # 1. create schema with embedding and bm25 functions
        schema = client.create_schema(enable_dynamic_field=False, auto_id=True)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("doc_id", DataType.VARCHAR, max_length=100)
        schema.add_field("document", DataType.VARCHAR, max_length=10000, enable_analyzer=True)
        schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=768)
        schema.add_field("bm25", DataType.SPARSE_FLOAT_VECTOR)

        # add bm25 function
        bm25_function = Function(
            name="bm25",
            input_field_names=["document"],
            output_field_names="bm25",
            function_type=FunctionType.BM25,
        )
        schema.add_function(bm25_function)

        # 2. prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="dense", index_type="FLAT", metric_type=dense_metric_type)
        index_params.add_index(
            field_name="sparse",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="IP",
        )
        index_params.add_index(
            field_name="bm25",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="BM25",
            params={"bm25_k1": 1.2, "bm25_b": 0.75},
        )

        # 3. create collection
        client.create_collection(
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # 4. insert data
        rows = []
        data_size = 3000
        for i in range(data_size):
            rows.append({
                "doc_id": str(i),
                "document": fake.text(),
                "sparse": {random.randint(1, 10000): random.random() for _ in range(100)},
                "dense": [random.random() for _ in range(768)]
            })
        client.insert(collection_name, rows)

        return collection_name

    def merge_and_dedup_hybrid_searchresults(self, result_a, result_b):
        final_result = []
        for i in range(len(result_a)):
            tmp_result = []
            tmp_ids = []
            for j in range(len(result_a[i])):
                tmp_result.append(result_a[i][j])
                tmp_ids.append(result_a[i][j]["id"])
            for j in range(len(result_b[i])):
                if result_b[i][j]["id"] not in tmp_ids:
                    tmp_result.append(result_b[i][j])
            final_result.append(tmp_result)
        return final_result

    def get_tei_rerank_results(self, query_texts, document_texts, tei_reranker_endpoint, enable_truncate=False):
        url = f"{tei_reranker_endpoint}/rerank"

        payload = json.dumps({
            "query": query_texts,
            "texts": document_texts
        })
        if enable_truncate:
            payload = json.dumps({
                "query": query_texts,
                "texts": document_texts,
                "truncate": True,
                "truncation_direction": "Right"
            })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        res = response.json()
        reranked_results = []
        for r in res:
            tmp = {
                "text": document_texts[r["index"]],
                "score": r["score"]
            }
            reranked_results.append(tmp)

        return reranked_results

    def get_vllm_rerank_results(self, query_texts, document_texts, vllm_reranker_endpoint, enable_truncate=False):
        url = f"{vllm_reranker_endpoint}/v2/rerank"

        payload = json.dumps({
            "query": query_texts,
            "documents": document_texts
        })
        if enable_truncate:
            payload = json.dumps({
                "query": query_texts,
                "documents": document_texts,
                "truncate_prompt_tokens": 512
            })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        res = response.json()["results"]

        log.debug("vllm rerank results:\n")
        for r in res:
            log.debug(f"r: {r}")
        reranked_results = []
        for r in res:
            tmp = {
                "text": r["document"]["text"],
                "score": r["relevance_score"]
            }
            reranked_results.append(tmp)

        return reranked_results

    def get_cohere_rerank_results(self, query_texts, document_texts,
                                  model_name="rerank-english-v3.0", max_tokens_per_doc=4096, **kwargs):
        COHERE_RERANKER_ENDPOINT = "https://api.cohere.ai"
        COHERE_API_KEY = os.getenv("COHERE_API_KEY")

        url = f"{COHERE_RERANKER_ENDPOINT}/v2/rerank"

        payload = {
            "model": model_name,
            "query": query_texts,
            "documents": document_texts,
            "top_n": len(document_texts)  # Cohere v2 uses "top_n" not "top_k"
        }

        if max_tokens_per_doc != 4096:
            payload["max_tokens_per_doc"] = max_tokens_per_doc

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {COHERE_API_KEY}'
        }

        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

        res = response.json()["results"]

        log.debug("cohere rerank results:\n")
        for r in res:
            log.debug(f"r: {r}")
        reranked_results = []
        for r in res:
            tmp = {
                "text": document_texts[r["index"]],  # Cohere returns index, not document text
                "score": r["relevance_score"]
            }
            reranked_results.append(tmp)

        return reranked_results

    def get_voyageai_rerank_results(self, query_texts, document_texts,
                                   model_name="rerank-2", truncation=True, **kwargs):
        VOYAGEAI_RERANKER_ENDPOINT = "https://api.voyageai.com"
        VOYAGEAI_API_KEY = os.getenv("VOYAGEAI_API_KEY")

        url = f"{VOYAGEAI_RERANKER_ENDPOINT}/v1/rerank"

        payload = {
            "model": model_name,
            "query": query_texts,
            "documents": document_texts,
            "top_k": len(document_texts),
            "truncation": truncation
        }

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {VOYAGEAI_API_KEY}'
        }

        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

        res = response.json()["data"]  # VoyageAI uses "data" field

        log.debug("voyageai rerank results:\n")
        for r in res:
            log.debug(f"r: {r}")
        reranked_results = []
        for r in res:
            tmp = {
                "text": document_texts[r["index"]],  # VoyageAI also returns index, not document text
                "score": r["relevance_score"]
            }
            reranked_results.append(tmp)

        return reranked_results

    def get_siliconflow_rerank_results(self, query_texts, document_texts,
                                      model_name="BAAI/bge-reranker-v2-m3", max_chunks_per_doc=None, overlap_tokens=None):
        SILICONFLOW_RERANKER_ENDPOINT = "https://api.siliconflow.cn"
        SILICONFLOW_API_KEY = os.getenv("SILICONFLOW_API_KEY")

        url = f"{SILICONFLOW_RERANKER_ENDPOINT}/v1/rerank"

        payload = {
            "model": model_name,
            "query": query_texts,
            "documents": document_texts
        }

        if max_chunks_per_doc is not None:
            payload["max_chunks_per_doc"] = max_chunks_per_doc
        if overlap_tokens is not None:
            payload["overlap_tokens"] = overlap_tokens

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {SILICONFLOW_API_KEY}'
        }

        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

        res = response.json()["results"]

        log.debug("siliconflow rerank results:\n")
        for r in res:
            log.debug(f"r: {r}")
        reranked_results = []
        for r in res:
            tmp = {
                "text": document_texts[r["index"]],
                "score": r["relevance_score"]
            }
            reranked_results.append(tmp)

        return reranked_results

    def display_side_by_side_comparison(self, query_text, milvus_results, gt_results, doc_to_original_mapping=None, milvus_scores=None, gt_scores=None):
        """
        Display side by side comparison of Milvus rerank results and ground truth results with PK values and scores
        """
        log.info(f"\n{'=' * 120}")
        log.info(f"Query: {query_text}")
        log.info(f"{'=' * 120}")

        # Display side by side comparison
        log.info(f"\n{'Milvus Rerank Results':<58} | {'Ground Truth Results':<58}")
        log.info(f"{'-' * 58} | {'-' * 58}")

        max_len = max(len(milvus_results), len(gt_results))

        for i in range(max_len):
            log.info(f"\nRank {i + 1}:")

            # Milvus result
            if i < len(milvus_results):
                milvus_doc = milvus_results[i].replace('\n', ' ')[:35] + "..." if len(milvus_results[i]) > 35 else \
                milvus_results[i].replace('\n', ' ')
                # Get PK if available
                milvus_pk = ""
                if doc_to_original_mapping and milvus_results[i] in doc_to_original_mapping:
                    milvus_pk = f" [PK: {doc_to_original_mapping[milvus_results[i]]['id']}]"
                # Get score if available
                milvus_score = ""
                if milvus_scores and i < len(milvus_scores):
                    milvus_score = f" [Score: {milvus_scores[i]:.8f}]"
                milvus_display = f"{milvus_doc}{milvus_pk}{milvus_score}"
                log.info(f"{milvus_display:<58}".ljust(58) + " | " + " " * 58)
            else:
                log.info(f"{'(no more results)':<58}".ljust(58) + " | " + " " * 58)

            # Ground truth result
            if i < len(gt_results):
                gt_doc = gt_results[i].replace('\n', ' ')[:35] + "..." if len(gt_results[i]) > 35 else gt_results[
                    i].replace('\n', ' ')
                # Get PK if available
                gt_pk = ""
                if doc_to_original_mapping and gt_results[i] in doc_to_original_mapping:
                    gt_pk = f" [PK: {doc_to_original_mapping[gt_results[i]]['id']}]"
                # Get score if available
                gt_score = ""
                if gt_scores and i < len(gt_scores):
                    gt_score = f" [Score: {gt_scores[i]:.8f}]"
                gt_display = f"{gt_doc}{gt_pk}{gt_score}"
                log.info(f"{' ' * 58} | {gt_display:<58}")
            else:
                log.info(f"{' ' * 58} | {'(no more results)':<58}")

            # Check if documents are the same
            if (i < len(milvus_results) and i < len(gt_results) and
                    milvus_results[i] == gt_results[i]):
                log.info(f"{'✓ Same document':<58} | {'✓ Same document':<58}")

            log.info(f"{'-' * 58} | {'-' * 58}")

    def compare_milvus_rerank_with_origin_rerank(self, query_texts, rerank_results, results_without_rerank,
                                                 enable_truncate=False,
                                                 provider_type=None,
                                                 **kwargs):
        # result length should be the same as nq
        if provider_type is None:
            raise Exception("provider_type parameter is required")

        assert len(results_without_rerank) == len(rerank_results)
        log.debug("results_without_rerank")
        for r in results_without_rerank:
            log.debug(r)
        log.debug("rerank_results")
        for r in rerank_results:
            log.debug(r)
        for i in range(len(results_without_rerank)):
            query_text = query_texts[i]
            document_texts = [x["document"] for x in results_without_rerank[i]]
            distances_without_rerank = [x["distance"] for x in results_without_rerank[i]]

            # Create mapping from document to original data (including pk)
            doc_to_original = {}
            for original_item in results_without_rerank[i]:
                doc_to_original[original_item["document"]] = original_item

            actual_rerank_results = [x["document"] for x in rerank_results[i]]
            distances = [x["distance"] for x in rerank_results[i]]
            log.debug(f"distances: {distances}")
            log.debug(f"distances_without_rerank: {distances_without_rerank}")
            limit = len(actual_rerank_results)

            # Call the appropriate rerank method based on provider type
            if provider_type == "tei":
                endpoint = kwargs.get("endpoint")
                if endpoint is None:
                    raise Exception("endpoint parameter is required for tei provider")
                raw_gt = self.get_tei_rerank_results(query_text, document_texts, endpoint,
                                                     enable_truncate=enable_truncate)[:limit]
            elif provider_type == "vllm":
                endpoint = kwargs.get("endpoint")
                if endpoint is None:
                    raise Exception("endpoint parameter is required for vllm provider")
                raw_gt = self.get_vllm_rerank_results(query_text, document_texts, endpoint,
                                                      enable_truncate=enable_truncate)[:limit]
            elif provider_type == "cohere":
                raw_gt = self.get_cohere_rerank_results(query_text, document_texts,
                                                        **kwargs)[:limit]
            elif provider_type == "voyageai":
                raw_gt = self.get_voyageai_rerank_results(query_text, document_texts,
                                                          **kwargs)[:limit]
            elif provider_type == "siliconflow":
                raw_gt = self.get_siliconflow_rerank_results(query_text, document_texts,
                                                             **kwargs)[:limit]
            else:
                raise Exception(f"Unsupported provider_type: {provider_type}")

            # Create list of (distance, pk, document) tuples for sorting
            gt_with_info = []
            for doc in raw_gt:
                original_item = doc_to_original.get(doc["text"])
                if original_item:
                    # Convert score to f32 precision for consistent sorting
                    f32_score = float(np.float32(doc["score"]))
                    gt_with_info.append((f32_score, original_item["id"], doc["text"]))

            # Sort by score descending first, then by pk (id) ascending when scores are equal
            gt_with_info.sort(key=lambda x: (-x[0], x[1]))

            # Extract the sorted documents and scores
            gt = [item[2] for item in gt_with_info]
            gt_scores = [item[0] for item in gt_with_info]

            # Side by side comparison of documents with scores
            self.display_side_by_side_comparison(query_text, actual_rerank_results, gt, doc_to_original,
                                               milvus_scores=distances, gt_scores=gt_scores)

            # Use strict comparison since scores are now normalized to f32 precision
            assert gt == actual_rerank_results, "Rerank result is different from ground truth rerank result"

    @pytest.mark.parametrize("ranker_model", [
        pytest.param("tei", marks=pytest.mark.tags(CaseLabel.L1)),
        pytest.param("vllm", marks=pytest.mark.tags(CaseLabel.L3)),
    ])
    @pytest.mark.parametrize("enable_truncate", [False, True])
    def test_milvus_client_single_vector_search_with_model_rerank(self, setup_collection, ranker_model, enable_truncate,
                                                                  tei_reranker_endpoint, vllm_reranker_endpoint):
        """
        target: test single vector search with model rerank using SciFact dataset
        method: test dense/sparse/bm25 search with model reranker separately and compare results with origin reranker
        expected: result should be the same
        """
        client = self._client()
        collection_name = setup_collection

        # 5. prepare search parameters for reranker
        nq = 2
        query_texts = [fake.text() for _ in range(nq)]
        if enable_truncate:
            # make query texts larger
            query_texts = [" ".join([fake.word() for _ in range(1024)]) for _ in range(nq)]
        tei_ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
                "truncate": enable_truncate,
                "truncation_direction": "Right"
            },
        )
        vllm_ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "vllm",
                "queries": query_texts,
                "endpoint": vllm_reranker_endpoint,
                "truncate": enable_truncate,
                "truncate_prompt_tokens": 512
            },
        )

        # 6. execute search with reranker
        if ranker_model == "tei":
            ranker = tei_ranker
        else:
            ranker = vllm_ranker

        for search_type in ["dense", "sparse", "bm25"]:
            log.info(f"Executing {search_type} search with model reranker")
            rerank_results = []
            results_without_rerank = None
            if search_type == "dense":

                data = [[random.random() for _ in range(768)] for _ in range(nq)]
                rerank_results = client.search(
                    collection_name,
                    data=data,
                    anns_field="dense",
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                results_without_rerank = client.search(
                    collection_name,
                    data=data,
                    anns_field="dense",
                    limit=10,
                    output_fields=["doc_id", "document"],
                )

            elif search_type == "sparse":
                data = [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)]
                rerank_results = client.search(
                    collection_name,
                    data=data,
                    anns_field="sparse",
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                results_without_rerank = client.search(
                    collection_name,
                    data=data,
                    anns_field="sparse",
                    limit=10,
                    output_fields=["doc_id", "document"],
                )
            elif search_type == "bm25":
                rerank_results = client.search(
                    collection_name,
                    data=query_texts,
                    anns_field="bm25",
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                    search_params={"metric_type": "BM25"}
                )
                results_without_rerank = client.search(
                    collection_name,
                    data=query_texts,
                    anns_field="bm25",
                    limit=10,
                    output_fields=["doc_id", "document"],
                )
            if ranker_model == "tei":
                self.compare_milvus_rerank_with_origin_rerank(query_texts, rerank_results, results_without_rerank,
                                                              enable_truncate=enable_truncate,
                                                              provider_type="tei",
                                                              endpoint=tei_reranker_endpoint)
            else:
                self.compare_milvus_rerank_with_origin_rerank(query_texts, rerank_results, results_without_rerank,
                                                              enable_truncate=enable_truncate,
                                                              provider_type="vllm",
                                                              endpoint=vllm_reranker_endpoint)

    @pytest.mark.parametrize("ranker_model", [
        pytest.param("tei", marks=pytest.mark.tags(CaseLabel.L1)),
        pytest.param("vllm", marks=pytest.mark.tags(CaseLabel.L3)),
    ])
    def test_milvus_client_hybrid_vector_search_with_model_rerank(self, setup_collection, ranker_model,
                                                                  tei_reranker_endpoint, vllm_reranker_endpoint):
        """
        target: test hybrid vector search with model rerank
        method: test dense+sparse/dense+bm25/sparse+bm25 search with model reranker
        expected: search successfully with model reranker
        """
        client = self._client()
        collection_name = setup_collection

        # 5. prepare search parameters for reranker
        nq = 2
        query_texts = [fake.text() for _ in range(nq)]
        tei_ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )
        vllm_ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "vllm",
                "queries": query_texts,
                "endpoint": vllm_reranker_endpoint,
            },
        )
        if ranker_model == "tei":
            ranker = tei_ranker
        else:
            ranker = vllm_ranker
        # 6. execute search with reranker
        for search_type in ["dense+sparse", "dense+bm25", "sparse+bm25"]:
            log.info(f"Executing {search_type} search with model reranker")
            rerank_results = []
            dense_search_param = {
                "data": [[random.random() for _ in range(768)] for _ in range(nq)],
                "anns_field": "dense",
                "param": {},
                "limit": 5,
            }
            dense = AnnSearchRequest(**dense_search_param)

            sparse_search_param = {
                "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)],
                "anns_field": "sparse",
                "param": {},
                "limit": 5,
            }
            bm25_search_param = {
                "data": query_texts,
                "anns_field": "bm25",
                "param": {},
                "limit": 5,
            }
            bm25 = AnnSearchRequest(**bm25_search_param)

            sparse = AnnSearchRequest(**sparse_search_param)
            results_without_rerank = None
            if search_type == "dense+sparse":

                rerank_results = client.hybrid_search(
                    collection_name,
                    reqs=[dense, sparse],
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                dense_results = client.search(
                    collection_name,
                    data=dense_search_param["data"],
                    anns_field="dense",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                sparse_results = client.search(
                    collection_name,
                    data=sparse_search_param["data"],
                    anns_field="sparse",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(dense_results, sparse_results)
            elif search_type == "dense+bm25":
                rerank_results = client.hybrid_search(
                    collection_name,
                    reqs=[dense, bm25],
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                dense_results = client.search(
                    collection_name,
                    data=dense_search_param["data"],
                    anns_field="dense",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                bm25_results = client.search(
                    collection_name,
                    data=bm25_search_param["data"],
                    anns_field="bm25",
                    limit=5,
                    output_fields=["doc_id", "document"],
                    search_params={"metric_type": "BM25"}
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(dense_results, bm25_results)
            elif search_type == "sparse+bm25":
                rerank_results = client.hybrid_search(
                    collection_name,
                    reqs=[sparse, bm25],
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                    search_params={"metric_type": "BM25"}
                )
                # Get results without rerank by using search separately and merging them
                sparse_results = client.search(
                    collection_name,
                    data=sparse_search_param["data"],
                    anns_field="sparse",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                bm25_results = client.search(
                    collection_name,
                    data=bm25_search_param["data"],
                    anns_field="bm25",
                    limit=5,
                    output_fields=["doc_id", "document"],
                    search_params={"metric_type": "BM25"}
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(sparse_results, bm25_results)
            if ranker_model == "tei":
                self.compare_milvus_rerank_with_origin_rerank(query_texts, rerank_results, results_without_rerank,
                                                              provider_type="tei",
                                                              endpoint=tei_reranker_endpoint)
            else:
                self.compare_milvus_rerank_with_origin_rerank(query_texts, rerank_results, results_without_rerank,
                                                              provider_type="vllm",
                                                              endpoint=vllm_reranker_endpoint)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("model_name", ["rerank-english-v3.0", "rerank-multilingual-v3.0"])
    @pytest.mark.parametrize("max_tokens_per_doc", [4096, 2048])
    def test_milvus_client_search_with_cohere_rerank_specific_params(self, setup_collection, model_name,
                                                                    max_tokens_per_doc):
        """
        target: test search with Cohere rerank model using specific parameters
        method: test dense search with Cohere reranker using different model_name and max_tokens_per_doc values
        expected: search successfully with Cohere reranker and specific parameters
        """
        client = self._client()
        collection_name = setup_collection

        # prepare search parameters for reranker
        nq = 2
        query_texts = [fake.text() for _ in range(nq)]

        cohere_ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "cohere",
                "queries": query_texts,
                "model_name": model_name,
                "max_tokens_per_doc": max_tokens_per_doc
            },
        )

        # execute dense search with Cohere reranker
        data = [[random.random() for _ in range(768)] for _ in range(nq)]
        rerank_results = client.search(
            collection_name,
            data=data,
            anns_field="dense",
            limit=10,
            output_fields=["doc_id", "document"],
            ranker=cohere_ranker,
            consistency_level="Strong",
        )

        results_without_rerank = client.search(
            collection_name,
            data=data,
            anns_field="dense",
            limit=10,
            output_fields=["doc_id", "document"],
        )

        self.compare_milvus_rerank_with_origin_rerank(query_texts, rerank_results, results_without_rerank,
                                                      provider_type="cohere",
                                                      model_name=model_name,
                                                      max_tokens_per_doc=max_tokens_per_doc)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("model_name", ["rerank-2", "rerank-2-lite"])
    @pytest.mark.parametrize("truncation", [True, False])
    def test_milvus_client_search_with_voyageai_rerank_specific_params(self, setup_collection, model_name,
                                                                      truncation):
        """
        target: test search with VoyageAI rerank model using specific parameters
        method: test dense search with VoyageAI reranker using different model_name and truncation values
        expected: search successfully with VoyageAI reranker and specific parameters
        """
        client = self._client()
        collection_name = setup_collection

        # prepare search parameters for reranker
        nq = 2
        query_texts = [fake.text() for _ in range(nq)]

        voyageai_ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "voyageai",
                "queries": query_texts,
                "model_name": model_name,
                "truncation": truncation
            },
        )

        # execute dense search with VoyageAI reranker
        data = [[random.random() for _ in range(768)] for _ in range(nq)]
        rerank_results = client.search(
            collection_name,
            data=data,
            anns_field="dense",
            limit=10,
            output_fields=["doc_id", "document"],
            ranker=voyageai_ranker,
            consistency_level="Strong",
        )

        results_without_rerank = client.search(
            collection_name,
            data=data,
            anns_field="dense",
            limit=10,
            output_fields=["doc_id", "document"],
        )

        self.compare_milvus_rerank_with_origin_rerank(query_texts, rerank_results, results_without_rerank,
                                                      provider_type="voyageai",
                                                      model_name=model_name,
                                                      truncation=truncation)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("model_name", ["BAAI/bge-reranker-v2-m3", "netease-youdao/bce-reranker-base_v1"])
    @pytest.mark.parametrize("max_chunks_per_doc,overlap_tokens", [(10, 80), (20, 120)])
    def test_milvus_client_search_with_siliconflow_rerank_specific_params(self, setup_collection, model_name,
                                                                         max_chunks_per_doc, overlap_tokens):
        """
        target: test search with SiliconFlow rerank model using specific parameters
        method: test dense search with SiliconFlow reranker using different model_name, max_chunks_per_doc and overlap_tokens values
        expected: search successfully with SiliconFlow reranker and specific parameters
        """
        client = self._client()
        collection_name = setup_collection

        # prepare search parameters for reranker
        nq = 2
        query_texts = [fake.text() for _ in range(nq)]

        siliconflow_ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "siliconflow",
                "queries": query_texts,
                "model_name": model_name,
                "max_chunks_per_doc": max_chunks_per_doc,
                "overlap_tokens": overlap_tokens
            },
        )

        # execute dense search with SiliconFlow reranker
        data = [[random.random() for _ in range(768)] for _ in range(nq)]
        rerank_results = client.search(
            collection_name,
            data=data,
            anns_field="dense",
            limit=10,
            output_fields=["doc_id", "document"],
            ranker=siliconflow_ranker,
            consistency_level="Strong",
        )

        results_without_rerank = client.search(
            collection_name,
            data=data,
            anns_field="dense",
            limit=10,
            output_fields=["doc_id", "document"],
        )

        self.compare_milvus_rerank_with_origin_rerank(query_texts, rerank_results, results_without_rerank,
                                                      provider_type="siliconflow",
                                                      model_name=model_name,
                                                      max_chunks_per_doc=max_chunks_per_doc,
                                                      overlap_tokens=overlap_tokens)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("model_name", ["rerank-english-v3.0", "rerank-multilingual-v3.0"])
    @pytest.mark.parametrize("max_tokens_per_doc", [4096, 2048])
    def test_milvus_client_hybrid_search_with_cohere_rerank_specific_params(self, setup_collection, model_name, max_tokens_per_doc):
        """
        target: test hybrid search with cohere rerank specific parameters
        method: test hybrid search with different cohere model names and max_tokens_per_doc values
        expected: hybrid search successfully with cohere reranker
        """
        client = self._client()
        collection_name = setup_collection

        nq = 2
        query_texts = [fake.text() for _ in range(nq)]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "cohere",
                "queries": query_texts,
                "model_name": model_name,
                "max_tokens_per_doc": max_tokens_per_doc
            },
        )

        # Test different hybrid search combinations
        for search_type in ["dense+sparse", "dense+bm25", "sparse+bm25"]:
            log.info(f"Executing {search_type} hybrid search with cohere reranker")

            dense_search_param = {
                "data": [[random.random() for _ in range(768)] for _ in range(nq)],
                "anns_field": "dense",
                "param": {},
                "limit": 5,
            }
            dense = AnnSearchRequest(**dense_search_param)

            sparse_search_param = {
                "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)],
                "anns_field": "sparse",
                "param": {},
                "limit": 5,
            }
            sparse = AnnSearchRequest(**sparse_search_param)

            bm25_search_param = {
                "data": query_texts,
                "anns_field": "bm25",
                "param": {},
                "limit": 5,
            }
            bm25 = AnnSearchRequest(**bm25_search_param)

            if search_type == "dense+sparse":
                reqs = [dense, sparse]
                # Get hybrid search results with reranker
                hybrid_results = client.hybrid_search(
                    collection_name,
                    reqs=reqs,
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                dense_results = client.search(
                    collection_name,
                    data=dense_search_param["data"],
                    anns_field="dense",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                sparse_results = client.search(
                    collection_name,
                    data=sparse_search_param["data"],
                    anns_field="sparse",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(dense_results, sparse_results)
            elif search_type == "dense+bm25":
                reqs = [dense, bm25]
                # Get hybrid search results with reranker
                hybrid_results = client.hybrid_search(
                    collection_name,
                    reqs=reqs,
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                dense_results = client.search(
                    collection_name,
                    data=dense_search_param["data"],
                    anns_field="dense",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                bm25_results = client.search(
                    collection_name,
                    data=bm25_search_param["data"],
                    anns_field="bm25",
                    limit=5,
                    output_fields=["doc_id", "document"],
                    search_params={"metric_type": "BM25"}
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(dense_results, bm25_results)
            else:  # sparse+bm25
                reqs = [sparse, bm25]
                # Get hybrid search results with reranker
                hybrid_results = client.hybrid_search(
                    collection_name,
                    reqs=reqs,
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                sparse_results = client.search(
                    collection_name,
                    data=sparse_search_param["data"],
                    anns_field="sparse",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                bm25_results = client.search(
                    collection_name,
                    data=bm25_search_param["data"],
                    anns_field="bm25",
                    limit=5,
                    output_fields=["doc_id", "document"],
                    search_params={"metric_type": "BM25"}
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(sparse_results, bm25_results)

            # Compare Milvus rerank results with origin rerank results
            self.compare_milvus_rerank_with_origin_rerank(query_texts, hybrid_results, results_without_rerank,
                                                          provider_type="cohere",
                                                          model_name=model_name,
                                                          max_tokens_per_doc=max_tokens_per_doc)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("model_name", ["rerank-2", "rerank-1"])
    @pytest.mark.parametrize("truncation", [True, False])
    def test_milvus_client_hybrid_search_with_voyageai_rerank_specific_params(self, setup_collection, model_name, truncation):
        """
        target: test hybrid search with voyageai rerank specific parameters
        method: test hybrid search with different voyageai model names and truncation values
        expected: hybrid search successfully with voyageai reranker
        """
        client = self._client()
        collection_name = setup_collection

        nq = 2
        query_texts = [fake.text() for _ in range(nq)]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "voyageai",
                "queries": query_texts,
                "model_name": model_name,
                "truncation": truncation
            },
        )

        # Test different hybrid search combinations
        for search_type in ["dense+sparse", "dense+bm25", "sparse+bm25"]:
            log.info(f"Executing {search_type} hybrid search with voyageai reranker")

            dense_search_param = {
                "data": [[random.random() for _ in range(768)] for _ in range(nq)],
                "anns_field": "dense",
                "param": {},
                "limit": 5,
            }
            dense = AnnSearchRequest(**dense_search_param)

            sparse_search_param = {
                "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)],
                "anns_field": "sparse",
                "param": {},
                "limit": 5,
            }
            sparse = AnnSearchRequest(**sparse_search_param)

            bm25_search_param = {
                "data": query_texts,
                "anns_field": "bm25",
                "param": {},
                "limit": 5,
            }
            bm25 = AnnSearchRequest(**bm25_search_param)

            if search_type == "dense+sparse":
                reqs = [dense, sparse]
                # Get hybrid search results with reranker
                hybrid_results = client.hybrid_search(
                    collection_name,
                    reqs=reqs,
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                dense_results = client.search(
                    collection_name,
                    data=dense_search_param["data"],
                    anns_field="dense",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                sparse_results = client.search(
                    collection_name,
                    data=sparse_search_param["data"],
                    anns_field="sparse",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(dense_results, sparse_results)
            elif search_type == "dense+bm25":
                reqs = [dense, bm25]
                # Get hybrid search results with reranker
                hybrid_results = client.hybrid_search(
                    collection_name,
                    reqs=reqs,
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                dense_results = client.search(
                    collection_name,
                    data=dense_search_param["data"],
                    anns_field="dense",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                bm25_results = client.search(
                    collection_name,
                    data=bm25_search_param["data"],
                    anns_field="bm25",
                    limit=5,
                    output_fields=["doc_id", "document"],
                    search_params={"metric_type": "BM25"}
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(dense_results, bm25_results)
            else:  # sparse+bm25
                reqs = [sparse, bm25]
                # Get hybrid search results with reranker
                hybrid_results = client.hybrid_search(
                    collection_name,
                    reqs=reqs,
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                sparse_results = client.search(
                    collection_name,
                    data=sparse_search_param["data"],
                    anns_field="sparse",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                bm25_results = client.search(
                    collection_name,
                    data=bm25_search_param["data"],
                    anns_field="bm25",
                    limit=5,
                    output_fields=["doc_id", "document"],
                    search_params={"metric_type": "BM25"}
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(sparse_results, bm25_results)

            # Compare Milvus rerank results with origin rerank results
            self.compare_milvus_rerank_with_origin_rerank(query_texts, hybrid_results, results_without_rerank,
                                                          provider_type="voyageai",
                                                          model_name=model_name,
                                                          truncation=truncation)


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("model_name", ["BAAI/bge-reranker-v2-m3", "netease-youdao/bce-reranker-base_v1"])
    @pytest.mark.parametrize("max_chunks_per_doc", [10, 5])
    @pytest.mark.parametrize("overlap_tokens", [80, 40])
    def test_milvus_client_hybrid_search_with_siliconflow_rerank_specific_params(self, setup_collection, model_name, max_chunks_per_doc, overlap_tokens):
        """
        target: test hybrid search with siliconflow rerank specific parameters
        method: test hybrid search with different siliconflow model names, max_chunks_per_doc and overlap_tokens values
        expected: hybrid search successfully with siliconflow reranker
        """
        client = self._client()
        collection_name = setup_collection

        nq = 2
        query_texts = [fake.text() for _ in range(nq)]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "siliconflow",
                "queries": query_texts,
                "model_name": model_name,
                "max_chunks_per_doc": max_chunks_per_doc,
                "overlap_tokens": overlap_tokens
            },
        )

        # Test different hybrid search combinations
        for search_type in ["dense+sparse", "dense+bm25", "sparse+bm25"]:
            log.info(f"Executing {search_type} hybrid search with siliconflow reranker")

            dense_search_param = {
                "data": [[random.random() for _ in range(768)] for _ in range(nq)],
                "anns_field": "dense",
                "param": {},
                "limit": 5,
            }
            dense = AnnSearchRequest(**dense_search_param)

            sparse_search_param = {
                "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)],
                "anns_field": "sparse",
                "param": {},
                "limit": 5,
            }
            sparse = AnnSearchRequest(**sparse_search_param)

            bm25_search_param = {
                "data": query_texts,
                "anns_field": "bm25",
                "param": {},
                "limit": 5,
            }
            bm25 = AnnSearchRequest(**bm25_search_param)

            if search_type == "dense+sparse":
                reqs = [dense, sparse]
                # Get hybrid search results with reranker
                hybrid_results = client.hybrid_search(
                    collection_name,
                    reqs=reqs,
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                dense_results = client.search(
                    collection_name,
                    data=dense_search_param["data"],
                    anns_field="dense",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                sparse_results = client.search(
                    collection_name,
                    data=sparse_search_param["data"],
                    anns_field="sparse",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(dense_results, sparse_results)
            elif search_type == "dense+bm25":
                reqs = [dense, bm25]
                # Get hybrid search results with reranker
                hybrid_results = client.hybrid_search(
                    collection_name,
                    reqs=reqs,
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                dense_results = client.search(
                    collection_name,
                    data=dense_search_param["data"],
                    anns_field="dense",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                bm25_results = client.search(
                    collection_name,
                    data=bm25_search_param["data"],
                    anns_field="bm25",
                    limit=5,
                    output_fields=["doc_id", "document"],
                    search_params={"metric_type": "BM25"}
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(dense_results, bm25_results)
            else:  # sparse+bm25
                reqs = [sparse, bm25]
                # Get hybrid search results with reranker
                hybrid_results = client.hybrid_search(
                    collection_name,
                    reqs=reqs,
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=ranker,
                    consistency_level="Strong",
                )
                # Get results without rerank by using search separately and merging them
                sparse_results = client.search(
                    collection_name,
                    data=sparse_search_param["data"],
                    anns_field="sparse",
                    limit=5,
                    output_fields=["doc_id", "document"],
                )
                bm25_results = client.search(
                    collection_name,
                    data=bm25_search_param["data"],
                    anns_field="bm25",
                    limit=5,
                    output_fields=["doc_id", "document"],
                    search_params={"metric_type": "BM25"}
                )
                results_without_rerank = self.merge_and_dedup_hybrid_searchresults(sparse_results, bm25_results)

            # Compare Milvus rerank results with origin rerank results
            self.compare_milvus_rerank_with_origin_rerank(query_texts, hybrid_results, results_without_rerank,
                                                          provider_type="siliconflow",
                                                          model_name=model_name,
                                                          max_chunks_per_doc=max_chunks_per_doc,
                                                          overlap_tokens=overlap_tokens)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_tei_model_rerank_nullable_field(self, tei_reranker_endpoint):
        """
        target: verify model reranker (TEI) works with nullable VarChar input field
        method: create collection with nullable document field, insert rows with some null values,
                search with TEI model reranker
        expected: search successfully, null document rows treated as empty strings for reranking
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create schema with nullable document field
        schema = client.create_schema(enable_dynamic_field=False, auto_id=True)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("document", DataType.VARCHAR, max_length=10000, nullable=True)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=768)
        # 2. prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="dense", index_type="FLAT", metric_type="COSINE")
        # 3. create collection
        client.create_collection(
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        # 4. insert data: some rows with null document
        rows = []
        data_size = 100
        for i in range(data_size):
            row = {
                "document": None if i % 10 == 0 else fake.text(),
                "dense": [random.random() for _ in range(768)]
            }
            rows.append(row)
        client.insert(collection_name, rows)
        # 5. search with TEI model reranker
        nq = 1
        query_texts = [fake.text() for _ in range(nq)]
        tei_ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )
        data = [[random.random() for _ in range(768)] for _ in range(nq)]
        rerank_results = client.search(
            collection_name,
            data=data,
            anns_field="dense",
            limit=10,
            output_fields=["document"],
            ranker=tei_ranker,
            consistency_level="Strong",
        )
        assert len(rerank_results) == nq
        assert len(rerank_results[0]) > 0
        # verify scores are in descending order
        scores = [r["distance"] for r in rerank_results[0]]
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], \
                f"Scores not in descending order: scores[{i}]={scores[i]} < scores[{i + 1}]={scores[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_tei_model_rerank_nullable_all_null(self, tei_reranker_endpoint):
        """
        target: verify model reranker (TEI) handles the case where all document field values are null
        method: create collection with nullable document field, insert rows with all null values,
                search with TEI model reranker
        expected: search successfully, all null documents treated as empty strings
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create schema with nullable document field
        schema = client.create_schema(enable_dynamic_field=False, auto_id=True)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("document", DataType.VARCHAR, max_length=10000, nullable=True)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=768)
        # 2. prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="dense", index_type="FLAT", metric_type="COSINE")
        # 3. create collection
        client.create_collection(
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        # 4. insert data with all null documents
        rows = []
        data_size = 100
        for i in range(data_size):
            row = {
                "document": None,
                "dense": [random.random() for _ in range(768)]
            }
            rows.append(row)
        client.insert(collection_name, rows)
        # 5. search with TEI model reranker
        nq = 1
        query_texts = [fake.text() for _ in range(nq)]
        tei_ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )
        data = [[random.random() for _ in range(768)] for _ in range(nq)]
        rerank_results = client.search(
            collection_name,
            data=data,
            anns_field="dense",
            limit=10,
            output_fields=["document"],
            ranker=tei_ranker,
            consistency_level="Strong",
        )
        assert len(rerank_results) == nq
        assert len(rerank_results[0]) > 0
        # verify all returned documents are null and scores are valid
        # model reranker treats null as empty string "", so scores are non-null float values
        scores = [r["distance"] for r in rerank_results[0]]
        for r in rerank_results[0]:
            assert r.get("document") is None, \
                f"Expected null document, got {r.get('document')}"
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], \
                f"Scores not in descending order: scores[{i}]={scores[i]} < scores[{i + 1}]={scores[i + 1]}"


class TestMilvusClientSearchModelRerankNegative(TestMilvusClientV2Base):
    """ Test case of model rerank negative scenarios """

    @pytest.fixture(scope="function")
    def setup_collection(self):
        """Setup collection for negative testing"""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create schema
        schema = client.create_schema(enable_dynamic_field=False, auto_id=True)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("doc_id", DataType.VARCHAR, max_length=100)
        schema.add_field("document", DataType.VARCHAR, max_length=10000)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=128)

        # 2. prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="dense", index_type="FLAT", metric_type="L2")

        # 3. create collection
        client.create_collection(
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # 4. insert data
        rows = []
        for i in range(100):
            rows.append({
                "doc_id": str(i),
                "document": fake.text()[:500],
                "dense": [random.random() for _ in range(128)]
            })
        client.insert(collection_name, rows)

        yield client, collection_name

        # cleanup
        client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_provider", ["invalid_provider", "openai", "huggingface", "", None, 123])
    def test_milvus_client_search_with_model_rerank_invalid_provider(self, setup_collection, invalid_provider,
                                                                     tei_reranker_endpoint):
        """
        target: test model rerank with invalid provider
        method: use invalid provider values
        expected: raise exception
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": invalid_provider,
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )

        data = [[random.random() for _ in range(128)]]
        error = {ct.err_code: 65535, ct.err_msg: "unknown rerank model provider"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_endpoint", ["", "invalid_url", "ftp://invalid.com", "localhost", None])
    def test_milvus_client_search_with_model_rerank_invalid_endpoint(self, setup_collection, invalid_endpoint):
        """
        target: test model rerank with invalid endpoint
        method: use invalid endpoint values
        expected: raise exception
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": invalid_endpoint,
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 65535, ct.err_msg: "not a valid http/https link"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_model_rerank_unreachable_endpoint(self, setup_collection):
        """
        target: test model rerank with unreachable endpoint
        method: use unreachable endpoint
        expected: raise connection error
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": "http://192.168.999.999:8080",  # unreachable IP
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 65535, ct.err_msg: "call service failed"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_queries", [None, "", 123, {"key": "value"}])
    def test_milvus_client_search_with_model_rerank_invalid_queries(self, setup_collection, invalid_queries,
                                                                    tei_reranker_endpoint):
        """
        target: test model rerank with invalid queries parameter
        method: use invalid queries values
        expected: raise exception
        """
        client, collection_name = setup_collection

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": invalid_queries,
                "endpoint": tei_reranker_endpoint,
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 65535, ct.err_msg: "parse rerank params [queries] failed"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_model_rerank_missing_queries(self, setup_collection, tei_reranker_endpoint):
        """
        target: test model rerank without queries parameter
        method: omit queries parameter
        expected: raise exception for missing required parameter
        """
        client, collection_name = setup_collection

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "endpoint": tei_reranker_endpoint,
                # missing "queries" parameter
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 65535, ct.err_msg: "rerank function missing required param: queries"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_model_rerank_missing_endpoint(self, setup_collection):
        """
        target: test model rerank without endpoint parameter
        method: omit endpoint parameter
        expected: raise exception for missing required parameter
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                # missing "endpoint" parameter
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 65535, ct.err_msg: "is not a valid http/https link"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_reranker_type", ["invalid", None, 123])
    def test_milvus_client_search_with_invalid_reranker_type(self, setup_collection, invalid_reranker_type,
                                                             tei_reranker_endpoint):
        """
        target: test model rerank with invalid reranker type
        method: use invalid reranker type values
        expected: raise exception
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": invalid_reranker_type,
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 65535, ct.err_msg: "unsupported reranker"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_empty_reranker_type(self, setup_collection, tei_reranker_endpoint):
        """
        target: test model rerank with empty reranker type
        method: use empty string as reranker type
        expected: raise exception
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 65535, ct.err_msg: "reranker name not specified"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_model_rerank_query_mismatch(self, setup_collection, tei_reranker_endpoint):
        """
        target: test model rerank with query count mismatch
        method: provide multiple queries but single search data
        expected: raise exception for query mismatch
        """
        client, collection_name = setup_collection
        query_texts = ["query1", "query2", "query3"]  # 3 queries

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )

        data = [[0.1] * 128]  # single search data
        error = {ct.err_code: 65535, ct.err_msg: "queries count (3) != nq count (1)"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_model_rerank_non_text_field(self, setup_collection, tei_reranker_endpoint):
        """
        target: test model rerank with non-text input field
        method: use numeric field for reranking input
        expected: raise exception for unsupported field type
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["id"],  # numeric field instead of text
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 65535, ct.err_msg: "model input field id must be VarChar, got Int64"}
        self.search(client, collection_name, data, anns_field="dense", limit=5, output_fields=["doc_id", "document"],
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_model_rerank_nonexistent_field(self, setup_collection, tei_reranker_endpoint):
        """
        target: test model rerank with non-existent input field
        method: use field that doesn't exist in collection
        expected: raise exception for field not found
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["nonexistent_field"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 1, ct.err_msg: "field not found"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_model_rerank_multiple_input_fields(self, setup_collection,
                                                                          tei_reranker_endpoint):
        """
        target: test model rerank with multiple input fields
        method: specify multiple fields for reranking input
        expected: raise exception for multiple input fields not supported
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document", "doc_id"],  # multiple fields
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
            },
        )

        data = [[0.1] * 128]
        error = {ct.err_code: 65535, ct.err_msg: "model reranker requires exactly 1 input field, got 2"}
        self.search(client, collection_name, data, anns_field="dense", limit=5,
                    ranker=ranker, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_model_rerank_extra_params(self, setup_collection, tei_reranker_endpoint):
        """
        target: test model rerank with extra unknown parameters
        method: add unknown parameters to params
        expected: search should work but ignore unknown parameters or raise warning
        """
        client, collection_name = setup_collection
        query_texts = ["test query"]

        ranker = Function(
            name="rerank_model",
            input_field_names=["document"],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "model",
                "provider": "tei",
                "queries": query_texts,
                "endpoint": tei_reranker_endpoint,
                "unknown_param": "value",  # extra parameter
                "another_param": 123,
            },
        )

        data = [[0.1] * 128]
        # This might succeed with warning, or fail depending on implementation
        res, result = self.search(
            client,
            collection_name,
            data=data,
            anns_field="dense",
            limit=5,
            ranker=ranker,
        )
        assert result is True


class TestMilvusClientSearchRRFWeightedRerank(TestMilvusClientV2Base):

    @pytest.fixture(scope="function")
    def setup_collection(self):
        """Setup collection for rrf/weighted rerank testing"""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dense_metric_type = "COSINE"

        # 1. create schema with embedding and bm25 functions
        schema = client.create_schema(enable_dynamic_field=False, auto_id=True)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("doc_id", DataType.VARCHAR, max_length=100)
        schema.add_field("document", DataType.VARCHAR, max_length=10000, enable_analyzer=True)
        schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=768)
        schema.add_field("bm25", DataType.SPARSE_FLOAT_VECTOR)

        # add bm25 function
        bm25_function = Function(
            name="bm25",
            input_field_names=["document"],
            output_field_names="bm25",
            function_type=FunctionType.BM25,
        )
        schema.add_function(bm25_function)

        # 2. prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="dense", index_type="FLAT", metric_type=dense_metric_type)
        index_params.add_index(
            field_name="sparse",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="IP",
        )
        index_params.add_index(
            field_name="bm25",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="BM25",
            params={"bm25_k1": 1.2, "bm25_b": 0.75},
        )

        # 3. create collection
        client.create_collection(
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        # 4. insert data
        rows = []
        data_size = 3000
        for i in range(data_size):
            rows.append({
                "doc_id": str(i),
                "document": fake.text(),
                "sparse": {random.randint(1, 10000): random.random() for _ in range(100)},
                "dense": [random.random() for _ in range(768)]
            })
        client.insert(collection_name, rows)

        return collection_name

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("ranker_model", ["rrf", "weight"])
    def test_milvus_client_hybrid_vector_search_with_rrf_weight_rerank(self, setup_collection, ranker_model):
        """
        target: test hybrid vector search with rrf/weight rerank
        method: test dense+sparse/dense+bm25/sparse+bm25 search with rrf/weight reranker
        expected: search successfully with rrf/weight reranker
        """
        from pymilvus import WeightedRanker, RRFRanker
        client = self._client()
        collection_name = setup_collection

        # 5. prepare search parameters for reranker
        query_texts = [fake.text() for _ in range(10)]
        rrf_func_ranker = Function(
            name="rrf_ranker",
            input_field_names=[],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "rrf",
                "k": 100
            },
        )
        weight_func_ranker = Function(
            name="weight_ranker",
            input_field_names=[],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "weighted",
                "weights": [0.1, 0.9],
                "norm_score": True
            },
        )
        func_ranker = None
        original_ranker = None
        if ranker_model == "rrf":
            func_ranker = rrf_func_ranker
            original_ranker = RRFRanker(k=100)

        if ranker_model == "weight":
            func_ranker = weight_func_ranker
            original_ranker = WeightedRanker(0.1, 0.9, norm_score=True)
        # 6. execute search with reranker
        for search_type in ["dense+sparse", "dense+bm25", "sparse+bm25"]:
            log.info(f"Executing {search_type} search with rrf/weight reranker")
            dense_search_param = {
                "data": [[random.random() for _ in range(768)] for _ in range(10)],
                "anns_field": "dense",
                "param": {},
                "limit": 5,
            }
            dense = AnnSearchRequest(**dense_search_param)

            sparse_search_param = {
                "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(10)],
                "anns_field": "sparse",
                "param": {},
                "limit": 5,
            }
            bm25_search_param = {
                "data": query_texts,
                "anns_field": "bm25",
                "param": {},
                "limit": 5,
            }
            bm25 = AnnSearchRequest(**bm25_search_param)

            sparse = AnnSearchRequest(**sparse_search_param)
            if search_type == "dense+sparse":

                function_rerank_results = client.hybrid_search(
                    collection_name,
                    reqs=[dense, sparse],
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=func_ranker,
                    consistency_level="Strong",
                )
                original_rerank_results = client.hybrid_search(
                    collection_name,
                    reqs=[dense, sparse],
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=original_ranker,
                    consistency_level="Strong",
                )
            elif search_type == "dense+bm25":
                function_rerank_results = client.hybrid_search(
                    collection_name,
                    reqs=[dense, bm25],
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=func_ranker,
                    consistency_level="Strong",
                )
                original_rerank_results = client.hybrid_search(
                    collection_name,
                    reqs=[dense, bm25],
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=original_ranker,
                    consistency_level="Strong",
                )
            elif search_type == "sparse+bm25":
                function_rerank_results = client.hybrid_search(
                    collection_name,
                    reqs=[sparse, bm25],
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=func_ranker,
                    consistency_level="Strong",
                    search_params={"metric_type": "BM25"}
                )
                original_rerank_results = client.hybrid_search(
                    collection_name,
                    reqs=[sparse, bm25],
                    limit=10,
                    output_fields=["doc_id", "document"],
                    ranker=original_ranker,
                    consistency_level="Strong",
                    search_params={"metric_type": "BM25"}
                )
            assert function_rerank_results == original_rerank_results
