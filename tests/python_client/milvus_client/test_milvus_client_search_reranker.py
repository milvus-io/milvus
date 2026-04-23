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


@pytest.mark.xdist_group("TestMilvusClientSearchInvalidRerankerShared")
class TestMilvusClientSearchInvalidRerankerShared(TestMilvusClientV2Base):
    """Invalid-reranker search tests that all use Schema A
    (VARCHAR PK + FLOAT_VECTOR dim=5 + INT64 reranker field nullable=False)
    and can therefore share a single collection."""

    @pytest.fixture(scope="module", autouse=True)
    def prepare_reranker_invalid_collection(self, request):
        client = self._client()
        collection_name = RERANKER_INVALID_SHARED_COLLECTION
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64,
                         is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=RERANKER_INVALID_DIM)
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        # force_teardown=False: this is a module-scoped shared collection; the base class's
        # per-test teardown_method would otherwise drop it after the first test runs.
        # Cleanup is handled by the request.addfinalizer below.
        self.create_collection(client, collection_name, dimension=RERANKER_INVALID_DIM,
                               schema=schema, index_params=index_params, force_teardown=False)

        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i),
                 default_vector_field_name: list(rng.random((1, RERANKER_INVALID_DIM))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)

        def teardown():
            try:
                if self.has_collection(client, RERANKER_INVALID_SHARED_COLLECTION)[0]:
                    self.drop_collection(client, RERANKER_INVALID_SHARED_COLLECTION)
            except Exception:
                pass
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_not_single_field(self):
        """
        target: test search with reranker with multiple fields
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name, default_primary_key_field_name],
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
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: "decay reranker requires exactly 1 input field, got 2"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_duplicate_fields(self):
        """
        target: test search with reranker with multiple duplicate fields
        method: create connection, collection, insert and search
        expected: raise exception
        """
        try:
            Function(
                name="my_reranker",
                input_field_names=[ct.default_reranker_field_name, ct.default_reranker_field_name],
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
        except Exception as e:
            log.info(e)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_invalid_function_type(self):
        """
        target: test search with reranker with invalid function type
        method: create connection, collection, insert and search
        expected: raise exception
        """
        try:
            Function(
                name="my_reranker",
                input_field_names=[ct.default_reranker_field_name],
                function_type=1,
                params={
                    "reranker": "decay",
                    "function": "gauss",
                    "origin": 0,
                    "offset": 0,
                    "decay": 0.5,
                    "scale": 100
                }
            )
        except Exception as e:
            log.info(e)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_multiple_fields(self):
        """
        target: test search with reranker with multiple fields
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": 1,
                "function": "gauss",
                "origin": 0,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: "unsupported reranker 1"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("not_supported_reranker", ["invalid"])
    def test_milvus_client_search_reranker_not_supported_reranker_value(self, not_supported_reranker):
        """
        target: test search with reranker with not supported reranker value
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": not_supported_reranker,
                "function": "gauss",
                "origin": 0,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: f"unsupported reranker {not_supported_reranker}"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("not_supported_function", [1, "invalid"])
    def test_milvus_client_search_reranker_not_supported_function_value(self, not_supported_function):
        """
        target: test search with reranker with multiple fields
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": not_supported_function,
                "origin": 0,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: f"decay: invalid function \"{not_supported_function}\", must be one of [gauss, exp, linear]"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_origin", ["invalid", [1]])
    def test_milvus_client_search_reranker_invalid_origin(self, invalid_origin):
        """
        target: test search with reranker with invalid origin
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": invalid_origin,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: f"decay param origin: {invalid_origin} is not a number"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_without_origin(self):
        """
        target: test search with reranker with no origin
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: "decay origin not specified"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_scale", ["invalid", [1]])
    def test_milvus_client_search_reranker_invalid_scale(self, invalid_scale):
        """
        target: test search with reranker with invalid scale
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
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
                "scale": invalid_scale
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: f"decay param scale: {invalid_scale} is not a number"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_without_scale(self):
        """
        target: test search with reranker with invalid scale
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": 0,
                "decay": 0.5
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: "decay scale not specified"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_scale", [0, -1.0])
    def test_milvus_client_search_reranker_scale_out_of_range(self, invalid_scale):
        """
        target: test search with reranker with invalid scale (out of range)
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
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
                "scale": invalid_scale
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: f"decay: scale must be > 0, got {invalid_scale}"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_offset", ["invalid", [1]])
    def test_milvus_client_search_reranker_invalid_offset(self, invalid_offset):
        """
        target: test search with reranker with invalid scale (out of range)
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": invalid_offset,
                "decay": 0.5,
                "scale": 100
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: f"decay param offset: {invalid_offset} is not a number"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_offset", [-1.0])
    def test_milvus_client_search_reranker_offset_out_of_range(self, invalid_offset):
        """
        target: test search with reranker with invalid scale (out of range)
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": invalid_offset,
                "decay": 0.5,
                "scale": 100
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: f"decay: offset must be >= 0, got {invalid_offset}"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_decay", ["invalid", [1]])
    def test_milvus_client_search_reranker_invalid_decay(self, invalid_decay):
        """
        target: test search with reranker with invalid decay (out of range)
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": 0,
                "decay": invalid_decay,
                "scale": 100
            }
        )
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, RERANKER_INVALID_DIM))
        error = {ct.err_code: 65535,
                 ct.err_msg: f"decay param decay: {invalid_decay} is not a number"}
        self.search(client, RERANKER_INVALID_SHARED_COLLECTION, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientSearchInvalidReranker(TestMilvusClientV2Base):
    """ Invalid reranker test cases — each test creates its own collection """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("not_support_datatype", [DataType.VARCHAR, DataType.JSON])
    def test_milvus_client_search_reranker_not_supported_field_type(self, not_support_datatype):
        """
        target: test search with reranker on not supported field type
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
        schema.add_field(default_string_field_name, not_support_datatype, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        my_rerank_fn = Function(
            name="my_rerank_fn",
            input_field_names=[default_string_field_name],
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
        if not_support_datatype == DataType.VARCHAR:
            err_msg = f"decay input field {default_string_field_name} must be numeric, got VarChar"
        if not_support_datatype == DataType.JSON:
            err_msg = "unsupported field type: JSON"
        vectors_to_search = rng.random((1, dim))
        error = {ct.err_code: 65535,
                 ct.err_msg: err_msg}
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_not_supported_field_type_array(self):
        """
        target: test search with reranker on not supported field type
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
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 "array_field": [i, i + 1]} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        my_rerank_fn = Function(
            name="my_rerank_fn",
            input_field_names=["array_field"],
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
        error = {ct.err_code: 65535,
                 ct.err_msg: "unsupported field type: Array"}
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_not_supported_field_type_vector(self):
        """
        target: test search with reranker on not supported field type
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
        my_rerank_fn = Function(
            name="my_rerank_fn",
            input_field_names=[default_vector_field_name],
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
        error = {ct.err_code: 65535,
                 ct.err_msg: "unsupported field type: FloatVector"}
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_invalid_reranker(self):
        """
        target: test search with reranker with invalid reranker
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
        schema.add_field(ct.default_reranker_field_name, DataType.INT64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        my_rerank_fn = "Function"
        vectors_to_search = rng.random((1, dim))
        error = {ct.err_code: 1,
                 ct.err_msg: "The search ranker must be a Function"}
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_invalid_name(self):
        """
        target: test search with reranker with invalid reranker name
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
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        try:
            Function(
                name=1,
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
        except Exception as e:
            log.info(e)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_invalid_input_field_names(self):
        """
        target: test search with reranker with invalid input field names
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
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        try:
            Function(
                name="my_reranker",
                input_field_names=1,
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
        except Exception as e:
            log.info(e)
        try:
            Function(
                name="my_reranker",
                input_field_names=[1],
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
        except Exception as e:
            log.info(e)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_reranker_not_exist_field(self):
        """
        target: test search with reranker with not exist field
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
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=["not_exist_field"],
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
        error = {ct.err_code: 65535,
                 ct.err_msg: "input field not_exist_field not found in collection schema"}
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 41533")
    @pytest.mark.parametrize("invalid_decay", [-1.0, 0, 1, 2.0])
    def test_milvus_client_search_reranker_decay_out_of_range(self, invalid_decay):
        """
        target: test search with reranker with invalid decay (out of range)
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
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
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
                "offset": 0,
                "decay": invalid_decay,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        error = {ct.err_code: 65535,
                 ct.err_msg: f"Decay function param: decay must 0 < decay < 1, but got {invalid_decay}"}
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_group_by_search_with_reranker(self):
        """
        target: test group search with reranker
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
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
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
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )
        vectors_to_search = rng.random((1, dim))
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    group_by_field=ct.default_reranker_field_name)
        self.add_collection_field(client, collection_name, field_name=ct.default_new_field_name, data_type=DataType.INT64,
                                  nullable=True)
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn, group_by_field=ct.default_new_field_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_with_reranker_on_dynamic_fields(self):
        """
        target: test group search with reranker on dynamic fields
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=False)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 ct.default_reranker_field_name: i, "dynamic_fields": i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=["dynamic_fields"],
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
        error = {ct.err_code: 65535,
                 ct.err_msg: "input field dynamic_fields not found in collection schema"}
        self.search(client, collection_name, vectors_to_search, ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)


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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)


@pytest.mark.xdist_group("TestMilvusClientSearchDecayRerankShared")
class TestMilvusClientSearchDecayRerankShared(TestMilvusClientV2Base):
    """Decay-rerank tests that share Schema A
    (INT64 pk + FLOAT_VECTOR dim=5 + INT64 reranker_field nullable=False).
    Two collections are provisioned: one growing (not flushed), one flushed,
    so Test #1's is_flush parametrization preserves its original segment-state coverage."""

    @pytest.fixture(scope="module", autouse=True)
    def prepare_decay_rerank_collections(self, request):
        client = self._client()
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: list(rng.random((1, DECAY_RERANK_SHARED_DIM))[0]),
                 ct.default_reranker_field_name: i} for i in range(default_nb)]

        for collection_name, do_flush in [
            (DECAY_RERANK_SHARED_COLLECTION_GROWING, False),
            (DECAY_RERANK_SHARED_COLLECTION_FLUSHED, True),
        ]:
            if self.has_collection(client, collection_name)[0]:
                self.drop_collection(client, collection_name)

            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(default_primary_key_field_name, DataType.INT64,
                             is_primary=True, auto_id=False)
            schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR,
                             dim=DECAY_RERANK_SHARED_DIM)
            schema.add_field(ct.default_reranker_field_name, DataType.INT64, nullable=False)
            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(default_vector_field_name, metric_type="COSINE")

            # force_teardown=False: module-scoped shared collection; per-test teardown_method
            # would otherwise drop it after the first test. Cleanup via request.addfinalizer below.
            self.create_collection(client, collection_name, dimension=DECAY_RERANK_SHARED_DIM,
                                   schema=schema, index_params=index_params,
                                   force_teardown=False)
            self.insert(client, collection_name, rows)
            if do_flush:
                self.flush(client, collection_name)

        def teardown():
            for cn in [DECAY_RERANK_SHARED_COLLECTION_GROWING,
                       DECAY_RERANK_SHARED_COLLECTION_FLUSHED]:
                try:
                    if self.has_collection(client, cn)[0]:
                        self.drop_collection(client, cn)
                except Exception:
                    pass
        request.addfinalizer(teardown)

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
        collection_name = (DECAY_RERANK_SHARED_COLLECTION_FLUSHED if is_flush
                           else DECAY_RERANK_SHARED_COLLECTION_GROWING)
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
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, DECAY_RERANK_SHARED_DIM))
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
        # Original test did not call flush — use the growing collection to preserve behavior.
        collection_name = DECAY_RERANK_SHARED_COLLECTION_GROWING
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
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, DECAY_RERANK_SHARED_DIM))
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
        self.drop_collection(client, collection_name)

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
        self.drop_collection(client, collection_name)


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
