import multiprocessing
import numbers
import random
import numpy
import threading
import pytest
import pandas as pd
import decimal
from decimal import Decimal, getcontext
from time import sleep
import heapq
from pymilvus import DataType

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from common.constants import *
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_EVENTUALLY
from base.high_level_api_wrapper import HighLevelApiWrapper
client_w = HighLevelApiWrapper()

prefix = "milvus_client_api_collection"
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
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name


class TestMilvusClientCollectionInvalid(TestcaseBase):
    """ Test case of create collection interface """

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
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_collection_invalid_collection_name(self, collection_name):
        """
        target: test fast create collection with invalid collection name
        method: create collection with invalid collection
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        # 1. create collection
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        client_w.create_collection(client, collection_name, default_dim,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_name_over_max_length(self):
        """
        target: test fast create collection with over max collection name length
        method: create collection with over max collection name length
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        # 1. create collection
        collection_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                             f"the length of a collection name must be less than 255 characters: "
                                             f"invalid parameter"}
        client_w.create_collection(client, collection_name, default_dim,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_name_empty(self):
        """
        target: test fast create collection name with empty
        method: create collection name with empty
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        # 1. create collection
        collection_name = "  "
        error = {ct.err_code: 0, ct.err_msg: "collection name should not be empty: invalid parameter"}
        client_w.create_collection(client, collection_name, default_dim,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [ct.min_dim-1, ct.max_dim+1])
    def test_milvus_client_collection_invalid_dim(self, dim):
        """
        target: test fast create collection name with invalid dim
        method: create collection name with invalid dim
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        error = {ct.err_code: 65535, ct.err_msg: f"invalid dimension: {dim}. should be in range 2 ~ 32768"}
        client_w.create_collection(client, collection_name, dim,
                                   check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="pymilvus issue 1554")
    def test_milvus_client_collection_invalid_primary_field(self):
        """
        target: test fast create collection name with invalid primary field
        method: create collection name with invalid primary field
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        error = {ct.err_code: 1, ct.err_msg: f"Param id_type must be int or string"}
        client_w.create_collection(client, collection_name, default_dim, id_type="invalid",
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_string_auto_id(self):
        """
        target: test fast create collection without max_length for string primary key
        method: create collection name with invalid primary field
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        error = {ct.err_code: 65535, ct.err_msg: f"type param(max_length) should be specified for varChar "
                                                 f"field of collection {collection_name}"}
        client_w.create_collection(client, collection_name, default_dim, id_type="string", auto_id=True,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_same_collection_different_params(self):
        """
        target: test create same collection with different params
        method: create same collection with different params
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        # 2. create collection with same params
        client_w.create_collection(client, collection_name, default_dim)
        # 3. create collection with same name and different params
        error = {ct.err_code: 1, ct.err_msg: f"create duplicate collection with different parameters, "
                                             f"collection: {collection_name}"}
        client_w.create_collection(client, collection_name, default_dim+1,
                                   check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="pymilvus issue 1872")
    @pytest.mark.parametrize("metric_type", [1, " ", "invalid"])
    def test_milvus_client_collection_invalid_metric_type(self, metric_type):
        """
        target: test create same collection with invalid metric type
        method: create same collection with invalid metric type
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        error = {ct.err_code: 65535,
                 ct.err_msg: "metric type not found or not supported, supported: [L2 IP COSINE HAMMING JACCARD]"}
        client_w.create_collection(client, collection_name, default_dim, metric_type=metric_type,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="pymilvus issue 1864")
    def test_milvus_client_collection_invalid_schema_field_name(self):
        """
        target: test create collection with invalid schema field name
        method: create collection with invalid schema field name
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        schema = client_w.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("%$#", DataType.VARCHAR, max_length=64,
                         is_primary=True, auto_id = False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=128)
        # 1. create collection
        error = {ct.err_code: 65535,
                 ct.err_msg: "metric type not found or not supported, supported: [L2 IP COSINE HAMMING JACCARD]"}
        client_w.create_collection(client, collection_name, schema=schema,
                                   check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientCollectionValid(TestcaseBase):
    """ Test case of create collection interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2", "IP"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["int", "string"])
    def id_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="pymilvus issue 1871")
    @pytest.mark.parametrize("dim", [ct.min_dim, default_dim, ct.max_dim])
    def test_milvus_client_collection_fast_creation_default(self, dim):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        client_w.using_database(client, "default")
        # 1. create collection
        client_w.create_collection(client, collection_name, dim)
        collections = client_w.list_collections(client)[0]
        assert collection_name in collections
        client_w.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": dim,
                                                  "consistency_level": 0})
        index = client_w.list_indexes(client, collection_name)[0]
        assert index == ['vector']
        # load_state = client_w.get_load_state(collection_name)[0]
        client_w.load_partitions(client, collection_name, "_default")
        client_w.release_partitions(client, collection_name, "_default")
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [ct.min_dim, default_dim, ct.max_dim])
    def test_milvus_client_collection_fast_creation_all_params(self, dim, metric_type, id_type, auto_id):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        max_length = 100
        # 1. create collection
        client_w.create_collection(client, collection_name, dim, id_type=id_type, metric_type=metric_type,
                                   auto_id=auto_id, max_length=max_length)
        collections = client_w.list_collections(client)[0]
        assert collection_name in collections
        client_w.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": dim,
                                                  "consistency_level": 0})
        index = client_w.list_indexes(client, collection_name)[0]
        assert index == ['vector']
        # load_state = client_w.get_load_state(collection_name)[0]
        client_w.release_collection(client, collection_name)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(reason="pymilvus issue 1864")
    def test_milvus_client_collection_self_creation_default(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        schema = client_w.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id_string", DataType.VARCHAR, max_length=64, is_primary=True, auto_id = False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=128)
        schema.add_field("title", DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field("array_field", DataType.Array, max_capacity=12,
                         element_type_params={"type": DataType.VARCHAR, "max_length": 64})
        index_params = client_w.prepare_index_params()
        index_params.add_index("embeddings", metric_type="cosine")
        index_params.add_index("title")
        client_w.create_collection(client, collection_name, schema=schema, index_params=index_params)
        collections = client_w.list_collections(client)[0]
        assert collection_name in collections
        client_w.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": 128,
                                                  "consistency_level": 0})
        index = client_w.list_indexes(client, collection_name)[0]
        assert index == ['vector']
        # load_state = client_w.get_load_state(collection_name)[0]
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_array_insert_search(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        collections = client_w.list_collections(client)[0]
        assert collection_name in collections
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
            default_float_field_name: i * 1.0,
            default_int32_array_field_name: [i, i+1, i+2],
            default_string_array_field_name: [str(i), str(i + 1), str(i + 2)]
        } for i in range(default_nb)]
        client_w.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        client_w.search(client, collection_name, vectors_to_search,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": len(vectors_to_search),
                                     "ids": insert_ids,
                                     "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 25110")
    def test_milvus_client_search_query_string(self):
        """
        target: test search (high level api) for string primary key
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        client_w.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": default_dim,
                                                  "auto_id": auto_id})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        client_w.insert(client, collection_name, rows)
        client_w.flush(client, collection_name)
        assert client_w.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        client_w.search(client, collection_name, vectors_to_search,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": len(vectors_to_search),
                                     "limit": default_limit})
        # 4. query
        client_w.query(client, collection_name, filter=default_search_exp,
                       check_task=CheckTasks.check_query_results,
                       check_items={exp_res: rows,
                                    "with_vec": True,
                                    "primary_field": default_primary_key_field_name})
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_different_metric_types_not_specifying_in_search_params(self, metric_type, auto_id):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, metric_type=metric_type, auto_id=auto_id,
                                   consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        if auto_id:
            for row in rows:
                row.pop(default_primary_key_field_name)
        client_w.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        # search_params = {"metric_type": metric_type}
        client_w.search(client, collection_name, vectors_to_search, limit=default_limit,
                        output_fields=[default_primary_key_field_name],
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": len(vectors_to_search),
                                     "limit": default_limit})
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("pymilvus issue #1866")
    def test_milvus_client_search_different_metric_types_specifying_in_search_params(self, metric_type, auto_id):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, metric_type=metric_type, auto_id=auto_id,
                                   consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        if auto_id:
            for row in rows:
                row.pop(default_primary_key_field_name)
        client_w.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        search_params = {"metric_type": metric_type}
        client_w.search(client, collection_name, vectors_to_search, limit=default_limit,
                        search_params=search_params,
                        output_fields=[default_primary_key_field_name],
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": len(vectors_to_search),
                                     "limit": default_limit})
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_delete_with_ids(self):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        pks = client_w.insert(client, collection_name, rows)[0]
        # 3. delete
        delete_num = 3
        client_w.delete(client, collection_name, ids=[i for i in range(delete_num)])
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        client_w.search(client, collection_name, vectors_to_search, limit=default_nb,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": len(vectors_to_search),
                                     "ids": insert_ids,
                                     "limit": limit})
        # 5. query
        client_w.query(client, collection_name, filter=default_search_exp,
                       check_task=CheckTasks.check_query_results,
                       check_items={exp_res: rows[delete_num:],
                                    "with_vec": True,
                                    "primary_field": default_primary_key_field_name})
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_delete_with_filters(self):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        pks = client_w.insert(client, collection_name, rows)[0]
        # 3. delete
        delete_num = 3
        client_w.delete(client, collection_name, filter=f"id < {delete_num}")
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        client_w.search(client, collection_name, vectors_to_search, limit=default_nb,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": len(vectors_to_search),
                                     "ids": insert_ids,
                                     "limit": limit})
        # 5. query
        client_w.query(client, collection_name, filter=default_search_exp,
                       check_task=CheckTasks.check_query_results,
                       check_items={exp_res: rows[delete_num:],
                                    "with_vec": True,
                                    "primary_field": default_primary_key_field_name})
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_rename_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        collections = client_w.list_collections(client)[0]
        assert collection_name in collections
        old_name = collection_name
        new_name = collection_name + "new"
        client_w.rename_collection(client, old_name, new_name)
        collections = client_w.list_collections(client)[0]
        assert new_name in collections
        assert old_name not in collections
        client_w.describe_collection(client, new_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": new_name,
                                                  "dim": default_dim,
                                                  "consistency_level": 0})
        index = client_w.list_indexes(client, new_name)[0]
        assert index == ['vector']
        # load_state = client_w.get_load_state(collection_name)[0]
        error = {ct.err_code: 100, ct.err_msg: f"collection not found"}
        client_w.load_partitions(client, old_name, "_default",
                                 check_task=CheckTasks.err_res, check_items=error)
        client_w.load_partitions(client, new_name, "_default")
        client_w.release_partitions(client, new_name, "_default")
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, new_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="db not ready")
    def test_milvus_client_collection_rename_collection_target_db(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        collections = client_w.list_collections(client)[0]
        assert collection_name in collections
        db_name = "new_db"
        client_w.using_database(client, db_name)
        old_name = collection_name
        new_name = collection_name + "new"
        client_w.rename_collection(client, old_name, new_name, target_db=db_name)
        collections = client_w.list_collections(client)[0]
        assert new_name in collections
        assert old_name not in collections
        client_w.describe_collection(client, new_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": new_name,
                                                  "dim": default_dim,
                                                  "consistency_level": 0})
        index = client_w.list_indexes(client, new_name)[0]
        assert index == ['vector']
        # load_state = client_w.get_load_state(collection_name)[0]
        error = {ct.err_code: 100, ct.err_msg: f"collection not found"}
        client_w.load_partitions(client, old_name, "_default",
                                 check_task=CheckTasks.err_res, check_items=error)
        client_w.load_partitions(client, new_name, "_default")
        client_w.release_partitions(client, new_name, "_default")
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, new_name)


class TestMilvusClientDropCollectionInvalid(TestcaseBase):
    """ Test case of drop collection interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_drop_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. collection name can only "
                                                f"contain numbers, letters and underscores: invalid parameter"}
        client_w.drop_collection(client, name,
                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_drop_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str("nonexisted")
        client_w.drop_collection(client, collection_name)


class TestMilvusClientReleaseCollectionInvalid(TestcaseBase):
    """ Test case of release collection interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_release_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. collection name can only "
                                                f"contain numbers, letters and underscores: invalid parameter"}
        client_w.release_collection(client, name,
                                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str("nonexisted")
        error = {ct.err_code: 1100, ct.err_msg: f"collection not found[database=default]"
                                                f"[collection={collection_name}]"}
        client_w.release_collection(client, collection_name,
                                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_release_collection_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        # 1. create collection
        collection_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        client_w.release_collection(client, collection_name, default_dim,
                                    check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientReleaseCollectionValid(TestcaseBase):
    """ Test case of release collection interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2", "IP"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["int", "string"])
    def id_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_unloaded_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        client_w.release_collection(client, collection_name)
        client_w.release_collection(client, collection_name)
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partially_loaded_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition")
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        client_w.create_partition(client, collection_name, partition_name)
        client_w.release_partitions(client, collection_name, ["_default", partition_name])
        client_w.release_collection(client, collection_name)
        client_w.load_collection(client, collection_name)
        client_w.release_partitions(client, collection_name, [partition_name])
        client_w.release_collection(client, collection_name)
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, collection_name)


class TestMilvusClientLoadCollectionInvalid(TestcaseBase):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_load_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. collection name can only "
                                                f"contain numbers, letters and underscores: invalid parameter"}
        client_w.load_collection(client, name,
                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str("nonexisted")
        error = {ct.err_code: 1100, ct.err_msg: f"collection not found[database=default]"
                                                f"[collection={collection_name}]"}
        client_w.load_collection(client, collection_name,
                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        client_w.load_collection(client, collection_name,
                                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_load_collection_without_index(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        client_w.release_collection(client, collection_name)
        client_w.drop_index(client, collection_name, "vector")
        error = {ct.err_code: 700, ct.err_msg: f"index not found[collection={collection_name}]"}
        client_w.load_collection(client, collection_name,
                                 check_task=CheckTasks.err_res, check_items=error)
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, collection_name)


class TestMilvusClientLoadCollectionValid(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2", "IP"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["int", "string"])
    def id_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_loaded_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        client_w.load_collection(client, collection_name)
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partially_loaded_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition")
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        client_w.create_partition(client, collection_name, partition_name)
        client_w.release_collection(client, collection_name)
        client_w.load_partitions(client, collection_name, [partition_name])
        client_w.load_collection(client, collection_name)
        client_w.release_collection(client, collection_name)
        client_w.load_partitions(client, collection_name, ["_default", partition_name])
        client_w.load_collection(client, collection_name)
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, collection_name)


class TestMilvusClientDescribeCollectionInvalid(TestcaseBase):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_describe_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. collection name can only "
                                                f"contain numbers, letters and underscores: invalid parameter"}
        client_w.describe_collection(client, name,
                                     check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_describe_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = "nonexisted"
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection collection not "
                                               f"found[database=default][collection=nonexisted]"}
        client_w.describe_collection(client, collection_name,
                                     check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_describe_collection_deleted_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        client_w.drop_collection(client, collection_name)
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection collection not "
                                               f"found[database=default]"}
        client_w.describe_collection(client, collection_name,
                                     check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientHasCollectionInvalid(TestcaseBase):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_has_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. collection name can only "
                                                f"contain numbers, letters and underscores: invalid parameter"}
        client_w.has_collection(client, name,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_has_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = "nonexisted"
        result = client_w.has_collection(client, collection_name)[0]
        assert result == False

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_has_collection_deleted_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        client_w.drop_collection(client, collection_name)
        result = client_w.has_collection(client, collection_name)[0]
        assert result == False


class TestMilvusClientRenameCollectionInValid(TestcaseBase):
    """ Test case of rename collection interface """

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_rename_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=1][collection={name}]"}
        client_w.rename_collection(client, name, "new_collection",
                                     check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_rename_collection_not_existed_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = "nonexisted"
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=1][collection={collection_name}]"}
        client_w.rename_collection(client, collection_name, "new_collection",
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_rename_collection_duplicated_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 65535, ct.err_msg: f"duplicated new collection name default:{collection_name}"
                                                 f"with other collection name or alias"}
        client_w.rename_collection(client, collection_name, collection_name,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_rename_deleted_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim)
        client_w.drop_collection(client, collection_name)
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection collection not "
                                               f"found[database=default]"}
        client_w.rename_collection(client, collection_name, "new_collection",
                                     check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientRenameCollectionValid(TestcaseBase):
    """ Test case of rename collection interface """

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_rename_collection_multiple_times(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 2. rename with invalid new_name
        new_name = "new_name_rename"
        client_w.create_collection(client, collection_name, default_dim)
        times = 3
        for _ in range(times):
            client_w.rename_collection(client, collection_name, new_name)
            client_w.rename_collection(client, new_name, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_rename_collection_deleted_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        another_collection_name = cf.gen_unique_str("another_collection")
        # 1. create 2 collections
        client_w.create_collection(client, collection_name, default_dim)
        client_w.create_collection(client, another_collection_name, default_dim)
        # 2. drop one collection
        client_w.drop_collection(client, another_collection_name)
        # 3. rename to dropped collection
        client_w.rename_collection(client, collection_name, another_collection_name)


class TestMilvusClientUsingDatabaseInvalid(TestcaseBase):
    """ Test case of using database interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="pymilvus issue 1900")
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_using_database_invalid_db_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        error = {ct.err_code: 800, ct.err_msg: f"Invalid collection name: {name}. collection name can only "
                                                f"contain numbers, letters and underscores: invalid parameter"}
        client_w.using_database(client, name,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_using_database_not_exist_db_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        db_name = cf.gen_unique_str("nonexisted")
        error = {ct.err_code: 800, ct.err_msg: f"database not found[database=non-default]"}
        client_w.using_database(client, db_name,
                                check_task=CheckTasks.err_res, check_items=error)[0]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="pymilvus issue 1900")
    def test_milvus_client_using_database_db_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        db_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {db_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        client_w.using_database(client, db_name,
                                check_task=CheckTasks.err_res, check_items=error)[0]