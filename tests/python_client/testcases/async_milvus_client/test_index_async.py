import random
import time
import numpy as np
import pytest
import asyncio
from pymilvus.client.types import LoadState, DataType
from pymilvus import AnnSearchRequest, RRFRanker

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log

pytestmark = pytest.mark.asyncio
prefix = "async"
partition_prefix = "async_partition"
async_default_nb = 5000
default_nb = ct.default_nb
default_dim = 128
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name

class TestAsyncMilvusClientIndexInvalid(TestMilvusClientV2Base):
    """ Test case of index interface """

    def teardown_method(self, method):
        self.init_async_milvus_client()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_milvus_client_wrap.close())
        super().teardown_method(method)

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_create_index_invalid_collection_name(self, name):
        """
        target: test create index with invalid collection name
        method: create index with invalid collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        # 1. create collection
        collection_name = cf.gen_unique_str(prefix)
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        await async_client.release_collection(collection_name)
        await async_client.drop_index(collection_name, "vector")
        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector")
        # 3. create index
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. the first character of a collection "
                                                f"name must be an underscore or letter: invalid parameter"}
        await async_client.create_index(name, index_params,
                                        check_task=CheckTasks.err_res, 
                                        check_items=error)
        # 4. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["a".join("a" for i in range(256))])
    async def test_async_milvus_client_create_index_collection_name_over_max_length(self, name):
        """
        target: test create index with over max collection name length
        method: create index with over max collection name length
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        # 1. create collection
        collection_name = cf.gen_unique_str(prefix)
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        await async_client.release_collection(collection_name)
        await async_client.drop_index(collection_name, "vector")
        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector")
        # 3. create index
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. the length of a collection name "
                                                f"must be less than 255 characters: invalid parameter"}
        await async_client.create_index(name, index_params,
                                        check_task=CheckTasks.err_res, 
                                        check_items=error)
        # 4. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_create_index_collection_name_not_existed(self):
        """
        target: test create index with nonexistent collection name
        method: create index with nonexistent collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        not_existed_collection_name = cf.gen_unique_str("not_existed_collection")
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        await async_client.release_collection(collection_name)
        await async_client.drop_index(collection_name, "vector")
        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector")
        # 3. create index
        error = {ct.err_code: 100,
                 ct.err_msg: f"can't find collection[database=default][collection={not_existed_collection_name}]"}
        await async_client.create_index(not_existed_collection_name, index_params,
                                        check_task=CheckTasks.err_res, 
                                        check_items=error)
        # 4. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    async def test_async_milvus_client_create_index_invalid_index_type(self, index):
        """
        target: test create index with invalid index type name
        method: create index with invalid index type name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        await async_client.release_collection(collection_name)
        await async_client.drop_index(collection_name, "vector")
        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type=index)
        # 3. create index
        error = {ct.err_code: 1100, ct.err_msg: f"invalid parameter[expected=valid index][actual=invalid index type: {index}"}
        # It's good to show what the valid indexes are
        await async_client.create_index(collection_name, index_params,
                                        check_task=CheckTasks.err_res, 
                                        check_items=error)
        # 4. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metric", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    async def test_async_milvus_client_create_index_invalid_metric_type(self, metric):
        """
        target: test create index with invalid metric type
        method: create index with invalid metric type
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        await async_client.release_collection(collection_name)
        await async_client.drop_index(collection_name, "vector")
        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", metric_type=metric)
        # 3. create index
        error = {ct.err_code: 1100, ct.err_msg: f"float vector index does not support metric type: {metric}: "
                                                f"invalid parameter[expected=valid index params][actual=invalid index params"}
        # It's good to show what the valid index params are
        await async_client.create_index(collection_name, index_params,
                                        check_task=CheckTasks.err_res, 
                                        check_items=error)
        # 4. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_drop_index_before_release(self):
        """
        target: test drop index when collection are not released
        method: drop index when collection are not released
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        # 2. drop index
        error = {ct.err_code: 65535, ct.err_msg: f"index cannot be dropped, collection is loaded, "
                                                 f"please release it first"}
        await async_client.drop_index(collection_name, "vector", check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_drop_index_invalid_collection_name(self, name):
        """
        target: test drop index with invalid collection name
        method: drop index with invalid collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        # 1. create collection
        collection_name = cf.gen_unique_str(prefix)
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        await async_client.release_collection(collection_name)
        # 2. drop index
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. the first character of a collection "
                                                f"name must be an underscore or letter: invalid parameter"}
        await async_client.drop_index(name, "vector", check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["a".join("a" for i in range(256))])
    async def test_async_milvus_client_drop_index_collection_name_over_max_length(self, name):
        """
        target: test drop index with over max collection name length
        method: drop index with over max collection name length
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        # 1. create collection
        collection_name = cf.gen_unique_str(prefix)
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        await async_client.release_collection(collection_name)
        # 2. drop index
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. the length of a collection name "
                                                f"must be less than 255 characters: invalid parameter"}
        await async_client.drop_index(name, "vector", check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

class TestAsyncMilvusClientIndexValid(TestMilvusClientV2Base):
    """ Test case of index interface """

    def teardown_method(self, method):
        self.init_async_milvus_client()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_milvus_client_wrap.close())
        super().teardown_method(method)

    @pytest.fixture(scope="function", params=["COSINE", "L2", "IP"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:7],
                             ct.default_all_indexes_params[:7]))
    async def test_async_milvus_client_create_drop_index_default(self, index, params, metric_type):
        """
        target: test create and drop index normal case
        method: create collection, index; insert; search and query; drop index
        expected: search/query successfully; create/drop index successfully
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        # 1. create collection
        collection_name = cf.gen_unique_str(prefix)
        await async_client.create_collection(collection_name, default_dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        
        await async_client.release_collection(collection_name)
        await async_client.drop_index(collection_name, "vector")
        res = self.list_indexes(client, collection_name)[0]
        assert res == []

        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type=index, metric_type=metric_type, params=params)
        # 3. create index
        await async_client.create_index(collection_name, index_params)

        # 4. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.load_collection(client, collection_name)
        
        tasks = []
        # 5. search
        vectors_to_search = rng.random((1, default_dim))
        search_task = self.async_milvus_client_wrap. \
                        search(collection_name, vectors_to_search,
                               check_task=CheckTasks.check_search_results,
                               check_items={"enable_milvus_client_api": True,
                                            "nq": len(vectors_to_search),
                                            "limit": default_limit,
                                            "pk_name": default_primary_key_field_name})
        tasks.append(search_task)
        # 6. query
        query_task = self.async_milvus_client_wrap. \
                        query(collection_name, filter=default_search_exp,
                              check_task=CheckTasks.check_query_results,
                              check_items={"exp_res": rows,
                                           "with_vec": True,
                                           "pk_name": default_primary_key_field_name})
        tasks.append(query_task)
        res = await asyncio.gather(*tasks)

        # 7. drop index
        await async_client.release_collection(collection_name)
        await async_client.drop_index(collection_name, "vector")
        res = self.list_indexes(client, collection_name)[0]
        assert res == []

        # 8. drop action
        self.drop_collection(client, collection_name)
