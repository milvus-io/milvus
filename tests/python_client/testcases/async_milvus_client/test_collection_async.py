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
default_dim = 2
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name

class TestAsyncMilvusClientCollectionInvalid(TestMilvusClientV2Base):
    """ Test case of collection interface """

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
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_create_collection_invalid_collection_name(self, collection_name):
        """
        target: test fast create collection with invalid collection name
        method: create collection with invalid collection
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        # 1. create collection
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        await async_client.create_collection(collection_name, default_dim,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_create_collection_name_over_max_length(self):
        """
        target: test fast create collection with over max collection name length
        method: create collection with over max collection name length
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        # 1. create collection
        collection_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection name must be less than 255 characters"}
        await async_client.create_collection(collection_name, default_dim,
                                             check_task=CheckTasks.err_res, check_items=error)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_release_collection_invalid_collection_name(self, collection_name):
        """
        target: test release collection with invalid collection name
        method: release collection with invalid collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap
        
        # 1. release collection
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {collection_name}. "
                             f"the first character of a collection name must be an underscore or letter"}
        await async_client.release_collection(collection_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_release_collection_not_existed(self):
        """
        target: test release collection with nonexistent name
        method: release collection with nonexistent name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap
        
        # 1. release collection
        collection_name = cf.gen_unique_str("nonexisted")
        error = {ct.err_code: 1100, ct.err_msg: f"collection not found[database=default]"
                                                f"[collection={collection_name}]"}
        await async_client.release_collection(collection_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_release_collection_name_over_max_length(self):
        """
        target: test fast create collection with over max collection name length
        method: create collection with over max collection name length
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap
        
        # 1. release collection
        collection_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection name must be less than 255 characters"}
        await async_client.release_collection(collection_name, check_task=CheckTasks.err_res, check_items=error)

class TestAsyncMilvusClientCollectionValid(TestMilvusClientV2Base):
    """ Test case of collection interface """

    def teardown_method(self, method):
        self.init_async_milvus_client()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_milvus_client_wrap.close())
        super().teardown_method(method)

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    
    @pytest.mark.tags(CaseLabel.L0)
    async def test_async_milvus_client_release_collection_default(self):
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
        # 2. create partition
        partition_name = cf.gen_unique_str(partition_prefix)
        await async_client.create_partition(collection_name, partition_name)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions
        # 3. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        tasks = []
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        search_task = async_client.search(collection_name, vectors_to_search,
                                          check_task=CheckTasks.check_search_results,
                                          check_items={"enable_milvus_client_api": True,
                                                        "nq": len(vectors_to_search),
                                                        "limit": default_limit})
        tasks.append(search_task)
        # 5. query
        query_task = async_client.query(collection_name, filter=default_search_exp,
                                        check_task=CheckTasks.check_query_results,
                                        check_items={"exp_res": rows,
                                                    "with_vec": True,
                                                    "primary_field": default_primary_key_field_name})
        tasks.append(query_task)
        res = await asyncio.gather(*tasks)

        # 6. release collection
        await async_client.release_collection(collection_name)
        # 7. search
        error = {ct.err_code: 101, ct.err_msg: f"collection not loaded"}
        await async_client.search(collection_name, vectors_to_search,
                                  check_task=CheckTasks.err_res, 
                                  check_items=error)
        # 8. query
        await async_client.query(collection_name, filter=default_search_exp,
                                 check_task=CheckTasks.err_res, 
                                 check_items=error)
        # 9. load collection
        await async_client.load_collection(collection_name)
        # 10. search
        await async_client.search(collection_name, vectors_to_search,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "limit": default_limit})
        # 11. query
        await async_client.query(collection_name, filter=default_search_exp,
                                 check_task=CheckTasks.check_query_results,
                                 check_items={"exp_res": rows,
                                              "with_vec": True,
                                              "primary_field": default_primary_key_field_name})

        # 12. drop action
        if self.has_partition(client, collection_name, partition_name)[0]:
            await async_client.release_partitions(collection_name, partition_name)
            await async_client.drop_partition(collection_name, partition_name)
            partitions = self.list_partitions(client, collection_name)[0]
            assert partition_name not in partitions
        await async_client.drop_collection(collection_name)
