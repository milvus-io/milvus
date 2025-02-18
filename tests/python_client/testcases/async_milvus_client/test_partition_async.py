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


class TestAsyncMilvusClientPartitionInvalid(TestMilvusClientV2Base):
    """ Test case of partition interface """

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
    async def test_async_milvus_client_create_partition_invalid_collection_name(self, collection_name):
        """
        target: test create partition with invalid collection name
        method: create partition with invalid collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create partition
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        await async_client.create_partition(collection_name, partition_name,
                                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_create_partition_collection_name_over_max_length(self):
        """
        target: test create partition with collection name over max length 255
        method: create partition with collection name over max length 255
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = "a".join("a" for i in range(256))
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create partition
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
                             f"must be less than 255 characters: invalid parameter"}
        await async_client.create_partition(collection_name, partition_name,
                                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_create_partition_collection_name_not_existed(self):
        """
        target: test create partition with nonexistent collection name
        method: create partition with nonexistent collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str("partition_not_exist")
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default]"
                                               f"[collection={collection_name}]"}
        await async_client.create_partition(collection_name, partition_name,
                                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_create_partition_invalid_partition_name(self, partition_name):
        """
        target: test create partition with invalid partition name
        method: create partition with invalid partition name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        # 2. create partition
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}"}
        await async_client.create_partition(collection_name, partition_name,
                                            check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_create_partition_partition_name_over_max_length(self):
        """
        target: test create partition with partition name over max length 255
        method: create partition with partition name over max length 255
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)

        partition_name = "a".join("a" for i in range(256))
        # 2. create partition
        error = {ct.err_code: 65535,
                 ct.err_msg: f"Invalid partition name: {partition_name}. The length of a partition name "
                             f"must be less than 255 characters."}
        await async_client.create_partition(collection_name, partition_name,
                                            check_task=CheckTasks.err_res, check_items=error)
        
        # 3. drop action
        await async_client.drop_collection(collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_create_partition_name_lists(self):
        """
        target: test create partition with wrong partition name format list
        method: create partition with wrong partition name format list
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        partition_names = [cf.gen_unique_str(partition_prefix), cf.gen_unique_str(partition_prefix)]
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)
        # 2. create partition
        error = {ct.err_code: 999, ct.err_msg: f"`partition_name` value {partition_names} is illegal"}
        await async_client.create_partition(collection_name, partition_names,
                                            check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_drop_partition_invalid_collection_name(self, collection_name):
        """
        target: test drop partition with invalid collection name
        method: drop partition with invalid collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create partition
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        await async_client.drop_partition(collection_name, partition_name,
                                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_drop_partition_collection_name_over_max_length(self):
        """
        target: test drop partition with collection name over max length 255
        method: drop partition with collection name over max length 255
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = "a".join("a" for i in range(256))
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create partition
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
                             f"must be less than 255 characters: invalid parameter"}
        await async_client.drop_partition(collection_name, partition_name,
                                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_drop_partition_collection_name_not_existed(self):
        """
        target: test drop partition with nonexistent collection name
        method: drop partition with nonexistent collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str("partition_not_exist")
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default]"
                                               f"[collection={collection_name}]"}
        await async_client.drop_partition(collection_name, partition_name,
                                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_drop_partition_invalid_partition_name(self, partition_name):
        """
        target: test drop partition with invalid partition name
        method: drop partition with invalid partition name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)
        # 2. create partition
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}."}
        await async_client.drop_partition(collection_name, partition_name,
                                          check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_drop_partition_name_lists(self):
        """
        target: test drop partition with wrong partition name format list
        method: drop partition with wrong partition name format list
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        partition_names = [cf.gen_unique_str(partition_prefix), cf.gen_unique_str(partition_prefix)]
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)
        # 2. create partition
        error = {ct.err_code: 1, ct.err_msg: f"`partition_name` value {partition_names} is illegal"}
        await async_client.drop_partition(collection_name, partition_names,
                                          check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_load_partitions_invalid_collection_name(self, name):
        """
        target: test load partitions with invalid collection name
        method: load partitions with invalid collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap
        
        # 1. load partitions
        partition_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. the first character of a collection name "
                                                f"must be an underscore or letter: invalid parameter"}
        await async_client.load_partitions(name, partition_name,
                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_load_partitions_collection_not_existed(self):
        """
        target: test load partitions with nonexistent collection name
        method: load partitions with nonexistent collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap
        
        # 1. load partitions
        collection_name = cf.gen_unique_str("nonexisted")
        partition_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1100, ct.err_msg: f"collection not found[database=default]"
                                                f"[collection={collection_name}]"}
        await async_client.load_partitions(collection_name, partition_name,
                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_load_partitions_collection_name_over_max_length(self):
        """
        target: test load partitions with collection name over max length 255
        method: load partitions with collection name over max length 255
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        # 1. load partitions
        collection_name = "a".join("a" for i in range(256))
        partition_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        await async_client.load_partitions(collection_name, partition_name,
                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="pymilvus issue 1896")
    @pytest.mark.parametrize("name", ["12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_load_partitions_invalid_partition_name(self, name):
        """
        target: test load partitions with invalid partition name
        method: load partitions with invalid partition name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid partition name: {name}. collection name can only "
                                                f"contain numbers, letters and underscores: invalid parameter"}
        await async_client.load_partitions(collection_name, name,
                                           check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="pymilvus issue 1896")
    async def test_async_milvus_client_load_partitions_partition_not_existed(self):
        """
        target: test load partitions with nonexistent partition name
        method: load partitions with nonexistent partition name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap
        
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("nonexisted")
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"partition not found[database=default]"
                                                f"[collection={collection_name}]"}
        await async_client.load_partitions(collection_name, partition_name,
                                           check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="pymilvus issue 1896")
    async def test_async_milvus_client_load_partitions_partition_name_over_max_length(self):
        """
        target: test load partitions with partition name over max length 255
        method: load partitions with partition name over max length 255
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        partition_name = "a".join("a" for i in range(256))
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        await async_client.load_partitions(collection_name, partition_name,
                                           check_task=CheckTasks.err_res, check_items=error)
    
        # 3. drop action
        await async_client.drop_collection(collection_name)
    
    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_load_partitions_without_index(self):
        """
        target: test load partitions after drop index
        method: load partitions after drop index
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim, consistency_level="Strong")
        # 2. drop index
        await async_client.release_collection(collection_name)
        await async_client.drop_index(collection_name, "vector")
        # 3. load partition
        error = {ct.err_code: 700, ct.err_msg: f"index not found[collection={collection_name}]"}
        await async_client.load_partitions(collection_name, partition_name,
                                           check_task=CheckTasks.err_res, check_items=error)
    
        # 4. drop action
        await async_client.drop_collection(collection_name)
      
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_release_partitions_invalid_collection_name(self, collection_name):
        """
        target: test release partitions with invalid collection name
        method: release partitions with invalid collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. release partitions
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                               f"collection name must be an underscore or letter: invalid parameter"}
        await async_client.release_partitions(collection_name, partition_name,
                                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_release_partitions_collection_name_over_max_length(self):
        """
        target: test release partitions with collection name over max length 255
        method: release partitions with collection name over max length 255
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = "a".join("a" for i in range(256))
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. release partitions
        error = {ct.err_code: 999,
                 ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
                             f"must be less than 255 characters: invalid parameter"}
        await async_client.release_partitions(collection_name, partition_name,
                                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_release_partitions_collection_name_not_existed(self):
        """
        target: test release partitions with nonexistent collection name
        method: release partitions with nonexistent collection name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str("collection_not_exist")
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. release partitions
        error = {ct.err_code: 999, ct.err_msg: f"collection not found[database=default]"
                                               f"[collection={collection_name}]"}
        await async_client.release_partitions(collection_name, partition_name,
                                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="pymilvus issue 1896")
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    async def test_async_milvus_client_release_partitions_invalid_partition_name(self, partition_name):
        """
        target: test release partitions with invalid partition name
        method: release partitions with invalid partition name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)
        # 2. release partitions
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}. The first character of a "
                                                 f"partition name must be an underscore or letter.]"}
        await async_client.release_partitions(collection_name, partition_name,
                                              check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="pymilvus issue 1896")
    async def test_async_milvus_client_release_partitions_invalid_partition_name_list(self):
        """
        target: test release partitions with invalid partition name list
        method: release partitions with invalid partition name list
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)
        # 2. release partition
        partition_name = ["12-s"]
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}. The first character of a "
                                                 f"partition name must be an underscore or letter.]"}
        await async_client.release_partitions(collection_name, partition_name,
                                              check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_release_partitions_name_lists_empty(self):
        """
        target: test release partitions with partition name list empty
        method: release partitions with partition name list empty
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        partition_names = []
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)
        # 2. release partition
        error = {ct.err_code: 999, ct.err_msg: f"invalid parameter[expected=any partition][actual=empty partition list"}
        await async_client.release_partitions(collection_name, partition_names,
                                              check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_release_partitions_name_lists_not_all_exists(self):
        """
        target: test release partitions with partition name lists not all exists
        method: release partitions with partition name lists not all exists
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        not_exist_partition = cf.gen_unique_str("partition_not_exist")
        partition_names = ["_default", not_exist_partition]
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)
        # 2. release partitions
        error = {ct.err_code: 999, ct.err_msg: f"partition not found[partition={not_exist_partition}]"}
        await async_client.release_partitions(collection_name, partition_names,
                                              check_task=CheckTasks.err_res, check_items=error)

        # 3. drop action
        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_release_partitions_partition_name_not_existed(self):
        """
        target: test release partitions with nonexistent partition name
        method: release partitions with nonexistent partition name
        expected: raise exception
        """
        client = self._client()
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition_not_exist")
        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)
        # 2. release partitions
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}
        await async_client.release_partitions(collection_name, partition_name,
                                              check_task=CheckTasks.err_res, check_items=error)
        partition_name = ""
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}
        await async_client.release_partitions(collection_name, partition_name,
                                              check_task=CheckTasks.err_res, check_items=error)
        # 3. drop action
        await async_client.drop_collection(collection_name)

class TestAsyncMilvusClientPartitionValid(TestMilvusClientV2Base):
    """ Test case of partition interface """

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
    async def test_async_milvus_client_create_drop_partition_default(self):
        """
        target: test create and drop partition normal case
        method: 1. create collection, partition 2. insert to partition 3. search and query 4. drop partition, collection
        expected: run successfully
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
        # 2. create partition
        partition_name = cf.gen_unique_str(partition_prefix)
        await async_client.create_partition(collection_name, partition_name)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions
        # 3. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows, partition_name=partition_name)
        tasks = []
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        search_task = async_client.search(collection_name, vectors_to_search,
                                          partition_names=[partition_name],
                                          check_task=CheckTasks.check_search_results,
                                          check_items={"enable_milvus_client_api": True,
                                                       "nq": len(vectors_to_search),
                                                       "limit": default_limit})
        tasks.append(search_task)
        # 5. query
        query_task = async_client.query(collection_name, filter=default_search_exp,
                                        partition_names=[partition_name],
                                        check_task=CheckTasks.check_query_results,
                                        check_items={"exp_res": rows,
                                                     "with_vec": True,
                                                     "primary_field": default_primary_key_field_name})
        tasks.append(query_task)
        res = await asyncio.gather(*tasks)
        
        # 6. drop action
        if self.has_partition(client, collection_name, partition_name)[0]:
            await async_client.release_partitions(collection_name, partition_name)
            await async_client.drop_partition(collection_name, partition_name)
            partitions = self.list_partitions(client, collection_name)[0]
            assert partition_name not in partitions
        await async_client.drop_collection(collection_name)


    @pytest.mark.tags(CaseLabel.L0)
    async def test_async_milvus_client_load_release_partitions(self):
        """
        target: test load and release partitions normal case
        method: 1. create collection, two partitions
                2. insert different data to two partitions
                3. search and query 
                4. release partitions, search and query 
                5. load partitions, search and query 
                4. drop partition, collection
        expected: run successfully
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
        # 2. create partition
        partition_name_1 = cf.gen_unique_str(partition_prefix)
        await async_client.create_partition(collection_name, partition_name_1)
        partition_name_2 = cf.gen_unique_str(partition_prefix)
        await async_client.create_partition(collection_name, partition_name_2)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name_1 in partitions
        assert partition_name_2 in partitions
        # 3. insert
        rng = np.random.default_rng(seed=19530)
        rows_default = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows_default)
        rows_1 = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb, 2 * default_nb)]
        self.insert(client, collection_name, rows_1, partition_name=partition_name_1)
        rows_2 = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(2 * default_nb, 3 * default_nb)]
        self.insert(client, collection_name, rows_2, partition_name=partition_name_2)
        tasks = []
        # 4. search and query
        vectors_to_search = rng.random((1, default_dim))
        # search single partition
        search_task = async_client.search(collection_name, vectors_to_search,
                                                partition_names=[partition_name_1],
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"enable_milvus_client_api": True,
                                                             "nq": len(vectors_to_search),
                                                             "limit": default_limit})
        tasks.append(search_task)
        # search multi partition
        search_task_multi = async_client.search(collection_name, vectors_to_search,
                                                          partition_names=[partition_name_1, partition_name_2],
                                                          check_task=CheckTasks.check_search_results,
                                                          check_items={"enable_milvus_client_api": True,
                                                                       "nq": len(vectors_to_search),
                                                                       "limit": default_limit})
        tasks.append(search_task_multi)
        # query single partition
        query_task = async_client.query(collection_name, filter=default_search_exp,
                                        partition_names=[partition_name_1],
                                        check_task=CheckTasks.check_query_results,
                                        check_items={"exp_res": rows_1,
                                                     "with_vec": True,
                                                     "primary_field": default_primary_key_field_name})
        tasks.append(query_task)
        # query multi partition
        query_task_multi = async_client.query(collection_name, filter=default_search_exp,
                                        partition_names=[partition_name_1, partition_name_2],
                                        check_task=CheckTasks.check_query_results,
                                        check_items={"exp_res": rows_1 + rows_2,
                                                     "with_vec": True,
                                                     "primary_field": default_primary_key_field_name})
        tasks.append(query_task_multi)
        res = await asyncio.gather(*tasks)
        # 5. release partitions, search and query
        await async_client.release_partitions(collection_name, partition_name_1)
        error = {ct.err_code: 201, ct.err_msg: "partition not loaded"}
        await async_client.search(collection_name, vectors_to_search,
                                  partition_names=[partition_name_1],
                                  check_task=CheckTasks.err_res, 
                                  check_items=error)
        
        await async_client.query(collection_name, filter=default_search_exp,
                                 partition_names=[partition_name_1],
                                 check_task=CheckTasks.err_res, 
                                 check_items=error)

        await async_client.search(collection_name, vectors_to_search,
                                  partition_names=[partition_name_2],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "limit": default_limit})
        await async_client.query(collection_name, filter=default_search_exp,
                                 partition_names=[partition_name_2],
                                 check_task=CheckTasks.check_query_results,
                                 check_items={"exp_res": rows_2,
                                              "with_vec": True,
                                              "primary_field": default_primary_key_field_name})

        # 6. load partitions, search and query
        tasks_after_load = []
        await async_client.load_partitions(collection_name, [partition_name_1, partition_name_2])
        search_task = async_client.search(collection_name, vectors_to_search,
                                          check_task=CheckTasks.check_search_results,
                                          check_items={"enable_milvus_client_api": True,
                                                       "nq": len(vectors_to_search),
                                                       "limit": default_limit})
        tasks_after_load.append(search_task)
        query_task = async_client.query(collection_name, filter=default_search_exp,
                                        check_task=CheckTasks.check_query_results,
                                        check_items={"exp_res": rows_default + rows_1 + rows_2,
                                                     "with_vec": True,
                                                     "primary_field": default_primary_key_field_name})
        tasks_after_load.append(query_task)
        res = await asyncio.gather(*tasks_after_load)
        
        # 7. drop action
        await async_client.drop_collection(collection_name)