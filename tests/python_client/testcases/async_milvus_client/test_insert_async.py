import numpy as np
import pytest
import asyncio
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

pytestmark = pytest.mark.asyncio
default_nb = ct.default_nb
default_dim = 128
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name


class TestAsyncMilvusClientInsert(TestMilvusClientV2Base):
    """
    ******************************************************************
      The following cases are used to test insert async
    ******************************************************************
    """

    def teardown_method(self, method):
        """
        Clean up async client connection after each test method.
        This ensures proper resource cleanup and prevents connection leaks.
        """
        if self.async_milvus_client_wrap.async_milvus_client is not None:
            asyncio.run(self.async_milvus_client_wrap.close())
        super().teardown_method(method)

    @pytest.mark.tags(CaseLabel.L1)
    async def test_async_milvus_client_insert(self):
        """
        target: test async insert via Milvus async client
        method: insert with async milvus client
        expected: verify insert_count / row_count
        """
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create collection
        await async_client.create_collection(collection_name, default_dim)

        # 2. prepare data
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]

        # 3. insert
        res, _ = await async_client.insert(collection_name, rows)

        assert res["insert_count"] == ct.default_nb

        # 4. verify count
        await async_client.flush(collection_name)
        num_entities, _ = await async_client.get_collection_stats(collection_name)
        assert num_entities["row_count"] == ct.default_nb

        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_insert_large(self):
        """
        target: test insert with async
        method: insert 5w entities
        expected: verify num entities
        """
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        nb = 50000
        collection_name = cf.gen_collection_name_by_testcase_name()

        await async_client.create_collection(collection_name, default_dim)

        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(nb)]

        res, _ = await async_client.insert(collection_name, rows)
        assert res["insert_count"] == nb

        await async_client.flush(collection_name)
        num_entities, _ = await async_client.get_collection_stats(collection_name)
        assert num_entities["row_count"] == nb

        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_insert_invalid_data(self):
        """
        target: test insert async with invalid data
        method: insert async with invalid data
        expected: raise exception
        """
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_collection_name_by_testcase_name()
        await async_client.create_collection(collection_name, default_dim)

        # missing vector field
        rows = [{ct.default_primary_key_field_name: 1}]

        error = {ct.err_code: 1, ct.err_msg: "Insert missed an field `vector` to collection without set nullable==true or set default_value"}

        await async_client.insert(collection_name, rows, check_task=CheckTasks.err_res, check_items=error)

        await async_client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_milvus_client_insert_invalid_partition(self):
        """
        target: test insert async with invalid partition
        method: insert async with invalid partition
        expected: raise exception
        """
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap

        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        await async_client.create_collection(collection_name, default_dim)

        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}

        await async_client.insert(collection_name, data=rows, partition_name=partition_name,
                                  check_task=CheckTasks.err_res, check_items=error)

        await async_client.drop_collection(collection_name)