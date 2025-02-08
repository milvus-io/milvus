import random
import time
import pytest
import asyncio
from pymilvus.client.types import LoadState, DataType
from pymilvus import AnnSearchRequest, RRFRanker

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log

pytestmark = pytest.mark.asyncio

prefix = "async"
async_default_nb = 5000
default_pk_name = "id"
default_vector_name = "vector"


class TestAsyncMilvusClient(TestcaseBase):

    def teardown_method(self, method):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_milvus_client_wrap.close())
        super().teardown_method(method)

    @pytest.mark.tags(CaseLabel.L0)
    async def test_async_client_default(self):
        # init client
        milvus_client = self._connect(enable_milvus_client_api=True)
        self.init_async_milvus_client()

        # create collection
        c_name = cf.gen_unique_str(prefix)
        await self.async_milvus_client_wrap.create_collection(c_name, dimension=ct.default_dim)
        collections, _ = self.high_level_api_wrap.list_collections(milvus_client)
        assert c_name in collections

        # insert entities
        rows = [
            {default_pk_name: i, default_vector_name: [random.random() for _ in range(ct.default_dim)]}
            for i in range(async_default_nb)]
        start_time = time.time()
        tasks = []
        step = 1000
        for i in range(0, async_default_nb, step):
            task = self.async_milvus_client_wrap.insert(c_name, rows[i:i + step])
            tasks.append(task)
        insert_res = await asyncio.gather(*tasks)
        end_time = time.time()
        log.info("Total time: {:.2f} seconds".format(end_time - start_time))
        for r in insert_res:
            assert r[0]['insert_count'] == step

        # dql tasks
        tasks = []
        # search default
        vector = cf.gen_vectors(ct.default_nq, ct.default_dim)
        default_search_task = self.async_milvus_client_wrap.search(c_name, vector, limit=ct.default_limit,
                                                                   check_task=CheckTasks.check_search_results,
                                                                   check_items={"enable_milvus_client_api": True,
                                                                                "nq": ct.default_nq,
                                                                                "limit": ct.default_limit})
        tasks.append(default_search_task)

        # search with filter & search_params
        sp = {"metric_type": "COSINE", "params": {"ef": "96"}}
        filter_params_search_task = self.async_milvus_client_wrap.search(c_name, vector, limit=ct.default_limit,
                                                                         filter=f"{default_pk_name} > 10",
                                                                         search_params=sp,
                                                                         check_task=CheckTasks.check_search_results,
                                                                         check_items={"enable_milvus_client_api": True,
                                                                                      "nq": ct.default_nq,
                                                                                      "limit": ct.default_limit})
        tasks.append(filter_params_search_task)

        # search output fields
        output_search_task = self.async_milvus_client_wrap.search(c_name, vector, limit=ct.default_limit,
                                                                  output_fields=["*"],
                                                                  check_task=CheckTasks.check_search_results,
                                                                  check_items={"enable_milvus_client_api": True,
                                                                               "nq": ct.default_nq,
                                                                               "limit": ct.default_limit})
        tasks.append(output_search_task)

        # query with filter and default output "*"
        exp_query_res = [{default_pk_name: i} for i in range(ct.default_limit)]
        filter_query_task = self.async_milvus_client_wrap.query(c_name,
                                                                filter=f"{default_pk_name} < {ct.default_limit}",
                                                                output_fields=[default_pk_name],
                                                                check_task=CheckTasks.check_query_results,
                                                                check_items={"exp_res": exp_query_res,
                                                                             "primary_field": default_pk_name})
        tasks.append(filter_query_task)
        # query with ids and output all fields
        ids_query_task = self.async_milvus_client_wrap.query(c_name,
                                                             ids=[i for i in range(ct.default_limit)],
                                                             output_fields=["*"],
                                                             check_task=CheckTasks.check_query_results,
                                                             check_items={"exp_res": rows[:ct.default_limit],
                                                                          "with_vec": True,
                                                                          "primary_field": default_pk_name})
        tasks.append(ids_query_task)
        # get with ids
        get_task = self.async_milvus_client_wrap.get(c_name,
                                                     ids=[0, 1],
                                                     output_fields=[default_pk_name, default_vector_name],
                                                     check_task=CheckTasks.check_query_results,
                                                     check_items={"exp_res": rows[:2], "with_vec": True,
                                                                  "primary_field": default_pk_name})
        tasks.append(get_task)
        await asyncio.gather(*tasks)

    @pytest.mark.tags(CaseLabel.L0)
    async def test_async_client_partition(self):
        # init client
        milvus_client = self._connect(enable_milvus_client_api=True)
        self.init_async_milvus_client()

        # create collection & partition
        c_name = cf.gen_unique_str(prefix)
        p_name = cf.gen_unique_str("par")
        await self.async_milvus_client_wrap.create_collection(c_name, dimension=ct.default_dim)
        collections, _ = self.high_level_api_wrap.list_collections(milvus_client)
        assert c_name in collections
        self.high_level_api_wrap.create_partition(milvus_client, c_name, p_name)
        partitions, _ = self.high_level_api_wrap.list_partitions(milvus_client, c_name)
        assert p_name in partitions

        # insert entities
        rows = [
            {default_pk_name: i, default_vector_name: [random.random() for _ in range(ct.default_dim)]}
            for i in range(async_default_nb)]
        start_time = time.time()
        tasks = []
        step = 1000
        for i in range(0, async_default_nb, step):
            task = self.async_milvus_client_wrap.insert(c_name, rows[i:i + step], partition_name=p_name)
            tasks.append(task)
        insert_res = await asyncio.gather(*tasks)
        end_time = time.time()
        log.info("Total time: {:.2f} seconds".format(end_time - start_time))
        for r in insert_res:
            assert r[0]['insert_count'] == step

        # count from default partition
        count_res, _ = await self.async_milvus_client_wrap.query(c_name, output_fields=["count(*)"],
                                                                 partition_names=[ct.default_partition_name])
        assert count_res[0]["count(*)"] == 0

        # dql tasks
        tasks = []
        # search default
        vector = cf.gen_vectors(ct.default_nq, ct.default_dim)
        default_search_task = self.async_milvus_client_wrap.search(c_name, vector, limit=ct.default_limit,
                                                                   partition_names=[p_name],
                                                                   check_task=CheckTasks.check_search_results,
                                                                   check_items={"enable_milvus_client_api": True,
                                                                                "nq": ct.default_nq,
                                                                                "limit": ct.default_limit})
        tasks.append(default_search_task)

        # search with filter & search_params
        sp = {"metric_type": "COSINE", "params": {"ef": "96"}}
        filter_params_search_task = self.async_milvus_client_wrap.search(c_name, vector, limit=ct.default_limit,
                                                                         filter=f"{default_pk_name} > 10",
                                                                         search_params=sp,
                                                                         partition_names=[p_name],
                                                                         check_task=CheckTasks.check_search_results,
                                                                         check_items={"enable_milvus_client_api": True,
                                                                                      "nq": ct.default_nq,
                                                                                      "limit": ct.default_limit})
        tasks.append(filter_params_search_task)

        # search output fields
        output_search_task = self.async_milvus_client_wrap.search(c_name, vector, limit=ct.default_limit,
                                                                  output_fields=["*"],
                                                                  partition_names=[p_name],
                                                                  check_task=CheckTasks.check_search_results,
                                                                  check_items={"enable_milvus_client_api": True,
                                                                               "nq": ct.default_nq,
                                                                               "limit": ct.default_limit})
        tasks.append(output_search_task)

        # query with filter and default output "*"
        exp_query_res = [{default_pk_name: i} for i in range(ct.default_limit)]
        filter_query_task = self.async_milvus_client_wrap.query(c_name,
                                                                filter=f"{default_pk_name} < {ct.default_limit}",
                                                                output_fields=[default_pk_name],
                                                                partition_names=[p_name],
                                                                check_task=CheckTasks.check_query_results,
                                                                check_items={"exp_res": exp_query_res,
                                                                             "primary_field": default_pk_name})
        tasks.append(filter_query_task)
        # query with ids and output all fields
        ids_query_task = self.async_milvus_client_wrap.query(c_name,
                                                             ids=[i for i in range(ct.default_limit)],
                                                             output_fields=["*"],
                                                             partition_names=[p_name],
                                                             check_task=CheckTasks.check_query_results,
                                                             check_items={"exp_res": rows[:ct.default_limit],
                                                                          "with_vec": True,
                                                                          "primary_field": default_pk_name})
        tasks.append(ids_query_task)
        # get with ids
        get_task = self.async_milvus_client_wrap.get(c_name,
                                                     ids=[0, 1], partition_names=[p_name],
                                                     output_fields=[default_pk_name, default_vector_name],
                                                     check_task=CheckTasks.check_query_results,
                                                     check_items={"exp_res": rows[:2], "with_vec": True,
                                                                  "primary_field": default_pk_name})
        tasks.append(get_task)
        await asyncio.gather(*tasks)

    @pytest.mark.tags(CaseLabel.L0)
    async def test_async_client_with_schema(self, schema):
        # init client
        pk_field_name = "id"
        milvus_client = self._connect(enable_milvus_client_api=True)
        self.init_async_milvus_client()

        # create collection
        c_name = cf.gen_unique_str(prefix)
        schema = self.async_milvus_client_wrap.create_schema(auto_id=False,
                                                             partition_key_field=ct.default_int64_field_name)
        schema.add_field(pk_field_name, DataType.VARCHAR, max_length=100, is_primary=True)
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_partition_key=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
        schema.add_field(default_vector_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
        await self.async_milvus_client_wrap.create_collection(c_name, schema=schema)
        collections, _ = self.high_level_api_wrap.list_collections(milvus_client)
        assert c_name in collections

        # insert entities
        rows = [
            {pk_field_name: str(i),
             ct.default_int64_field_name: i,
             ct.default_float_vec_field_name: [random.random() for _ in range(ct.default_dim)],
             default_vector_name: [random.random() for _ in range(ct.default_dim)],
             } for i in range(async_default_nb)]
        start_time = time.time()
        tasks = []
        step = 1000
        for i in range(0, async_default_nb, step):
            task = self.async_milvus_client_wrap.insert(c_name, rows[i:i + step])
            tasks.append(task)
        insert_res = await asyncio.gather(*tasks)
        end_time = time.time()
        log.info("Total time: {:.2f} seconds".format(end_time - start_time))
        for r in insert_res:
            assert r[0]['insert_count'] == step

        # flush
        self.high_level_api_wrap.flush(milvus_client, c_name)
        stats, _ = self.high_level_api_wrap.get_collection_stats(milvus_client, c_name)
        assert stats["row_count"] == async_default_nb

        # create index -> load
        index_params, _ = self.high_level_api_wrap.prepare_index_params(milvus_client,
                                                                        field_name=ct.default_float_vec_field_name,
                                                                        index_type="HNSW", metric_type="COSINE", M=30,
                                                                        efConstruction=200)
        index_params.add_index(field_name=default_vector_name, index_type="IVF_SQ8",
                               metric_type="L2", nlist=32)
        await self.async_milvus_client_wrap.create_index(c_name, index_params)
        await self.async_milvus_client_wrap.load_collection(c_name)

        _index, _ = self.high_level_api_wrap.describe_index(milvus_client, c_name, default_vector_name)
        assert _index["indexed_rows"] == async_default_nb
        assert _index["state"] == "Finished"
        _load, _ = self.high_level_api_wrap.get_load_state(milvus_client, c_name)
        assert _load["state"] == LoadState.Loaded

        # dql tasks
        tasks = []
        # search default
        vector = cf.gen_vectors(ct.default_nq, ct.default_dim)
        default_search_task = self.async_milvus_client_wrap.search(c_name, vector, limit=ct.default_limit,
                                                                   anns_field=ct.default_float_vec_field_name,
                                                                   search_params={"metric_type": "COSINE",
                                                                                  "params": {"ef": "96"}},
                                                                   check_task=CheckTasks.check_search_results,
                                                                   check_items={"enable_milvus_client_api": True,
                                                                                "nq": ct.default_nq,
                                                                                "limit": ct.default_limit})
        tasks.append(default_search_task)

        # hybrid_search
        search_param = {
            "data": cf.gen_vectors(ct.default_nq, ct.default_dim, vector_data_type="FLOAT_VECTOR"),
            "anns_field": ct.default_float_vec_field_name,
            "param": {"metric_type": "COSINE", "params": {"ef": "96"}},
            "limit": ct.default_limit,
            "expr": f"{ct.default_int64_field_name} > 10"}
        req = AnnSearchRequest(**search_param)

        search_param2 = {
            "data": cf.gen_vectors(ct.default_nq, ct.default_dim, vector_data_type="FLOAT_VECTOR"),
            "anns_field": default_vector_name,
            "param": {"metric_type": "L2", "params": {"nprobe": "32"}},
            "limit": ct.default_limit
        }
        req2 = AnnSearchRequest(**search_param2)
        _output_fields = [ct.default_int64_field_name, ct.default_string_field_name]
        filter_params_search_task = self.async_milvus_client_wrap.hybrid_search(c_name, [req, req2], RRFRanker(),
                                                                                limit=5,
                                                                                check_task=CheckTasks.check_search_results,
                                                                                check_items={
                                                                                    "enable_milvus_client_api": True,
                                                                                    "nq": ct.default_nq,
                                                                                    "limit": 5})
        tasks.append(filter_params_search_task)

        # get with ids
        get_task = self.async_milvus_client_wrap.get(c_name, ids=['0', '1'], output_fields=[ct.default_int64_field_name,
                                                                                            pk_field_name])
        tasks.append(get_task)
        await asyncio.gather(*tasks)

    @pytest.mark.tags(CaseLabel.L0)
    async def test_async_client_dml(self):
        # init client
        milvus_client = self._connect(enable_milvus_client_api=True)
        self.init_async_milvus_client()

        # create collection
        c_name = cf.gen_unique_str(prefix)
        await self.async_milvus_client_wrap.create_collection(c_name, dimension=ct.default_dim)
        collections, _ = self.high_level_api_wrap.list_collections(milvus_client)
        assert c_name in collections

        # insert entities
        rows = [
            {default_pk_name: i, default_vector_name: [random.random() for _ in range(ct.default_dim)]}
            for i in range(ct.default_nb)]
        start_time = time.time()
        tasks = []
        step = 1000
        for i in range(0, ct.default_nb, step):
            task = self.async_milvus_client_wrap.insert(c_name, rows[i:i + step])
            tasks.append(task)
        insert_res = await asyncio.gather(*tasks)
        end_time = time.time()
        log.info("Total time: {:.2f} seconds".format(end_time - start_time))
        for r in insert_res:
            assert r[0]['insert_count'] == step

        # dml tasks
        # query id -> upsert id -> query id -> delete id -> query id
        _id = 10
        get_res, _ = await self.async_milvus_client_wrap.get(c_name, ids=[_id],
                                                             output_fields=[default_pk_name, default_vector_name])
        assert len(get_res) == 1

        # upsert
        upsert_row = [{
            default_pk_name: _id, default_vector_name: [random.random() for _ in range(ct.default_dim)]
        }]
        upsert_res, _ = await self.async_milvus_client_wrap.upsert(c_name, upsert_row)
        assert upsert_res["upsert_count"] == 1

        # get _id after upsert
        get_res, _ = await self.async_milvus_client_wrap.get(c_name, ids=[_id],
                                                             output_fields=[default_pk_name, default_vector_name])
        for j in range(5):
            assert abs(get_res[0][default_vector_name][j] - upsert_row[0][default_vector_name][j]) < ct.epsilon

        # delete
        del_res, _ = await self.async_milvus_client_wrap.delete(c_name, ids=[_id])
        assert del_res["delete_count"] == 1

        # query after delete
        get_res, _ = await self.async_milvus_client_wrap.get(c_name, ids=[_id],
                                                             output_fields=[default_pk_name, default_vector_name])
        assert len(get_res) == 0

    @pytest.mark.tags(CaseLabel.L2)
    async def test_async_client_with_db(self):
        # init client
        milvus_client = self._connect(enable_milvus_client_api=True)
        db_name = cf.gen_unique_str("db")
        self.high_level_api_wrap.create_database(milvus_client, db_name)
        self.high_level_api_wrap.close(milvus_client)
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        milvus_client, _ = self.connection_wrap.MilvusClient(uri=uri, db_name=db_name)
        self.async_milvus_client_wrap.init_async_client(uri, db_name=db_name)

        # create collection
        c_name = cf.gen_unique_str(prefix)
        await self.async_milvus_client_wrap.create_collection(c_name, dimension=ct.default_dim)
        collections, _ = self.high_level_api_wrap.list_collections(milvus_client)
        assert c_name in collections

        # insert entities
        rows = [
            {default_pk_name: i, default_vector_name: [random.random() for _ in range(ct.default_dim)]}
            for i in range(async_default_nb)]
        start_time = time.time()
        tasks = []
        step = 1000
        for i in range(0, async_default_nb, step):
            task = self.async_milvus_client_wrap.insert(c_name, rows[i:i + step])
            tasks.append(task)
        insert_res = await asyncio.gather(*tasks)
        end_time = time.time()
        log.info("Total time: {:.2f} seconds".format(end_time - start_time))
        for r in insert_res:
            assert r[0]['insert_count'] == step

        # dql tasks
        tasks = []
        # search default
        vector = cf.gen_vectors(ct.default_nq, ct.default_dim)
        default_search_task = self.async_milvus_client_wrap.search(c_name, vector, limit=ct.default_limit,
                                                                   check_task=CheckTasks.check_search_results,
                                                                   check_items={"enable_milvus_client_api": True,
                                                                                "nq": ct.default_nq,
                                                                                "limit": ct.default_limit})
        tasks.append(default_search_task)

        # query with filter and default output "*"
        exp_query_res = [{default_pk_name: i} for i in range(ct.default_limit)]
        filter_query_task = self.async_milvus_client_wrap.query(c_name,
                                                                filter=f"{default_pk_name} < {ct.default_limit}",
                                                                output_fields=[default_pk_name],
                                                                check_task=CheckTasks.check_query_results,
                                                                check_items={"exp_res": exp_query_res,
                                                                             "primary_field": default_pk_name})
        tasks.append(filter_query_task)

        # get with ids
        get_task = self.async_milvus_client_wrap.get(c_name,
                                                     ids=[0, 1],
                                                     output_fields=[default_pk_name, default_vector_name],
                                                     check_task=CheckTasks.check_query_results,
                                                     check_items={"exp_res": rows[:2], "with_vec": True,
                                                                  "primary_field": default_pk_name})
        tasks.append(get_task)
        await asyncio.gather(*tasks)

    @pytest.mark.tags(CaseLabel.L0)
    async def test_async_client_close(self):
        # init async client
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        self.async_milvus_client_wrap.init_async_client(uri)

        # create collection
        c_name = cf.gen_unique_str(prefix)
        await self.async_milvus_client_wrap.create_collection(c_name, dimension=ct.default_dim)

        # close -> search raise error
        await self.async_milvus_client_wrap.close()
        vector = cf.gen_vectors(1, ct.default_dim)
        error = {ct.err_code: 1, ct.err_msg: "should create connection first"}
        await self.async_milvus_client_wrap.search(c_name, vector, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.skip("connect with zilliz cloud")
    async def test_async_client_with_token(self):
        # init client
        milvus_client = self._connect(enable_milvus_client_api=True)
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        token = cf.param_info.param_token
        milvus_client, _ = self.connection_wrap.MilvusClient(uri=uri, token=token)
        self.async_milvus_client_wrap.init_async_client(uri, token=token)

        # create collection
        c_name = cf.gen_unique_str(prefix)
        await self.async_milvus_client_wrap.create_collection(c_name, dimension=ct.default_dim)
        collections, _ = self.high_level_api_wrap.list_collections(milvus_client)
        assert c_name in collections

        # insert entities
        rows = [
            {default_pk_name: i, default_vector_name: [random.random() for _ in range(ct.default_dim)]}
            for i in range(ct.default_nb)]
        start_time = time.time()
        tasks = []
        step = 1000
        for i in range(0, ct.default_nb, step):
            task = self.async_milvus_client_wrap.insert(c_name, rows[i:i + step])
            tasks.append(task)
        insert_res = await asyncio.gather(*tasks)
        end_time = time.time()
        log.info("Total time: {:.2f} seconds".format(end_time - start_time))
        for r in insert_res:
            assert r[0]['insert_count'] == step

        # dql tasks
        tasks = []
        # search default
        vector = cf.gen_vectors(ct.default_nq, ct.default_dim)
        default_search_task = self.async_milvus_client_wrap.search(c_name, vector, limit=ct.default_limit,
                                                                   check_task=CheckTasks.check_search_results,
                                                                   check_items={"enable_milvus_client_api": True,
                                                                                "nq": ct.default_nq,
                                                                                "limit": ct.default_limit})
        tasks.append(default_search_task)

        # query with filter and default output "*"
        exp_query_res = [{default_pk_name: i} for i in range(ct.default_limit)]
        filter_query_task = self.async_milvus_client_wrap.query(c_name,
                                                                filter=f"{default_pk_name} < {ct.default_limit}",
                                                                output_fields=[default_pk_name],
                                                                check_task=CheckTasks.check_query_results,
                                                                check_items={"exp_res": exp_query_res,
                                                                             "primary_field": default_pk_name})
        tasks.append(filter_query_task)
        await asyncio.gather(*tasks)
