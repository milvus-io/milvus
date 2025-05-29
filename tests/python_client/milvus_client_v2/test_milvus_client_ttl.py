import pytest
import time
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from utils.util_pymilvus import *
from base.client_v2_base import TestMilvusClientV2Base
from pymilvus import DataType, AnnSearchRequest, WeightedRanker


class TestMilvusClientTTL(TestMilvusClientV2Base):
    """ Test case of Time To Live """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("flush_enable", [True, False])
    @pytest.mark.parametrize("on_insert", [True, False])
    def test_milvus_client_ttl_default(self, flush_enable, on_insert):
        """
        target: verify that data is invisible after ttl
        method: create collection with ttl, insert data, wait for ttl, search data
        expected: data is invisible
        """
        client = self._client()
        dim = 65
        ttl = 10
        nb = 1000
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("embeddings_2", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("visible", DataType.BOOL, nullable=True)
        self.create_collection(client, collection_name, schema=schema, properties={"collection.ttl.seconds": ttl})
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info['properties']["collection.ttl.seconds"] == str(ttl)

        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="embeddings", index_type="IVF_FLAT", metric_type="COSINE", nlist=128)
        index_params.add_index(field_name="embeddings_2", index_type="IVF_FLAT", metric_type="COSINE", nlist=128)
        self.create_index(client, collection_name, index_params=index_params)

        # load collection
        self.load_collection(client, collection_name)

        # insert data
        insert_times = 2
        for i in range(insert_times):
            vectors = cf.gen_vectors(nb, dim=dim)
            vectors_2 = cf.gen_vectors(nb, dim=dim)
            rows = []
            start_id = i * nb
            for j in range(nb):
                row = {
                    "id": start_id + j,
                    "embeddings": list(vectors[j]),
                    "embeddings_2": list(vectors_2[j]),
                    "visible": False
                }
                rows.append(row)
            if on_insert is True:
                self.insert(client, collection_name, rows)
            else:
                self.upsert(client, collection_name, rows)

        # search until timeout or get empty results
        start_time = time.time()
        timeout = ttl * 5
        nq = 1
        search_ttl_effective = False
        query_ttl_effective = False
        hybrid_search_ttl_effective = False
        search_vectors = cf.gen_vectors(nq, dim=dim)
        sub_search1 = AnnSearchRequest(search_vectors, "embeddings", {"level": 1}, 20)
        sub_search2 = AnnSearchRequest(search_vectors, "embeddings_2", {"level": 1}, 20)
        ranker = WeightedRanker(0.2, 0.8)
        # flush collection if flush_enable is True
        if flush_enable:
            t1 = time.time()
            self.flush(client, collection_name)
            log.info(f"flush completed in {time.time() - t1}s")
        while time.time() - start_time < timeout:
            if search_ttl_effective is False:
                res1 = self.search(client, collection_name, search_vectors, anns_field='embeddings',
                                   search_params={}, limit=10, consistency_level='Strong')[0]
            if query_ttl_effective is False:
                res2 = self.query(client, collection_name, filter='',
                                  output_fields=["count(*)"], consistency_level='Strong')[0]
            if hybrid_search_ttl_effective is False:
                res3 = self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker,
                                          limit=10, consistency_level='Strong')[0]
            if len(res1[0]) == 0 and search_ttl_effective is False:
                log.info(f"search ttl effects in {round(time.time() - start_time, 4)}s")
                search_ttl_effective = True
            if res2[0].get('count(*)', None) == 0 and query_ttl_effective is False:
                log.info(f"query ttl effects in {round(time.time() - start_time, 4)}s")
                query_ttl_effective = True
            if len(res3[0]) == 0 and hybrid_search_ttl_effective is False:
                log.info(f"hybrid search ttl effects in {round(time.time() - start_time, 4)}s")
                hybrid_search_ttl_effective = True
            if search_ttl_effective is True and query_ttl_effective is True and hybrid_search_ttl_effective is True:
                break
            time.sleep(1)

        delta_tt = round(time.time() - start_time, 4)
        log.info(f"ttl effects in {delta_tt}s")
        assert ttl - 2 <= delta_tt <= ttl + 5

        # query count(*)
        res = self.query(client, collection_name, filter='', output_fields=["count(*)"])[0]
        assert res[0].get('count(*)', None) == 0

        # insert more data
        for i in range(insert_times):
            vectors = cf.gen_vectors(nb, dim=dim)
            vectors_2 = cf.gen_vectors(nb, dim=dim)
            rows = []
            start_id = (insert_times + i) * nb
            for j in range(nb):
                row = {
                    "id": start_id + j,
                    "embeddings": list(vectors[j]),
                    "embeddings_2": list(vectors_2[j]),
                    "visible": True
                }
                rows.append(row)
            if on_insert is True:
                self.insert(client, collection_name, rows)
            else:
                self.upsert(client, collection_name, rows)

        # flush collection if flush_enable is True
        if flush_enable:
            t1 = time.time()
            self.flush(client, collection_name)
            log.info(f"flush completed in {time.time() - t1}s")

        # search data again after insert more data
        res = self.search(client, collection_name, search_vectors,
                          search_params={}, anns_field='embeddings',
                          limit=10, consistency_level='Strong')[0]
        assert len(res[0]) > 0
        # query count(*)
        res = self.query(client, collection_name, filter='visible==False',
                         output_fields=["count(*)"], consistency_level='Strong')[0]
        assert res[0].get('count(*)', None) == 0

        # hybrid search
        res = self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker,
                                 limit=10, consistency_level='Strong')[0]
        assert len(res[0]) > 0

        # query count(visible)
        res = self.query(client, collection_name, filter='visible==True',
                         output_fields=["count(*)"], consistency_level='Strong')[0]
        assert res[0].get('count(*)', None) > 0

        # alter ttl to 1000s
        self.alter_collection_properties(client, collection_name, properties={"collection.ttl.seconds": 1000})
        # search data after alter ttl
        res = self.search(client, collection_name, search_vectors,
                          search_params={}, anns_field='embeddings',
                          filter='visible==False', limit=10, consistency_level='Strong')[0]
        assert len(res[0]) > 0

        # hybrid search data after alter ttl
        sub_search1 = AnnSearchRequest(search_vectors, "embeddings", {"level": 1}, 20, expr='visible==False')
        sub_search2 = AnnSearchRequest(search_vectors, "embeddings_2", {"level": 1}, 20, expr='visible==False')
        res = self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker,
                                 limit=10, consistency_level='Strong')[0]
        assert len(res[0]) > 0

        # query count(*)
        res = self.query(client, collection_name, filter='visible==False',
                         output_fields=["count(*)"], consistency_level='Strong')[0]
        assert res[0].get('count(*)', None) == insert_times * nb
        res = self.query(client, collection_name, filter='',
                         output_fields=["count(*)"], consistency_level='Strong')[0]
        assert res[0].get('count(*)', None) == insert_times * nb * 2
