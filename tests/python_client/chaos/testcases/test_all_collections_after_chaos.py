import time
import pytest
from faker import Faker
from pymilvus import Collection
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from utils.util_common import get_collections

fake = Faker()

class TestAllCollection(TestcaseBase):
    """ Test case of end to end"""

    @pytest.fixture(scope="function", params=get_collections(file_name="chaos_test_all_collections.json"))
    def collection_name(self, request):
        if request.param == [] or request.param == "":
            pytest.skip("The collection name is invalid")
        yield request.param

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." %
                 method.__name__)
        log.info("skip drop collection")

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_default(self, collection_name):
        self._connect()
        # create
        name = collection_name if collection_name else cf.gen_unique_str("Checker_")
        t0 = time.time()
        schema = Collection(name=name).schema
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        tt = time.time() - t0
        assert collection_w.name == name
        # get collection info
        schema = collection_w.schema
        dim = cf.get_dim_by_schema(schema=schema)
        int64_field_name = cf.get_int64_field_name(schema=schema)
        float_vector_field_name = cf.get_float_vec_field_name(schema=schema)
        float_vector_field_name_list = cf.get_float_vec_field_name_list(schema=schema)
        text_match_fields = cf.get_text_match_field_name(schema=schema)
        bm25_vec_field_name_list = cf.get_bm25_vec_field_name_list(schema=schema)

        # compact collection before getting num_entities
        collection_w.flush(timeout=180)
        collection_w.compact()
        collection_w.wait_for_compaction_completed(timeout=720)

        entities = collection_w.num_entities
        log.info(f"assert create collection: {tt}, init_entities: {entities}")

        # insert
        offset = -3000
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema, start=offset)
        t0 = time.time()
        _, res = collection_w.insert(data)
        tt = time.time() - t0
        log.info(f"assert insert: {tt}")
        assert res

        # flush
        t0 = time.time()
        _, check_result = collection_w.flush(timeout=180)
        assert check_result
        # assert collection_w.num_entities == len(data[0]) + entities
        tt = time.time() - t0
        entities = collection_w.num_entities
        log.info(f"assert flush: {tt}, entities: {entities}")

        # show index infos
        index_infos = [index.to_dict() for index in collection_w.indexes]
        log.info(f"index info: {index_infos}")
        fields_created_index = [index["field"] for index in index_infos]

        # create index if not have
        index_params = {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 48, "efConstruction": 500}}

        for f in float_vector_field_name_list:
            if f not in fields_created_index:
                t0 = time.time()
                index, _ = collection_w.create_index(field_name=float_vector_field_name,
                                                     index_params=index_params)
                tt = time.time() - t0
                log.info(f"create index for field {f} cost: {tt} seconds")
        # show index infos
        index_infos = [index.to_dict() for index in collection_w.indexes]
        log.info(f"index info: {index_infos}")

        # load
        collection_w.load()

        # search
        search_vectors = cf.gen_vectors(1, dim)
        dense_search_params = {"metric_type": "L2", "params": {"ef": 64}}
        t0 = time.time()
        res_1, _ = collection_w.search(data=search_vectors,
                                       anns_field=float_vector_field_name,
                                       param=dense_search_params, limit=1)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res_1) == 1

        # full text search
        if len(bm25_vec_field_name_list) > 0:
            queries = [fake.text() for _ in range(1)]
            bm25_search_params = {"metric_type": "BM25", "params": {}}
            t0 = time.time()
            res_2, _ = collection_w.search(data=queries,
                                           anns_field=bm25_vec_field_name_list[0],
                                            param=bm25_search_params, limit=1)
            tt = time.time() - t0
            log.info(f"assert full text search: {tt}")
            assert len(res_2) == 1

        # query
        term_expr = f'{int64_field_name} in {[i for i in range(offset, 0)]}'
        t0 = time.time()
        res, _ = collection_w.query(term_expr)
        tt = time.time() - t0
        log.info(f"assert query result {len(res)}: {tt}")
        assert len(res) >= len(data[0])

        # text match
        if len(text_match_fields) > 0:
            queries = [fake.text().replace("\n", " ") for _ in range(1)]

            expr = f"text_match({text_match_fields[0]}, '{queries[0]}')"
            t0 = time.time()
            res, _ = collection_w.query(expr)
            tt = time.time() - t0
            log.info(f"assert text match: {tt}")
            assert len(res) >= 0

        # insert data
        d = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        collection_w.insert(d)

        # release and load
        t0 = time.time()
        collection_w.release()
        collection_w.load()
        tt = time.time() - t0
        log.info(f"release and load: {tt}")

        # search
        nq = 5
        topk = 5
        search_vectors = cf.gen_vectors(nq, dim)
        t0 = time.time()
        res, _ = collection_w.search(data=search_vectors,
                                     anns_field=float_vector_field_name,
                                     param=dense_search_params, limit=topk)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res) == nq
        assert len(res[0]) <= topk

        # full text search
        if len(bm25_vec_field_name_list) > 0:
            queries = [fake.text() for _ in range(1)]
            bm25_search_params = {"metric_type": "BM25", "params": {}}
            t0 = time.time()
            res_2, _ = collection_w.search(data=queries,
                                           anns_field=bm25_vec_field_name_list[0],
                                            param=bm25_search_params, limit=1)
            tt = time.time() - t0
            log.info(f"assert full text search: {tt}")
            assert len(res_2) == 1

        # query
        term_expr = f'{int64_field_name} > -3000'
        t0 = time.time()
        res, _ = collection_w.query(term_expr)
        tt = time.time() - t0
        log.info(f"assert query result {len(res)}: {tt}")
        assert len(res) > 0

        # text match
        if len(text_match_fields) > 0:
            queries = [fake.text().replace("\n", " ") for _ in range(1)]
            expr = f"text_match({text_match_fields[0]}, '{queries[0]}')"
            t0 = time.time()
            res, _ = collection_w.query(expr)
            tt = time.time() - t0
            log.info(f"assert text match: {tt}")
            assert len(res) >= 0
