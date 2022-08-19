import time
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from utils.util_common import get_collections

class TestAllCollection(TestcaseBase):
    """ Test case of end to end"""
    @pytest.fixture(scope="function", params=get_collections())
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
        # create
        name = collection_name
        t0 = time.time()
        collection_w = self.init_collection_wrap(name=name, active_trace=True)
        tt = time.time() - t0
        assert collection_w.name == name
        entities = collection_w.num_entities
        log.info(f"assert create collection: {tt}, init_entities: {entities}")

        # insert
        data = cf.gen_default_list_data()
        t0 = time.time()
        _, res = collection_w.insert(data)
        tt = time.time() - t0
        log.info(f"assert insert: {tt}")
        assert res

        # flush
        t0 = time.time()
        _, check_result = collection_w.flush(timeout=180)
        assert check_result
        assert collection_w.num_entities == len(data[0]) + entities
        tt = time.time() - t0
        entities = collection_w.num_entities
        log.info(f"assert flush: {tt}, entities: {entities}")

        # search
        collection_w.load()
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        t0 = time.time()
        res_1, _ = collection_w.search(data=search_vectors,
                                       anns_field=ct.default_float_vec_field_name,
                                       param=search_params, limit=1)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res_1) == 1
        collection_w.release()

        # index
        d = cf.gen_default_list_data()
        collection_w.insert(d)
        log.info(f"assert index entities: {collection_w.num_entities}")
        _index_params = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "L2"}
        t0 = time.time()
        index, _ = collection_w.create_index(field_name=ct.default_float_vec_field_name,
                                             index_params=_index_params,
                                             name=cf.gen_unique_str())
        tt = time.time() - t0
        log.info(f"assert index: {tt}")
        assert len(collection_w.indexes) == 1

        # search
        t0 = time.time()
        collection_w.load()
        tt = time.time() - t0
        log.info(f"assert load: {tt}")
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        t0 = time.time()
        res_1, _ = collection_w.search(data=search_vectors,
                                       anns_field=ct.default_float_vec_field_name,
                                       param=search_params, limit=1)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")

        # query
        term_expr = f'{ct.default_int64_field_name} in [1001,1201,4999,2999]'
        t0 = time.time()
        res, _ = collection_w.query(term_expr)
        tt = time.time() - t0
        log.info(f"assert query result {len(res)}: {tt}")
