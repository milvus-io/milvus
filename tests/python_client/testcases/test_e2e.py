import time
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log

prefix = "e2e_"


class TestE2e(TestcaseBase):
    """ Test case of end to end"""
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_default(self):
        # create
        collection_name = cf.gen_collection_name_by_testcase_name()
        t0 = time.time()
        collection_w = self.init_collection_wrap(name=collection_name, active_trace=True)
        tt = time.time() - t0
        assert collection_w.name == collection_name

        # index
        index_params = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "L2"}
        t0 = time.time()
        index, _ = collection_w.create_index(field_name=ct.default_float_vec_field_name,
                                             index_params=index_params,
                                             index_name=cf.gen_unique_str())
        index, _ = collection_w.create_index(field_name=ct.default_string_field_name,
                                             index_params={},
                                             index_name=cf.gen_unique_str())
        tt = time.time() - t0
        log.info(f"assert index: {tt}")
        assert len(collection_w.indexes) == 2

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

        # load
        collection_w.load()

        # search
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        t0 = time.time()
        res_1, _ = collection_w.search(data=search_vectors,
                                       anns_field=ct.default_float_vec_field_name,
                                       param=search_params, limit=1)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res_1) == 1

        # release
        collection_w.release()

        # insert
        d = cf.gen_default_list_data()
        collection_w.insert(d)

        # search
        t0 = time.time()
        collection_w.load()
        tt = time.time() - t0
        log.info(f"assert load: {tt}")
        nq = 5
        topk = 5
        search_vectors = cf.gen_vectors(nq, ct.default_dim)
        t0 = time.time()
        res, _ = collection_w.search(data=search_vectors,
                                     anns_field=ct.default_float_vec_field_name,
                                     param=search_params, limit=topk)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res) == nq
        assert len(res[0]) <= topk
        # query
        term_expr = f'{ct.default_int64_field_name} in [1, 2, 3, 4]'
        t0 = time.time()
        res, _ = collection_w.query(term_expr)
        tt = time.time() - t0
        log.info(f"assert query result {len(res)}: {tt}")
        assert len(res) >= 4
