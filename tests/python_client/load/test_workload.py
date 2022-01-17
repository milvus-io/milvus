import datetime
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from pymilvus import utility


rounds = 100
per_nb = 100000
default_field_name = ct.default_float_vec_field_name
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}


class TestLoad(TestcaseBase):
    """ Test case of end to end"""
    @pytest.mark.tags(CaseLabel.L3)
    def test_load_default(self):
        name = 'load_test_collection_1'
        name2 = 'load_test_collection_2'
        # create
        # collection_w = self.init_collection_wrap(name=name)
        # collection_w2 = self.init_collection_wrap(name=name2)
        # assert collection_w.name == name

        for i in range(50):
            name = f"load_collection2_{i}"
            self.init_collection_wrap(name=name)
        log.debug(f"total collections: {len(utility.list_collections())}")

        # # insert
        # data = cf.gen_default_list_data(per_nb)
        # log.debug(f"data len: {len(data[0])}")
        # for i in range(rounds):
        #     t0 = datetime.datetime.now()
        #     ins_res, res = collection_w.insert(data, timeout=180)
        #     tt = datetime.datetime.now() - t0
        #     log.debug(f"round{i} insert: {len(ins_res.primary_keys)} entities in {tt}s")
        #     assert res    # and per_nb == len(ins_res.primary_keys)
        #
        #     t0 = datetime.datetime.now()
        #     ins_res2, res = collection_w2.insert(data, timeout=180)
        #     tt = datetime.datetime.now() - t0
        #     log.debug(f"round{i} insert2: {len(ins_res2.primary_keys)} entities in {tt}s")
        #     assert res
        #
        #     # flush
        #     t0 = datetime.datetime.now()
        #     log.debug(f"current collection num_entities: {collection_w.num_entities}")
        #     tt = datetime.datetime.now() - t0
        #     log.debug(f"round{i} flush in {tt}")
        #
        #     t0 = datetime.datetime.now()
        #     log.debug(f"current collection2 num_entities: {collection_w2.num_entities}")
        #     tt = datetime.datetime.now() - t0
        #     log.debug(f"round{i} flush2 in {tt}")

        # index, res = collection_w.create_index(default_field_name, default_index_params, timeout=60)
        # assert res

        # # search
        # collection_w.load()
        # search_vectors = cf.gen_vectors(1, ct.default_dim)
        # t0 = datetime.datetime.now()
        # res_1, _ = collection_w.search(data=search_vectors,
        #                                anns_field=ct.default_float_vec_field_name,
        #                                param={"nprobe": 16}, limit=1)
        # tt = datetime.datetime.now() - t0
        # log.debug(f"assert search: {tt}")
        # assert len(res_1) == 1
        # # collection_w.release()
        #
        # # index
        # collection_w.insert(cf.gen_default_dataframe_data(nb=5000))
        # assert collection_w.num_entities == len(data[0]) + 5000
        # _index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
        # t0 = datetime.datetime.now()
        # index, _ = collection_w.create_index(field_name=ct.default_float_vec_field_name,
        #                                      index_params=_index_params,
        #                                      name=cf.gen_unique_str())
        # tt = datetime.datetime.now() - t0
        # log.debug(f"assert index: {tt}")
        # assert len(collection_w.indexes) == 1
        #
        # # query
        # term_expr = f'{ct.default_int64_field_name} in [3001,4001,4999,2999]'
        # t0 = datetime.datetime.now()
        # res, _ = collection_w.query(term_expr)
        # tt = datetime.datetime.now() - t0
        # log.debug(f"assert query: {tt}")
        # assert len(res) == 4
