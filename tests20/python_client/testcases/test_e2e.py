import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel

prefix = "e2e_"


class TestE2e(TestcaseBase):
    """ Test case of end to end"""
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", [(cf.gen_unique_str(prefix))])
    def test_milvus_default(self, name):
        from utils.util_log import test_log as log
        # create
        collection_w = self.init_collection_wrap(name=name)
        log.debug("assert create")
        assert collection_w.name == name

        # insert
        data = cf.gen_default_list_data()
        _, res = collection_w.insert(data)
        log.debug("assert insert")
        assert res

        # flush
        log.debug("assert flush")
        assert collection_w.num_entities == len(data[0])

        # search
        collection_w.load()
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        res_1, _ = collection_w.search(data=search_vectors,
                                       anns_field=ct.default_float_vec_field_name,
                                       param={"nprobe": 16}, limit=1)
        log.debug("assert search")
        assert len(res_1) == 1

        # index
        collection_w.insert(cf.gen_default_dataframe_data(nb=4000))
        assert collection_w.num_entities == len(data[0]) + 4000
        _index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
        index, _ = collection_w.create_index(field_name=ct.default_float_vec_field_name,
                                             index_params=_index_params,
                                             name=cf.gen_unique_str())
        log.debug("assert index")
        assert len(collection_w.indexes) == 1

        # # query
        # term_expr = f'{ct.default_int64_field_name} in [1,2,3,4]'
        # res, _ = collection_w.query(term_expr)
        # assert len(res) == 4
