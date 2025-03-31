import numpy as np
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_EVENTUALLY
from pymilvus import AnnSearchRequest, RRFRanker, WeightedRanker
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection
)
from common.constants import *
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
import heapq
from time import sleep
from decimal import Decimal, getcontext
import decimal
import multiprocessing
import numbers
import random
import math
import numpy
import threading
import pytest
import pandas as pd
from faker import Faker

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

# patch faker to generate text with specific distribution
cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)

prefix = "search_collection"
search_num = 10
max_dim = ct.max_dim
min_dim = ct.min_dim
epsilon = ct.epsilon
hybrid_search_epsilon = 0.01
gracefulTime = ct.gracefulTime
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
max_limit = ct.max_limit
default_search_exp = "int64 >= 0"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name
default_index_params = ct.default_index
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
range_search_supported_indexes = ct.all_index_types[:7]
uid = "test_search"
nq = 1
epsilon = 0.001
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
search_param = {"nprobe": 1}
entity = gen_entities(1, is_normal=True)
entities = gen_entities(default_nb, is_normal=True)
raw_vectors, binary_entities = gen_binary_entities(default_nb)
default_query, _ = gen_search_vectors_params(field_name, entities, default_top_k, nq)
index_name1 = cf.gen_unique_str("float")
index_name2 = cf.gen_unique_str("varhar")
half_nb = ct.default_nb // 2
max_hybrid_search_req_num = ct.max_hybrid_search_req_num


class TestSearchGroupBy(TestcaseBase):
    """ Test case of search group by """

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_max_group_size_and_max_limit(self):
        """
        target: test search group by with max group size and max limit
        method: 1. create a collection with data
                2. search with group by int32 with max group size and max limit

        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("group_size", [0, -1])
    @pytest.mark.xfail(reason="issue #36146")
    def test_search_negative_group_size(self, group_size):
        """
        target: test search group by with negative group size
        """
        collection_w = self.init_collection_general(prefix, auto_id=True, insert_data=True, is_index=True)[0]
        search_params = ct.default_search_params
        search_vectors = cf.gen_vectors(1, dim=ct.default_dim)
        # verify
        error = {ct.err_code: 999, ct.err_msg: "group_size must be greater than 1"}
        collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                            param=search_params, limit=10,
                            group_by_field=ct.default_int64_field_name,
                            group_size=group_size,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", ["JACCARD", "HAMMING"])
    def test_search_binary_vec_group_by(self, metric):
        """
        target: test search on birany vector does not support group by
        method: 1. create a collection with binary vectors
                2. create index with different metric types
                3. search with group by
                verified error code and msg
        """
        collection_w = self.init_collection_general(prefix, auto_id=True, insert_data=False, is_index=False,
                                                    is_binary=True)[0]
        _index = {"index_type": "BIN_FLAT", "metric_type": metric, "params": {"M": 16, "efConstruction": 128}}
        collection_w.create_index(ct.default_binary_vec_field_name, index_params=_index)
        # insert with the same values for scalar fields
        for _ in range(10):
            data = cf.gen_default_binary_dataframe_data(nb=100, auto_id=True)[0]
            collection_w.insert(data)

        collection_w.flush()
        collection_w.create_index(ct.default_binary_vec_field_name, index_params=_index)
        collection_w.load()

        search_params = {"metric_type": metric, "params": {"ef": 128}}
        nq = 2
        limit = 10
        search_vectors = cf.gen_binary_vectors(nq, dim=ct.default_dim)[1]

        # verify the results are same if group by pk
        err_code = 999
        err_msg = "not support search_group_by operation based on binary"
        collection_w.search(data=search_vectors, anns_field=ct.default_binary_vec_field_name,
                            param=search_params, limit=limit,
                            group_by_field=ct.default_int64_field_name,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": err_code, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("grpby_unsupported_field", [ct.default_float_field_name, ct.default_json_field_name,
                                                         ct.default_double_field_name, ct.default_float_vec_field_name])
    def test_search_group_by_unsupported_field(self, grpby_unsupported_field):
        """
        target: test search group by with the unsupported field
        method: 1. create a collection with data
                2. create index
                3. search with group by the unsupported fields
                verify: the error code and msg
        """
        metric = "IP"
        collection_w = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                    is_all_data_type=True, with_json=True, )[0]
        _index = {"index_type": "HNSW", "metric_type": metric, "params": {"M": 16, "efConstruction": 128}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index)
        collection_w.load()

        search_params = {"metric_type": metric, "params": {"ef": 64}}
        nq = 1
        limit = 1
        search_vectors = cf.gen_vectors(nq, dim=ct.default_dim)

        # search with groupby
        err_code = 999
        err_msg = f"unsupported data type"
        collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                            param=search_params, limit=limit,
                            group_by_field=grpby_unsupported_field,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": err_code, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_group_by_unsupported_index(self, index):
        """
        target: test search group by with the unsupported vector index
        method: 1. create a collection with data
                2. create a groupby unsupported index
                3. search with group by
                verify: the error code and msg
        """
        if index in ["HNSW", "IVF_FLAT", "FLAT", "IVF_SQ8", "DISKANN", "SCANN"]:
            pass  # Only HNSW and IVF_FLAT are supported
        else:
            metric = "L2"
            collection_w = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                        is_all_data_type=True, with_json=False)[0]
            params = cf.get_index_params_params(index)
            index_params = {"index_type": index, "params": params, "metric_type": metric}
            collection_w.create_index(ct.default_float_vec_field_name, index_params)
            collection_w.load()

            search_params = {"params": {}}
            nq = 1
            limit = 1
            search_vectors = cf.gen_vectors(nq, dim=ct.default_dim)

            # search with groupby
            err_code = 999
            err_msg = f"current index:{index} doesn't support"
            collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                param=search_params, limit=limit,
                                group_by_field=ct.default_int8_field_name,
                                check_task=CheckTasks.err_res,
                                check_items={"err_code": err_code, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_group_by_multi_fields(self):
        """
        target: test search group by with the multi fields
        method: 1. create a collection with data
                2. create index
                3. search with group by the multi fields
                verify: the error code and msg
        """
        metric = "IP"
        collection_w = self.init_collection_general(prefix, insert_data=False, is_index=False,
                                                    is_all_data_type=True, with_json=True, )[0]
        _index = {"index_type": "HNSW", "metric_type": metric, "params": {"M": 16, "efConstruction": 128}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index)
        collection_w.load()

        search_params = {"metric_type": metric, "params": {"ef": 128}}
        nq = 1
        limit = 1
        search_vectors = cf.gen_vectors(nq, dim=ct.default_dim)

        # search with groupby
        err_code = 1700
        err_msg = f"groupBy field not found in schema"
        collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                            param=search_params, limit=limit,
                            group_by_field=[ct.default_string_field_name, ct.default_int32_field_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": err_code, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("grpby_nonexist_field", ["nonexit_field", 100])
    def test_search_group_by_nonexit_fields(self, grpby_nonexist_field):
        """
        target: test search group by with the nonexisting field
        method: 1. create a collection with data
                2. create index
                3. search with group by the unsupported fields
                verify: the error code and msg
        """
        metric = "IP"
        collection_w = self.init_collection_general(prefix, insert_data=False, is_index=False,
                                                    is_all_data_type=True, with_json=True, )[0]
        _index = {"index_type": "HNSW", "metric_type": metric, "params": {"M": 16, "efConstruction": 128}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index)

        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, index_param)
        collection_w.load()

        search_params = {"metric_type": metric, "params": {"ef": 128}}
        nq = 1
        limit = 1
        search_vectors = cf.gen_vectors(nq, dim=ct.default_dim)

        # search with groupby
        err_code = 1700
        err_msg = f"groupBy field not found in schema: field not found[field={grpby_nonexist_field}]"
        collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                            param=search_params, limit=limit,
                            group_by_field=grpby_nonexist_field,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": err_code, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_iterator_not_support_group_by(self):
        """
        target: test search iterator does not support group by
        method: 1. create a collection with data
                2. create index HNSW
                3. search iterator with group by
                4. search with filtering every value of group_by_field
                verify: error code and msg
        """
        metric = "COSINE"
        collection_w = self.init_collection_general(prefix, auto_id=True, insert_data=False, is_index=False,
                                                    is_all_data_type=True, with_json=False)[0]
        # insert with the same values for scalar fields
        for _ in range(10):
            data = cf.gen_dataframe_all_data_type(nb=100, auto_id=True, with_json=False)
            collection_w.insert(data)

        collection_w.flush()
        _index = {"index_type": "HNSW", "metric_type": metric, "params": {"M": 16, "efConstruction": 128}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index)
        collection_w.load()

        grpby_field = ct.default_int32_field_name
        search_vectors = cf.gen_vectors(1, dim=ct.default_dim)
        search_params = {"metric_type": metric}
        batch_size = 10

        err_code = 1100
        err_msg = "Not allowed to do groupBy when doing iteration"
        collection_w.search_iterator(search_vectors, ct.default_float_vec_field_name,
                                     search_params, batch_size, group_by_field=grpby_field,
                                     output_fields=[grpby_field],
                                     check_task=CheckTasks.err_res,
                                     check_items={"err_code": err_code, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_not_support_group_by(self):
        """
        target: test range search does not support group by
        method: 1. create a collection with data
                2. create index hnsw
                3. range search with group by
                verify: the error code and msg
        """
        metric = "COSINE"
        collection_w = self.init_collection_general(prefix, auto_id=True, insert_data=False, is_index=False,
                                                    is_all_data_type=True, with_json=False)[0]
        _index = {"index_type": "HNSW", "metric_type": metric, "params": {"M": 16, "efConstruction": 128}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index)
        # insert with the same values for scalar fields
        for _ in range(10):
            data = cf.gen_dataframe_all_data_type(nb=100, auto_id=True, with_json=False)
            collection_w.insert(data)

        collection_w.flush()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index)
        collection_w.load()

        nq = 1
        limit = 5
        search_vectors = cf.gen_vectors(nq, dim=ct.default_dim)
        grpby_field = ct.default_int32_field_name
        range_search_params = {"metric_type": "COSINE", "params": {"radius": 0.1,
                                                                   "range_filter": 0.5}}
        err_code = 1100
        err_msg = f"Not allowed to do range-search"
        collection_w.search(search_vectors, ct.default_float_vec_field_name,
                            range_search_params, limit,
                            default_search_exp, group_by_field=grpby_field,
                            output_fields=[grpby_field],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": err_code, "err_msg": err_msg})
