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


class TestSearchDiskann(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search about diskann index
    ******************************************************************
    """

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_delete_data(self, _async):
        """
        target: test delete after creating index
        method: 1.create collection , insert data,
                2.create  diskann index
                3.delete data, the search
        expected: assert index and deleted id not in search result
        """
        # 1. initialize with data
        dim = 100
        auto_id = True
        enable_dynamic_field = True
        collection_w, _, _, ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. create index
        default_index = {"index_type": "DISKANN",
                         "metric_type": "L2", "params": {}}
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index)
        collection_w.load()
        tmp_expr = f'{ct.default_int64_field_name} in {[0]}'

        expr = f'{ct.default_int64_field_name} in {ids[:half_nb]}'

        # delete half of data
        del_res = collection_w.delete(expr)[0]
        assert del_res.delete_count == half_nb

        collection_w.delete(tmp_expr)
        default_search_params = {
            "metric_type": "L2", "params": {"search_list": 30}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "pk_name": ct.default_int64_field_name}
                            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_scalar_field(self, _async):
        """
        target: test search with scalar field
        method: 1.create collection , insert data
                2.create more index ,then load
                3.search with expr
        expected: assert index and search successfully
        """
        # 1. initialize with data
        dim = 66
        enable_dynamic_field = True
        collection_w, _, _, ids = \
            self.init_collection_general(prefix, True, dim=dim, primary_field=ct.default_string_field_name,
                                         is_index=False, enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. create index
        default_index = {"index_type": "IVF_SQ8",
                         "metric_type": "COSINE", "params": {"nlist": 64}}
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index)
        index_params = {}
        if not enable_dynamic_field:
            collection_w.create_index(
                ct.default_float_field_name, index_params=index_params)
            collection_w.create_index(
                ct.default_int64_field_name, index_params=index_params)
        else:
            collection_w.create_index(
                ct.default_string_field_name, index_params=index_params)
        collection_w.load()
        default_expr = "int64 in [1, 2, 3, 4]"
        limit = 4
        default_search_params = {"metric_type": "COSINE", "params": {"nprobe": 64}}
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name, default_string_field_name]
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         default_search_params, limit, default_expr,
                                         output_fields=output_fields, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": ids,
                                                      "limit": limit,
                                                      "_async": _async,
                                                      "pk_name": ct.default_int64_field_name})
