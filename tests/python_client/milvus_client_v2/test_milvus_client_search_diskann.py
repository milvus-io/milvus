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

    @pytest.fixture(scope="function", params=[32, 128])
    def dim(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_diskann_index(self, _async):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is int field
                2.create diskann index ,  then load
                3.search
        expected: search successfully
        """
        # 1. initialize with data
        dim = 100
        auto_id = False
        enable_dynamic_field = True
        nb = 2000
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, auto_id=auto_id,
                                                                      nb=nb, dim=dim, is_index=False,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]

        # 2. create index
        default_index = {"index_type": "DISKANN",
                         "metric_type": "L2", "params": {}}
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index)
        collection_w.load()

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
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "pk_name": ct.default_int64_field_name,
                                         "_async": _async}
                            )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("search_list", [20, 200])
    def test_search_with_limit_20(self, _async, search_list):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is int field
                2.create diskann index ,  then load
                3.search
        expected: search successfully
        """
        limit = 20
        # 1. initialize with data
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, is_index=False,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]

        # 2. create index
        default_index = {"index_type": "DISKANN", "metric_type": "L2", "params": {}}
        collection_w.create_index(ct.default_float_vec_field_name, default_index)
        collection_w.load()

        search_params = {"metric_type": "L2", "params": {"search_list": search_list}}
        output_fields = [default_int64_field_name, default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, limit, default_search_exp,
                            output_fields=output_fields, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async,
                                         "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_invalid_params_with_diskann_B(self):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is int field
                2.create  diskann index
                3.search with invalid params, [k, 200] when k <= 20
        expected: search report an error
        """
        # 1. initialize with data
        dim = 100
        limit = 20
        auto_id = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_index=False)[0:4]
        # 2. create index
        default_index = {"index_type": "DISKANN", "metric_type": "L2", "params": {}}
        collection_w.create_index(ct.default_float_vec_field_name, default_index)
        collection_w.load()
        default_search_params = {"metric_type": "L2", "params": {"search_list": limit-1}}
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_int64_field_name, default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999,
                                         "err_msg": f"should be larger than k({limit})"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_diskann_with_string_pk(self):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is string field
                2.create diskann index
                3.search with invalid metric type
        expected: search successfully
        """
        # 1. initialize with data
        dim = 128
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=False, dim=dim, is_index=False,
                                         primary_field=ct.default_string_field_name,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. create index
        default_index = {"index_type": "DISKANN",
                         "metric_type": "L2", "params": {}}
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index)
        collection_w.load()
        search_list = 20
        default_search_params = {"metric_type": "L2",
                                 "params": {"search_list": search_list}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "pk_name": ct.default_int64_field_name}
                            )

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

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_diskann_and_more_index(self, _async):
        """
        target: test delete after creating index
        method: 1.create collection , insert data
                2.create more index ,then load
                3.delete half data, search
        expected: assert index and deleted id not in search result
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        enable_dynamic_field = True
        collection_w, _, _, ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field, language="French")[0:4]
        # 2. create index
        default_index = {"index_type": "DISKANN",
                         "metric_type": "COSINE", "params": {}}
        collection_w.create_index(ct.default_float_vec_field_name, default_index, index_name=index_name1)
        if not enable_dynamic_field:
            index_params_one = {}
            collection_w.create_index("float", index_params_one, index_name="a")
            index_param_two = {}
            collection_w.create_index("varchar", index_param_two, index_name="b")

        collection_w.load()
        tmp_expr = f'{ct.default_int64_field_name} in {[0]}'

        expr = f'{ct.default_int64_field_name} in {ids[:half_nb]}'

        # delete half of data
        del_res = collection_w.delete(expr)[0]
        assert del_res.delete_count == half_nb

        collection_w.delete(tmp_expr)
        default_search_params = {"metric_type": "COSINE", "params": {"search_list": 30}}
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_int64_field_name, default_float_field_name, default_string_field_name]
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
                                         "pk_name": ct.default_int64_field_name})

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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [10, 100, 1000])
    def test_search_diskann_search_list_equal_to_limit(self, limit, _async):
        """
        target: test search diskann index when search_list equal to limit
        method: 1.create collection , insert data, primary_field is int field
                2.create diskann index ,  then load
                3.search
        expected: search successfully
        """
        # 1. initialize with data
        dim = 77
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, auto_id=auto_id,
                                                                      dim=dim, is_index=False,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]

        # 2. create index
        default_index = {"index_type": "DISKANN",
                         "metric_type": "L2", "params": {}}
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index)
        collection_w.load()

        search_params = {"metric_type": "L2", "params": {"search_list": limit}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, limit,
                            default_search_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async,
                                         "pk_name": ct.default_int64_field_name}
                            )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #23672")
    def test_search_diskann_search_list_up_to_min(self, _async):
        """
        target: test search diskann index when search_list up to min
        method: 1.create collection , insert data, primary_field is int field
                2.create diskann index ,  then load
                3.search
        expected: search successfully
        """
        # 1. initialize with data
        dim = 100
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, auto_id=auto_id,
                                                                      dim=dim, is_index=False)[0:4]

        # 2. create index
        default_index = {"index_type": "DISKANN",
                         "metric_type": "L2", "params": {}}
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index)
        collection_w.load()

        search_params = {"metric_type": "L2",
                         "params": {"k": 200, "search_list": 201}}
        search_vectors = [[random.random() for _ in range(dim)]
                          for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name, default_string_field_name]
        collection_w.search(search_vectors[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "pk_name": ct.default_int64_field_name})
