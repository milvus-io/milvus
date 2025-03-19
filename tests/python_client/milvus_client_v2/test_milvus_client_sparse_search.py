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


class TestSparseSearch(TestcaseBase):
    """ Add some test cases for the sparse vector """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    @pytest.mark.parametrize("inverted_index_algo", ct.inverted_index_algo)
    def test_sparse_index_search(self, index, inverted_index_algo):
        """
        target: verify that sparse index for sparse vectors can be searched properly
        method: create connection, collection, insert and search
        expected: search successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema(auto_id=False)
        collection_w = self.init_collection_wrap(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(nb=3000)
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        params.update({"inverted_index_algo": inverted_index_algo})
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)
        collection_w.load()

        _params = cf.get_search_params_params(index)
        _params.update({"dim_max_score_ratio": 1.05})
        search_params = {"params": _params}
        collection_w.search(data[-1][0:default_nq], ct.default_sparse_vec_field_name,
                            search_params, default_limit,
                            output_fields=[ct.default_sparse_vec_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "original_entities": [data],
                                         "output_fields": [ct.default_sparse_vec_field_name]})
        expr = "int64 < 100 "
        collection_w.search(data[-1][0:default_nq], ct.default_sparse_vec_field_name,
                            search_params, default_limit,
                            expr=expr, output_fields=[ct.default_sparse_vec_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "original_entities": [data],
                                         "output_fields": [ct.default_sparse_vec_field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    @pytest.mark.parametrize("dim", [32768, ct.max_sparse_vector_dim])
    def test_sparse_index_dim(self, index, dim):
        """
        target: validating the sparse index in different dimensions
        method: create connection, collection, insert and hybrid search
        expected: search successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema(auto_id=False)
        collection_w = self.init_collection_wrap(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(dim=dim)
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)

        collection_w.load()
        collection_w.search(data[-1][0:default_nq], ct.default_sparse_vec_field_name,
                            ct.default_sparse_search_params, limit=1,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": 1})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    @pytest.mark.parametrize("inverted_index_algo", ct.inverted_index_algo)
    def test_sparse_index_enable_mmap_search(self, index, inverted_index_algo):
        """
        target: verify that the sparse indexes of sparse vectors can be searched properly after turning on mmap
        method: create connection, collection, enable mmap,  insert and search
        expected: search successfully , query result is correct
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema(auto_id=False)
        collection_w = self.init_collection_wrap(c_name, schema=schema)

        first_nb = 3000
        data = cf.gen_default_list_sparse_data(nb=first_nb, start=0)
        collection_w.insert(data)

        params = cf.get_index_params_params(index)
        params.update({"inverted_index_algo": inverted_index_algo})
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)

        collection_w.set_properties({'mmap.enabled': True})
        pro = collection_w.describe()[0].get("properties")
        assert pro["mmap.enabled"] == 'True'
        collection_w.alter_index(index, {'mmap.enabled': True})
        assert collection_w.index()[0].params["mmap.enabled"] == 'True'
        data2 = cf.gen_default_list_sparse_data(nb=2000, start=first_nb)  # id shall be continuous
        all_data = []  # combine 2 insert datas for next checking
        for i in range(len(data2)):
            all_data.append(data[i] + data2[i])
        collection_w.insert(data2)
        collection_w.flush()
        collection_w.load()
        collection_w.search(data[-1][0:default_nq], ct.default_sparse_vec_field_name,
                            ct.default_sparse_search_params, default_limit,
                            output_fields=[ct.default_sparse_vec_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "original_entities": [all_data],
                                         "output_fields": [ct.default_sparse_vec_field_name]})
        expr_id_list = [0, 1, 10, 100]
        term_expr = f'{ct.default_int64_field_name} in {expr_id_list}'
        res = collection_w.query(term_expr)[0]
        assert len(res) == 4

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("drop_ratio_build", [0.01])
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_search_sparse_ratio(self, drop_ratio_build, index):
        """
        target: create a sparse index by adjusting the ratio parameter.
        method: create a sparse index by adjusting the ratio parameter.
        expected: search successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema(auto_id=False)
        collection_w = self.init_collection_wrap(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(nb=4000)
        collection_w.insert(data)
        collection_w.flush()
        params = {"index_type": index, "metric_type": "IP", "params": {"drop_ratio_build": drop_ratio_build}}
        collection_w.create_index(ct.default_sparse_vec_field_name, params, index_name=index)
        collection_w.load()
        assert collection_w.has_index(index_name=index)[0] is True
        _params = {"drop_ratio_search": 0.2}
        for dim_max_score_ratio in [0.5, 0.99, 1, 1.3]:
            _params.update({"dim_max_score_ratio": dim_max_score_ratio})
            search_params = {"metric_type": "IP", "params": _params}
            collection_w.search(data[-1][0:default_nq], ct.default_sparse_vec_field_name,
                                search_params, default_limit,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "limit": default_limit})
        error = {ct.err_code: 999,
                 ct.err_msg: "should be in range [0.500000, 1.300000]"}
        for invalid_ratio in [0.49, 1.4]:
            _params.update({"dim_max_score_ratio": invalid_ratio})
            search_params = {"metric_type": "IP", "params": _params}
            collection_w.search(data[-1][0:default_nq], ct.default_sparse_vec_field_name,
                                search_params, default_limit,
                                check_task=CheckTasks.err_res,
                                check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_sparse_vector_search_output_field(self, index):
        """
        target: create sparse vectors and search
        method: create sparse vectors and search
        expected: normal search
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w = self.init_collection_wrap(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(nb=4000)
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)

        collection_w.load()
        d = cf.gen_default_list_sparse_data(nb=10)
        collection_w.search(d[-1][0:default_nq], ct.default_sparse_vec_field_name,
                            ct.default_sparse_search_params, default_limit,
                            output_fields=["float", "sparse_vector"],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "output_fields": ["float", "sparse_vector"]
                                         })

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    @pytest.mark.parametrize("inverted_index_algo", ct.inverted_index_algo)
    def test_sparse_vector_search_iterator(self, index, inverted_index_algo):
        """
        target: create sparse vectors and search iterator
        method: create sparse vectors and search iterator
        expected: normal search
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w = self.init_collection_wrap(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(nb=4000)
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        params.update({"inverted_index_algo": inverted_index_algo})
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)

        collection_w.load()
        batch_size = 100
        collection_w.search_iterator(data[-1][0:1], ct.default_sparse_vec_field_name,
                                     ct.default_sparse_search_params, limit=500, batch_size=batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"batch_size": batch_size})
