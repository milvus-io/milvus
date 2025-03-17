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


class TestSearchIterator(TestcaseBase):
    """ Test case of search iterator """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("metric_type", ct.float_metrics)
    @pytest.mark.parametrize("vector_data_type", ["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def test_range_search_iterator_default(self, metric_type, vector_data_type):
        """
        target: test iterator range search
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the range requirements
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        collection_w = self.init_collection_general(prefix, True, dim=default_dim, is_index=False,
                                                    vector_data_type=vector_data_type)[0]
        collection_w.create_index(field_name, {"metric_type": metric_type})
        collection_w.load()
        search_vector = cf.gen_vectors(1, default_dim, vector_data_type)
        search_params = {"metric_type": metric_type}
        collection_w.search_iterator(search_vector, field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"metric_type": metric_type,
                                                  "batch_size": batch_size})

        limit = 200
        res = collection_w.search(search_vector, field_name, param=search_params, limit=200,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": 1, "limit": limit})[0]
        # 2. search iterator
        if metric_type != "L2":
            radius = res[0][limit // 2].distance - 0.1  # pick a radius to make sure there exists results
            range_filter = res[0][0].distance + 0.1
            search_params = {"metric_type": metric_type, "params": {"radius": radius, "range_filter": range_filter}}
            collection_w.search_iterator(search_vector, field_name, search_params, batch_size,
                                         check_task=CheckTasks.check_search_iterator,
                                         check_items={"metric_type": metric_type, "batch_size": batch_size,
                                                      "radius": radius,
                                                      "range_filter": range_filter})
        else:
            radius = res[0][limit // 2].distance + 0.1
            range_filter = res[0][0].distance - 0.1
            search_params = {"metric_type": metric_type, "params": {"radius": radius, "range_filter": range_filter}}
            collection_w.search_iterator(search_vector, field_name, search_params, batch_size,
                                         check_task=CheckTasks.check_search_iterator,
                                         check_items={"metric_type": metric_type, "batch_size": batch_size,
                                                      "radius": radius,
                                                      "range_filter": range_filter})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_iterator_binary(self):
        """
        target: test search iterator binary
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 200
        collection_w = self.init_collection_general(
            prefix, True, is_binary=True)[0]
        # 2. search iterator
        _, binary_vectors = cf.gen_binary_vectors(2, ct.default_dim)
        collection_w.search_iterator(binary_vectors[:1], binary_field_name,
                                     ct.default_search_binary_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metrics", ct.float_metrics)
    def test_search_iterator_with_expression(self, metrics):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        dim = 128
        collection_w = self.init_collection_general(
            prefix, True, dim=dim, is_index=False)[0]
        collection_w.create_index(field_name, {"metric_type": metrics})
        collection_w.load()
        # 2. search iterator
        search_params = {"metric_type": metrics}
        expression = "1000.0 <= float < 2000.0"
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     expr=expression, check_task=CheckTasks.check_search_iterator,
                                     check_items={})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("batch_size", [10, 100, 777, 1000])
    def test_search_iterator_with_different_limit(self, batch_size):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search iterator
        search_params = {"metric_type": "COSINE"}
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_iterator_invalid_nq(self):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        dim = 128
        collection_w = self.init_collection_general(
            prefix, True, dim=dim, is_index=False)[0]
        collection_w.create_index(field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search iterator
        search_params = {"metric_type": "L2"}
        collection_w.search_iterator(vectors[:2], field_name, search_params, batch_size,
                                     check_task=CheckTasks.err_res,
                                     check_items={"err_code": 1,
                                                  "err_msg": "Not support search iteration over multiple vectors at present"})
