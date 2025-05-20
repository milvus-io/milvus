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
range_search_supported_indexes = ct.all_index_types[:8]
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


class TestCollectionSearchJSON(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function",
                    params=[default_nb, default_nb_medium])
    def nb(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[2, 500])
    def nq(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[32, 128])
    def dim(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["JACCARD", "HAMMING"])
    def metrics(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def is_flush(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[0, 0.5, 1])
    def null_data_percent(self, request):
        yield request.param

    """
    ******************************************************************
    #  The followings are invalid base cases
    ******************************************************************
    """

    @pytest.mark.skip("Supported json like: 1, \"abc\", [1,2,3,4]")
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_json_expression_object(self):
        """
        target: test search with comparisons jsonField directly
        method: search with expressions using jsonField name directly
        expected: Raise error
        """
        # 1. initialize with data
        nq = 1
        dim = 128
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, dim=dim)[0:5]
        # 2. search before insert time_stamp
        log.info("test_search_json_expression_object: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 3. search after insert time_stamp
        json_search_exp = "json_field > 0"
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            json_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: "can not comparisons jsonField directly"})

    """
    ******************************************************************
    #  The followings are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_json_expression_default(self, nq, is_flush, enable_dynamic_field):
        """
        target: test search case with default json expression
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=True, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field, language="Hindi")[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_json_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_json_nullable_load_before_insert(self, nq, is_flush, enable_dynamic_field):
        """
        target: test search case with default json expression
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize collection
        dim = 64
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, False, auto_id=True, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_json_field_name: 1})[0:5]
        # insert data
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nb)]
        data = [[np.float32(i) for i in range(default_nb)], [str(i) for i in range(default_nb)], [], vectors]
        collection_w.insert(data)
        collection_w.num_entities
        # 2. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "limit": default_limit,
                                         "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 37113")
    def test_search_json_nullable_insert_before_load(self, nq, is_flush, enable_dynamic_field):
        """
        target: test search case with default json expression
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize collection
        dim = 64
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, False, auto_id=True, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_json_field_name: 1})[0:5]
        collection_w.release()
        # insert data
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nb)]
        data = [[np.float32(i) for i in range(default_nb)], [str(i) for i in range(default_nb)], [], vectors]
        collection_w.insert(data)
        collection_w.num_entities
        collection_w.load()
        # 2. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "limit": default_limit,
                                         "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_expression_json_contains(self, enable_dynamic_field):
        """
        target: test search with expression using json_contains
        method: search with expression (json_contains)
        expected: search successfully
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(
            prefix, enable_dynamic_field=enable_dynamic_field)[0]

        # 2. insert data
        array = []
        for i in range(default_nb):
            data = {
                default_int64_field_name: i,
                default_float_field_name: i * 1.0,
                default_string_field_name: str(i),
                default_json_field_name: {"number": i, "list": [i, i + 1, i + 2]},
                default_float_vec_field_name: gen_vectors(1, default_dim)[0]
            }
            array.append(data)
        collection_w.insert(array)

        # 2. search
        collection_w.load()
        log.info("test_search_with_output_field_json_contains: Searching collection %s" %
                 collection_w.name)
        expressions = [
            "json_contains(json_field['list'], 100)", "JSON_CONTAINS(json_field['list'], 100)"]
        for expression in expressions:
            collection_w.search(vectors[:default_nq], default_search_field,
                                default_search_params, default_limit, expression,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "limit": 3,
                                             "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_json_contains_list(self, auto_id):
        """
        target: test search with expression using json_contains
        method: search with expression (json_contains)
        expected: search successfully
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(
            prefix, auto_id=auto_id, enable_dynamic_field=True)[0]

        # 2. insert data
        limit = 100
        array = []
        for i in range(default_nb):
            data = {
                default_int64_field_name: i,
                default_json_field_name: [j for j in range(i, i + limit)],
                default_float_vec_field_name: gen_vectors(1, default_dim)[0]
            }
            if auto_id:
                data.pop(default_int64_field_name, None)
            array.append(data)
        collection_w.insert(array)

        # 2. search
        collection_w.load()
        log.info("test_search_with_output_field_json_contains: Searching collection %s" %
                 collection_w.name)
        expressions = [
            "json_contains(json_field, 100)", "JSON_CONTAINS(json_field, 100)"]
        for expression in expressions:
            collection_w.search(vectors[:default_nq], default_search_field,
                                default_search_params, limit, expression,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "limit": limit,
                                             "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_json_contains_combined_with_normal(self, enable_dynamic_field):
        """
        target: test search with expression using json_contains
        method: search with expression (json_contains)
        expected: search successfully
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(
            prefix, enable_dynamic_field=enable_dynamic_field)[0]

        # 2. insert data
        limit = 100
        array = []
        for i in range(default_nb):
            data = {
                default_int64_field_name: i,
                default_float_field_name: i * 1.0,
                default_string_field_name: str(i),
                default_json_field_name: {"number": i, "list": [str(j) for j in range(i, i + limit)]},
                default_float_vec_field_name: gen_vectors(1, default_dim)[0]
            }
            array.append(data)
        collection_w.insert(array)

        # 2. search
        collection_w.load()
        log.info("test_search_with_output_field_json_contains: Searching collection %s" %
                 collection_w.name)
        tar = 1000
        expressions = [f"json_contains(json_field['list'], '{tar}') && int64 > {tar - limit // 2}",
                       f"JSON_CONTAINS(json_field['list'], '{tar}') && int64 > {tar - limit // 2}"]
        for expression in expressions:
            collection_w.search(vectors[:default_nq], default_search_field,
                                default_search_params, limit, expression,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "limit": limit // 2,
                                             "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains", "ARRAY_CONTAINS"])
    def test_search_expr_array_contains(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. create a collection
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)

        # 2. insert data
        string_field_value = [[str(j) for j in range(i, i + 3)] for i in range(ct.default_nb)]
        data = cf.gen_array_dataframe_data()
        data[ct.default_string_array_field_name] = string_field_value
        collection_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name, {})

        # 3. search
        collection_w.load()
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, '1000')"
        res = collection_w.search(vectors[:default_nq], default_search_field, {},
                                  limit=ct.default_nb, expr=expression)[0]
        exp_ids = cf.assert_json_contains(expression, string_field_value)
        assert set(res[0].ids) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains", "ARRAY_CONTAINS"])
    def test_search_expr_not_array_contains(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. create a collection
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)

        # 2. insert data
        string_field_value = [[str(j) for j in range(i, i + 3)] for i in range(ct.default_nb)]
        data = cf.gen_array_dataframe_data()
        data[ct.default_string_array_field_name] = string_field_value
        collection_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name, {})

        # 3. search
        collection_w.load()
        expression = f"not {expr_prefix}({ct.default_string_array_field_name}, '1000')"
        res = collection_w.search(vectors[:default_nq], default_search_field, {},
                                  limit=ct.default_nb, expr=expression)[0]
        exp_ids = cf.assert_json_contains(expression, string_field_value)
        assert set(res[0].ids) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_all", "ARRAY_CONTAINS_ALL"])
    def test_search_expr_array_contains_all(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. create a collection
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)

        # 2. insert data
        string_field_value = [[str(j) for j in range(i, i + 3)] for i in range(ct.default_nb)]
        data = cf.gen_array_dataframe_data()
        data[ct.default_string_array_field_name] = string_field_value
        collection_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name, {})

        # 3. search
        collection_w.load()
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, ['1000'])"
        res = collection_w.search(vectors[:default_nq], default_search_field, {},
                                  limit=ct.default_nb, expr=expression)[0]
        exp_ids = cf.assert_json_contains(expression, string_field_value)
        assert set(res[0].ids) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_any", "ARRAY_CONTAINS_ANY",
                                             "not array_contains_any", "not ARRAY_CONTAINS_ANY"])
    def test_search_expr_array_contains_any(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. create a collection
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)

        # 2. insert data
        string_field_value = [[str(j) for j in range(i, i + 3)] for i in range(ct.default_nb)]
        data = cf.gen_array_dataframe_data()
        data[ct.default_string_array_field_name] = string_field_value
        collection_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name, {})

        # 3. search
        collection_w.load()
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, ['1000'])"
        res = collection_w.search(vectors[:default_nq], default_search_field, {},
                                  limit=ct.default_nb, expr=expression)[0]
        exp_ids = cf.assert_json_contains(expression, string_field_value)
        assert set(res[0].ids) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_all", "ARRAY_CONTAINS_ALL",
                                             "array_contains_any", "ARRAY_CONTAINS_ANY"])
    def test_search_expr_array_contains_invalid(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains(a, b) b not list
        expected: report error
        """
        # 1. create a collection
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)

        # 2. insert data
        data = cf.gen_array_dataframe_data()
        collection_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name, {})

        # 3. search
        collection_w.load()
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, '1000')"
        error = {ct.err_code: 1100,
                 ct.err_msg: f"cannot parse expression: {expression}, "
                             f"error: ContainsAll operation element must be an array"}
        if expr_prefix in ["array_contains_any", "ARRAY_CONTAINS_ANY"]:
            error = {ct.err_code: 1100,
                     ct.err_msg: f"cannot parse expression: {expression}, "
                                 f"error: ContainsAny operation element must be an array"}
        collection_w.search(vectors[:default_nq], default_search_field, {},
                            limit=ct.default_nb, expr=expression,
                            check_task=CheckTasks.err_res, check_items=error)
