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


class TestCollectionSearchInvalid(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=ct.get_invalid_vectors)
    def get_invalid_vectors(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_metric_type)
    def get_invalid_metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_ints)
    def get_invalid_limit(self, request):
        if isinstance(request.param, int) and request.param >= 0:
            pytest.skip("positive int is valid type for limit")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_ints)
    def get_invalid_guarantee_timestamp(self, request):
        if request.param == 9999999999:
            pytest.skip("9999999999 is valid for guarantee_timestamp")
        if request.param is None:
            pytest.skip("None is valid for guarantee_timestamp")
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def vector_data_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_no_connection(self):
        """
        target: test search without connection
        method: create and delete connection, then search
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. remove connection
        log.info("test_search_no_connection: removing connection")
        self.connection_wrap.remove_connection(alias='default')
        log.info("test_search_no_connection: removed connection")
        # 3. search without connection
        log.info("test_search_no_connection: searching without connection")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "should create connection first"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_no_collection(self):
        """
        target: test the scenario which search the non-exist collection
        method: 1. create collection
                2. drop collection
                3. search the dropped collection
        expected: raise exception and report the error
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. Drop collection
        collection_w.drop()
        # 3. Search without collection
        log.info("test_search_no_collection: Searching without collection ")
        collection_w.search(vectors, default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "collection not found"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_missing(self):
        """
        target: test search with incomplete parameters
        method: search with incomplete parameters
        expected: raise exception and report the error
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search collection with missing parameters
        log.info("test_search_param_missing: Searching collection %s "
                 "with missing parameters" % collection_w.name)
        try:
            collection_w.search()
        except TypeError as e:
            assert "missing 4 required positional arguments: 'data', " \
                   "'anns_field', 'param', and 'limit'" in str(e)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_vectors(self, get_invalid_vectors):
        """
        target: test search with invalid parameter values
        method: search with invalid data
        expected: raise exception and report the error
        """
        if get_invalid_vectors in [[" "], ['a']]:
            pytest.skip("['a'] and [' '] is valid now")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, dim=32)[0]
        # 2. search with invalid field
        invalid_vectors = get_invalid_vectors
        log.info("test_search_param_invalid_vectors: searching with "
                 "invalid vectors: {}".format(invalid_vectors))
        collection_w.search(invalid_vectors, default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999,
                                         "err_msg": "`search_data` value {} is illegal".format(invalid_vectors)})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_dim(self):
        """
        target: test search with invalid parameter values
        method: search with invalid dim
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, 1)[0]
        # 2. search with invalid dim
        log.info("test_search_param_invalid_dim: searching with invalid dim")
        wrong_dim = 129
        vectors = [[random.random() for _ in range(wrong_dim)] for _ in range(default_nq)]
        # The error message needs to be improved.
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": 'vector dimension mismatch'})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_field_name", ct.invalid_resource_names)
    def test_search_param_invalid_field(self, invalid_field_name):
        """
        target: test search with invalid parameter type
        method: search with invalid field type
        expected: raise exception and report the error
        """
        if invalid_field_name in [None, ""]:
            pytest.skip("None is legal")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid field
        collection_w.load()
        error = {"err_code": 999, "err_msg": f"failed to create query plan: failed to get field schema by name"}
        collection_w.search(vectors[:default_nq], invalid_field_name, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 30356")
    def test_search_param_invalid_metric_type(self, get_invalid_metric_type):
        """
        target: test search with invalid parameter values
        method: search with invalid metric type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid metric_type
        log.info("test_search_param_invalid_metric_type: searching with invalid metric_type")
        invalid_metric = get_invalid_metric_type
        search_params = {"metric_type": invalid_metric, "params": {"nprobe": 10}}
        collection_w.search(vectors[:default_nq], default_search_field, search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "metric type not match"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 30356")
    def test_search_param_metric_type_not_match(self):
        """
        target: test search with invalid parameter values
        method: search with invalid metric type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid metric_type
        log.info("test_search_param_metric_type_not_match: searching with not matched metric_type")
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        collection_w.search(vectors[:default_nq], default_search_field, search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "metric type not match: invalid parameter"
                                                    "[expected=COSINE][actual=L2]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_invalid_params_type(self, index):
        """
        target: test search with invalid search params
        method: test search with invalid params type
        expected: raise exception and report the error
        """
        if index == "FLAT":
            pytest.skip("skip in FLAT index")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 2000,
                                                                      is_index=False)[0:4]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        invalid_search_params = cf.gen_invalid_search_params_type()
        # TODO: update the error msg assertion as #37543 fixed
        for invalid_search_param in invalid_search_params:
            if index == invalid_search_param["index_type"]:
                search_params = {"metric_type": "L2",
                                 "params": invalid_search_param["search_params"]}
                log.info("search_params: {}".format(search_params))
                collection_w.search(vectors[:default_nq], default_search_field,
                                    search_params, default_limit,
                                    default_search_exp,
                                    check_task=CheckTasks.err_res,
                                    check_items={"err_code": 999,
                                                 "err_msg": "fail to search on QueryNode"})

    @pytest.mark.skip("not support now")
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("search_k", [-10, -1, 0, 10, 125])
    def test_search_param_invalid_annoy_index(self, search_k):
        """
        target: test search with invalid search params matched with annoy index
        method: search with invalid param search_k out of [top_k, ∞)
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(
            prefix, True, 3000, is_index=False)[0]
        # 2. create annoy index and load
        index_annoy = {"index_type": "ANNOY", "params": {
            "n_trees": 512}, "metric_type": "L2"}
        collection_w.create_index("float_vector", index_annoy)
        collection_w.load()
        # 3. search
        annoy_search_param = {"index_type": "ANNOY",
                              "search_params": {"search_k": search_k}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            annoy_search_param, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "Search params check failed"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_limit_type(self, get_invalid_limit):
        """
        target: test search with invalid limit type
        method: search with invalid limit type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid field
        invalid_limit = get_invalid_limit
        log.info("test_search_param_invalid_limit_type: searching with "
                 "invalid limit: %s" % invalid_limit)
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            invalid_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "`limit` value %s is illegal" % invalid_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [0, 16385])
    def test_search_param_invalid_limit_value(self, limit):
        """
        target: test search with invalid limit value
        method: search with invalid limit: 0 and maximum
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid limit (topK)
        err_msg = f"topk [{limit}] is invalid, it should be in range [1, 16384]"
        if limit == 0:
            err_msg = "`limit` value 0 is illegal"
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_search_expr", ["'non_existing_field'==2", 1])
    def test_search_param_invalid_expr_type(self, invalid_search_expr):
        """
        target: test search with invalid parameter type
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        collection_w.load()
        # 2 search with invalid expr
        error = {"err_code": 999, "err_msg": "failed to create query plan: cannot parse expression"}
        if invalid_search_expr == 1:
            error = {"err_code": 999, "err_msg": "The type of expr must be string"}
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, invalid_search_expr,
                            check_task=CheckTasks.err_res,
                            check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_field_compare_expressions())
    def test_search_with_expression_join_two_fields(self, expression):
        """
        target: test search with expressions linking two fields such as 'and'
        method: create a collection and search with different conjunction
        expected: raise exception and report the error
        """
        # 1. create a collection
        nb = 1
        dim = 2
        fields = [cf.gen_int64_field("int64_1"), cf.gen_int64_field("int64_2"),
                  cf.gen_float_vec_field(dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, primary_field="int64_1")
        collection_w = self.init_collection_wrap(schema=schema)

        # 2. insert data
        values = pd.Series(data=[i for i in range(0, nb)])
        dataframe = pd.DataFrame({"int64_1": values, "int64_2": values,
                                  ct.default_float_vec_field_name: cf.gen_vectors(nb, dim)})
        collection_w.insert(dataframe)

        # 3. search with expression
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        expression = expression.replace("&&", "and").replace("||", "or")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, nb, expression,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999,
                                         "err_msg": "failed to create query plan: "
                                                    "cannot parse expression: %s" % expression})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_expr_value", ["string", 1.2, None, [1, 2, 3]])
    def test_search_param_invalid_expr_value(self, invalid_expr_value):
        """
        target: test search with invalid parameter values
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2 search with invalid expr
        invalid_search_expr = f"{ct.default_int64_field_name}=={invalid_expr_value}"
        log.info("test_search_param_invalid_expr_value: searching with "
                 "invalid expr: %s" % invalid_search_expr)
        collection_w.load()
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, invalid_search_expr,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999,
                                         "err_msg": "failed to create query plan: cannot parse expression: %s"
                                                    % invalid_search_expr})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_expr_bool_value", [1.2, 10, "string"])
    def test_search_param_invalid_expr_bool(self, invalid_expr_bool_value):
        """
        target: test search with invalid parameter values
        method: search with invalid bool search expressions
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, is_all_data_type=True)[0]
        collection_w.load()
        # 2 search with invalid bool expr
        invalid_search_expr_bool = f"{default_bool_field_name} == {invalid_expr_bool_value}"
        log.info("test_search_param_invalid_expr_bool: searching with "
                 "invalid expr: %s" % invalid_search_expr_bool)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, invalid_search_expr_bool,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "failed to create query plan"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_invalid_bool(self):
        """
        target: test search invalid bool
        method: test search invalid bool
        expected: searched failed
        """
        collection_w = self.init_collection_general(prefix, True, is_all_data_type=True)[0]
        expressions = ["bool", "true", "false"]
        for expression in expressions:
            log.debug(f"search with expression: {expression}")
            collection_w.search(vectors[:default_nq], default_search_field,
                                default_search_params, default_limit, expression,
                                check_task=CheckTasks.err_res,
                                check_items={"err_code": 1100,
                                             "err_msg": "failed to create query plan: predicate is not a "
                                                        "boolean expression: %s, data type: Bool" % expression})
        expression = "!bool"
        log.debug(f"search with expression: {expression}")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, expression,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1100,
                                         "err_msg": "cannot parse expression: !bool, "
                                                    "error: not op can only be applied on boolean expression"})
        expression = "int64 > 0 and bool"
        log.debug(f"search with expression: {expression}")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, expression,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1100,
                                         "err_msg": "cannot parse expression: int64 > 0 and bool, "
                                                    "error: 'and' can only be used between boolean expressions"})
        expression = "int64 > 0 or false"
        log.debug(f"search with expression: {expression}")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, expression,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1100,
                                         "err_msg": "cannot parse expression: int64 > 0 or false, "
                                                    "error: 'or' can only be used between boolean expressions"})


    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", ["int64 like 33", "float LIKE 33"])
    def test_search_with_expression_invalid_like(self, expression):
        """
        target: test search int64 and float with like
        method: test search int64 and float with like
        expected: searched failed
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()
        log.info(
            "test_search_with_expression: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "failed to create query plan: cannot parse "
                                                                    "expression: %s" % expression})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_invalid_array_one(self):
        """
        target: test search with invalid array expressions
        method: test search with invalid array expressions:
                the order of array > the length of array
        expected: searched successfully with correct limit(topK)
        """
        # 1. create a collection
        nb = ct.default_nb
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)
        data = cf.gen_row_data_by_schema(schema=schema)
        data[1][ct.default_int32_array_field_name] = [1]
        collection_w.insert(data)
        collection_w.create_index("float_vector", ct.default_index)
        collection_w.load()

        # 2. search (subscript > max_capacity)
        expression = "int32_array[101] > 0"
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, nb, expression)
        assert len(res[0]) == 0

        # 3. search (max_capacity > subscript > actual length of array)
        expression = "int32_array[51] > 0"
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, default_limit, expression)
        assert len(res[0]) == default_limit

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_invalid_array_two(self):
        """
        target: test search with invalid array expressions
        method: test search with invalid array expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. create a collection
        nb = ct.default_nb
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)
        data = cf.gen_row_data_by_schema(schema=schema)
        collection_w.insert(data)
        collection_w.create_index("float_vector", ct.default_index)
        collection_w.load()

        # 2. search
        expression = "int32_array[0] - 1 < 1"
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, nb, expression)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_partitions", [[None], [1, 2]])
    def test_search_partitions_invalid_type(self, invalid_partitions):
        """
        target: test search invalid partition
        method: search with invalid partition type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search the invalid partition
        err_msg = "`partition_name_array` value {} is illegal".format(invalid_partitions)
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, invalid_partitions,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999,
                                         "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_partitions", [["non_existing"], [ct.default_partition_name, "non_existing"]])
    def test_search_partitions_non_existing(self, invalid_partitions):
        """
        target: test search invalid partition
        method: search with invalid partition type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search the invalid partition
        err_msg = "partition name non_existing not found"
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, invalid_partitions,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_output_fields", [[None], [1, 2], ct.default_int64_field_name])
    def test_search_with_output_fields_invalid_type(self, invalid_output_fields):
        """
        target: test search with output fields
        method: search with invalid output_field
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search
        err_msg = f"`output_fields` value {invalid_output_fields} is illegal"
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=invalid_output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999,
                                         ct.err_msg: err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("non_exiting_output_fields",
                             [["non_exiting"], [ct.default_int64_field_name, "non_exiting"]])
    def test_search_with_output_fields_non_existing(self, non_exiting_output_fields):
        """
        target: test search with output fields
        method: search with invalid output_field
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search
        err_msg = f"field non_exiting not exist"
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=non_exiting_output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999,
                                         ct.err_msg: err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_release_collection(self):
        """
        target: test the scenario which search the released collection
        method: 1. create collection
                2. release collection
                3. search the released collection
        expected: raise exception and report the error
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. release collection
        collection_w.release()
        # 3. Search the released collection
        log.info("test_search_release_collection: Searching without collection ")
        collection_w.search(vectors, default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_release_partition(self):
        """
        target: test the scenario which search the released collection
        method: 1. create collection
                2. release partition
                3. search the released partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 10, partition_num, is_index=False)[0]
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        par = collection_w.partitions
        par_name = par[partition_num].name
        par[partition_num].load()
        # 2. release partition
        par[partition_num].release()
        # 3. Search the released partition
        log.info("test_search_release_partition: Searching the released partition")
        limit = 10
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_empty_collection(self, vector_data_type):
        """
        target: test search with empty connection
        method: 1. search the empty collection before load
                2. search the empty collection after load
                3. search collection with data inserted but not load again
        expected: 1. raise exception if not loaded
                  2. return topk=0  if loaded
                  3. return topk successfully
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix, is_index=False, vector_data_type=vector_data_type)[0]
        # 2. search collection without data before load
        log.info("test_search_with_empty_collection: Searching empty collection %s"
                 % collection_w.name)
        # err_msg = "collection" + collection_w.name + "was not loaded into memory"
        err_msg = "collection not loaded"
        vectors = cf.gen_vectors_based_on_vector_type(default_nq, default_dim, vector_data_type)
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, timeout=1,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 101,
                                         "err_msg": err_msg})
        # 3. search collection without data after load
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})
        # 4. search with data inserted but not load again
        insert_res = cf.insert_data(collection_w, vector_data_type=vector_data_type)[3]
        assert collection_w.num_entities == default_nb
        # Using bounded staleness, maybe we cannot search the "inserted" requests,
        # since the search requests arrived query nodes earlier than query nodes consume the insert requests.
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_res,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_empty_collection_with_partition(self):
        """
        target: test search with empty collection
        method: 1. collection an empty collection with partitions
                2. load
                3. search
        expected: return 0 result
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        par = collection_w.partitions
        # 2. search collection without data after load
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})
        # 2. search a partition without data after load
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            [par[1].name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_partition_deleted(self):
        """
        target: test search deleted partition
        method: 1. create a collection with partitions
                2. delete a partition
                3. search the deleted partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 1000, partition_num, is_index=False)[0]
        # 2. delete partitions
        log.info("test_search_partition_deleted: deleting a partition")
        par = collection_w.partitions
        deleted_par_name = par[partition_num].name
        collection_w.drop_partition(deleted_par_name)
        log.info("test_search_partition_deleted: deleted a partition")
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 3. search after delete partitions
        log.info("test_search_partition_deleted: searching deleted partition")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            [deleted_par_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "partition name search_partition_0 not found"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partition_not_existed(self):
        """
        target: test search not existed partition
        method: search with not existed partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 3. search the non exist partition
        partition_name = "search_non_exist"
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, [partition_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "partition name %s not found" % partition_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("reorder_k", [100])
    def test_search_scann_with_invalid_reorder_k(self, reorder_k):
        """
        target: test search with invalid nq
        method: search with invalid nq
        expected: raise exception and report the error
        """
        # initialize with data
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        index_params = {"index_type": "SCANN", "metric_type": "L2", "params": {"nlist": 1024}}
        collection_w.create_index(default_search_field, index_params)
        # search
        search_params = {"metric_type": "L2", "params": {"nprobe": 10, "reorder_k": reorder_k}}
        collection_w.load()
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, reorder_k + 1,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "reorder_k(100) should be larger than k(101)"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [16385])
    def test_search_with_invalid_nq(self, nq):
        """
        target: test search with invalid nq
        method: search with invalid nq
        expected: raise exception and report the error
        """
        # initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # search
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "nq (number of search vector per search "
                                                    "request) should be in range [1, 16384]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 15407")
    def test_search_param_invalid_binary(self):
        """
        target: test search within binary data (invalid parameter)
        method: search with wrong metric type
        expected: raise exception and report the error
        """
        # 1. initialize with binary data
        collection_w = self.init_collection_general(
            prefix, True, is_binary=True, is_index=False)[0]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        # 3. search with exception
        binary_vectors = cf.gen_binary_vectors(3000, default_dim)[1]
        wrong_search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector", wrong_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "unsupported"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_flat_with_L2(self):
        """
        target: search binary collection using FlAT with L2
        method: search binary collection using FLAT with L2
        expected: raise exception and report error
        """
        # 1. initialize with binary data
        collection_w = self.init_collection_general(prefix, True, is_binary=True)[0]
        # 2. search and assert
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(2, default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit, "int64 >= 0",
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "metric type not match: invalid "
                                                    "parameter[expected=JACCARD][actual=L2]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("issue #28465")
    @pytest.mark.parametrize("output_fields", ["int63", ""])
    @pytest.mark.parametrize("enable_dynamic", [True, False])
    def test_search_with_output_fields_not_exist(self, output_fields, enable_dynamic):
        """
        target: test search with output fields
        method: search with non-exist output_field
        expected: raise exception
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic)[0]
        # 2. search
        log.info("test_search_with_output_fields_not_exist: Searching collection %s" %
                 collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=[output_fields],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "field int63 not exist"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="Now support output vector field")
    @pytest.mark.parametrize("output_fields", [[default_search_field], ["*"]])
    def test_search_output_field_vector(self, output_fields):
        """
        target: test search with vector as output field
        method: search with one vector output_field or
                wildcard for vector
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search
        log.info("test_search_output_field_vector: Searching collection %s" %
                 collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=output_fields)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("index", ct.all_index_types[-2:])
    def test_search_output_field_vector_after_gpu_index(self, index):
        """
        target: test search with vector as output field
        method: 1. create a collection and insert data
                2. create an index which doesn't output vectors
                3. load and search
        expected: raise exception and report the error
        """
        # 1. create a collection and insert data
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]

        # 2. create an index which doesn't output vectors
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index(field_name, default_index)

        # 3. load and search
        collection_w.load()
        search_params = cf.gen_search_param(index)[0]
        collection_w.search(vectors[:default_nq], field_name, search_params,
                            default_limit, output_fields=[field_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "not supported"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", [["*%"], ["**"], ["*", "@"]])
    def test_search_output_field_invalid_wildcard(self, output_fields):
        """
        target: test search with invalid output wildcard
        method: search with invalid output_field wildcard
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search
        log.info("test_search_output_field_invalid_wildcard: Searching collection %s" %
                 collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": f"field {output_fields[-1]} not exist"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ignore_growing", [1.2, "string", [True]])
    def test_search_invalid_ignore_growing_param(self, ignore_growing):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. search with param ignore_growing invalid
        expected: raise exception
        """
        # 1. create a collection
        collection_w = self.init_collection_general(prefix, True, 10)[0]

        # 2. insert data again
        data = cf.gen_default_dataframe_data(start=100)
        collection_w.insert(data)

        # 3. search with param ignore_growing=True
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}, "ignore_growing": ignore_growing}
        vector = [[random.random() for _ in range(default_dim)] for _ in range(nq)]
        collection_w.search(vector[:default_nq], default_search_field, search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999,
                                         "err_msg": "parse ignore growing field failed"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_guarantee_timestamp(self, get_invalid_guarantee_timestamp):
        """
        target: test search with invalid guarantee timestamp
        method: search with invalid guarantee timestamp
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid guaranteeetimestamp
        log.info(
            "test_search_param_invalid_guarantee_timestamp: searching with invalid guarantee timestamp")
        invalid_guarantee_time = get_invalid_guarantee_timestamp
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            guarantee_timestamp=invalid_guarantee_time,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "`guarantee_timestamp` value %s is illegal"
                                                    % invalid_guarantee_time})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("round_decimal", [7, -2, 999, 1.0, None, [1], "string", {}])
    def test_search_invalid_round_decimal(self, round_decimal):
        """
        target: test search with invalid round decimal
        method: search with invalid round decimal
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search
        log.info("test_search_invalid_round_decimal: Searching collection %s" %
                 collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, round_decimal=round_decimal,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": f"`round_decimal` value {round_decimal} is illegal"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 30365")
    @pytest.mark.parametrize("invalid_radius", [[0.1], "str"])
    def test_range_search_invalid_radius(self, invalid_radius):
        """
        target: test range search with invalid radius
        method: range search with invalid radius
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. range search
        log.info("test_range_search_invalid_radius: Range searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "L2",
                               "params": {"nprobe": 10, "radius": invalid_radius, "range_filter": 0}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999, "err_msg": "type must be number"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 30365")
    @pytest.mark.parametrize("invalid_range_filter", [[0.1], "str"])
    def test_range_search_invalid_range_filter(self, invalid_range_filter):
        """
        target: test range search with invalid range_filter
        method: range search with invalid range_filter
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        # 2. create index
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "L2"}
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        # 3. load
        collection_w.load()
        # 2. range search
        log.info("test_range_search_invalid_range_filter: Range searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "L2",
                               "params": {"nprobe": 10, "radius": 1, "range_filter": invalid_range_filter}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999, "err_msg": "type must be number"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 30365")
    def test_range_search_invalid_radius_range_filter_L2(self):
        """
        target: test range search with invalid radius and range_filter for L2
        method: range search with radius smaller than range_filter
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        # 2. create index
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "L2"}
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        # 3. load
        collection_w.load()
        # 4. range search
        log.info("test_range_search_invalid_radius_range_filter_L2: Range searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "L2", "params": {"nprobe": 10, "radius": 1, "range_filter": 10}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "range_filter must less than radius except IP"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 30365")
    def test_range_search_invalid_radius_range_filter_IP(self):
        """
        target: test range search with invalid radius and range_filter for IP
        method: range search with radius larger than range_filter
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        # 2. create index
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "IP"}
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        # 3. load
        collection_w.load()
        # 4. range search
        log.info("test_range_search_invalid_radius_range_filter_IP: Range searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "IP",
                               "params": {"nprobe": 10, "radius": 10, "range_filter": 1}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "range_filter must more than radius when IP"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="It will report error before range search")
    @pytest.mark.parametrize("metric", ["SUPERSTRUCTURE", "SUBSTRUCTURE"])
    def test_range_search_data_type_metric_type_mismatch(self, metric):
        """
        target: test range search after unsupported metrics
        method: test range search after SUPERSTRUCTURE and SUBSTRUCTURE metrics
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  dim=default_dim, is_index=False)[0:5]
        # 2. create index and load
        default_index = {"index_type": "BIN_FLAT",
                         "params": {"nlist": 128}, "metric_type": metric}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. range search
        search_params = cf.gen_search_param("BIN_FLAT", metric_type=metric)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        for search_param in search_params:
            search_param["params"]["radius"] = 1000
            search_param["params"]["range_filter"] = 0
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp,
                                check_task=CheckTasks.err_res,
                                check_items={"err_code": 1,
                                             "err_msg": f"Data type and metric type miss-match"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="SUPERSTRUCTURE and SUBSTRUCTURE are supported again now")
    @pytest.mark.parametrize("metric", ["SUPERSTRUCTURE", "SUBSTRUCTURE"])
    def test_range_search_binary_not_supported_metrics(self, metric):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with SUPERSTRUCTURE
                (1) The returned limit(topK) are impacted by dimension (dim) of data
                (2) Searched topK is smaller than set limit when dim is large
                (3) It does not support "BIN_IVF_FLAT" index
                (4) Only two values for distance: 0 and 1, 0 means hits, 1 means not
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        nq = 1
        dim = 8
        collection_w, _, binary_raw_vector, insert_ids, time_stamp \
            = self.init_collection_general(prefix, True, default_nb, is_binary=True,
                                           dim=dim, is_index=False)[0:5]
        # 2. create index
        default_index = {"index_type": "BIN_FLAT",
                         "params": {"nlist": 128}, "metric_type": metric}
        collection_w.create_index("binary_vector", default_index,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 1,
                                               "err_msg": f"metric type {metric} not found or not supported, "
                                                          "supported: [HAMMING JACCARD]"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_dynamic_compare_two_fields(self):
        """
        target: test search compare with two fields for dynamic collection
        method: 1.create collection , insert data, enable dynamic function
                2.search with two fields comparisons
        expected: Raise exception
        """
        # create collection, insert tmp_nb, flush and load
        collection_w = self.init_collection_general(prefix, insert_data=True, nb=1,
                                                    primary_field=ct.default_string_field_name,
                                                    is_index=False,
                                                    enable_dynamic_field=True)[0]

        # create index
        index_params_one = {"index_type": "IVF_SQ8", "metric_type": "COSINE", "params": {"nlist": 64}}
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params_one, index_name=index_name1)
        index_params_two = {}
        collection_w.create_index(ct.default_string_field_name, index_params=index_params_two, index_name=index_name2)
        assert collection_w.has_index(index_name=index_name2)
        collection_w.load()
        # delete entity
        expr = 'float >= int64'
        # search with id 0 vectors
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            expr,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "query failed: N6milvus21ExecOperatorExceptionE :Operator::GetOutput failed"})