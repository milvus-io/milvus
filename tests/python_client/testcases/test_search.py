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
                                         "err_msg": "should create connect first"})

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
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 5000,
                                                                      is_index=False)[0:4]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        invalid_search_params = cf.gen_invalid_search_params_type()
        for invalid_search_param in invalid_search_params:
            if index == invalid_search_param["index_type"]:
                search_params = {"metric_type": "L2",
                                 "params": invalid_search_param["search_params"]}
                log.info("search_params: {}".format(search_params))
                collection_w.search(vectors[:default_nq], default_search_field,
                                    search_params, default_limit,
                                    default_search_exp,
                                    check_task=CheckTasks.err_res,
                                    check_items={"err_code": 65535,
                                                 "err_msg": "failed to search: invalid param in json:"
                                                            " invalid json key invalid_key"})

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
        log.info("test_search_param_invalid_limit_value: searching with "
                 "invalid limit (topK) = %s" % limit)
        err_msg = f"topk [{limit}] is invalid, top k should be in range [1, 16384], but got {limit}"
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
    @pytest.mark.parametrize("expression", cf.gen_invalid_bool_expressions())
    def test_search_with_expression_invalid_bool(self, expression):
        """
        target: test search invalid bool
        method: test search invalid bool
        expected: searched failed
        """
        collection_w = self.init_collection_general(prefix, True, is_all_data_type=True)[0]
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, expression,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1100,
                                         "err_msg": "failed to create query plan: predicate is not a "
                                                    "boolean expression: %s, data type: Bool" % expression})

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
    @pytest.mark.parametrize("non_exiting_output_fields", [["non_exiting"], [ct.default_int64_field_name, "non_exiting"]])
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
        err_msg = "collection" + collection_w.name + "was not loaded into memory"
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
    @pytest.mark.parametrize("index", ct.all_index_types[1:7])
    def test_search_different_index_invalid_params(self, index):
        """
        target: test search with different index
        method: test search with different index
        expected: searched successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 5000,
                                                                      partition_num=1,
                                                                      is_index=False)[0:4]
        # 2. create different index
        params = cf.get_index_params_params(index)
        if params.get("m"):
            if (default_dim % params["m"]) != 0:
                params["m"] = default_dim // 4
        log.info("test_search_different_index_invalid_params: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_different_index_invalid_params: Created index-%s" % index)
        collection_w.load()
        # 3. search
        log.info("test_search_different_index_invalid_params: Searching after "
                 "creating index-%s" % index)
        search_params = cf.gen_invalid_search_param(index)
        collection_w.search(vectors, default_search_field,
                            search_params[0], default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535, "err_msg": "type must be number, but is string"})

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
        vector = [[random.random() for _ in range(default_dim)]
                  for _ in range(nq)]
        collection_w.search(vector[:default_nq], default_search_field, search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999,
                                         "err_msg": "parse search growing failed"})

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
                                         "err_msg": "UnknownError: unsupported right datatype JSON of compare expr"})


class TestCollectionSearch(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[default_nb_medium])
    def nb(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[200])
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

    @pytest.fixture(scope="function", params=["IP", "COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def random_primary_key(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def vector_data_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["STL_SORT", "INVERTED"])
    def scalar_index(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_normal(self, nq, dim, auto_id, is_flush, enable_dynamic_field, vector_data_type):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. generate search data
        vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)
        # 3. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_normal_without_specify_metric_type(self):
        """
        target: test search without specify metric type
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        nq = 2
        dim = 32
        auto_id = True
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=dim, is_flush=True)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        search_params = {"params": {"nprobe": 10}}
        # 2. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_normal_without_specify_anns_field(self):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        nq = 2
        dim = 32
        auto_id = True
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=dim, is_flush=True)[0:5]
        # 2. search after insert
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], "",
                            default_search_params, default_limit,
                            default_search_exp,
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_hit_vectors(self, nq):
        """
        target: test search with vectors in collections
        method: create connections,collection insert and search vectors in collections
        expected: search successfully with limit(topK) and can be hit at top 1 (min distance is 0)
        """
        dim = 64
        auto_id = False
        enable_dynamic_field = True
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # get vectors that inserted into collection
        vectors = []
        if enable_dynamic_field:
            for vector in _vectors[0]:
                vector = vector[ct.default_float_vec_field_name]
                vectors.append(vector)
        else:
            vectors = np.array(_vectors[0]).tolist()
            vectors = [vectors[i][-1] for i in range(nq)]
        log.info("test_search_with_hit_vectors: searching collection %s" %
                 collection_w.name)
        search_res, _ = collection_w.search(vectors[:nq], default_search_field,
                                            default_search_params, default_limit,
                                            default_search_exp,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nq,
                                                         "ids": insert_ids,
                                                         "limit": default_limit})
        log.info("test_search_with_hit_vectors: checking the distance of top 1")
        for hits in search_res:
            # verify that top 1 hit is itself,so min distance is 0
            assert 1.0 - hits.distances[0] <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_vector_fields(self, nq, is_flush, vector_data_type):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. generate search data
        vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(default_search_field)
        # 3. search after insert
        for search_field in vector_name_list:
            collection_w.search(vectors[:nq], search_field,
                                default_search_params, default_limit,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_random_primary_key(self, random_primary_key):
        """
        target: test search for collection with random primary keys
        method: create connection, collection, insert and search
        expected: Search without errors and data consistency
        """
        # 1. initialize collection with random primary key

        collection_w, _vectors, _, insert_ids, time_stamp = \
            self.init_collection_general(
                prefix, True, 10, random_primary_key=random_primary_key)[0:5]
        # 2. search
        log.info("test_search_random_primary_key: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=[default_int64_field_name,
                                           default_float_field_name,
                                           default_json_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 10,
                                         "original_entities": _vectors,
                                         "output_fields": [default_int64_field_name,
                                                           default_float_field_name,
                                                           default_json_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dup_times", [1, 2, 3])
    def test_search_with_dup_primary_key(self, _async, dup_times):
        """
        target: test search with duplicate primary key
        method: 1.insert same data twice
                2.search
        expected: search results are de-duplicated
        """
        # initialize with data
        nb = ct.default_nb
        nq = ct.default_nq
        dim = 128
        auto_id = True
        collection_w, insert_data, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                                auto_id=auto_id,
                                                                                dim=dim)[0:4]
        # insert dup data multi times
        for i in range(dup_times):
            insert_res, _ = collection_w.insert(insert_data[0])
            insert_ids.extend(insert_res.primary_keys)
        # search
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:nq], default_search_field,
                                            default_search_params, default_limit,
                                            default_search_exp, _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nq,
                                                         "ids": insert_ids,
                                                         "limit": default_limit,
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()
        # assert that search results are de-duplicated
        for hits in search_res:
            ids = hits.ids
            assert sorted(list(set(ids))) == sorted(ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("search_params", [{}, {"metric_type": "COSINE"}])
    def test_search_with_default_search_params(self, _async, search_params):
        """
        target: test search with default search params
        method: search with default search params
        expected: search successfully
        """
        # initialize with data
        collection_w, insert_data, _, insert_ids = self.init_collection_general(prefix, True)[
            0:4]
        # search
        collection_w.search(vectors[:nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_accurate_search_with_multi_segments(self):
        """
        target: search collection with multi segments accurately
        method: insert and flush twice
        expect: result pk should be [19,9,18]
        """
        # 1. create a collection, insert data and flush
        nb = 10
        dim = 64
        collection_w = self.init_collection_general(
            prefix, True, nb, dim=dim, is_index=False)[0]

        # 2. insert data and flush again for two segments
        data = cf.gen_default_dataframe_data(nb=nb, dim=dim, start=nb)
        collection_w.insert(data)
        collection_w.flush()

        # 3. create index and load
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # 4. get inserted original data
        inserted_vectors = collection_w.query(expr="int64 >= 0", output_fields=[
                                              ct.default_float_vec_field_name])
        original_vectors = []
        for single in inserted_vectors[0]:
            single_vector = single[ct.default_float_vec_field_name]
            original_vectors.append(single_vector)

        # 5. Calculate the searched ids
        limit = 2*nb
        vectors = [[random.random() for _ in range(dim)] for _ in range(1)]
        distances = []
        for original_vector in original_vectors:
            distance = cf.cosine(vectors, original_vector)
            distances.append(distance)
        distances_max = heapq.nlargest(limit, distances)
        distances_index_max = map(distances.index, distances_max)

        # 6. search
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={
                                "nq": 1,
                                "limit": limit,
                                "ids": list(distances_index_max)
                            })

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_empty_vectors(self, _async):
        """
        target: test search with empty query vector
        method: search using empty query vector
        expected: search successfully with 0 results
        """
        # 1. initialize without data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w = self.init_collection_general(prefix, True,
                                                    auto_id=auto_id, dim=dim,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 2. search collection without data
        log.info("test_search_with_empty_vectors: Searching collection %s "
                 "using empty vector" % collection_w.name)
        collection_w.search([], default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 0,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_ndarray(self, _async):
        """
        target: test search with ndarray
        method: search using ndarray data
        expected: search successfully
        """
        # 1. initialize without data
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search collection without data
        log.info("test_search_with_ndarray: Searching collection %s "
                 "using ndarray" % collection_w.name)
        vectors = np.random.randn(default_nq, dim)
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("search_params", [{}, {"params": {}}, {"params": {"nprobe": 10}}])
    def test_search_normal_default_params(self, search_params, _async):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. rename collection
        new_collection_name = cf.gen_unique_str(prefix + "new")
        self.utility_wrap.rename_collection(
            collection_w.name, new_collection_name)
        collection_w = self.init_collection_general(auto_id=auto_id, dim=dim, name=new_collection_name,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 3. search
        log.info("test_search_normal_default_params: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="partition load and release constraints")
    def test_search_before_after_delete(self, nq, _async):
        """
        target: test search function before and after deletion
        method: 1. search the collection
                2. delete a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        nb = 1000
        limit = 1000
        partition_num = 1
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num,
                                                                      auto_id=auto_id, dim=dim)[0:4]
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info(
            "test_search_before_after_delete: searching before deleting partitions")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})
        # 3. delete partitions
        log.info("test_search_before_after_delete: deleting a partition")
        par = collection_w.partitions
        deleted_entity_num = par[partition_num].num_entities
        print(deleted_entity_num)
        entity_num = nb - deleted_entity_num
        collection_w.drop_partition(par[partition_num].name)
        log.info("test_search_before_after_delete: deleted a partition")
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 4. search non-deleted part after delete partitions
        log.info(
            "test_search_before_after_delete: searching after deleting partitions")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids[:entity_num],
                                         "limit": limit - deleted_entity_num,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_collection_after_release_load(self, nq, _async):
        """
        target: search the pre-released collection after load
        method: 1. create collection
                2. release collection
                3. load collection
                4. search the pre-released collection
        expected: search successfully
        """
        # 1. initialize without data
        nb= 2000
        dim = 64
        auto_id = True
        enable_dynamic_field = True
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  1, auto_id=auto_id,
                                                                                  dim=dim,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. release collection
        log.info("test_search_collection_after_release_load: releasing collection %s" %
                 collection_w.name)
        collection_w.release()
        log.info("test_search_collection_after_release_load: released collection %s" %
                 collection_w.name)
        # 3. Search the pre-released collection after load
        log.info("test_search_collection_after_release_load: loading collection %s" %
                 collection_w.name)
        collection_w.load()
        log.info("test_search_collection_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_load_flush_load(self, nq, _async):
        """
        target: test search when load before flush
        method: 1. insert data and load
                2. flush, and load
                3. search the collection
        expected: search success with limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w = self.init_collection_general(prefix, auto_id=auto_id, dim=dim,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 2. insert data
        insert_ids = cf.insert_data(collection_w, nb, auto_id=auto_id, dim=dim,
                                    enable_dynamic_field=enable_dynamic_field)[3]
        # 3. load data
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 4. flush and load
        collection_w.num_entities
        collection_w.load()
        # 5. search
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.skip("enable this later using session/strong consistency")
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_new_data(self, nq, _async):
        """
        target: test search new inserted data without load
        method: 1. search the collection
                2. insert new data
                3. search the collection without load again
                4. Use guarantee_timestamp to guarantee data consistency
        expected: new data should be searched
        """
        # 1. initialize with data
        dim = 128
        auto_id = False
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb_old,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim)[0:5]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info("test_search_new_data: searching for original data after load")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        # 3. insert new data
        nb_new = 300
        _, _, _, insert_ids_new, time_stamp = cf.insert_data(collection_w, nb_new,
                                                             auto_id=auto_id, dim=dim,
                                                             insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        # 4. search for new data without load
        # Using bounded staleness, maybe we could not search the "inserted" entities,
        # since the search requests arrived query nodes earlier than query nodes consume the insert requests.
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            guarantee_timestamp=time_stamp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_different_data_distribution_with_index(self, auto_id, _async):
        """
        target: test search different data distribution with index
        method: 1. connect milvus
                2. create a collection
                3. insert data
                4. create an index
                5. Load and search
        expected: Search successfully
        """
        # 1. connect, create collection and insert data
        dim = 64
        self._connect()
        collection_w = self.init_collection_general(
            prefix, False, dim=dim, is_index=False)[0]
        dataframe = cf.gen_default_dataframe_data(dim=dim, start=-1500)
        collection_w.insert(dataframe)

        # 2. create index
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)

        # 3. load and search
        collection_w.load()
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_max_dim(self, _async):
        """
        target: test search with max configuration
        method: create connection, collection, insert and search with max dim
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 100,
                                                                      auto_id=auto_id,
                                                                      dim=max_dim)[0:4]
        # 2. search
        nq = 2
        log.info("test_search_max_dim: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(max_dim)]
                   for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, nq,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nq,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_min_dim(self, _async):
        """
        target: test search with min configuration
        method: create connection, collection, insert and search with dim=1
        expected: search successfully
        """
        # 1. initialize with data
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 100,
                                                                      auto_id=auto_id,
                                                                      dim=min_dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        nq = 2
        log.info("test_search_min_dim: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(min_dim)]
                   for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, nq,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nq,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [1, 20, 100, 8000, 16384])
    def test_search_different_nq(self, nq):
        """
        target: test search with different nq
        method: create collection, insert, load and search with different nq ∈ [1, 16384]
        expected: search successfully with different nq
        """
        collection_w, _, _, insert_ids = self.init_collection_general(
            prefix, True, nb=20000)[0:4]
        log.info("test_search_max_nq: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("shards_num", [-256, 0, ct.max_shards_num // 2, ct.max_shards_num])
    def test_search_with_non_default_shard_nums(self, shards_num, _async):
        """
        target: test search with non_default shards_num
        method: connect milvus, create collection with several shard numbers , insert, load and search
        expected: search successfully with the non_default shards_num
        """
        auto_id = False
        self._connect()
        # 1. create collection
        name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(
            name=name, shards_num=shards_num)
        # 2. rename collection
        new_collection_name = cf.gen_unique_str(prefix + "new")
        self.utility_wrap.rename_collection(
            collection_w.name, new_collection_name)
        collection_w = self.init_collection_wrap(
            name=new_collection_name, shards_num=shards_num)
        # 3. insert
        dataframe = cf.gen_default_dataframe_data()
        collection_w.insert(dataframe)
        # 4. create index and load
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 5. search
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("M", [4, 64])
    @pytest.mark.parametrize("efConstruction", [8, 512])
    def test_search_HNSW_index_with_max_ef(self, M, efConstruction, _async):
        """
        target: test search HNSW index with max ef
        method: connect milvus, create collection , insert, create index, load and search
        expected: search successfully
        """
        dim = M * 4
        auto_id = True
        enable_dynamic_field = False
        self._connect()
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=False,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        HNSW_index_params = {"M": M, "efConstruction": efConstruction}
        HNSW_index = {"index_type": "HNSW",
                      "params": HNSW_index_params, "metric_type": "L2"}
        collection_w.create_index("float_vector", HNSW_index)
        collection_w.load()
        search_param = {"metric_type": "L2", "params": {"ef": 32768}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("M", [4, 64])
    @pytest.mark.parametrize("efConstruction", [8, 512])
    def test_search_HNSW_index_with_redundant_param(self, M, efConstruction, _async):
        """
        target: test search HNSW index with redundant param
        method: connect milvus, create collection , insert, create index, load and search
        expected: search successfully
        """
        dim = M * 4
        auto_id = False
        enable_dynamic_field = False
        self._connect()
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=False,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        # nlist is of no use
        HNSW_index_params = {
            "M": M, "efConstruction": efConstruction, "nlist": 100}
        HNSW_index = {"index_type": "HNSW",
                      "params": HNSW_index_params, "metric_type": "L2"}
        collection_w.create_index("float_vector", HNSW_index)
        collection_w.load()
        search_param = {"metric_type": "L2", "params": {
            "ef": 32768, "nprobe": 10}}  # nprobe is of no use
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("M", [4, 64])
    @pytest.mark.parametrize("efConstruction", [8, 512])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_search_HNSW_index_with_min_ef(self, M, efConstruction, limit, _async):
        """
        target: test search HNSW index with min ef
        method: connect milvus, create collection , insert, create index, load and search
        expected: search successfully
        """
        dim = M * 4
        ef = limit
        auto_id = True
        enable_dynamic_field = True
        self._connect()
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=False,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        HNSW_index_params = {"M": M, "efConstruction": efConstruction}
        HNSW_index = {"index_type": "HNSW",
                      "params": HNSW_index_params, "metric_type": "L2"}
        collection_w.create_index("float_vector", HNSW_index)
        collection_w.load()
        search_param = {"metric_type": "L2", "params": {"ef": ef}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_different_index_with_params(self, index, _async, scalar_index):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  is_all_data_type=True,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=False,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. create index on vector field and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field
        scalar_index_params = {"index_type": scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.load()
        # 4. search
        search_params = cf.gen_search_param(index, "COSINE")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            limit = default_limit
            if index == "HNSW":
                limit = search_param["params"]["ef"]
                if limit > max_limit:
                    limit = default_nb
            if index == "DISKANN":
                limit = search_param["params"]["search_list"]
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.skip(reason="waiting for the address of bf16 data generation slow problem")
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_different_index_with_params_all_vector_type_multiple_vectors(self, index,
                                                                                       _async,
                                                                                       scalar_index):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        auto_id = False
        enable_dynamic_field = False
        if index == "DISKANN":
            pytest.skip("https://github.com/milvus-io/milvus/issues/30793")
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  is_all_data_type=True,
                                                                                  auto_id=auto_id,
                                                                                  dim=default_dim, is_index=False,
                                                                                  enable_dynamic_field=enable_dynamic_field,
                                                                                  multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. create index on vector field and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field
        scalar_index_params = {"index_type": scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.load()
        # 4. search
        search_params = cf.gen_search_param(index, "COSINE")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            limit = default_limit
            if index == "HNSW":
                limit = search_param["params"]["ef"]
                if limit > max_limit:
                    limit = default_nb
            if index == "DISKANN":
                limit = search_param["params"]["search_list"]
            collection_w.search(vectors[:default_nq], vector_name_list[0],
                                search_param, limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_search_after_different_index_with_params_gpu(self, index, _async):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=False,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("search_params", cf.gen_autoindex_search_params())
    @pytest.mark.skip("issue #24533 #24555")
    def test_search_default_search_params_fit_for_autoindex(self, search_params, _async):
        """
        target: test search using autoindex
        method: test search using autoindex and its corresponding search params
        expected: search successfully
        """
        # 1. initialize with data
        auto_id = True
        collection_w = self.init_collection_general(
            prefix, True, auto_id=auto_id, is_index=False)[0]
        # 2. create index and load
        collection_w.create_index("float_vector", {})
        collection_w.load()
        # 3. search
        log.info("Searching with search params: {}".format(search_params))
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.skip("issue #27252")
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_different_index_with_min_dim(self, index, _async):
        """
        target: test search after different index with min dim
        method: test search after different index and corresponding search params with dim = 1
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=min_dim, is_index=False)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(min_dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_search_after_different_index_with_min_dim_gpu(self, index, _async):
        """
        target: test search after different index with min dim
        method: test search after different index and corresponding search params with dim = 1
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=min_dim, is_index=False)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        if params.get("m"):
            params["m"] = min_dim
        if params.get("PQM"):
            params["PQM"] = min_dim
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(min_dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_index_different_metric_type(self, index, _async, metric_type):
        """
        target: test search with different metric type
        method: test search with different metric type
        expected: searched successfully
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        enable_dynamic_field = True
        collection_w, _vectors, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                         partition_num=1,
                                                                                         auto_id=auto_id,
                                                                                         dim=dim, is_index=False,
                                                                                         enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. get vectors that inserted into collection
        original_vectors = []
        if enable_dynamic_field:
            for vector in _vectors[0]:
                vector = vector[ct.default_float_vec_field_name]
                original_vectors.append(vector)
        else:
            for _vector in _vectors:
                vectors_tmp = np.array(_vector).tolist()
                vectors_single = [vectors_tmp[i][-1] for i in range(2500)]
                original_vectors.append(vectors_single)
        log.info(len(original_vectors))
        # 3. create different index
        params = cf.get_index_params_params(index)
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        log.info("test_search_after_index_different_metric_type: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": metric_type}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_after_index_different_metric_type: Created index-%s" % index)
        collection_w.load()
        # 4. search
        search_params = cf.gen_search_param(index, metric_type)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            limit = default_limit
            if index == "HNSW":
                limit = search_param["params"]["ef"]
                if limit > max_limit:
                    limit = default_nb
            if index == "DISKANN":
                limit = search_param["params"]["search_list"]
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": limit,
                                             "_async": _async,
                                             "metric": metric_type,
                                             "vector_nq": vectors[:default_nq],
                                             "original_vectors": original_vectors})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 24957")
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_release_recreate_index(self, index, _async, metric_type):
        """
        target: test search after new metric with different metric type
        method: test search after new metric with different metric type
        expected: searched successfully
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _vectors, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                         partition_num=1,
                                                                                         auto_id=auto_id,
                                                                                         dim=dim, is_index=False,
                                                                                         enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. get vectors that inserted into collection
        original_vectors = []
        if enable_dynamic_field:
            for vector in _vectors[0]:
                vector = vector[ct.default_float_vec_field_name]
                original_vectors.append(vector)
        else:
            for _vector in _vectors:
                vectors_tmp = np.array(_vector).tolist()
                vectors_single = [vectors_tmp[i][-1] for i in range(2500)]
                original_vectors.append(vectors_single)
        # 3. create different index
        params = cf.get_index_params_params(index)
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        log.info("test_search_after_release_recreate_index: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_after_release_recreate_index: Created index-%s" % index)
        collection_w.load()
        # 4. search
        search_params = cf.gen_search_param(index, "COSINE")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async)
        # 5. re-create index
        collection_w.release()
        collection_w.drop_index()
        default_index = {"index_type": index, "params": params, "metric_type": metric_type}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async,
                                             "metric": metric_type,
                                             "vector_nq": vectors[:default_nq],
                                             "original_vectors": original_vectors})

    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_search_after_index_different_metric_type_gpu(self, index, _async):
        """
        target: test search with different metric type
        method: test search with different metric type
        expected: searched successfully
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=False,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. create different index
        params = cf.get_index_params_params(index)
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        log.info("test_search_after_index_different_metric_type: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_after_index_different_metric_type: Created index-%s" % index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index, "IP")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_collection_multiple_times(self, nq, _async):
        """
        target: test search for multiple times
        method: search for multiple times
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for multiple times
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        for i in range(search_num):
            log.info(
                "test_search_collection_multiple_times: searching round %d" % (i + 1))
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_sync_async_multiple_times(self, nq):
        """
        target: test async search after sync search case
        method: create connection, collection, insert,
                sync search and async search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. search
        log.info("test_search_sync_async_multiple_times: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        for i in range(search_num):
            log.info(
                "test_search_sync_async_multiple_times: searching round %d" % (i + 1))
            for _async in [False, True]:
                collection_w.search(vectors[:nq], default_search_field,
                                    default_search_params, default_limit,
                                    default_search_exp, _async=_async,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": nq,
                                                 "ids": insert_ids,
                                                 "limit": default_limit,
                                                 "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #12680")
    # TODO: add one more for binary vectors
    # @pytest.mark.parametrize("vec_fields", [[cf.gen_float_vec_field(name="test_vector1")],
    #                                         [cf.gen_binary_vec_field(name="test_vector1")],
    #                                         [cf.gen_binary_vec_field(), cf.gen_binary_vec_field("test_vector1")]])
    def test_search_multiple_vectors_with_one_indexed(self):
        """
        target: test indexing on one vector fields when there are multi float vec fields
        method: 1. create collection with multiple float vector fields
                2. insert data and build index on one of float vector fields
                3. load collection and search
        expected: load and search successfully
        """
        vec_fields = [cf.gen_float_vec_field(name="test_vector1")]
        schema = cf.gen_schema_multi_vector_fields(vec_fields)
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_dataframe_multi_vec_fields(vec_fields=vec_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        _index = {"index_type": "IVF_FLAT", "params": {
            "nlist": 128}, "metric_type": "L2"}
        res, ch = collection_w.create_index(
            field_name="test_vector1", index_params=_index)
        assert ch is True
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(2)]
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        res_1, _ = collection_w.search(data=vectors, anns_field="test_vector1",
                                       param=search_params, limit=1)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_index_one_partition(self, _async):
        """
        target: test search from partition
        method: search from one partition
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 1200
        auto_id = False
        enable_dynamic_field = True
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  is_index=False,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]

        # 2. create index
        default_index = {"index_type": "IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search in one partition
        log.info(
            "test_search_index_one_partition: searching (1000 entities) through one partition")
        limit = 1000
        par = collection_w.partitions
        if limit > par[1].num_entities:
            limit_check = par[1].num_entities
        else:
            limit_check = limit
        search_params = {"metric_type": "L2", "params": {"nprobe": 128}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, limit, default_search_exp,
                            [par[1].name], _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids[par[0].num_entities:],
                                         "limit": limit_check,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partitions(self, nq, _async):
        """
        target: test search from partitions
        method: search from partitions
        expected: searched successfully
        """
        # 1. initialize with data
        dim = 64
        nb = 1000
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num=1,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      is_index=False)[0:4]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search through partitions
        log.info("test_search_index_partitions: searching (1000 entities) through partitions")
        par = collection_w.partitions
        log.info("test_search_index_partitions: partitions: %s" % par)
        search_params = {"metric_type": "L2", "params": {"nprobe": 64}}
        collection_w.search(vectors[:nq], default_search_field,
                            search_params, ct.default_limit, default_search_exp,
                            [par[0].name, par[1].name], _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": ct.default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_names",
                             [["(.*)"], ["search(.*)"]])
    def test_search_index_partitions_fuzzy(self, nq, partition_names):
        """
        target: test search from partitions
        method: search from partitions with fuzzy
                partition name
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 2000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num=1,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      is_index=False,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create index
        nlist = 128
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": nlist}, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search through partitions
        log.info("test_search_index_partitions_fuzzy: searching through partitions")
        limit = 1000
        limit_check = limit
        par = collection_w.partitions
        search_params = {"metric_type": "COSINE", "params": {"nprobe": nlist}}
        if partition_names == ["search(.*)"]:
            insert_ids = insert_ids[par[0].num_entities:]
            if limit > par[1].num_entities:
                limit_check = par[1].num_entities
        collection_w.search(vectors[:nq], default_search_field,
                            search_params, limit, default_search_exp,
                            partition_names,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "partition name %s not found" % partition_names})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partition_empty(self, nq, _async):
        """
        target: test search the empty partition
        method: search from the empty partition
        expected: searched successfully with 0 results
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        collection_w = self.init_collection_general(prefix, True, auto_id=auto_id,
                                                    dim=dim, is_index=False)[0]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create empty partition
        partition_name = "search_partition_empty"
        collection_w.create_partition(
            partition_name=partition_name, description="search partition empty")
        par = collection_w.partitions
        log.info("test_search_index_partition_empty: partitions: %s" % par)
        # 3. create index
        default_index = {"index_type": "IVF_FLAT", "params": {
            "nlist": 128}, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 4. search the empty partition
        log.info("test_search_index_partition_empty: searching %s "
                 "entities through empty partition" % default_limit)
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, [partition_name],
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_jaccard_flat_index(self, nq, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with JACCARD
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 64
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = self.init_collection_general(prefix, True, 2,
                                                                                                  is_binary=True,
                                                                                                  auto_id=auto_id,
                                                                                                  dim=dim,
                                                                                                  is_index=False,
                                                                                                  is_flush=is_flush)[0:5]
        # 2. create index on sclalar and vector field
        default_index = {"index_type": "INVERTED", "params": {}}
        collection_w.create_index(ct.default_float_field_name, default_index)
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.jaccard(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.jaccard(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_hamming_flat_index(self, nq, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with HAMMING
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 64
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=False,
                                                                                      is_flush=is_flush)[0:4]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "HAMMING"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        collection_w.load()
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.hamming(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.hamming(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "HAMMING", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("tanimoto obsolete")
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_tanimoto_flat_index(self, nq, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with TANIMOTO
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 64
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=False,
                                                                                      is_flush=is_flush)[0:4]
        log.info("auto_id= %s, _async= %s" % (auto_id, _async))
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "TANIMOTO"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "TANIMOTO", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT"])
    def test_search_binary_substructure_flat_index(self, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with SUBSTRUCTURE.
                (1) The returned limit(topK) are impacted by dimension (dim) of data
                (2) Searched topK is smaller than set limit when dim is large
                (3) It does not support "BIN_IVF_FLAT" index
                (4) Only two values for distance: 0 and 1, 0 means hits, 1 means not
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        nq = 1
        dim = 8
        auto_id = True
        collection_w, _, binary_raw_vector, insert_ids, time_stamp \
            = self.init_collection_general(prefix, True, default_nb, is_binary=True, auto_id=auto_id,
                                           dim=dim, is_index=False, is_flush=is_flush)[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {"nlist": 128}, "metric_type": "SUBSTRUCTURE"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. generate search vectors
        _, binary_vectors = cf.gen_binary_vectors(nq, dim)
        # 4. search and compare the distance
        search_params = {"metric_type": "SUBSTRUCTURE", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async)[0]
        if _async:
            res.done()
            res = res.result()
        assert res[0].distances[0] == 0.0
        assert len(res) <= default_limit

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT"])
    def test_search_binary_superstructure_flat_index(self, _async, index, is_flush):
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
        auto_id = True
        collection_w, _, binary_raw_vector, insert_ids, time_stamp \
            = self.init_collection_general(prefix, True, default_nb, is_binary=True, auto_id=auto_id,
                                           dim=dim, is_index=False, is_flush=is_flush)[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {"nlist": 128}, "metric_type": "SUPERSTRUCTURE"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. generate search vectors
        _, binary_vectors = cf.gen_binary_vectors(nq, dim)
        # 4. search and compare the distance
        search_params = {"metric_type": "SUPERSTRUCTURE", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async)[0]
        if _async:
            res.done()
            res = res.result()
        assert len(res[0]) <= default_limit
        assert res[0].distances[0] == 0.0

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_without_flush(self, metrics):
        """
        target: test search without flush for binary data (no index)
        method: create connection, collection, insert, load and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize a collection without data
        auto_id = True
        collection_w = self.init_collection_general(
            prefix, is_binary=True, auto_id=auto_id, is_index=False)[0]
        # 2. insert data
        insert_ids = cf.insert_data(
            collection_w, default_nb, is_binary=True, auto_id=auto_id)[3]
        # 3. load data
        index_params = {"index_type": "BIN_FLAT", "params": {
            "nlist": 128}, "metric_type": metrics}
        collection_w.create_index("binary_vector", index_params)
        collection_w.load()
        # 4. search
        log.info("test_search_binary_without_flush: searching collection %s" %
                 collection_w.name)
        binary_vectors = cf.gen_binary_vectors(default_nq, default_dim)[1]
        search_params = {"metric_type": metrics, "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_normal_expressions())
    def test_search_with_expression(self, expression, _async):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        enable_dynamic_field = False
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True,
                                                                             nb, dim=dim,
                                                                             is_index=False,
                                                                             enable_dynamic_field=enable_dynamic_field)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i, _id in enumerate(insert_ids):
            if enable_dynamic_field:
                int64 = _vectors[i][ct.default_int64_field_name]
                float = _vectors[i][ct.default_float_field_name]
            else:
                int64 = _vectors.int64[i]
                float = _vectors.float[i]
            if not expression or eval(expression):
                filter_ids.append(_id)

        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false"])
    def test_search_with_expression_bool(self, _async, bool_type):
        """
        target: test search with different bool expressions
        method: search with different bool expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                             is_all_data_type=True,
                                                                             auto_id=auto_id,
                                                                             dim=dim, is_index=False,
                                                                             enable_dynamic_field=enable_dynamic_field)[0:4]

        # 2. create index and load
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, index_param)
        collection_w.load()

        # 3. filter result with expression in collection
        filter_ids = []
        bool_type_cmp = bool_type
        if bool_type == "true":
            bool_type_cmp = True
        if bool_type == "false":
            bool_type_cmp = False
        if enable_dynamic_field:
            for i, _id in enumerate(insert_ids):
                if _vectors[0][i][f"{ct.default_bool_field_name}"] == bool_type_cmp:
                    filter_ids.append(_id)
        else:
            for i in range(len(_vectors[0])):
                if _vectors[0][i].dtypes == bool:
                    num = i
                    break
            for i, _id in enumerate(insert_ids):
                if _vectors[0][num][i] == bool_type_cmp:
                    filter_ids.append(_id)

        # 4. search with different expressions
        expression = f"{default_bool_field_name} == {bool_type}"
        log.info("test_search_with_expression_bool: searching with bool expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]

        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_array_field_expressions())
    def test_search_with_expression_array(self, expression, _async):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        enable_dynamic_field = False
        # 1. create a collection
        nb = ct.default_nb
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema, enable_dynamic_field=enable_dynamic_field)

        # 2. insert data
        array_length = 10
        data = []
        for i in range(nb):
            arr = {ct.default_int64_field_name: i,
                   ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0],
                   ct.default_int32_array_field_name: [np.int32(i) for i in range(array_length)],
                   ct.default_float_array_field_name: [np.float32(i) for i in range(array_length)],
                   ct.default_string_array_field_name: [str(i) for i in range(array_length)]}
            data.append(arr)
        collection_w.insert(data)

        # 3. filter result with expression in collection
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i in range(nb):
            int32_array = data[i][ct.default_int32_array_field_name]
            float_array = data[i][ct.default_float_array_field_name]
            string_array = data[i][ct.default_string_array_field_name]
            if not expression or eval(expression):
                filter_ids.append(i)

        # 4. create index
        collection_w.create_index("float_vector", ct.default_index)
        collection_w.load()

        # 5. search with expression
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression, _async=_async)
        if _async:
            search_res.done()
            search_res = search_res.result()

        for hits in search_res:
            ids = hits.ids
            assert set(ids) == set(filter_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("exists", ["exists"])
    @pytest.mark.parametrize("json_field_name", ["json_field", "json_field['number']", "json_field['name']",
                                                 "float_array", "not_exist_field", "new_added_field"])
    def test_search_with_expression_exists(self, exists, json_field_name, _async):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        enable_dynamic_field = True
        if not enable_dynamic_field:
            pytest.skip("not allowed")
        # 1. initialize with data
        nb = 100
        schema = cf.gen_array_collection_schema(with_json=True, enable_dynamic_field=enable_dynamic_field)
        collection_w = self.init_collection_wrap(schema=schema, enable_dynamic_field=enable_dynamic_field)
        log.info(schema.fields)
        if enable_dynamic_field:
            data = cf.gen_row_data_by_schema(nb, schema=schema)
            for i in range(nb):
                data[i]["new_added_field"] = i
            log.info(data[0])
        else:
            data = cf.gen_array_dataframe_data(nb, with_json=True)
            log.info(data.head(1))
        collection_w.insert(data)

        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        expression = exists + " " + json_field_name
        if enable_dynamic_field:
            limit = nb if json_field_name in data[0].keys() else 0
        else:
            limit = nb if json_field_name in data.columns.to_list() else 0
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "limit": limit,
                                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 24514")
    @pytest.mark.parametrize("expression", cf.gen_normal_expressions_field(default_float_field_name))
    def test_search_with_expression_auto_id(self, expression, _async):
        """
        target: test search with different expressions
        method: test search with different expressions with auto id
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        enable_dynamic_field = True
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                             auto_id=True,
                                                                             dim=dim,
                                                                             is_index=False,
                                                                             enable_dynamic_field=enable_dynamic_field)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i, _id in enumerate(insert_ids):
            if enable_dynamic_field:
                exec(
                    f"{default_float_field_name} = _vectors[i][f'{default_float_field_name}']")
            else:
                exec(
                    f"{default_float_field_name} = _vectors.{default_float_field_name}[i]")
            if not expression or eval(expression):
                filter_ids.append(_id)

        # 2. create index
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with different expressions
        log.info(
            "test_search_with_expression_auto_id: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_all_data_type(self, nq, _async):
        """
        target: test search using all supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        nb = 3000
        dim = 64
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      is_all_data_type=True,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      multiple_dim_array=[dim, dim])[0:4]
        # 2. search
        log.info("test_search_expression_all_data_type: Searching collection %s" %
                 collection_w.name)
        search_exp = "int64 >= 0 && int32 >= 0 && int16 >= 0 " \
                     "&& int8 >= 0 && float >= 0 && double >= 0"
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        for search_field in vector_name_list:
            vector_data_type = search_field.lstrip("multiple_vector_")
            vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)
            res = collection_w.search(vectors[:nq], search_field,
                                      default_search_params, default_limit,
                                      search_exp, _async=_async,
                                      output_fields=[default_int64_field_name,
                                                     default_float_field_name,
                                                     default_bool_field_name],
                                      check_task=CheckTasks.check_search_results,
                                      check_items={"nq": nq,
                                                   "ids": insert_ids,
                                                   "limit": default_limit,
                                                   "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert (default_int64_field_name and default_float_field_name and default_bool_field_name) \
            in res[0][0].fields

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field", ct.all_scalar_data_types[:3])
    def test_search_expression_different_data_type(self, field):
        """
        target: test search expression using different supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        num = int(field[3:])
        offset = 2 ** (num - 1)
        default_schema = cf.gen_collection_schema_all_datatype()
        collection_w = self.init_collection_wrap(schema=default_schema)
        collection_w = cf.insert_data(collection_w, is_all_data_type=True, insert_offset=offset-1000)[0]

        # 2. create index and load
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, index_param)
        collection_w.load()

        # 3. search using expression which field value is out of bound
        log.info("test_search_expression_different_data_type: Searching collection %s" % collection_w.name)
        expression = f"{field} >= {offset}"
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, expression, output_fields=[field],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": 0})[0]
        # 4. search normal using all the scalar type as output fields
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, output_fields=[field],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "output_fields": [field]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_comparative_expression(self, _async):
        """
        target: test search with expression comparing two fields
        method: create a collection, insert data and search with comparative expression
        expected: search successfully
        """
        # 1. create a collection
        nb = 10
        dim = 2
        fields = [cf.gen_int64_field("int64_1"), cf.gen_int64_field("int64_2"),
                  cf.gen_float_vec_field(dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, primary_field="int64_1")
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str("comparison"), schema=schema)

        # 2. inset data
        values = pd.Series(data=[i for i in range(0, nb)])
        dataframe = pd.DataFrame({"int64_1": values, "int64_2": values,
                                  ct.default_float_vec_field_name: cf.gen_vectors(nb, dim)})
        insert_res = collection_w.insert(dataframe)[0]

        insert_ids = []
        filter_ids = []
        insert_ids.extend(insert_res.primary_keys)
        for _id in enumerate(insert_ids):
            filter_ids.extend(_id)

        # 3. search with expression
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        expression = "int64_1 <= int64_2"
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        res = collection_w.search(vectors[:nq], default_search_field,
                                  default_search_params, default_limit,
                                  expression, _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        filter_ids_set = set(filter_ids)
        for hits in res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_with_double_quotes(self):
        """
        target: test search with expressions with double quotes
        method: test search with expressions with double quotes
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        string_value = [(f"'{cf.gen_str_by_length(3)}'{cf.gen_str_by_length(3)}\""
                         f"{cf.gen_str_by_length(3)}\"") for _ in range(default_nb)]
        data = cf.gen_default_dataframe_data()
        data[default_string_field_name] = string_value
        insert_ids = data[default_int64_field_name]
        collection_w.insert(data)

        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        _id = random.randint(0, default_nb)
        string_value[_id] = string_value[_id].replace("\"", "\\\"")
        expression = f"{default_string_field_name} == \"{string_value[_id]}\""
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": 1})
        assert search_res[0].ids == [_id]

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields_empty(self, nq, _async):
        """
        target: test search with output fields
        method: search with empty output_field
        expected: search success
        """
        # 1. initialize with data
        nb = 1500
        dim = 32
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search
        log.info("test_search_with_output_fields_empty: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=[],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": []})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_output_field(self, _async):
        """
        target: test search with output fields
        method: search with one output_field
        expected: search success
        """
        # 1. initialize with data
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_search_with_output_field: Searching collection %s" % collection_w.name)

        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=[default_int64_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": [default_int64_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_output_vector_field(self, _async):
        """
        target: test search with output fields
        method: search with one output_field
        expected: search success
        """
        # 1. initialize with data
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_search_with_output_field: Searching collection %s" % collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=[field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": [field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields(self, _async):
        """
        target: test search with output fields
        method: search with multiple output_field
        expected: search success
        """
        # 1. initialize with data
        nb = 2000
        dim = 64
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      is_all_data_type=True,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search
        log.info("test_search_with_output_fields: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        output_fields = [default_int64_field_name, default_float_field_name]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_output_array_field(self, enable_dynamic_field):
        """
        target: test search output array field
        method: create connection, collection, insert and search
        expected: search successfully
        """
        # 1. create a collection
        auto_id = True
        schema = cf.gen_array_collection_schema(auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)

        # 2. insert data
        if enable_dynamic_field:
            data = cf.gen_row_data_by_schema(schema=schema)
        else:
            data = cf.gen_array_dataframe_data(auto_id=auto_id)

        collection_w.insert(data)

        # 3. create index and load
        collection_w.create_index(default_search_field)
        collection_w.load()

        # 4. search output array field, check
        output_fields = [ct.default_int64_field_name, ct.default_int32_array_field_name,
                         ct.default_float_array_field_name]
        collection_w.search(vectors[:default_nq], default_search_field, {}, default_limit,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    @pytest.mark.parametrize("metrics", ct.float_metrics)
    @pytest.mark.parametrize("limit", [20, 1200])
    def test_search_output_field_vector_after_different_index_metrics(self, index, metrics, limit):
        """
        target: test search with output vector field after different index
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        collection_w, _vectors = self.init_collection_general(prefix, True, is_index=False)[:2]

        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": metrics}
        collection_w.create_index(field_name, default_index)
        collection_w.load()

        # 3. search with output field vector
        search_params = cf.gen_search_param(index, metrics)
        for search_param in search_params:
            log.info(search_param)
            if index == "HNSW":
                limit = search_param["params"]["ef"]
                if limit > max_limit:
                    limit = default_nb
            if index == "DISKANN":
                limit = search_param["params"]["search_list"]
            collection_w.search(vectors[:1], default_search_field,
                                search_param, limit, default_search_exp,
                                output_fields=[field_name],
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": 1,
                                             "limit": limit,
                                             "original_entities": _vectors,
                                             "output_fields": [field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metrics", ct.binary_metrics[:2])
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_output_field_vector_after_binary_index(self, metrics, index):
        """
        target: test search with output vector field after binary index
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        # 1. create a collection and insert data
        collection_w = self.init_collection_general(prefix, is_binary=True, is_index=False)[0]
        data = cf.gen_default_binary_dataframe_data()[0]
        collection_w.insert(data)

        # 2. create index and load
        params = {"M": 48, "efConstruction": 500} if index == "HNSW" else {"nlist": 128}
        default_index = {"index_type": index, "metric_type": metrics, "params": params}
        collection_w.create_index(binary_field_name, default_index)
        collection_w.load()

        # 3. search with output field vector
        search_params = cf.gen_search_param(index, metrics)
        binary_vectors = cf.gen_binary_vectors(1, default_dim)[1]
        for search_param in search_params:
            res = collection_w.search(binary_vectors, binary_field_name,
                                      search_param, 2, default_search_exp,
                                      output_fields=[binary_field_name])[0]

            # 4. check the result vectors should be equal to the inserted
            assert res[0][0].entity.binary_vector == data[binary_field_name][res[0][0].id]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metrics", ct.structure_metrics)
    @pytest.mark.parametrize("index", ["BIN_FLAT"])
    def test_search_output_field_vector_after_structure_metrics(self, metrics, index):
        """
        target: test search with output vector field after binary index
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        dim = 8
        # 1. create a collection and insert data
        collection_w = self.init_collection_general(prefix, dim=dim, is_binary=True, is_index=False)[0]
        data = cf.gen_default_binary_dataframe_data(dim=dim)[0]
        collection_w.insert(data)

        # 2. create index and load
        default_index = {"index_type": index, "metric_type": metrics, "params": {"nlist": 128}}
        collection_w.create_index(binary_field_name, default_index)
        collection_w.load()

        # 3. search with output field vector
        search_params = {"metric_type": metrics, "params": {"nprobe": 10}}
        binary_vectors = cf.gen_binary_vectors(ct.default_nq, dim)[1]
        res = collection_w.search(binary_vectors, binary_field_name,
                                  search_params, 2, default_search_exp,
                                  output_fields=[binary_field_name])[0]

        # 4. check the result vectors should be equal to the inserted
        assert res[0][0].entity.binary_vector == data[binary_field_name][res[0][0].id]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [32, 77, 768])
    def test_search_output_field_vector_with_different_dim(self, dim):
        """
        target: test search with output vector field after binary index
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        # 1. create a collection and insert data
        collection_w, _vectors = self.init_collection_general(prefix, True, dim=dim)[:2]

        # 2. search with output field vector
        vectors = cf.gen_vectors(default_nq, dim=dim)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            output_fields=[field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "original_entities": _vectors,
                                         "output_fields": [field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_output_vector_field_and_scalar_field(self, enable_dynamic_field):
        """
        target: test search with output vector field and scalar field
        method: 1. initialize a collection
                2. search with output field vector
                3. check no field missing
        expected: search success
        """
        # 1. initialize a collection
        collection_w, _vectors = self.init_collection_general(prefix, True,
                                                              enable_dynamic_field=enable_dynamic_field)[:2]

        # search with output field vector
        output_fields = [default_float_field_name, default_string_field_name, default_search_field]
        original_entities = []
        if enable_dynamic_field:
            entities = []
            for vector in _vectors[0]:
                entities.append({default_int64_field_name: vector[default_int64_field_name],
                                 default_float_field_name: vector[default_float_field_name],
                                 default_string_field_name: vector[default_string_field_name],
                                 default_search_field: vector[default_search_field]})
            original_entities.append(pd.DataFrame(entities))
        else:
            original_entities = _vectors
        collection_w.search(vectors[:1], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1,
                                         "limit": default_limit,
                                         "original_entities": original_entities,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_output_vector_field_and_pk_field(self, enable_dynamic_field):
        """
        target: test search with output vector field and pk field
        method: 1. initialize a collection
                2. search with output field vector
                3. check no field missing
        expected: search success
        """
        # 1. initialize a collection
        collection_w = self.init_collection_general(prefix, True,
                                                    enable_dynamic_field=enable_dynamic_field)[0]

        # 2. search with output field vector
        output_fields = [default_int64_field_name, default_string_field_name, default_search_field]
        collection_w.search(vectors[:1], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1,
                                         "limit": default_limit,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_output_field_vector_with_partition(self):
        """
        target: test search with output vector field
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        # 1. create a collection and insert data
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        partition_w = self.init_partition_wrap(collection_w)
        data = cf.gen_default_dataframe_data()
        partition_w.insert(data)

        # 2. create index and load
        collection_w.create_index(field_name, default_index_params)
        collection_w.load()

        # 3. search with output field vector
        partition_w.search(vectors[:1], default_search_field,
                           default_search_params, default_limit, default_search_exp,
                           output_fields=[field_name],
                           check_task=CheckTasks.check_search_results,
                           check_items={"nq": 1,
                                        "limit": default_limit,
                                        "original_entities": [data],
                                        "output_fields": [field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("wildcard_output_fields", [["*"], ["*", default_int64_field_name],
                                                        ["*", default_search_field]])
    def test_search_with_output_field_wildcard(self, wildcard_output_fields, _async):
        """
        target: test search with output fields using wildcard
        method: search with one output_field (wildcard)
        expected: search success
        """
        # 1. initialize with data
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id)[0:4]
        # 2. search
        log.info("test_search_with_output_field_wildcard: Searching collection %s" % collection_w.name)
        output_fields = cf.get_wildcard_output_field_names(collection_w, wildcard_output_fields)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=wildcard_output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_output_fields", [["%"], [""], ["-"]])
    def test_search_with_invalid_output_fields(self, invalid_output_fields):
        """
        target: test search with output fields using wildcard
        method: search with one output_field (wildcard)
        expected: search success
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, auto_id=auto_id)[0:4]
        # 2. search
        log.info("test_search_with_output_field_wildcard: Searching collection %s" % collection_w.name)
        error1 = {"err_code": 65535, "err_msg": "field %s not exist" % invalid_output_fields[0]}
        error2 = {"err_code": 1, "err_msg": "`output_fields` value %s is illegal" % invalid_output_fields[0]}
        error = error2 if invalid_output_fields == [""] else error1
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=invalid_output_fields,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_collections(self, nq, _async):
        """
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        """
        nb = 1000
        dim = 64
        auto_id = True
        self._connect()
        collection_num = 10
        for i in range(collection_num):
            # 1. initialize with data
            log.info("test_search_multi_collections: search round %d" % (i + 1))
            collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                          auto_id=auto_id,
                                                                          dim=dim)[0:4]
            # 2. search
            vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
            log.info("test_search_multi_collections: searching %s entities (nq = %s) from collection %s" %
                     (default_limit, nq, collection_w.name))
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_concurrent_multi_threads(self, nq, _async):
        """
        target: test concurrent search with multi-processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        # 1. initialize with data
        nb = 3000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        threads_num = 10
        threads = []
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]

        def search(collection_w):
            vectors = [[random.random() for _ in range(dim)]
                       for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info("test_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.skip(reason="Not running for now")
    @pytest.mark.tags(CaseLabel.L2)
    def test_search_insert_in_parallel(self):
        """
        target: test search and insert in parallel
        method: One process do search while other process do insert
        expected: No exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index(ct.default_float_vec_field_name, default_index)
        collection_w.load()

        def do_insert():
            df = cf.gen_default_dataframe_data(10000)
            for i in range(11):
                collection_w.insert(df)
                log.info(f'Collection num entities is : {collection_w.num_entities}')

        def do_search():
            while True:
                results, _ = collection_w.search(cf.gen_vectors(nq, ct.default_dim), default_search_field,
                                                 default_search_params, default_limit, default_search_exp, timeout=30)
                ids = []
                for res in results:
                    ids.extend(res.ids)
                expr = f'{ct.default_int64_field_name} in {ids}'
                collection_w.query(expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                                   timeout=30)

        p_insert = multiprocessing.Process(target=do_insert, args=())
        p_search = multiprocessing.Process(target=do_search, args=(), daemon=True)

        p_insert.start()
        p_search.start()

        p_insert.join()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_search_round_decimal(self, round_decimal):
        """
        target: test search with valid round decimal
        method: search with valid round decimal
        expected: search successfully
        """
        import math
        tmp_nb = 500
        tmp_nq = 1
        tmp_limit = 5
        enable_dynamic_field = False
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, nb=tmp_nb,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 2. search
        log.info("test_search_round_decimal: Searching collection %s" % collection_w.name)
        res, _ = collection_w.search(vectors[:tmp_nq], default_search_field,
                                     default_search_params, tmp_limit)

        res_round, _ = collection_w.search(vectors[:tmp_nq], default_search_field,
                                           default_search_params, tmp_limit, round_decimal=round_decimal)

        abs_tol = pow(10, 1 - round_decimal)
        for i in range(tmp_limit):
            dis_expect = round(res[0][i].distance, round_decimal)
            dis_actual = res_round[0][i].distance
            # log.debug(f'actual: {dis_actual}, expect: {dis_expect}')
            # abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
            assert math.isclose(dis_actual, dis_expect, rel_tol=0, abs_tol=abs_tol)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large(self):
        """
        target: test search with large expression
        method: test search with large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        dim = 64
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      nb, dim=dim,
                                                                      is_index=False,
                                                                      enable_dynamic_field=enable_dynamic_field,
                                                                      with_json=False)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression: searching with expression: %s" % expression)

        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nums,
                                                         "ids": insert_ids,
                                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large_two(self):
        """
        target: test search with large expression
        method: test one of the collection ids to another collection search for it, with the large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        dim = 64
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      nb, dim=dim,
                                                                      is_index=False,
                                                                      enable_dynamic_field=enable_dynamic_field,
                                                                      with_json=False)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        vectors_id = [random.randint(0, nums)for _ in range(nums)]
        expression = f"{default_int64_field_name} in {vectors_id}"
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={
                                                "nq": nums,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                            })

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_bounded(self, nq, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "bounded"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async,
                                         })

        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_BOUNDED)
        kwargs.update({"consistency_level": consistency_level})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old,
                                                    enable_dynamic_field=enable_dynamic_field)
        insert_ids.extend(insert_ids_new)

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_strong(self, nq, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "Strong"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old,
                                                    enable_dynamic_field=enable_dynamic_field)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get("consistency_level", CONSISTENCY_STRONG)
        kwargs.update({"consistency_level": consistency_level})

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_eventually(self, nq, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "eventually"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 64
        auto_id = True
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old,
                                                    enable_dynamic_field=enable_dynamic_field)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_EVENTUALLY)
        kwargs.update({"consistency_level": consistency_level})
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_session(self, nq, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "session"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 64
        auto_id = False
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})

        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_SESSION)
        kwargs.update({"consistency_level": consistency_level})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old,
                                                    enable_dynamic_field=enable_dynamic_field)
        insert_ids.extend(insert_ids_new)
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_ignore_growing(self, nq, _async):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. search with param ignore_growing=True
        expected: searched successfully
        """
        # 1. create a collection
        dim = 64
        collection_w = self.init_collection_general(prefix, True, dim=dim)[0]

        # 2. insert data again
        data = cf.gen_default_dataframe_data(dim=dim, start=10000)
        collection_w.insert(data)

        # 3. search with param ignore_growing=True
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}, "ignore_growing": True}
        vector = [[random.random() for _ in range(dim)] for _ in range(nq)]
        res = collection_w.search(vector[:nq], default_search_field, search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        for ids in res[0].ids:
            assert ids < 10000

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_ignore_growing_two(self, nq, _async):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. search with param ignore_growing=True(outside search_params)
        expected: searched successfully
        """
        # 1. create a collection
        dim = 64
        collection_w = self.init_collection_general(prefix, True, dim=dim)[0]

        # 2. insert data again
        data = cf.gen_default_dataframe_data(dim=dim, start=10000)
        collection_w.insert(data)

        # 3. search with param ignore_growing=True
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        vector = [[random.random() for _ in range(dim)] for _ in range(nq)]
        res = collection_w.search(vector[:nq], default_search_field, search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  ignore_growing=True,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        for ids in res[0].ids:
            assert ids < 10000

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["_co11ection", "co11_ection"])
    @pytest.mark.parametrize("index_name", ["_1ndeX", "In_0"])
    def test_search_collection_naming_rules(self, name, index_name, _async):
        """
        target: test search collection naming rules
        method: 1. Connect milvus
                2. Create a field with a name which uses all the supported elements in the naming rules
                3. Create a collection with a name which uses all the supported elements in the naming rules
                4. Create an index with a name which uses all the supported elements in the naming rules
                5. Insert data (5000) into collection
                6. Search collection
        expected: searched successfully
        """
        field_name1 = "_1nt"
        field_name2 = "f10at_"
        collection_name = cf.gen_unique_str(name)
        self._connect()
        fields = [cf.gen_int64_field(), cf.gen_int64_field(field_name1),
                  cf.gen_float_vec_field(field_name2, dim=default_dim)]
        schema = cf.gen_collection_schema(
            fields=fields, primary_field=default_int64_field_name)
        collection_w = self.init_collection_wrap(name=collection_name, schema=schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={"name": collection_name, "schema": schema})
        collection_w.create_index(field_name1, index_name=index_name)
        int_values = pd.Series(data=[i for i in range(0, default_nb)])
        float_vec_values = cf.gen_vectors(default_nb, default_dim)
        dataframe = pd.DataFrame({default_int64_field_name: int_values,
                                  field_name1: int_values, field_name2: float_vec_values})
        collection_w.insert(dataframe)
        collection_w.create_index(
            field_name2, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.search(vectors[:default_nq], field_name2, default_search_params,
                            default_limit, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["_PartiTi0n", "pArt1_ti0n"])
    def test_search_partition_naming_rules_without_index(self, nq, partition_name):
        """
        target: test search collection naming rules
        method: 1. Connect milvus
                2. Create a collection
                3. Create a partition with a name which uses all the supported elements in the naming rules
                4. Insert data into collection
                5. without index with a name which uses all the supported elements in the naming rules
                6. Search partition (should successful)
        expected: searched successfully
        """
        nb = 5000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        self._connect()
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, False, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        collection_w.create_partition(partition_name)
        insert_ids = cf.insert_data(collection_w, nb, auto_id=auto_id, dim=dim,
                                    enable_dynamic_field=enable_dynamic_field)[3]
        collection_w.load()
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, [
                                partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["_PartiTi0n", "pArt1_ti0n"])
    @pytest.mark.parametrize("index_name", ["_1ndeX", "In_0"])
    def test_search_partition_naming_rules_with_index(self, nq, partition_name, index_name):
        """
        target: test search collection naming rules
        method: 1. Connect milvus
                2. Create a collection
                3. Create a partition with a name which uses all the supported elements in the naming rules
                4. Insert data into collection
                5. with index with a name which uses all the supported elements in the naming rules
                6. Search partition (should successful)
        expected: searched successfully
        """
        nb = 5000
        dim = 64
        auto_id = False
        enable_dynamic_field = True
        self._connect()
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, False, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim, is_index=False,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        collection_w.create_partition(partition_name)
        insert_ids = cf.insert_data(collection_w, nb, auto_id=auto_id, dim=dim,
                                    enable_dynamic_field=enable_dynamic_field)[3]
        collection_w.create_index(
            default_search_field, default_index_params, index_name=index_name)
        collection_w.load()
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, [
                                partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #22582")
    def test_search_during_upsert(self):
        """
        target: test search during upsert
        method: 1. create a collection and search
                2. search during upsert
                3. compare two search results
        expected: the two search results is the same
        """
        nq = 5
        upsert_nb = 1000
        collection_w = self.init_collection_general(prefix, True)[0]
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(nq)]
        res1 = collection_w.search(
            vectors[:nq], default_search_field, default_search_params, default_limit)[0]

        def do_upsert():
            data = cf.gen_default_data_for_upsert(upsert_nb)[0]
            collection_w.upsert(data=data)

        t = threading.Thread(target=do_upsert, args=())
        t.start()
        res2 = collection_w.search(
            vectors[:nq], default_search_field, default_search_params, default_limit)[0]
        t.join()
        assert [res1[i].ids for i in range(nq)] == [
            res2[i].ids for i in range(nq)]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("not support default_value now")
    def test_search_using_all_types_of_default_value(self, auto_id):
        """
        target: test create collection with default_value
        method: create a schema with all fields using default value and search
        expected: search results are as expected
        """
        fields = [
            cf.gen_int64_field(name='pk', is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_int8_field(default_value=numpy.int8(8)),
            cf.gen_int16_field(default_value=numpy.int16(16)),
            cf.gen_int32_field(default_value=numpy.int32(32)),
            cf.gen_int64_field(default_value=numpy.int64(64)),
            cf.gen_float_field(default_value=numpy.float32(3.14)),
            cf.gen_double_field(default_value=numpy.double(3.1415)),
            cf.gen_bool_field(default_value=False),
            cf.gen_string_field(default_value="abc")
        ]
        schema = cf.gen_collection_schema(fields, auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)
        data = [
            [i for i in range(ct.default_nb)],
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        if auto_id:
            del data[0]
        collection_w.insert(data)
        collection_w.create_index(field_name, default_index_params)
        collection_w.load()
        res = collection_w.search(vectors[:1], default_search_field, default_search_params,
                                  default_limit, default_search_exp,
                                  output_fields=["*"],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": 1,
                                               "limit": default_limit})[0]
        res = res[0][0].entity._row_data
        assert res[ct.default_int8_field_name] == 8
        assert res[ct.default_int16_field_name] == 16
        assert res[ct.default_int32_field_name] == 32
        assert res[ct.default_int64_field_name] == 64
        assert res[ct.default_float_field_name] == numpy.float32(3.14)
        assert res[ct.default_double_field_name] == 3.1415
        assert res[ct.default_bool_field_name] is False
        assert res[ct.default_string_field_name] == "abc"

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[1:4])
    def test_search_repeatedly_ivf_index_same_limit(self, index):
        """
        target: test create collection repeatedly
        method: search twice, check the results is the same
        expected: search results are as expected
        """
        nb = 5000
        limit = 30
        # 1. create a collection
        collection_w = self.init_collection_general(prefix, True, nb, is_index=False)[0]

        # 2. insert data again
        params = cf.get_index_params_params(index)
        index_params = {"metric_type": "COSINE", "index_type": index, "params": params}
        collection_w.create_index(default_search_field, index_params)

        # 3. search with param ignore_growing=True
        collection_w.load()
        search_params = cf.gen_search_param(index, "COSINE")[0]
        vector = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        res1 = collection_w.search(vector[:default_nq], default_search_field, search_params, limit)[0]
        res2 = collection_w.search(vector[:default_nq], default_search_field, search_params, limit)[0]
        for i in range(default_nq):
            assert res1[i].ids == res2[i].ids

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[1:4])
    def test_search_repeatedly_ivf_index_different_limit(self, index):
        """
        target: test create collection repeatedly
        method: search twice, check the results is the same
        expected: search results are as expected
        """
        nb = 5000
        limit = random.randint(10, 100)
        # 1. create a collection
        collection_w = self.init_collection_general(prefix, True, nb, is_index=False)[0]

        # 2. insert data again
        params = cf.get_index_params_params(index)
        index_params = {"metric_type": "COSINE", "index_type": index, "params": params}
        collection_w.create_index(default_search_field, index_params)

        # 3. search with param ignore_growing=True
        collection_w.load()
        search_params = cf.gen_search_param(index, "COSINE")[0]
        vector = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        res1 = collection_w.search(vector, default_search_field, search_params, limit)[0]
        res2 = collection_w.search(vector, default_search_field, search_params, limit * 2)[0]
        for i in range(default_nq):
            assert res1[i].ids == res2[i].ids[:limit]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metrics", ct.binary_metrics[:2])
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    @pytest.mark.parametrize("dim", [32768, 65536, ct.max_binary_vector_dim-8, ct.max_binary_vector_dim])
    def test_binary_indexed_large_dim_vectors_search(self, dim, metrics, index):
        """
        target: binary vector large dim search
        method: binary vector large dim search
        expected: search success
        """
        # 1. create a collection and insert data
        collection_w = self.init_collection_general(prefix, dim=dim, is_binary=True, is_index=False)[0]
        data = cf.gen_default_binary_dataframe_data(nb=200, dim=dim)[0]
        collection_w.insert(data)

        # 2. create index and load
        params = {"M": 48, "efConstruction": 500} if index == "HNSW" else {"nlist": 128}
        default_index = {"index_type": index, "metric_type": metrics, "params": params}
        collection_w.create_index(binary_field_name, default_index)
        collection_w.load()

        # 3. search with output field vector
        search_params = cf.gen_search_param(index, metrics)
        binary_vectors = cf.gen_binary_vectors(1, dim)[1]
        for search_param in search_params:
            res = collection_w.search(binary_vectors, binary_field_name,
                                      search_param, 2, default_search_exp,
                                      output_fields=[binary_field_name])[0]

            # 4. check the result vectors should be equal to the inserted
            assert res[0][0].entity.binary_vector == data[binary_field_name][res[0][0].id]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [ct.max_binary_vector_dim + 1, ct.max_binary_vector_dim + 8])
    def test_binary_indexed_over_max_dim(self, dim):
        """
        target: tests exceeding the maximum binary vector dimension
        method: tests exceeding the maximum binary vector dimension
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        binary_schema = cf.gen_default_binary_collection_schema(dim=dim)
        self.init_collection_wrap(c_name, schema=binary_schema,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 999, "err_msg": f"invalid dimension: {dim}."})


class TestSearchBase(TestcaseBase):
    @pytest.fixture(
        scope="function",
        params=[1, 10]
    )
    def get_top_k(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=[1, 10, 1100]
    )
    def get_nq(self, request):
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_flat_top_k(self, get_nq):
        """
        target: test basic search function, all the search params is correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = 16385  # max top k is 16384
        nq = get_nq
        collection_w, data, _, insert_ids = self.init_collection_general(prefix, insert_data=True, nb=nq)[0:4]
        collection_w.load()
        if top_k <= max_top_k:
            res, _ = collection_w.search(vectors[:nq], default_search_field, default_search_params, top_k)
            assert len(res[0]) <= top_k
        else:
            collection_w.search(vectors[:nq], default_search_field, default_search_params, top_k,
                                check_task=CheckTasks.err_res,
                                check_items={"err_code": 65535,
                                             "err_msg": f"topk [{top_k}] is invalid, top k should be in range"
                                                        f" [1, 16384], but got {top_k}"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_index_empty_partition(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: add vectors into collection, search with the given vectors, check the result
        expected: the length of the result is top_k, search collection with partition tag return empty
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create partition
        partition_name = "search_partition_empty"
        collection_w.create_partition(partition_name=partition_name, description="search partition empty")
        par = collection_w.partitions
        # collection_w.load()
        # 3. create different index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 4. search
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     default_search_params, top_k,
                                     default_search_exp)

        assert len(res[0]) <= top_k

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, top_k,
                            default_search_exp, [partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_index_partitions(self, index, get_top_k):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data in 2 partitions
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create different index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)

        # 3. load and search
        collection_w.load()
        par = collection_w.partitions
        collection_w.search(vectors[:nq], default_search_field,
                            ct.default_search_params, top_k,
                            default_search_exp, [par[0].name, par[1].name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "limit": top_k,
                                         "ids": insert_ids})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_ip_flat(self, get_top_k):
        """
        target: test basic search function, all the search params are correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create ip index
        default_index = {"index_type": "IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     search_params, top_k,
                                     default_search_exp)
        assert len(res[0]) <= top_k

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_ip_after_index(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim

        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create ip index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     search_params, top_k,
                                     default_search_exp)
        assert len(res[0]) <= top_k

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [2, 128, 768])
    @pytest.mark.parametrize("nb", [1, 2, 10, 100])
    def test_search_ip_brute_force(self, nb, dim):
        """
        target: https://github.com/milvus-io/milvus/issues/17378. Ensure the logic of IP distances won't be changed.
        method: search with the given vectors, check the result
        expected: The inner product of vector themselves should be positive.
        """
        top_k = 1

        # 1. initialize with data
        collection_w, insert_entities, _, insert_ids, _ = self.init_collection_general(prefix, True, nb,
                                                                                       is_binary=False,
                                                                                       is_index=False,
                                                                                       dim=dim)[0:5]
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "IP"}
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        insert_vectors = insert_entities[0][default_search_field].tolist()

        # 2. load collection.
        collection_w.load()

        # 3. search and then check if the distances are expected.
        res, _ = collection_w.search(insert_vectors[:nb], default_search_field,
                                     ct.default_search_ip_params, top_k,
                                     default_search_exp)
        for i, v in enumerate(insert_vectors):
            assert len(res[i]) == 1
            ref = ip(v, v)
            got = res[i][0].distance
            assert abs(got - ref) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_ip_index_empty_partition(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: add vectors into collection, search with the given vectors, check the result
        expected: the length of the result is top_k, search collection with partition tag return empty
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create partition
        partition_name = "search_partition_empty"
        collection_w.create_partition(partition_name=partition_name, description="search partition empty")
        par = collection_w.partitions
        # 3. create different index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 4. search
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     search_params, top_k,
                                     default_search_exp)

        assert len(res[0]) <= top_k

        collection_w.search(vectors[:nq], default_search_field,
                            search_params, top_k,
                            default_search_exp, [partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_ip_index_partitions(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create partition
        par_name = collection_w.partitions[0].name
        # 3. create different index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 4. search
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     search_params, top_k,
                                     default_search_exp, [par_name])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_cosine_all_indexes(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True,
                                                                                  is_index=False)[0:5]
        # 2. create index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 3. search
        search_params = {"metric_type": "COSINE"}
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     search_params, default_limit, default_search_exp,
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": default_nq,
                                                  "ids": insert_ids,
                                                  "limit": default_limit})

        # 4. check cosine distance
        for i in range(default_nq):
            for distance in res[i].distances:
                assert 1 >= distance >= -1

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_cosine_results_same_as_l2(self):
        """
        target: test search results of l2 and cosine keep the same
        method: 1. search L2
                2. search cosine
                3. compare the results
        expected: raise no exception
        """
        nb = ct.default_nb
        # 1. prepare original data and normalized data
        original_vec = [[random.random() for _ in range(ct.default_dim)] for _ in range(nb)]
        normalize_vec = preprocessing.normalize(original_vec, axis=1, norm='l2')
        normalize_vec = normalize_vec.tolist()
        data = cf.gen_default_dataframe_data()

        # 2. create L2 collection and insert normalized data
        collection_w1 = self.init_collection_general(prefix, is_index=False)[0]
        data[ct.default_float_vec_field_name] = normalize_vec
        collection_w1.insert(data)

        # 2. create index L2
        default_index = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "L2"}
        collection_w1.create_index("float_vector", default_index)
        collection_w1.load()

        # 3. search L2
        search_params = {"params": {"nprobe": 10}, "metric_type": "L2"}
        res_l2, _ = collection_w1.search(vectors[:default_nq], default_search_field,
                                         search_params, default_limit, default_search_exp)

        # 4. create cosine collection and insert original data
        collection_w2 = self.init_collection_general(prefix, is_index=False)[0]
        data[ct.default_float_vec_field_name] = original_vec
        collection_w2.insert(data)

        # 5. create index cosine
        default_index = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "COSINE"}
        collection_w2.create_index("float_vector", default_index)
        collection_w2.load()

        # 6. search cosine
        search_params = {"params": {"nprobe": 10}, "metric_type": "COSINE"}
        res_cosine, _ = collection_w2.search(vectors[:default_nq], default_search_field,
                                             search_params, default_limit, default_search_exp)

        # 7. check the search results
        for i in range(default_nq):
            assert res_l2[i].ids == res_cosine[i].ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_cosine_results_same_as_ip(self):
        """
        target: test search results of ip and cosine keep the same
        method: 1. search IP
                2. search cosine
                3. compare the results
        expected: raise no exception
        """
        # 1. create collection and insert data
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]

        # 2. search IP
        default_index = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        search_params = {"params": {"nprobe": 10}, "metric_type": "IP"}
        res_ip, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                        search_params, default_limit, default_search_exp)

        # 3. search cosine
        collection_w.release()
        collection_w.drop_index()
        default_index = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        search_params = {"params": {"nprobe": 10}, "metric_type": "COSINE"}
        res_cosine, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            search_params, default_limit, default_search_exp)

        # 4. check the search results
        for i in range(default_nq):
            assert res_ip[i].ids == res_cosine[i].ids
            log.info(res_cosine[i].distances)
            log.info(res_ip[i].distances)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_without_connect(self):
        """
        target: test search vectors without connection
        method: use disconnected instance, call search method and check if search successfully
        expected: raise exception
        """
        self._connect()

        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True,
                                                                                  ct.default_nq)[0:5]
        vectors = [[random.random() for _ in range(ct.default_dim)]
                   for _ in range(nq)]

        collection_w.load()
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list

        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     ct.default_search_params, ct.default_top_k,
                                     default_search_exp,
                                     check_task=CheckTasks.err_res,
                                     check_items={"err_code": 1,
                                                  "err_msg": "'should create connect first.'"})

    @pytest.mark.tags(CaseLabel.L2)
    # @pytest.mark.timeout(300)
    def test_search_concurrent_multithreads_single_connection(self, _async):
        """
        target: test concurrent search with multi processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        threads_num = 10
        threads = []
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(
            prefix, True, ct.default_nb)[0:5]

        def search(collection_w):
            vectors = [[random.random() for _ in range(ct.default_dim)]
                       for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info(
            "test_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_collections(self):
        """
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        """
        num = 10
        top_k = 10
        nq = 20

        for i in range(num):
            collection = gen_unique_str(uid + str(i))
            collection_w, _, _, insert_ids, time_stamp = \
                self.init_collection_general(
                    collection, True, ct.default_nb)[0:5]
            assert len(insert_ids) == default_nb
            vectors = [[random.random() for _ in range(ct.default_dim)]
                       for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, top_k,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": top_k})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:6])
    def test_each_index_with_mmap_enabled_search(self, index):
        """
        target: test each index with mmap enabled search
        method: test each index with mmap enabled search
        expected: search success
        """
        self._connect()
        nb = 2000
        dim = 32
        collection_w = self.init_collection_general(prefix, True, nb, dim=dim, is_index=False)[0]
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index(field_name, default_index, index_name="mmap_index")
        # mmap index
        collection_w.alter_index("mmap_index", {'mmap.enabled': True})
        # search
        collection_w.load()
        search_params = cf.gen_search_param(index)[0]
        vector = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        collection_w.search(vector, default_search_field, search_params, ct.default_limit,
                            output_fields=["*"],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": ct.default_limit})
        # enable mmap
        collection_w.release()
        collection_w.alter_index("mmap_index", {'mmap.enabled': False})
        collection_w.load()
        collection_w.search(vector, default_search_field, search_params, ct.default_limit,
                            output_fields=["*"],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[7:9])
    def test_enable_mmap_search_for_binary_indexes(self, index):
        """
        target: enable mmap for binary indexes
        method: enable mmap for binary indexes
        expected: search success
        """
        self._connect()
        dim = 64
        nb = 2000
        collection_w = self.init_collection_general(prefix, True, nb, dim=dim, is_index=False, is_binary=True)[0]
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index,
                         "params": params, "metric_type": "JACCARD"}
        collection_w.create_index(ct.default_binary_vec_field_name, default_index, index_name="binary_idx_name")
        collection_w.alter_index("binary_idx_name", {'mmap.enabled': True})
        collection_w.set_properties({'mmap.enabled': True})
        collection_w.load()
        pro = collection_w.describe()[0].get("properties")
        assert pro["mmap.enabled"] == 'True'
        assert collection_w.index()[0].params["mmap.enabled"] == 'True'
        # search
        binary_vectors = cf.gen_binary_vectors(default_nq, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = ["*"]
        collection_w.search(binary_vectors, ct.default_binary_vec_field_name, search_params,
                            default_limit, default_search_string_exp, output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})


class TestSearchDSL(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_vector_only(self):
        """
        target: test search normal scenario
        method: search vector only
        expected: search status ok, the length of result
        """
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, ct.default_nb)[0:5]
        vectors = [[random.random() for _ in range(ct.default_dim)]
                   for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, ct.default_top_k,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": ct.default_top_k})
class TestSearchArray(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("array_element_data_type", [DataType.INT64])
    def test_search_array_with_inverted_index(self, array_element_data_type):
        # create collection
        additional_params = {"max_length": 1000} if array_element_data_type == DataType.VARCHAR else {}
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="contains", dtype=DataType.ARRAY, element_type=array_element_data_type, max_capacity=2000, **additional_params),
            FieldSchema(name="contains_any", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="contains_all", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="equals", dtype=DataType.ARRAY, element_type=array_element_data_type, max_capacity=2000, **additional_params),
            FieldSchema(name="array_length_field", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="array_access", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        # insert data
        train_data, query_expr = cf.prepare_array_test_data(3000, hit_rate=0.05)
        collection_w.insert(train_data)
        index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 48, "efConstruction": 500}}
        collection_w.create_index("emb", index_params=index_params)
        for f in ["contains", "contains_any", "contains_all", "equals", "array_length_field", "array_access"]:
            collection_w.create_index(f, {"index_type": "INVERTED"})
        collection_w.load()

        for item in query_expr:
            expr = item["expr"]
            ground_truth_candidate = item["ground_truth"]
            res, _ = collection_w.search(
                data = [np.array([random.random() for j in range(128)], dtype=np.dtype("float32"))],
                anns_field="emb",
                param={"metric_type": "L2", "params": {"M": 32, "efConstruction": 360}},
                limit=10,
                expr=expr,
                output_fields=["*"],
            )
            assert len(res) == 1
            for i in range(len(res)):
                assert len(res[i]) == 10
                for hit in res[i]:
                    assert hit.id in ground_truth_candidate


class TestSearchString(TestcaseBase):

    """
    ******************************************************************
      The following cases are used to test search about string
    ******************************************************************
    """

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

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_search_string_field_not_primary: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_string_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_true(self, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 64
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, dim=dim, primary_field=ct.default_string_field_name,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_search_string_field_is_primary_true: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_string_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_true_multi_vector_fields(self, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 64
        enable_dynamic_field = False
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, dim=dim, primary_field=ct.default_string_field_name,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array)[0:4]
        # 2. search
        log.info("test_search_string_field_is_primary_true: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        vector_list = cf.extract_vector_field_name_list(collection_w)
        for search_field in vector_list:
            collection_w.search(vectors[:default_nq], search_field,
                                default_search_params, default_limit,
                                default_search_string_exp,
                                output_fields=output_fields,
                                _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_string_field_is_primary_true(self, _async):
        """
        target: test range search with string expr and string field is primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 64
        enable_dynamic_field = True
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, dim=dim, primary_field=ct.default_string_field_name,
                                         enable_dynamic_field=enable_dynamic_field, is_index=False,
                                         multiple_dim_array=multiple_dim_array)[0:4]
        vector_list = cf.extract_vector_field_name_list(collection_w)
        collection_w.create_index(field_name, {"metric_type": "L2"})
        for vector_field_name in vector_list:
            collection_w.create_index(vector_field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search
        log.info("test_search_string_field_is_primary_true: searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "L2",
                               "params": {"radius": 1000, "range_filter": 0}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        for search_field in vector_list:
            collection_w.search(vectors[:default_nq], search_field,
                                range_search_params, default_limit,
                                default_search_string_exp,
                                output_fields=output_fields,
                                _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_mix_expr(self, _async):
        """
        target: test search with mix string and int expr
        method: create collection and insert data
                create index and collection load
                collection search uses mix expr
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_search_string_mix_expr: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_mix_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_with_invalid_expr(self):
        """
        target: test search data
        method: create collection and insert data
                create index and collection load
                collection search uses invalid string expr
        expected: Raise exception
        """
        # 1. initialize with data
        auto_id = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim)[0:4]
        # 2. search
        log.info("test_search_string_with_invalid_expr: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_invaild_string_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1100,
                                         "err_msg": "failed to create query plan: cannot "
                                                    "parse expression: varchar >= 0"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_normal_string_expressions([ct.default_string_field_name]))
    def test_search_with_different_string_expr(self, expression, _async):
        """
        target: test search with different string expressions
        method: test search with different string expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        dim = 64
        nb = 1000
        enable_dynamic_field = True
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True,
                                                                             nb, dim=dim,
                                                                             is_index=False,
                                                                             enable_dynamic_field=enable_dynamic_field)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        filter_ids = []
        expression = expression.replace("&&", "and").replace("||", "or")
        for i, _id in enumerate(insert_ids):
            if enable_dynamic_field:
                int64 = _vectors[i][ct.default_int64_field_name]
                varchar = _vectors[i][ct.default_string_field_name]
            else:
                int64 = _vectors.int64[i]
                varchar = _vectors.varchar[i]
            if not expression or eval(expression):
                filter_ids.append(_id)

        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_binary(self, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        dim = 64
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      dim=dim,
                                                                                      is_index=False,
                                                                                      primary_field=ct.default_string_field_name)[0:4]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. search with exception
        binary_vectors = cf.gen_binary_vectors(3000, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = [default_string_field_name]
        collection_w.search(binary_vectors[:default_nq], "binary_vector", search_params,
                            default_limit, default_search_string_exp, output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 2,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_binary(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create an binary collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with binary data
        dim = 128
        auto_id = True
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=False)[0:4]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 2. search with exception
        binary_vectors = cf.gen_binary_vectors(3000, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector", search_params,
                            default_limit, default_search_string_exp,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 2,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_mix_expr_with_binary(self, _async):
        """
        target: test search with mix string and int expr
        method: create an binary collection and insert data
                create index and collection load
                collection search uses mix expr
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 128
        auto_id = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=dim, is_binary=True, is_index=False)[0:4]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 2. search
        log.info("test_search_mix_expr_with_binary: searching collection %s" %
                 collection_w.name)
        binary_vectors = cf.gen_binary_vectors(3000, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            default_search_mix_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary_prefix(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=default_dim, is_index=False)[0:4]
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param, index_name="a")
        index_param_two = {}
        collection_w.create_index("varchar", index_param_two, index_name="b")
        collection_w.load()
        # 2. search
        log.info("test_search_string_field_not_primary: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            # search all buckets
                            {"metric_type": "L2", "params": {
                                "nprobe": 100}}, default_limit,
                            perfix_expr,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 1,
                                         "_async": _async}
                            )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_index(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        auto_id = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=default_dim, is_index=False)[0:4]
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param, index_name="a")
        index_param = {"index_type": "Trie", "params": {}}
        collection_w.create_index("varchar", index_param, index_name="b")
        collection_w.load()
        # 2. search
        log.info("test_search_string_field_not_primary: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            # search all buckets
                            {"metric_type": "L2", "params": {
                                "nprobe": 100}}, default_limit,
                            perfix_expr,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 1,
                                         "_async": _async}
                            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_all_index_with_compare_expr(self, _async):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is string field
                2.create string and float index ,delete entities, query
                3.search
        expected: assert index and deleted id not in search result
        """
        # create collection, insert tmp_nb, flush and load
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, insert_data=True,
                                                                            primary_field=ct.default_string_field_name,
                                                                            is_index=False)[0:4]

        # create index
        index_params_one = {"index_type": "IVF_SQ8",
                            "metric_type": "COSINE", "params": {"nlist": 64}}
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params_one, index_name=index_name1)
        index_params_two = {}
        collection_w.create_index(
            ct.default_string_field_name, index_params=index_params_two, index_name=index_name2)
        assert collection_w.has_index(index_name=index_name2)

        collection_w.release()
        collection_w.load()
        # delete entity
        expr = 'float >= int64'
        # search with id 0 vectors
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name,  default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            expr,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_insert_empty(self, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection ,string field is primary
                collection load and insert data
                collection search uses string expr in string field
        expected: Search successfully
        """
        # 1. initialize with data
        collection_w, _, _, _ = \
            self.init_collection_general(
                prefix, False, primary_field=ct.default_string_field_name)[0:4]

        nb = 3000
        data = cf.gen_default_list_data(nb)
        data[2] = [""for _ in range(nb)]
        collection_w.insert(data=data)

        collection_w.load()

        search_string_exp = "varchar >= \"\""
        limit = 1

        # 2. search
        log.info("test_search_string_field_is_primary_true: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit,
                            search_string_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary_is_empty(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        collection_w, _, _, _ = \
            self.init_collection_general(
                prefix, False, primary_field=ct.default_int64_field_name, is_index=False)[0:4]

        nb = 3000
        data = cf.gen_default_list_data(nb)
        insert_ids = data[0]
        data[2] = [""for _ in range(nb)]

        collection_w.insert(data)
        assert collection_w.num_entities == nb

        # 2. create index
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        search_string_exp = "varchar >= \"\""

        # 3. search
        log.info("test_search_string_field_not_primary: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            search_string_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})


class TestSearchPagination(TestcaseBase):
    """ Test case of search pagination """

    @pytest.fixture(scope="function", params=[0, 10, 100])
    def offset(self, request):
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

    @pytest.fixture(scope="function", params=["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def vector_data_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("limit", [10, 20])
    def test_search_with_pagination(self, offset, limit, _async):
        """
        target: test search with pagination
        method: 1. connect and create a collection
                2. search pagination with offset
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        # 1. create a collection
        auto_id = True
        enable_dynamic_field = False
        collection_w = self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 2. search pagination with offset
        search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}, "offset": offset}
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         search_param, limit,
                                         default_search_exp, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "limit": limit,
                                                      "_async": _async})[0]
        # 3. search with offset+limit
        res = collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                                  limit+offset, default_search_exp, _async=_async)[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
            res.done()
            res = res.result()
        res_distance = res[0].distances[offset:]
        # assert sorted(search_res[0].distances, key=numpy.float32) == sorted(res_distance, key=numpy.float32)
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_string_with_pagination(self, offset, _async):
        """
        target: test search string with pagination
        method: 1. connect and create a collection
                2. search pagination with offset
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        # 1. create a collection
        auto_id = True
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}, "offset": offset}
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         search_param, default_limit,
                                         default_search_string_exp,
                                         output_fields=output_fields,
                                         _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": insert_ids,
                                                      "limit": default_limit,
                                                      "_async": _async})[0]
        # 3. search with offset+limit
        res = collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                                  default_limit + offset, default_search_string_exp, _async=_async)[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
            res.done()
            res = res.result()
        res_distance = res[0].distances[offset:]
        # assert sorted(search_res[0].distances, key=numpy.float32) == sorted(res_distance, key=numpy.float32)
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_binary_with_pagination(self, offset):
        """
        target: test search binary with pagination
        method: 1. connect and create a collection
                2. search pagination with offset
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        # 1. create a collection
        auto_id = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(
                prefix, True, is_binary=True, auto_id=auto_id, dim=default_dim)[0:4]
        # 2. search
        search_param = {"metric_type": "JACCARD",
                        "params": {"nprobe": 10}, "offset": offset}
        binary_vectors = cf.gen_binary_vectors(default_nq, default_dim)[1]
        search_res = collection_w.search(binary_vectors[:default_nq], "binary_vector",
                                         search_param, default_limit,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": insert_ids,
                                                      "limit": default_limit})[0]
        # 3. search with offset+limit
        search_binary_param = {
            "metric_type": "JACCARD", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:default_nq], "binary_vector", search_binary_param,
                                  default_limit + offset)[0]

        assert len(search_res[0].ids) == len(res[0].ids[offset:])
        assert sorted(search_res[0].distances, key=numpy.float32) == sorted(
            res[0].distances[offset:], key=numpy.float32)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_all_vector_type_with_pagination(self, vector_data_type):
        """
        target: test search with pagination using different vector datatype
        method: 1. connect and create a collection
                2. search pagination with offset
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        # 1. create a collection
        auto_id = False
        enable_dynamic_field = True
        offset = 100
        limit = 20
        collection_w = self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim,
                                                    enable_dynamic_field=enable_dynamic_field,
                                                    vector_data_type=vector_data_type)[0]
        # 2. search pagination with offset
        search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}, "offset": offset}
        vectors = cf.gen_vectors_based_on_vector_type(default_nq, default_dim, vector_data_type)
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         search_param, limit,
                                         default_search_exp,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "limit": limit})[0]
        # 3. search with offset+limit
        res = collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                                  limit + offset, default_search_exp)[0]
        res_distance = res[0].distances[offset:]
        # assert sorted(search_res[0].distances, key=numpy.float32) == sorted(res_distance, key=numpy.float32)
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [100, 3000, 10000])
    def test_search_with_pagination_topK(self, limit, _async):
        """
        target: test search with pagination limit + offset = topK
        method: 1. connect and create a collection
                2. search pagination with offset
                3. search with topK
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        # 1. create a collection
        topK = 16384
        auto_id = True
        offset = topK - limit
        collection_w = self.init_collection_general(
            prefix, True, nb=20000, auto_id=auto_id, dim=default_dim)[0]
        # 2. search
        search_param = {"metric_type": "COSINE",
                        "params": {"nprobe": 10}, "offset": offset}
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         search_param, limit,
                                         default_search_exp, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "limit": limit,
                                                      "_async": _async})[0]
        # 3. search with topK
        res = collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                                  topK, default_search_exp, _async=_async)[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
            res.done()
            res = res.result()
        res_distance = res[0].distances[offset:]
        # assert sorted(search_res[0].distances, key=numpy.float32) == sorted(res_distance, key=numpy.float32)
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_normal_expressions())
    def test_search_pagination_with_expression(self, offset, expression, _async):
        """
        target: test search pagination with expression
        method: create connection, collection, insert and search with expression
        expected: search successfully
        """
        # 1. create a collection
        nb = 2500
        dim = 38
        enable_dynamic_field = False
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True, nb=nb,
                                                                             dim=dim,
                                                                             enable_dynamic_field=enable_dynamic_field)[0:4]
        # filter result with expression in collection
        _vectors = _vectors[0]
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i, _id in enumerate(insert_ids):
            if enable_dynamic_field:
                int64 = _vectors[i][ct.default_int64_field_name]
                float = _vectors[i][ct.default_float_field_name]
            else:
                int64 = _vectors.int64[i]
                float = _vectors.float[i]
            if not expression or eval(expression):
                filter_ids.append(_id)
        # 2. search
        collection_w.load()
        limit = min(default_limit, len(filter_ids))
        if offset >= len(filter_ids):
            limit = 0
        elif len(filter_ids) - offset < default_limit:
            limit = len(filter_ids) - offset
        search_param = {"metric_type": "COSINE",
                        "params": {"nprobe": 10}, "offset": offset}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            search_param, default_limit, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": limit,
                                                         "_async": _async})
        # 3. search with offset+limit
        res = collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                                  default_limit + offset, expression, _async=_async)[0]
        if _async:
            res.done()
            res = res.result()
            search_res.done()
            search_res = search_res.result()
        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)
        res_distance = res[0].distances[offset:]
        # assert sorted(search_res[0].distances, key=numpy.float32) == sorted(res_distance, key=numpy.float32)
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_pagination_with_index_partition(self, offset, _async):
        """
        target: test search pagination with index and partition
        method: create connection, collection, insert data, create index and search
        expected: searched successfully
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      partition_num=1,
                                                                      auto_id=auto_id,
                                                                      is_index=False)[0:4]
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search through partitions
        par = collection_w.partitions
        limit = 100
        search_params = {"metric_type": "L2",
                         "params": {"nprobe": 10}, "offset": offset}
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         search_params, limit, default_search_exp,
                                         [par[0].name, par[1].name], _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": insert_ids,
                                                      "limit": limit,
                                                      "_async": _async})[0]
        # 3. search through partitions with offset+limit
        search_params = {"metric_type": "L2"}
        res = collection_w.search(vectors[:default_nq], default_search_field, search_params,
                                  limit + offset, default_search_exp,
                                  [par[0].name, par[1].name], _async=_async)[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
            res.done()
            res = res.result()
        res_distance = res[0].distances[offset:]
        # assert cf.sort_search_distance(search_res[0].distances) == cf.sort_search_distance(res_distance)
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("Same with the previous, collection must have index now")
    def test_search_pagination_with_partition(self, offset, _async):
        """
        target: test search pagination with partition
        method: create connection, collection, insert data and search
        expected: searched successfully
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      partition_num=1,
                                                                      auto_id=auto_id)[0:4]
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        collection_w.load()
        # 2. search through partitions
        par = collection_w.partitions
        limit = 1000
        search_param = {"metric_type": "L2",
                        "params": {"nprobe": 10}, "offset": offset}
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         search_param, limit, default_search_exp,
                                         [par[0].name, par[1].name], _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": insert_ids,
                                                      "limit": limit,
                                                      "_async": _async})[0]
        # 3. search through partitions with offset+limit
        res = collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                                  limit + offset, default_search_exp,
                                  [par[0].name, par[1].name], _async=_async)[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
            res.done()
            res = res.result()
        assert res[0].distances == sorted(res[0].distances)
        assert search_res[0].distances == sorted(search_res[0].distances)
        assert search_res[0].distances == res[0].distances[offset:]
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_pagination_with_inserted_data(self, offset, _async):
        """
        target: test search pagination with inserted data
        method: create connection, collection, insert data and search
                check the results by searching with limit+offset
        expected: searched successfully
        """
        # 1. create collection
        collection_w = self.init_collection_general(
            prefix, False, dim=default_dim)[0]
        # 2. insert data
        data = cf.gen_default_dataframe_data(dim=default_dim)
        collection_w.insert(data)
        collection_w.load()
        # 3. search
        search_params = {"offset": offset}
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         search_params, default_limit,
                                         default_search_exp, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "limit": default_limit,
                                                      "_async": _async})[0]
        # 4. search through partitions with offset+limit
        search_params = {}
        res = collection_w.search(vectors[:default_nq], default_search_field, search_params,
                                  default_limit + offset, default_search_exp, _async=_async)[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
            res.done()
            res = res.result()
        res_distance = res[0].distances[offset:]
        assert sorted(search_res[0].distances) == sorted(res_distance)
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_pagination_empty(self, offset, _async):
        """
        target: test search pagination empty
        method: connect, create collection, insert data and search
        expected: search successfully
        """
        # 1. initialize without data
        auto_id = False
        collection_w = self.init_collection_general(
            prefix, True, auto_id=auto_id, dim=default_dim)[0]
        # 2. search collection without data
        search_param = {"metric_type": "COSINE",
                        "params": {"nprobe": 10}, "offset": offset}
        search_res = collection_w.search([], default_search_field, search_param,
                                         default_limit, default_search_exp, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": 0,
                                                      "_async": _async})[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
        assert len(search_res) == 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [3000, 5000])
    def test_search_pagination_with_offset_over_num_entities(self, offset):
        """
        target: test search pagination with offset over num_entities
        method: create connection, collection, insert 3000 entities and search with offset over 3000
        expected: return an empty list
        """
        # 1. initialize
        collection_w = self.init_collection_general(
            prefix, True, dim=default_dim)[0]
        # 2. search
        search_param = {"metric_type": "COSINE",
                        "params": {"nprobe": 10}, "offset": offset}
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  search_param, default_limit,
                                  default_search_exp,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "limit": 0})[0]
        assert res[0].ids == []

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_pagination_after_different_index(self, index, offset, _async):
        """
        target: test search pagination after different index
        method: test search pagination after different index and corresponding search params
        expected: search successfully
        """
        # 1. initialize with data
        dim = 128
        auto_id = True
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 1000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=False)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            res = collection_w.search(vectors[:default_nq], default_search_field, search_param,
                                      default_limit + offset, default_search_exp, _async=_async)[0]
            search_param["offset"] = offset
            log.info("Searching with search params: {}".format(search_param))
            search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                             search_param, default_limit,
                                             default_search_exp, _async=_async,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": default_nq,
                                                          "ids": insert_ids,
                                                          "limit": default_limit,
                                                          "_async": _async})[0]
            if _async:
                search_res.done()
                search_res = search_res.result()
                res.done()
                res = res.result()
            res_distance = res[0].distances[offset:]
            # assert sorted(search_res[0].distances, key=numpy.float32) == sorted(res_distance, key=numpy.float32)
            assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [100, default_nb // 2])
    def test_search_offset_different_position(self, offset):
        """
        target: test search pagination with offset in different position
        method: create connection, collection, insert entities and search with offset
        expected: search successfully
        """
        # 1. initialize
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search with offset in params
        search_params = {"metric_type": "COSINE",
                         "params": {"nprobe": 10}, "offset": offset}
        res1 = collection_w.search(vectors[:default_nq], default_search_field,
                                   search_params, default_limit)[0]

        # 3. search with offset outside params
        res2 = collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                                   default_limit, offset=offset)[0]
        assert res1[0].ids == res2[0].ids

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [1, 5, 20])
    def test_search_sparse_with_pagination(self, offset):
        """
        target: test search sparse with pagination
        method: 1. connect and create a collection
                2. search pagination with offset
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        # 1. create a collection
        auto_id = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(
                prefix, True,  auto_id=auto_id, vector_data_type=ct.sparse_vector)[0:4]
        # 2. search with offset+limit
        search_param = {"metric_type": "IP", "params": {"drop_ratio_search": "0.2"}, "offset": offset}
        search_vectors = cf.gen_default_list_sparse_data()[-1][-2:]
        search_res = collection_w.search(search_vectors, ct.default_sparse_vec_field_name,
                                         search_param, default_limit)[0]
        # 3. search
        _search_param = {"metric_type": "IP", "params": {"drop_ratio_search": "0.2"}}
        res = collection_w.search(search_vectors[:default_nq], ct.default_sparse_vec_field_name, _search_param,
                                  default_limit + offset)[0]
        assert len(search_res[0].ids) == len(res[0].ids[offset:])
        assert sorted(search_res[0].distances, key=numpy.float32) == sorted(
            res[0].distances[offset:], key=numpy.float32)


class TestSearchPaginationInvalid(TestcaseBase):
    """ Test case of search pagination """

    @pytest.fixture(scope="function", params=[0, 10])
    def offset(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are invalid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("offset", [" ", [1, 2], {1}, "12 s"])
    def test_search_pagination_with_invalid_offset_type(self, offset):
        """
        target: test search pagination with invalid offset type
        method: create connection, collection, insert and search with invalid offset type
        expected: raise exception
        """
        # 1. initialize
        collection_w = self.init_collection_general(
            prefix, True, dim=default_dim)[0]
        # 2. search
        search_param = {"metric_type": "COSINE",
                        "params": {"nprobe": 10}, "offset": offset}
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "offset [%s] is invalid" % offset})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("offset", [-1, 16385])
    def test_search_pagination_with_invalid_offset_value(self, offset):
        """
        target: test search pagination with invalid offset value
        method: create connection, collection, insert and search with invalid offset value
        expected: raise exception
        """
        # 1. initialize
        collection_w = self.init_collection_general(prefix, True, dim=default_dim)[0]
        # 2. search
        search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}, "offset": offset}
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "offset [%d] is invalid, should be in range "
                                                    "[1, 16384], but got %d" % (offset, offset)})


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
                                                                      nb=nb, dim=dim,
                                                                      is_index=False,
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
                         default_float_field_name,  default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
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
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [1])
    @pytest.mark.parametrize("search_list", [-1, 0])
    def test_search_invalid_params_with_diskann_A(self, search_list, limit):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is int field
                2.create diskann index
                3.search with invalid params, where  topk <=20, search list [topk, 2147483647]
        expected: search report an error
        """
        # 1. initialize with data
        dim = 90
        auto_id = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_index=False)[0:4]
        # 2. create index
        default_index = {"index_type": "DISKANN", "metric_type": "L2", "params": {}}
        collection_w.create_index(ct.default_float_vec_field_name, default_index)
        collection_w.load()
        default_search_params = {"metric_type": "L2", "params": {"search_list": search_list}}
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name,  default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "param search_list_size out of range [ 1,2147483647 ]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [20])
    @pytest.mark.parametrize("search_list", [19])
    def test_search_invalid_params_with_diskann_B(self, search_list, limit):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is int field
                2.create  diskann index
                3.search with invalid params, [k, 200] when k <= 20
        expected: search report an error
        """
        # 1. initialize with data
        dim = 100
        auto_id = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_index=False)[0:4]
        # 2. create index
        default_index = {"index_type": "DISKANN", "metric_type": "L2", "params": {}}
        collection_w.create_index(ct.default_float_vec_field_name, default_index)
        collection_w.load()
        default_search_params = {"metric_type": "L2", "params": {"search_list": search_list}}
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_int64_field_name, default_float_field_name,  default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "UnknownError"})

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
                         default_float_field_name,  default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit}
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
                         default_float_field_name,  default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": ids,
                                         "limit": default_limit,
                                         "_async": _async}
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
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. create index
        default_index = {"index_type": "DISKANN",
                         "metric_type": "COSINE", "params": {}}
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index, index_name=index_name1)
        if not enable_dynamic_field:
            index_params_one = {}
            collection_w.create_index(
                "float", index_params_one, index_name="a")
            index_param_two = {}
            collection_w.create_index(
                "varchar", index_param_two, index_name="b")

        collection_w.load()
        tmp_expr = f'{ct.default_int64_field_name} in {[0]}'

        expr = f'{ct.default_int64_field_name} in {ids[:half_nb]}'

        # delete half of data
        del_res = collection_w.delete(expr)[0]
        assert del_res.delete_count == half_nb

        collection_w.delete(tmp_expr)
        default_search_params = {
            "metric_type": "COSINE", "params": {"search_list": 30}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name,  default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": ids,
                                         "limit": default_limit,
                                         "_async": _async}
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
        vectors = [[random.random() for _ in range(dim)]for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name,  default_string_field_name]
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         default_search_params, limit, default_expr,
                                         output_fields=output_fields, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": ids,
                                                      "limit": limit,
                                                      "_async": _async})

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
        enable_dynamic_field= False
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
                                         "_async": _async}
                            )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue #23672")
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
                                         "_async": _async})


class TestCollectionRangeSearch(TestcaseBase):
    """ Test case of range search interface """

    @pytest.fixture(scope="function", params=ct.all_index_types[:7])
    def index_type(self, request):
        tags = request.config.getoption("--tags")
        if CaseLabel.L2 not in tags:
            if request.param not in ct.L0_index_types:
                pytest.skip(f"skip index type {request.param}")
        yield request.param

    @pytest.fixture(scope="function", params=ct.float_metrics)
    def metric(self, request):
        tags = request.config.getoption("--tags")
        if CaseLabel.L2 not in tags:
            if request.param != ct.default_L0_metric:
                pytest.skip(f"skip index type {request.param}")
        yield request.param

    @pytest.fixture(scope="function", params=[default_nb, default_nb_medium])
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

    @pytest.fixture(scope="function", params=["JACCARD", "HAMMING", "TANIMOTO"])
    def metrics(self, request):
        if request.param == "TANIMOTO":
            pytest.skip("TANIMOTO not supported now")
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def is_flush(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    """
    ******************************************************************
    #  The followings are valid range search cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    @pytest.mark.parametrize("with_growing", [False, True])
    def test_range_search_default(self, index_type, metric, vector_data_type, with_growing):
        """
        target: verify the range search returns correct results
        method: 1. create collection, insert 10k vectors,
                2. search with topk=1000
                3. range search from the 30th-330th distance as filter
                4. verified the range search results is same as the search results in the range
        """
        collection_w = self.init_collection_general(prefix, auto_id=True, insert_data=False, is_index=False,
                                                    vector_data_type=vector_data_type, with_json=False)[0]
        nb = 1000
        rounds = 10
        for i in range(rounds):
            data = cf.gen_general_default_list_data(nb=nb, auto_id=True, vector_data_type=vector_data_type,
                                                    with_json=False, start=i*nb)
            collection_w.insert(data)

        collection_w.flush()
        _index_params = {"index_type": "FLAT", "metric_type": metric, "params": {}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index_params)
        collection_w.load()

        if with_growing is True:
            # add some growing segments
            for j in range(rounds//2):
                data = cf.gen_general_default_list_data(nb=nb, auto_id=True, vector_data_type=vector_data_type,
                                                        with_json=False, start=(rounds+j)*nb)
                collection_w.insert(data)

        search_params = {"params": {}}
        nq = 1
        search_vectors = cf.gen_vectors(nq, ct.default_dim, vector_data_type=vector_data_type)
        search_res = collection_w.search(search_vectors, default_search_field,
                                         search_params, limit=1000)[0]
        assert len(search_res[0].ids) == 1000
        log.debug(f"search topk=1000 returns {len(search_res[0].ids)}")
        check_topk = 300
        check_from = 30
        ids = search_res[0].ids[check_from:check_from + check_topk]
        radius = search_res[0].distances[check_from + check_topk]
        range_filter = search_res[0].distances[check_from]

        # rebuild the collection with test target index
        collection_w.release()
        collection_w.indexes[0].drop()
        _index_params = {"index_type": index_type, "metric_type": metric,
                         "params": cf.get_index_params_params(index_type)}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index_params)
        collection_w.load()

        params = cf.get_search_params_params(index_type)
        params.update({"radius": radius, "range_filter": range_filter})
        if index_type == "HNSW":
            params.update({"ef": check_topk+100})
        if index_type == "IVF_PQ":
            params.update({"max_empty_result_buckets": 100})
        range_search_params = {"params": params}
        range_res = collection_w.search(search_vectors, default_search_field,
                                        range_search_params, limit=check_topk)[0]
        range_ids = range_res[0].ids
        # assert len(range_ids) == check_topk
        log.debug(f"range search radius={radius}, range_filter={range_filter}, range results num: {len(range_ids)}")
        hit_rate = round(len(set(ids).intersection(set(range_ids))) / len(set(ids)), 2)
        log.debug(f"{vector_data_type} range search results {index_type} {metric} with_growing {with_growing} hit_rate: {hit_rate}")
        assert hit_rate >= 0.2    # issue #32630 to improve the accuracy

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("range_filter", [1000, 1000.0])
    @pytest.mark.parametrize("radius", [0, 0.0])
    @pytest.mark.skip()
    def test_range_search_multi_vector_fields(self, nq, dim, auto_id, is_flush, radius, range_filter, enable_dynamic_field):
        """
        target: test range search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        multiple_dim_array = [dim, dim]
        collection_w, _vectors, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. get vectors that inserted into collection
        vectors = []
        if enable_dynamic_field:
            for vector in _vectors[0]:
                vector = vector[ct.default_float_vec_field_name]
                vectors.append(vector)
        else:
            vectors = np.array(_vectors[0]).tolist()
            vectors = [vectors[i][-1] for i in range(nq)]
        # 3. range search
        range_search_params = {"metric_type": "COSINE", "params": {"radius": radius,
                                                                   "range_filter": range_filter}}
        vector_list = cf. extract_vector_field_name_list(collection_w)
        vector_list.append(default_search_field)
        for search_field in vector_list:
            search_res = collection_w.search(vectors[:nq], search_field,
                                             range_search_params, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": nq,
                                                          "ids": insert_ids,
                                                          "limit": default_limit})[0]
            log.info("test_range_search_normal: checking the distance of top 1")
            for hits in search_res:
                # verify that top 1 hit is itself,so min distance is 1.0
                assert abs(hits.distances[0] - 1.0) <= epsilon
                # distances_tmp = list(hits.distances)
                # assert distances_tmp.count(1.0) == 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_cosine(self):
        """
        target: test range search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        range_filter = random.uniform(0, 1)
        radius = random.uniform(-1, range_filter)

        # 2. range search
        range_search_params = {"metric_type": "COSINE",
                               "params": {"radius": radius, "range_filter": range_filter}}
        search_res = collection_w.search(vectors[:nq], default_search_field,
                                         range_search_params, default_limit,
                                         default_search_exp)[0]

        # 3. check search results
        for hits in search_res:
            for distance in hits.distances:
                assert range_filter >= distance > radius

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_only_range_filter(self):
        """
        target: test range search with only range filter
        method: create connection, collection, insert and search
        expected: range search successfully as normal search
        """
        # 1. initialize with data
        collection_w, _vectors, _, insert_ids, time_stamp = self.init_collection_general(
            prefix, True, nb=10)[0:5]
        # 2. get vectors that inserted into collection
        vectors = np.array(_vectors[0]).tolist()
        vectors = [vectors[i][-1] for i in range(default_nq)]
        # 3. range search with L2
        range_search_params = {"metric_type": "COSINE",
                               "params": {"range_filter": 1}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})
        # 4. range search with IP
        range_search_params = {"metric_type": "IP",
                               "params": {"range_filter": 1}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "metric type not match: "
                                                     "invalid parameter[expected=COSINE][actual=IP]"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_only_radius(self):
        """
        target: test range search with only radius
        method: create connection, collection, insert and search
        expected: search successfully with filtered limit(topK)
        """
        # 1. initialize with data
        collection_w, _vectors, _, insert_ids, time_stamp = self.init_collection_general(
            prefix, True, nb=10, is_index=False)[0:5]
        collection_w.create_index(field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. get vectors that inserted into collection
        vectors = np.array(_vectors[0]).tolist()
        vectors = [vectors[i][-1] for i in range(default_nq)]
        # 3. range search with L2
        range_search_params = {"metric_type": "L2", "params": {"radius": 0}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})
        # 4. range search with IP
        range_search_params = {"metric_type": "IP", "params": {"radius": 0}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "metric type not match: invalid "
                                                     "parameter[expected=L2][actual=IP]"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_radius_range_filter_not_in_params(self):
        """
        target: test range search radius and range filter not in params
        method: create connection, collection, insert and search
        expected: search successfully as normal search
        """
        # 1. initialize with data
        collection_w, _vectors, _, insert_ids, time_stamp = self.init_collection_general(
            prefix, True, nb=10)[0:5]
        # 2. get vectors that inserted into collection
        vectors = np.array(_vectors[0]).tolist()
        vectors = [vectors[i][-1] for i in range(default_nq)]
        # 3. range search with L2
        range_search_params = {"metric_type": "COSINE",
                               "radius": 0, "range_filter": 1}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})
        # 4. range search with IP
        range_search_params = {"metric_type": "IP",
                               "radius": 1, "range_filter": 0}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "metric type not match: invalid "
                                                     "parameter[expected=COSINE][actual=IP]"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dup_times", [1, 2])
    def test_range_search_with_dup_primary_key(self, auto_id, _async, dup_times):
        """
        target: test range search with duplicate primary key
        method: 1.insert same data twice
                2.range search
        expected: range search results are de-duplicated
        """
        # 1. initialize with data
        collection_w, insert_data, _, insert_ids = self.init_collection_general(prefix, True, default_nb,
                                                                                auto_id=auto_id,
                                                                                dim=default_dim)[0:4]
        # 2. insert dup data multi times
        for i in range(dup_times):
            insert_res, _ = collection_w.insert(insert_data[0])
            insert_ids.extend(insert_res.primary_keys)
        # 3. range search
        vectors = np.array(insert_data[0]).tolist()
        vectors = [vectors[i][-1] for i in range(default_nq)]
        log.info(vectors)
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 1000}}
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         range_search_params, default_limit,
                                         default_search_exp, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": insert_ids,
                                                      "limit": default_limit,
                                                      "_async": _async})[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
        # assert that search results are de-duplicated
        for hits in search_res:
            ids = hits.ids
            assert sorted(list(set(ids))) == sorted(ids)

    @pytest.mark.tags(CaseLabel.L2)
    def test_accurate_range_search_with_multi_segments(self):
        """
        target: range search collection with multi segments accurately
        method: insert and flush twice
        expect: result pk should be [19,9,18]
        """
        # 1. create a collection, insert data and flush
        nb = 10
        dim = 64
        collection_w = self.init_collection_general(
            prefix, True, nb, dim=dim, is_index=False)[0]

        # 2. insert data and flush again for two segments
        data = cf.gen_default_dataframe_data(nb=nb, dim=dim, start=nb)
        collection_w.insert(data)
        collection_w.flush()

        # 3. create index and load
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # 4. get inserted original data
        inserted_vectors = collection_w.query(expr="int64 >= 0", output_fields=[
                                              ct.default_float_vec_field_name])
        original_vectors = []
        for single in inserted_vectors[0]:
            single_vector = single[ct.default_float_vec_field_name]
            original_vectors.append(single_vector)

        # 5. Calculate the searched ids
        limit = 2*nb
        vectors = [[random.random() for _ in range(dim)] for _ in range(1)]
        distances = []
        for original_vector in original_vectors:
            distance = cf.cosine(vectors, original_vector)
            distances.append(distance)
        distances_max = heapq.nlargest(limit, distances)
        distances_index_max = map(distances.index, distances_max)

        # 6. Search
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 1}}
        collection_w.search(vectors, default_search_field,
                            range_search_params, limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={
                                "nq": 1,
                                "limit": limit,
                                "ids": list(distances_index_max)
                            })

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_empty_vectors(self, _async):
        """
        target: test range search with empty query vector
        method: search using empty query vector
        expected: search successfully with 0 results
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(
            prefix, True, dim=default_dim)[0]
        # 2. search collection without data
        log.info("test_range_search_with_empty_vectors: Range searching collection %s "
                 "using empty vector" % collection_w.name)
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 0}}
        collection_w.search([], default_search_field, range_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 0,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="partition load and release constraints")
    def test_range_search_before_after_delete(self, nq, _async):
        """
        target: test range search before and after deletion
        method: 1. search the collection
                2. delete a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        # 1. initialize with data
        nb = 1000
        limit = 1000
        partition_num = 1
        dim = 100
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info(
            "test_range_search_before_after_delete: searching before deleting partitions")
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})
        # 3. delete partitions
        log.info("test_range_search_before_after_delete: deleting a partition")
        par = collection_w.partitions
        deleted_entity_num = par[partition_num].num_entities
        print(deleted_entity_num)
        entity_num = nb - deleted_entity_num
        collection_w.release(par[partition_num].name)
        collection_w.drop_partition(par[partition_num].name)
        log.info("test_range_search_before_after_delete: deleted a partition")
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 4. search non-deleted part after delete partitions
        log.info(
            "test_range_search_before_after_delete: searching after deleting partitions")
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids[:entity_num],
                                         "limit": limit - deleted_entity_num,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_collection_after_release_load(self, _async):
        """
        target: range search the pre-released collection after load
        method: 1. create collection
                2. release collection
                3. load collection
                4. range search the pre-released collection
        expected: search successfully
        """
        # 1. initialize without data
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, default_nb,
                                                                                  1, auto_id=auto_id,
                                                                                  dim=default_dim,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. release collection
        log.info("test_range_search_collection_after_release_load: releasing collection %s" %
                 collection_w.name)
        collection_w.release()
        log.info("test_range_search_collection_after_release_load: released collection %s" %
                 collection_w.name)
        # 3. Search the pre-released collection after load
        log.info("test_range_search_collection_after_release_load: loading collection %s" %
                 collection_w.name)
        collection_w.load()
        log.info(
            "test_range_search_collection_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:default_nq], default_search_field, range_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_load_flush_load(self, _async):
        """
        target: test range search when load before flush
        method: 1. insert data and load
                2. flush, and load
                3. search the collection
        expected: search success with limit(topK)
        """
        # 1. initialize with data
        dim = 100
        enable_dynamic_field = True
        collection_w = self.init_collection_general(
            prefix, dim=dim, enable_dynamic_field=enable_dynamic_field)[0]
        # 2. insert data
        insert_ids = cf.insert_data(
            collection_w, default_nb, dim=dim, enable_dynamic_field=enable_dynamic_field)[3]
        # 3. load data
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 4. flush and load
        collection_w.num_entities
        collection_w.load()
        # 5. search
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_new_data(self, nq):
        """
        target: test search new inserted data without load
        method: 1. search the collection
                2. insert new data
                3. search the collection without load again
                4. Use guarantee_timestamp to guarantee data consistency
        expected: new data should be range searched
        """
        # 1. initialize with data
        limit = 1000
        nb_old = 500
        dim = 111
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb_old,
                                                                                  dim=dim,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"radius": 0,
                                                                   "range_filter": 1000}}
        log.info("test_range_search_new_data: searching for original data after load")
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old})
        # 3. insert new data
        nb_new = 300
        _, _, _, insert_ids_new, time_stamp = cf.insert_data(collection_w, nb_new, dim=dim,
                                                             enable_dynamic_field=enable_dynamic_field,
                                                             insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        # 4. search for new data without load
        # Using bounded staleness, maybe we could not search the "inserted" entities,
        # since the search requests arrived query nodes earlier than query nodes consume the insert requests.
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp,
                            guarantee_timestamp=time_stamp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_different_data_distribution_with_index(self, _async):
        """
        target: test search different data distribution with index
        method: 1. connect to milvus
                2. create a collection
                3. insert data
                4. create an index
                5. Load and search
        expected: Range search successfully
        """
        # 1. connect, create collection and insert data
        dim = 100
        self._connect()
        collection_w = self.init_collection_general(
            prefix, False, dim=dim, is_index=False)[0]
        dataframe = cf.gen_default_dataframe_data(dim=dim, start=-1500)
        collection_w.insert(dataframe)

        # 2. create index
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)

        # 3. load and range search
        collection_w.load()
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        range_search_params = {"metric_type": "L2", "params": {"radius": 1000,
                                                               "range_filter": 0}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("not fixed yet")
    @pytest.mark.parametrize("shards_num", [-256, 0, 1, 10, 31, 63])
    def test_range_search_with_non_default_shard_nums(self, shards_num, _async):
        """
        target: test range search with non_default shards_num
        method: connect milvus, create collection with several shard numbers , insert, load and search
        expected: search successfully with the non_default shards_num
        """
        self._connect()
        # 1. create collection
        name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(
            name=name, shards_num=shards_num)
        # 2. rename collection
        new_collection_name = cf.gen_unique_str(prefix + "new")
        self.utility_wrap.rename_collection(
            collection_w.name, new_collection_name)
        collection_w = self.init_collection_wrap(
            name=new_collection_name, shards_num=shards_num)
        # 3. insert
        dataframe = cf.gen_default_dataframe_data()
        collection_w.insert(dataframe)
        # 4. create index and load
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 5. range search
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", range_search_supported_indexes)
    def test_range_search_after_different_index_with_params(self, index):
        """
        target: test range search after different index
        method: test range search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 96
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False,
                                                                                  enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. range search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            search_param["params"]["radius"] = 1000
            search_param["params"]["range_filter"] = 0
            if index.startswith("IVF_"):
                search_param["params"].pop("nprobe")
            if index == "SCANN":
                search_param["params"].pop("nprobe")
                search_param["params"].pop("reorder_k")
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", range_search_supported_indexes)
    def test_range_search_after_index_different_metric_type(self, index):
        """
        target: test range search with different metric type
        method: test range search with different metric type
        expected: searched successfully
        """
        if index == "SCANN":
            pytest.skip("https://github.com/milvus-io/milvus/issues/32648")
        # 1. initialize with data
        dim = 208
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        # 2. create different index
        params = cf.get_index_params_params(index)
        log.info("test_range_search_after_index_different_metric_type: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_range_search_after_index_different_metric_type: Created index-%s" % index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index, "IP")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            search_param["params"]["radius"] = 0
            search_param["params"]["range_filter"] = 1000
            if index.startswith("IVF_"):
                search_param["params"].pop("nprobe")
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_index_one_partition(self, _async):
        """
        target: test range search from partition
        method: search from one partition
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 3000
        auto_id = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  is_index=False)[0:5]

        # 2. create index
        default_index = {"index_type": "IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search in one partition
        log.info(
            "test_range_search_index_one_partition: searching (1000 entities) through one partition")
        limit = 1000
        par = collection_w.partitions
        if limit > par[1].num_entities:
            limit_check = par[1].num_entities
        else:
            limit_check = limit
        range_search_params = {"metric_type": "L2",
                               "params": {"radius": 1000, "range_filter": 0}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, limit, default_search_exp,
                            [par[1].name], _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids[par[0].num_entities:],
                                         "limit": limit_check,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_jaccard_flat_index(self, nq, _async, index, is_flush):
        """
        target: range search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with JACCARD
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 48
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = self.init_collection_general(prefix, True, 2,
                                                                                                  is_binary=True,
                                                                                                  auto_id=auto_id,
                                                                                                  dim=dim,
                                                                                                  is_index=False,
                                                                                                  is_flush=is_flush)[
            0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.jaccard(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.jaccard(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "JACCARD",
                         "params": {"radius": 1000, "range_filter": 0}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_jaccard_invalid_params(self, index):
        """
        target: range search binary_collection with out of range params [0, 1]
        method: range search binary_collection with out of range params
        expected: return empty
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = self.init_collection_general(prefix, True, 2,
                                                                                                  is_binary=True,
                                                                                                  dim=default_dim,
                                                                                                  is_index=False,)[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(
            3000, default_dim)
        # 4. range search
        search_params = {"metric_type": "JACCARD",
                         "params": {"radius": -1, "range_filter": -10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})
        # 5. range search
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10, "radius": 10,
                                                              "range_filter": 2}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_hamming_flat_index(self, nq, _async, index, is_flush):
        """
        target: range search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with HAMMING
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 80
        auto_id = True
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=False,
                                                                                      is_flush=is_flush)[0:4]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "HAMMING"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        collection_w.load()
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.hamming(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.hamming(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "HAMMING",
                         "params": {"radius": 1000, "range_filter": 0}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_hamming_invalid_params(self, index):
        """
        target: range search binary_collection with out of range params
        method: range search binary_collection with out of range params
        expected: return empty
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = self.init_collection_general(prefix, True, 2,
                                                                                                  is_binary=True,
                                                                                                  dim=default_dim,
                                                                                                  is_index=False,)[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "HAMMING"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(
            3000, default_dim)
        # 4. range search
        search_params = {"metric_type": "HAMMING", "params": {"nprobe": 10, "radius": -1,
                                                              "range_filter": -10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("tanimoto obsolete")
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_tanimoto_flat_index(self, _async, index, is_flush):
        """
        target: range search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with TANIMOTO
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 100
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=False,
                                                                                      is_flush=is_flush)[0:4]
        log.info("auto_id= %s, _async= %s" % (auto_id, _async))
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "TANIMOTO"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[1])
        # 4. search
        search_params = {"metric_type": "TANIMOTO", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:1], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async)[0]
        if _async:
            res.done()
            res = res.result()
        limit = 0
        radius = 1000
        range_filter = 0
        # filter the range search results to be compared
        for distance_single in res[0].distances:
            if radius > distance_single >= range_filter:
                limit += 1
        # 5. range search and compare the distance
        search_params = {"metric_type": "TANIMOTO", "params": {"radius": radius,
                                                               "range_filter": range_filter}}
        res = collection_w.search(binary_vectors[:1], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": 1,
                                               "ids": insert_ids,
                                               "limit": limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("tanimoto obsolete")
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_tanimoto_invalid_params(self, index):
        """
        target: range search binary_collection with out of range params [0,inf)
        method: range search binary_collection with out of range params
        expected: return empty
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = self.init_collection_general(prefix, True, 2,
                                                                                                  is_binary=True,
                                                                                                  dim=default_dim,
                                                                                                  is_index=False,)[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(
            3000, default_dim)
        # 4. range search
        search_params = {"metric_type": "JACCARD",
                         "params": {"radius": -1, "range_filter": -10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_binary_without_flush(self, metrics):
        """
        target: test range search without flush for binary data (no index)
        method: create connection, collection, insert, load and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize a collection without data
        auto_id = True
        collection_w = self.init_collection_general(
            prefix, is_binary=True, auto_id=auto_id, is_index=False)[0]
        # 2. insert data
        insert_ids = cf.insert_data(
            collection_w, default_nb, is_binary=True, auto_id=auto_id)[3]
        # 3. load data
        index_params = {"index_type": "BIN_FLAT", "params": {
            "nlist": 128}, "metric_type": metrics}
        collection_w.create_index("binary_vector", index_params)
        collection_w.load()
        # 4. search
        log.info("test_range_search_binary_without_flush: searching collection %s" %
                 collection_w.name)
        binary_vectors = cf.gen_binary_vectors(default_nq, default_dim)[1]
        search_params = {"metric_type": metrics, "params": {"radius": 1000,
                                                            "range_filter": 0}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_normal_expressions())
    def test_range_search_with_expression(self, expression, _async, enable_dynamic_field):
        """
        target: test range search with different expressions
        method: test range search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 200
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True,
                                                                             nb, dim=dim,
                                                                             is_index=False,
                                                                             enable_dynamic_field=enable_dynamic_field)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i, _id in enumerate(insert_ids):
            if enable_dynamic_field:
                int64 = _vectors[i][ct.default_int64_field_name]
                float = _vectors[i][ct.default_float_field_name]
            else:
                int64 = _vectors.int64[i]
                float = _vectors.float[i]
            if not expression or eval(expression):
                filter_ids.append(_id)

        # 2. create index
        index_param = {"index_type": "FLAT",
                       "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        log.info(
            "test_range_search_with_expression: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        range_search_params = {"metric_type": "L2", "params": {"radius": 1000,
                                                               "range_filter": 0}}
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            range_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_output_field(self, _async, enable_dynamic_field):
        """
        target: test range search with output fields
        method: range search with one output_field
        expected: search success
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_range_search_with_output_field: Searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "COSINE", "params": {"radius": 0,
                                                                   "range_filter": 1000}}
        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  range_search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  output_fields=[default_int64_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert default_int64_field_name in res[0][0].fields

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_concurrent_multi_threads(self, nq, _async):
        """
        target: test concurrent range search with multi-processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        # 1. initialize with data
        threads_num = 10
        threads = []
        dim = 66
        auto_id = False
        nb = 4000
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim)[0:5]

        def search(collection_w):
            vectors = [[random.random() for _ in range(dim)]
                       for _ in range(nq)]
            range_search_params = {"metric_type": "COSINE", "params": {"radius": 0,
                                                                       "range_filter": 1000}}
            collection_w.search(vectors[:nq], default_search_field,
                                range_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info(
            "test_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_range_search_round_decimal(self, round_decimal):
        """
        target: test range search with valid round decimal
        method: range search with valid round decimal
        expected: search successfully
        """
        import math
        tmp_nb = 500
        tmp_nq = 1
        tmp_limit = 5
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, nb=tmp_nb)[0]
        # 2. search
        log.info("test_search_round_decimal: Searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        res = collection_w.search(vectors[:tmp_nq], default_search_field,
                                  range_search_params, tmp_limit)[0]

        res_round = collection_w.search(vectors[:tmp_nq], default_search_field,
                                        range_search_params, tmp_limit, round_decimal=round_decimal)[0]

        abs_tol = pow(10, 1 - round_decimal)
        # log.debug(f'abs_tol: {abs_tol}')
        for i in range(tmp_limit):
            dis_expect = round(res[0][i].distance, round_decimal)
            dis_actual = res_round[0][i].distance
            # log.debug(f'actual: {dis_actual}, expect: {dis_expect}')
            # abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
            assert math.isclose(dis_actual, dis_expect,
                                rel_tol=0, abs_tol=abs_tol)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("known issue #27518")
    def test_range_search_with_expression_large(self, dim):
        """
        target: test range search with large expression
        method: test range search with large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      nb, dim=dim,
                                                                      is_index=False)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression: searching with expression: %s" % expression)

        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        # calculate the distance to make sure in range(0, 1000)
        search_params = {"metric_type": "L2"}
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            search_params, 500, expression)
        for i in range(nums):
            if len(search_res[i]) < 10:
                assert False
            for j in range(len(search_res[i])):
                if search_res[i][j].distance < 0 or search_res[i][j].distance >= 1000:
                    assert False
        # range search
        range_search_params = {"metric_type": "L2", "params": {"radius": 1000, "range_filter": 0}}
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            range_search_params, default_limit, expression)
        for i in range(nums):
            log.info(i)
            assert len(search_res[i]) == default_limit

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_consistency_bounded(self, nq, _async):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "bounded"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 200
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async,
                                         })

        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_BOUNDED)
        kwargs.update({"consistency_level": consistency_level})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            )

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_consistency_strong(self, nq, _async):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "Strong"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 100
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get("consistency_level", CONSISTENCY_STRONG)
        kwargs.update({"consistency_level": consistency_level})
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_consistency_eventually(self, nq, _async):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "eventually"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 128
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_EVENTUALLY)
        kwargs.update({"consistency_level": consistency_level})
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs
                            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_with_consistency_session(self, nq, dim, auto_id, _async):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "session"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})

        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_SESSION)
        kwargs.update({"consistency_level": consistency_level})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_sparse(self):
        """
        target: test sparse index normal range search
        method: create connection, collection, insert and range search
        expected: range search successfully
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, nb=5000,
                                                    with_json=True,
                                                    vector_data_type=ct.sparse_vector)[0]
        range_filter = random.uniform(0.5, 1)
        radius = random.uniform(0, 0.5)

        # 2. range search
        range_search_params = {"metric_type": "IP",
                               "params": {"radius": radius, "range_filter": range_filter}}
        d = cf.gen_default_list_sparse_data(nb=1)
        search_res = collection_w.search(d[-1][-1:], ct.default_sparse_vec_field_name,
                                         range_search_params, default_limit,
                                         default_search_exp)[0]

        # 3. check search results
        for hits in search_res:
            for distance in hits.distances:
                assert range_filter >= distance > radius


class TestCollectionLoadOperation(TestcaseBase):
    """ Test case of search combining load and other functions """

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_load_collection_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load the collection
                5. release one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        collection_w.load()
        partition_w1.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_load_collection_release_collection(self):
        """
        target: test delete load collection release collection
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load the collection
                5. release the collection
                6. load one partition
                7. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        collection_w.load()
        collection_w.release()
        partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_load_partition_release_collection(self):
        """
        target: test delete load partition release collection
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        partition_w1.load()
        collection_w.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_release_collection_load_partition(self):
        """
        target: test delete load collection release collection
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load one partition
                5. release the collection
                6. load the other partition
                7. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        partition_w1.load()
        collection_w.release()
        partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_load_partition_drop_partition(self):
        """
        target: test delete load partition drop partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        partition_w2.load()
        partition_w2.release()
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_delete_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. delete half data in each partition
                5. release one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load
        collection_w.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        partition_w1.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_delete_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. release the collection and load one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load
        partition_w1.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        collection_w.release()
        partition_w1.load()
        # search on collection, partition1, partition2
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}]})
        partition_w1.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}]})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_delete_drop_partition(self):
        """
        target: test load partition delete drop partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. drop the non-loaded partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load
        partition_w1.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_release_partition_delete(self):
        """
        target: test load collection release partition delete
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. release one partition
                5. delete half data in each partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load && release
        collection_w.load()
        partition_w1.release()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_release_collection_delete(self):
        """
        target: test load partition release collection delete
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. release the collection
                5. delete half data in each partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load && release
        partition_w1.load()
        collection_w.release()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        collection_w.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_drop_partition_delete(self):
        """
        target: test load partition drop partition delete
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. release and drop the partition
                5. delete half data in each partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load && release
        partition_w2.load()
        partition_w2.release()
        partition_w2.drop()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_load_collection_release_partition(self):
        """
        target: test compact load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. compact
                4. load the collection
                5. release one partition
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # load && release
        collection_w.load()
        partition_w1.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_load_collection_release_collection(self):
        """
        target: test compact load collection release collection
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. compact
                4. load the collection
                5. release the collection
                6. load one partition
                7. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # load && release
        collection_w.load()
        collection_w.release()
        partition_w1.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_load_partition_release_collection(self):
        """
        target: test compact load partition release collection
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. compact
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # load && release
        partition_w2.load()
        collection_w.release()
        partition_w1.load()
        partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 300})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_compact_drop_partition(self):
        """
        target: test load collection compact drop partition
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. load the collection
                4. compact
                5. release one partition and drop
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # load
        collection_w.load()
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # release
        partition_w2.release()
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_compact_release_collection(self):
        """
        target: test load partition compact release collection
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. load one partition
                4. compact
                5. release the collection
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # load
        partition_w2.load()
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # release
        collection_w.release()
        partition_w2.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_release_partition_compact(self):
        """
        target: test load collection release partition compact
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. load the collection
                4. release one partition
                5. compact
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # load && release
        collection_w.load()
        partition_w1.release()
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_collection_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load the collection
                5. release one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # flush
        collection_w.flush()
        # load && release
        collection_w.load()
        partition_w1.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_collection_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load the collection
                5. release the collection
                6. load one partition
                7. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # flush
        collection_w.flush()
        # load && release
        collection_w.load()
        collection_w.release()
        partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_partition_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # flush
        collection_w.flush()
        # load && release
        partition_w2.load()
        collection_w.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_partition_drop_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load one partition
                5. release and drop the partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # flush
        collection_w.flush()
        # load && release
        partition_w2.load()
        partition_w2.release()
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_collection_drop_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load collection
                5. release and drop one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # flush
        collection_w.flush()
        # load && release
        collection_w.load()
        partition_w2.release()
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_flush_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. flush
                5. search on the collection -> len(res)==200
                5. release one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load
        collection_w.load()
        # flush
        collection_w.flush()
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        # release
        partition_w2.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_flush_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. flush
                5. release the collection
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load
        partition_w2.load()
        # flush
        collection_w.flush()
        # release
        collection_w.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_flush_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. flush
                5. drop the non-loaded partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load
        partition_w1.load()
        # flush
        collection_w.flush()
        # release
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_partition_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. release one partition
                5. flush
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load && release
        collection_w.load()
        partition_w2.release()
        # flush
        collection_w.flush()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_collection_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. release the collection
                5. load one partition
                6. flush
                7. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load && release
        collection_w.load()
        collection_w.release()
        partition_w2.load()
        # flush
        collection_w.flush()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_release_collection_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the partition
                4. release the collection
                5. flush
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load && release
        partition_w2.load()
        collection_w.release()
        # flush
        collection_w.flush()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_drop_partition_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. release and drop the partition
                5. flush
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load && release
        partition_w2.load()
        partition_w2.release()
        partition_w2.drop()
        # flush
        collection_w.flush()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_release_collection_multi_times(self):
        """
        target: test load and release multiple times
        method: 1. create a collection and 2 partitions
                2. load and release multiple times
                3. search
        expected: No exception
        """
        # init the collection
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load and release
        for i in range(5):
            collection_w.release()
            partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_all_partitions(self):
        """
        target: test load and release all partitions
        method: 1. create a collection and 2 partitions
                2. load collection and release all partitions
                3. search
        expected: No exception
        """
        # init the collection
        collection_w = self.init_collection_general(prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load and release
        collection_w.load()
        partition_w1.release()
        partition_w2.release()
        # search on collection
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #24446")
    def test_search_load_collection_create_partition(self):
        """
        target: test load collection and create partition and search
        method: 1. create a collection and 2 partitions
                2. load collection and create a partition
                3. search
        expected: No exception
        """
        # init the collection
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load and release
        collection_w.load()
        partition_w3 = collection_w.create_partition("_default3")[0]
        # search on collection
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_load_partition_create_partition(self):
        """
        target: test load partition and create partition and search
        method: 1. create a collection and 2 partitions
                2. load partition and create a partition
                3. search
        expected: No exception
        """
        # init the collection
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load and release
        partition_w1.load()
        partition_w3 = collection_w.create_partition("_default3")[0]
        # search on collection
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})


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
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(
            prefix, True, dim=dim)[0:5]
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
                                         enable_dynamic_field=enable_dynamic_field)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_json_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

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
                                             "limit": 3})

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
                                             "limit": limit})

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
                                             "limit": limit // 2})

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
        string_field_value = [[str(j) for j in range(i, i+3)] for i in range(ct.default_nb)]
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
        collection_w.search(vectors[:default_nq], default_search_field, {},
                            limit=ct.default_nb, expr=expression,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1100,
                                         ct.err_msg: "failed to create query plan: cannot parse "
                                                     "expression: %s, error: contains_any operation "
                                                     "element must be an array" % expression})


class TestSearchIterator(TestcaseBase):
    """ Test case of search iterator """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_data_type", ["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def test_search_iterator_normal(self, vector_data_type):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
        """
        # 1. initialize with data
        dim = 128
        collection_w = self.init_collection_general(prefix, True, dim=dim, is_index=False,
                                                    vector_data_type=vector_data_type)[0]
        collection_w.create_index(field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search iterator
        search_params = {"metric_type": "L2"}
        vectors = cf.gen_vectors_based_on_vector_type(1, dim, vector_data_type)
        batch_size = 200
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"batch_size": batch_size})
        batch_size = 600
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"batch_size": batch_size})

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
    def test_range_search_iterator_L2(self):
        """
        target: test iterator range search
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        collection_w.create_index(field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search iterator
        search_params = {"metric_type": "L2", "params": {"radius": 35.0, "range_filter": 34.0}}
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"metric_type": "L2",
                                                  "radius": 35.0,
                                                  "range_filter": 34.0})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_iterator_IP(self):
        """
        target: test iterator range search
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        collection_w.create_index(field_name, {"metric_type": "IP"})
        collection_w.load()
        # 2. search iterator
        search_params = {"metric_type": "IP", "params": {"radius": 0, "range_filter": 45}}
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"metric_type": "IP",
                                                  "radius": 0,
                                                  "range_filter": 45})

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_iterator_COSINE(self):
        """
        target: test iterator range search
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        collection_w.create_index(field_name, {"metric_type": "COSINE"})
        collection_w.load()
        # 2. search iterator
        search_params = {"metric_type": "COSINE", "params": {"radius": 0.8, "range_filter": 1}}
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"metric_type": "COSINE",
                                                  "radius": 0.8,
                                                  "range_filter": 1})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_iterator_only_radius(self):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        collection_w.create_index(field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search iterator
        search_params = {"metric_type": "L2", "params": {"radius": 35.0}}
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"metric_type": "L2",
                                                  "radius": 35.0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("issue #25145")
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    @pytest.mark.parametrize("metrics", ct.float_metrics)
    def test_search_iterator_after_different_index_metrics(self, index, metrics):
        """
        target: test search iterator using different index
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": metrics}
        collection_w.create_index(field_name, default_index)
        collection_w.load()
        # 2. search iterator
        search_params = {"metric_type": metrics}
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
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
                                                  "err_msg": "Not support multiple vector iterator at present"})


class TestSearchGroupBy(TestcaseBase):
    """ Test case of search group by """

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("index_type, metric", zip(["FLAT", "IVF_FLAT", "HNSW"], ct.float_metrics))
    @pytest.mark.parametrize("vector_data_type", ["FLOAT16_VECTOR", "FLOAT_VECTOR", "BFLOAT16_VECTOR"])
    def test_search_group_by_default(self, index_type, metric, vector_data_type):
        """
        target: test search group by
        method: 1. create a collection with data
                2. create index with different metric types
                3. search with group by
                verify no duplicate values for group_by_field
                4. search with filtering every value of group_by_field
                verify: verify that every record in groupby results is the top1 for that value of the group_by_field
        """
        collection_w = self.init_collection_general(prefix, auto_id=True, insert_data=False, is_index=False,
                                                    vector_data_type=vector_data_type,
                                                    is_all_data_type=True, with_json=False)[0]
        _index_params = {"index_type": index_type, "metric_type": metric, "params": {"M": 16, "efConstruction": 128}}
        if index_type in ["IVF_FLAT", "FLAT"]:
            _index_params = {"index_type": index_type, "metric_type": metric, "params": {"nlist": 128}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index_params)
        # insert with the same values for scalar fields
        for _ in range(50):
            data = cf.gen_dataframe_all_data_type(nb=100, auto_id=True, with_json=False)
            collection_w.insert(data)

        collection_w.flush()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index_params)
        collection_w.load()

        search_params = {"metric_type": metric, "params": {"ef": 128}}
        nq = 2
        limit = 15
        search_vectors = cf.gen_vectors(nq, dim=ct.default_dim)
        # verify the results are same if gourp by pk
        res1 = collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                   param=search_params, limit=limit, consistency_level=CONSISTENCY_STRONG,
                                   group_by_field=ct.default_int64_field_name)[0]
        res2 = collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                   param=search_params, limit=limit, consistency_level=CONSISTENCY_STRONG)[0]
        hits_num = 0
        for i in range(nq):
            # assert res1[i].ids == res2[i].ids
            hits_num += len(set(res1[i].ids).intersection(set(res2[i].ids)))
        hit_rate = hits_num / (nq * limit)
        log.info(f"groupy primary key hits_num: {hits_num}, nq: {nq}, limit: {limit}, hit_rate: {hit_rate}")
        assert hit_rate >= 0.60

        # verify that every record in groupby results is the top1 for that value of the group_by_field
        supported_grpby_fields = [ct.default_int8_field_name, ct.default_int16_field_name,
                                  ct.default_int32_field_name, ct.default_bool_field_name,
                                  ct.default_string_field_name]
        for grpby_field in supported_grpby_fields:
            res1 = collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                       param=search_params, limit=limit,
                                       group_by_field=grpby_field,
                                       output_fields=[grpby_field])[0]
            for i in range(nq):
                grpby_values = []
                dismatch = 0
                results_num = 2 if grpby_field == ct.default_bool_field_name else limit
                for l in range(results_num):
                    top1 = res1[i][l]
                    top1_grpby_pk = top1.id
                    top1_grpby_value = top1.fields.get(grpby_field)
                    expr = f"{grpby_field}=={top1_grpby_value}"
                    if grpby_field == ct.default_string_field_name:
                        expr = f"{grpby_field}=='{top1_grpby_value}'"
                    grpby_values.append(top1_grpby_value)
                    res_tmp = collection_w.search(data=[search_vectors[i]], anns_field=ct.default_float_vec_field_name,
                                                  param=search_params, limit=1,
                                                  expr=expr,
                                                  output_fields=[grpby_field])[0]
                    top1_expr_pk = res_tmp[0][0].id
                    if top1_grpby_pk != top1_expr_pk:
                        dismatch += 1
                        log.info(f"{grpby_field} on {metric} dismatch_item, top1_grpby_dis: {top1.distance}, top1_expr_dis: {res_tmp[0][0].distance}")
                log.info(f"{grpby_field} on {metric}  top1_dismatch_num: {dismatch}, results_num: {results_num}, dismatch_rate: {dismatch / results_num}")
                baseline = 1 if grpby_field == ct.default_bool_field_name else 0.2    # skip baseline check for boolean
                assert dismatch / results_num <= baseline
                # verify no dup values of the group_by_field in results
                assert len(grpby_values) == len(set(grpby_values))

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

        # verify the results are same if gourp by pk
        err_code = 999
        err_msg = "not support search_group_by operation based on binary"
        collection_w.search(data=search_vectors, anns_field=ct.default_binary_vec_field_name,
                            param=search_params, limit=limit,
                            group_by_field=ct.default_int64_field_name,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": err_code, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("grpby_field", [ct.default_string_field_name, ct.default_int8_field_name])
    def test_search_group_by_with_field_indexed(self, grpby_field):
        """
        target: test search group by with the field indexed
        method: 1. create a collection with data
                2. create index for the vector field and the groupby field
                3. search with group by
                4. search with filtering every value of group_by_field
                verify: verify that every record in groupby results is the top1 for that value of the group_by_field
        """
        metric = "COSINE"
        collection_w = self.init_collection_general(prefix, auto_id=True, insert_data=False, is_index=False,
                                                    is_all_data_type=True, with_json=False)[0]
        _index = {"index_type": "HNSW", "metric_type": metric, "params": {"M": 16, "efConstruction": 128}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index)
        # insert with the same values(by insert rounds) for scalar fields
        for _ in range(50):
            data = cf.gen_dataframe_all_data_type(nb=100, auto_id=True, with_json=False)
            collection_w.insert(data)

        collection_w.flush()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index)
        collection_w.create_index(grpby_field)
        collection_w.load()

        search_params = {"metric_type": metric, "params": {"ef": 128}}
        nq = 2
        limit = 20
        search_vectors = cf.gen_vectors(nq, dim=ct.default_dim)

        # verify that every record in groupby results is the top1 for that value of the group_by_field
        res1 = collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                   param=search_params, limit=limit,
                                   group_by_field=grpby_field,
                                   output_fields=[grpby_field])[0]
        for i in range(nq):
            grpby_values = []
            dismatch = 0
            results_num = 2 if grpby_field == ct.default_bool_field_name else limit
            for l in range(results_num):
                top1 = res1[i][l]
                top1_grpby_pk = top1.id
                top1_grpby_value = top1.fields.get(grpby_field)
                expr = f"{grpby_field}=={top1_grpby_value}"
                if grpby_field == ct.default_string_field_name:
                    expr = f"{grpby_field}=='{top1_grpby_value}'"
                grpby_values.append(top1_grpby_value)
                res_tmp = collection_w.search(data=[search_vectors[i]], anns_field=ct.default_float_vec_field_name,
                                              param=search_params, limit=1,
                                              expr=expr,
                                              output_fields=[grpby_field])[0]
                top1_expr_pk = res_tmp[0][0].id
                log.info(f"nq={i}, limit={l}")
                # assert top1_grpby_pk == top1_expr_pk
                if top1_grpby_pk != top1_expr_pk:
                    dismatch += 1
                    log.info(f"{grpby_field} on {metric} dismatch_item, top1_grpby_dis: {top1.distance}, top1_expr_dis: {res_tmp[0][0].distance}")
                log.info(f"{grpby_field} on {metric}  top1_dismatch_num: {dismatch}, results_num: {results_num}, dismatch_rate: {dismatch / results_num}")
                baseline = 1 if grpby_field == ct.default_bool_field_name else 0.2    # skip baseline check for boolean
                assert dismatch / results_num <= baseline
            # verify no dup values of the group_by_field in results
            assert len(grpby_values) == len(set(grpby_values))

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
                                                    is_all_data_type=True, with_json=True,)[0]
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
        if index in ["HNSW", "IVF_FLAT", "FLAT"]:
            pass    # Only HNSW and IVF_FLAT are supported
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
            err_msg = "doesn't support search_group_by"
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
    # @pytest.mark.xfail(reason="issue #30828")
    def test_search_pagination_group_by(self):
        """
        target: test search pagination with group by
        method: 1. create a collection with data
                2. create index HNSW
                3. search with groupby and pagination
                4. search with groupby and limits=pages*page_rounds
                verify: search with groupby and pagination returns correct results
        """
        # 1. create a collection
        metric = "COSINE"
        collection_w = self.init_collection_general(prefix, auto_id=True, insert_data=False, is_index=False,
                                                    is_all_data_type=True, with_json=False)[0]
        # insert with the same values for scalar fields
        for _ in range(50):
            data = cf.gen_dataframe_all_data_type(nb=100, auto_id=True, with_json=False)
            collection_w.insert(data)

        collection_w.flush()
        _index = {"index_type": "HNSW", "metric_type": metric, "params": {"M": 16, "efConstruction": 128}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index)
        collection_w.load()
        # 2. search pagination with offset
        limit = 10
        page_rounds = 3
        search_param = {"metric_type": metric}
        grpby_field = ct.default_string_field_name
        search_vectors = cf.gen_vectors(1, dim=ct.default_dim)
        all_pages_ids = []
        all_pages_grpby_field_values = []
        for r in range(page_rounds):
            page_res = collection_w.search(search_vectors, anns_field=default_search_field,
                                           param=search_param, limit=limit, offset=limit * r,
                                           expr=default_search_exp, group_by_field=grpby_field,
                                           output_fields=["*"],
                                           check_task=CheckTasks.check_search_results,
                                           check_items={"nq": 1, "limit": limit},
                                           )[0]
            for j in range(limit):
                all_pages_grpby_field_values.append(page_res[0][j].get(grpby_field))
            all_pages_ids += page_res[0].ids
        hit_rate = round(len(set(all_pages_grpby_field_values)) / len(all_pages_grpby_field_values), 3)
        assert hit_rate >= 0.8

        total_res = collection_w.search(search_vectors, anns_field=default_search_field,
                                        param=search_param, limit=limit * page_rounds,
                                        expr=default_search_exp, group_by_field=grpby_field,
                                        output_fields=[grpby_field],
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": 1, "limit": limit * page_rounds}
                                        )[0]
        hit_num = len(set(total_res[0].ids).intersection(set(all_pages_ids)))
        hit_rate = round(hit_num / (limit * page_rounds), 3)
        assert hit_rate >= 0.8
        log.info(f"search pagination with groupby hit_rate: {hit_rate}")
        grpby_field_values = []
        for i in range(limit * page_rounds):
            grpby_field_values.append(total_res[0][i].fields.get(grpby_field))
        assert len(grpby_field_values) == len(set(grpby_field_values))

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

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_not_support_group_by(self):
        """
        target: verify that hybrid search does not support groupby
        method: 1. create a collection with multiple vector fields
                2. create index hnsw and load
                3. hybrid_search with group by
                verify: the error code and msg
        """
        # 1. initialize collection with data
        dim = 33
        index_type = "HNSW"
        metric_type = "COSINE"
        _index_params = {"index_type": index_type, "metric_type": metric_type, "params": {"M": 16, "efConstruction": 128}}
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim,  is_index=False,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, _index_params)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for vector_name in vector_name_list:
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(1)],
                "anns_field": vector_name,
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit,
                # "group_by_field": ct.default_int64_field_name,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        err_code = 9999
        err_msg = f"not support search_group_by operation in the hybrid search"
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9, 1), default_limit,
                                   group_by_field=ct.default_int64_field_name,
                                   check_task=CheckTasks.err_res,
                                   check_items={"err_code": err_code, "err_msg": err_msg})

        # 5. hybrid search with group by on one vector field
        req_list = []
        for vector_name in vector_name_list[:1]:
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(1)],
                "anns_field": vector_name,
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit,
                # "group_by_field": ct.default_int64_field_name,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        collection_w.hybrid_search(req_list, RRFRanker(), default_limit,
                                   group_by_field=ct.default_int64_field_name,
                                   check_task=CheckTasks.err_res,
                                   check_items={"err_code": err_code, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_multi_vectors_search_one_vector_group_by(self):
        """
        target: test search group by works on a collection with multi vectors
        method: 1. create a collection with multiple vector fields
                2. create index hnsw and load
                3. search on the vector with hnsw index with group by
                verify: search successfully
        """
        dim = 33
        index_type = "HNSW"
        metric_type = "COSINE"
        _index_params = {"index_type": index_type, "metric_type": metric_type,
                         "params": {"M": 16, "efConstruction": 128}}
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, _index_params)
        collection_w.load()

        nq = 2
        limit = 10
        search_params = {"metric_type": metric_type, "params": {"ef": 32}}
        for vector_name in vector_name_list:
            search_vectors = cf.gen_vectors(nq, dim=dim)
            # verify the results are same if gourp by pk
            collection_w.search(data=search_vectors, anns_field=vector_name,
                                param=search_params, limit=limit,
                                group_by_field=ct.default_int64_field_name,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq, "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_sparse_vectors_group_by(self, index):
        """
        target: test search group by works on a collection with sparse vector
        method: 1. create a collection
                2. create index
                3. grouping search
        verify: search successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=schema)
        nb = 5000
        data = cf.gen_default_list_sparse_data(nb=nb)
        # update float fields
        _data = [random.randint(1, 100) for _ in range(nb)]
        str_data = [str(i) for i in _data]
        data[2] = str_data
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)
        collection_w.load()

        nq = 2
        limit = 20
        search_params = ct.default_sparse_search_params

        search_vectors = cf.gen_default_list_sparse_data(nb=nq)[-1][-2:]
        # verify the results are same if gourp by pk
        res = collection_w.search(data=search_vectors, anns_field=ct.default_sparse_vec_field_name,
                            param=search_params, limit=limit,
                            group_by_field="varchar",
                            output_fields=["varchar"],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq, "limit": limit})

        hit = res[0]
        set_varchar = set()
        for item in hit:
            a = list(item.fields.values())
            set_varchar.add(a[0])
        # groupy by is in effect, then there are no duplicate varchar values
        assert len(hit) == len(set_varchar)


class TestCollectionHybridSearchValid(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[1, 10])
    def nq(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[default_nb_medium])
    def nb(self, request):
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

    @pytest.fixture(scope="function", params=["IP", "COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def random_primary_key(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def vector_data_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases for hybrid_search
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("offset", [0, 5])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_normal(self, nq, is_flush, offset, primary_field, vector_data_type):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        self._connect()
        # create db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)
        # using db and create collection
        self.database_wrap.using_database(db_name)

        # 1. initialize collection with data
        dim = 64
        enable_dynamic_field = True
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field, enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 32}, "offset": offset}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the baseline of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                                 single_search_param, default_limit,
                                                 default_search_exp,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "ids": insert_ids,
                                                              "limit": default_limit})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search baseline
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                offset=offset,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

        # 9. drop db
        collection_w.drop()
        self.database_wrap.drop_database(db_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [16384])
    def test_hybrid_search_normal_max_nq(self, nq):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [1]
        vectors = cf.gen_vectors_based_on_vector_type(nq, default_dim, "FLOAT_VECTOR")
        log.debug("binbin")
        log.debug(vectors)
        # 4. get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 32288")
    @pytest.mark.parametrize("nq", [0, 16385])
    def test_hybrid_search_normal_over_max_nq(self, nq):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [1]
        vectors = cf.gen_vectors_based_on_vector_type(nq, default_dim, "FLOAT_VECTOR")
        # 4. get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 5. hybrid search
        err_msg = "nq (number of search vector per search request) should be in range [1, 16384]"
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.err_res,
                                   check_items={"err_code": 65535,
                                                "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_no_limit(self):
        """
        target: test hybrid search with no limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        vectors = cf.gen_vectors_based_on_vector_type(nq, default_dim, "FLOAT_VECTOR")

        # get hybrid search req list
        search_param = {
            "data": vectors,
            "anns_field": vector_name_list[0],
            "param": {"metric_type": "COSINE"},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_WeightedRanker_empty_reqs(self, primary_field):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. hybrid search with empty reqs
        collection_w.hybrid_search([], WeightedRanker(), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 0})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 29839")
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_as_search(self, nq, primary_field, is_flush):
        """
        target: test hybrid search to search as the original search interface
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK), and the result should be equal to search
        """
        # 1. initialize collection with data
        dim = 3
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]

        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        for search_field in vector_name_list:
            # 2. prepare search params
            req_list = []
            search_param = {
                "data": vectors,
                "anns_field": search_field,
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # 3. hybrid search
            hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(1), default_limit,
                                                    check_task=CheckTasks.check_search_results,
                                                    check_items={"nq": nq,
                                                                 "ids": insert_ids,
                                                                 "limit": default_limit})[0]
            search_res = collection_w.search(vectors[:nq], search_field,
                                            default_search_params, default_limit,
                                            default_search_exp,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nq,
                                                         "ids": insert_ids,
                                                         "limit": default_limit})[0]
            # 4. the effect of hybrid search to one field should equal to search
            log.info("The distance list is:\n")
            for i in range(nq):
                log.info(hybrid_res[0].distances)
                log.info(search_res[0].distances)
                assert hybrid_res[i].ids == search_res[i].ids

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_different_metric_type(self, nq, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different metric type
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 128
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_flush=is_flush, is_index=False,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for vector_name in vector_name_list:
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
                "anns_field": vector_name,
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9, 1), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_different_metric_type_each_field(self, nq, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different metric type
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 91
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_flush=is_flush, is_index=False,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "L2"}
        collection_w.create_index(vector_name_list[0], flat_index)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "IP"}
        collection_w.create_index(vector_name_list[1], flat_index)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        collection_w.create_index(vector_name_list[2], flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        search_param = {
            "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
            "anns_field": vector_name_list[0],
            "param": {"metric_type": "L2", "offset": 0},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)
        req_list.append(req)
        search_param = {
            "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
            "anns_field": vector_name_list[1],
            "param": {"metric_type": "IP", "offset": 0},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)
        req_list.append(req)
        search_param = {
            "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
            "anns_field": vector_name_list[2],
            "param": {"metric_type": "COSINE", "offset": 0},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)
        req_list.append(req)
        # 4. hybrid search
        hybrid_search = collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9, 1), default_limit,
                                                   check_task=CheckTasks.check_search_results,
                                                   check_items={"nq": nq,
                                                                "ids": insert_ids,
                                                                "limit": default_limit})[0]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    @pytest.mark.skip(reason="issue 29923")
    def test_hybrid_search_different_dim(self, nq, primary_field, metric_type):
        """
        target: test hybrid search for fields with different dim
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        default_limit = 100
        # 1. initialize collection with data
        dim = 121
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        hybrid_search_0 = collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                                     check_task=CheckTasks.check_search_results,
                                                     check_items={"nq": nq,
                                                                  "ids": insert_ids,
                                                                  "limit": default_limit})[0]
        hybrid_search_1 = collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                                     check_task=CheckTasks.check_search_results,
                                                     check_items={"nq": nq,
                                                                  "ids": insert_ids,
                                                                  "limit": default_limit})[0]
        for i in range(nq):
            assert hybrid_search_0[i].ids == hybrid_search_1[i].ids
            assert hybrid_search_0[i].distances == hybrid_search_1[i].distances

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_overall_limit_larger_sum_each_limit(self, nq, primary_field, metric_type):

        """
        target: test hybrid search: overall limit which is larger than sum of each limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 200
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        id_list_nq = []
        vectors = []
        default_search_params = {"metric_type": metric_type, "offset": 0}
        for i in range(len(vector_name_list)):
            vectors.append([])
        for i in range(nq):
            id_list_nq.append([])
        for k in range(nq):
            for i in range(len(vector_name_list)):
                vectors_search = [random.random() for _ in range(multiple_dim_array[i])]
                vectors[i].append(vectors_search)
        # 4. search for the comparision for hybrid search
        for i in range(len(vector_name_list)):
            search_res = collection_w.search(vectors[i], vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": nq,
                                                          "ids": insert_ids,
                                                          "limit": default_limit})[0]
            for k in range(nq):
                id_list_nq[k].extend(search_res[k].ids)
        # 5. prepare hybrid search params
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors[i],
                "anns_field": vector_name_list[i],
                "param": default_search_params,
                "limit": default_limit,
                "expr": default_search_exp}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 6. hybrid search
        hybrid_search = \
            collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit * len(req_list) + 1)[0]
        assert len(hybrid_search) == nq
        for i in range(nq):
            assert len(hybrid_search[i].ids) == len(list(set(id_list_nq[i])))
            assert set(hybrid_search[i].ids) == set(id_list_nq[i])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_overall_different_limit(self, primary_field, metric_type):
        """
        target: test hybrid search with different limit params
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 100
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit - i,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_min_limit(self, primary_field, metric_type):
        """
        target: test hybrid search with minimum limit params
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 99
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        id_list = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(1)]
            search_params = {"metric_type": metric_type, "offset": 0}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": search_params,
                "limit": min_dim,
                "expr": default_search_exp}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             search_params, min_dim,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": min_dim})[0]
            id_list.extend(search_res[0].ids)
        # 4. hybrid search
        hybrid_search = collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit)[0]
        assert len(hybrid_search) == 1
        assert len(hybrid_search[0].ids) == len(list(set(id_list)))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_max_limit(self, primary_field, metric_type):
        """
        target: test hybrid search with maximum limit params
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 66
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type},
                "limit": max_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_max_min_limit(self, primary_field, metric_type):
        """
        target: test hybrid search with maximum and minimum limit params
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 66
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            limit = max_limit
            if i == 1:
                limit = 1
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type},
                "limit": limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_same_anns_field(self, primary_field, metric_type):
        """
        target: test hybrid search: multiple search on same anns field
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 55
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[0],
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_different_offset_single_field(self, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different offset
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 100
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=False, dim=dim, is_flush=is_flush, is_index=False,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type, "offset": i},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9, 1), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_max_reqs_num(self, primary_field):
        """
        target: test hybrid search with maximum reqs number
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 128
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        reqs_max_num = max_hybrid_search_req_num
        # 3. prepare search params
        req_list = []
        for i in range(reqs_max_num):
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(1)],
                "anns_field": default_search_field,
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        weights = [random.random() for _ in range(len(req_list))]
        log.info(weights)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_WeightedRanker_different_parameters(self, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different offset
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 63
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=True, dim=dim, is_flush=is_flush, is_index=False,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(1)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type, "offset": i},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.2, 0.03, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("issue: #29840")
    def test_hybrid_search_invalid_WeightedRanker_params(self):
        """
        target: test hybrid search with invalid params type to WeightedRanker
        method: create connection, collection, insert and search
        expected: raise exception
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim, is_index=False,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        reqs_num = 2
        # 3. prepare search params
        req_list = []
        for i in range(reqs_num):
            search_param = {
                "data": [[random.random() for _ in range(default_dim)] for _ in range(1)],
                "anns_field": default_search_field,
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with list in WeightedRanker
        collection_w.hybrid_search(req_list, WeightedRanker([0.9, 0.1]), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit})
        # 5. hybrid search with two-dim list in WeightedRanker
        weights = [[random.random() for _ in range(1)] for _ in range(len(req_list))]
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_over_maximum_reqs_num(self):
        """
        target: test hybrid search over maximum reqs number
        method: create connection, collection, insert and search
        expected: raise exception
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim, is_index=False,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        reqs_max_num = max_hybrid_search_req_num + 1
        # 3. prepare search params
        req_list = []
        for i in range(reqs_max_num):
            search_param = {
                "data": [[random.random() for _ in range(default_dim)] for _ in range(1)],
                "anns_field": default_search_field,
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        weights = [random.random() for _ in range(len(req_list))]
        log.info(weights)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.err_res,
                                   check_items={"err_code": 65535,
                                                "err_msg": 'maximum of ann search requests is 1024'})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_range_search(self, primary_field):
        """
        target: test hybrid search with range search
        method: create connection, collection, insert and search
        expected: raise exception (not support yet)
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim, is_index=False,
                                         primary_field=primary_field,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        reqs_max_num = 2
        # 3. prepare search params
        req_list = []
        for i in range(reqs_max_num):
            search_param = {
                "data": [[random.random() for _ in range(default_dim)] for _ in range(1)],
                "anns_field": default_search_field,
                "param": {"metric_type": "COSINE", "params": {"radius": 0, "range_filter": 1000}},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        weights = [random.random() for _ in range(len(req_list))]
        log.info(weights)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_RRFRanker_default_parameter(self, primary_field):
        """
        target: test hybrid search with default value to RRFRanker
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True,  dim=default_dim, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params for each vector field
        req_list = []
        search_res_dict_array = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_res_dict = {}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the base line of hybrid_search
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": default_limit})[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1/(j + 60 +1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search base line for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_search_0 = collection_w.hybrid_search(req_list, RRFRanker(), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            assert score_answer[i] - hybrid_search_0[0].distances[i] < hybrid_search_epsilon
        # 7. run hybrid search with the same parameters twice, and compare the results
        hybrid_search_1 = collection_w.hybrid_search(req_list, RRFRanker(), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]

        assert hybrid_search_0[0].ids == hybrid_search_1[0].ids
        assert hybrid_search_0[0].distances == hybrid_search_1[0].distances

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("k", [1, 60, 1000, 16383])
    @pytest.mark.parametrize("offset", [0, 1, 5])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/32650")
    def test_hybrid_search_RRFRanker_different_k(self, is_flush, k, offset):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 200
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=False, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params for each vector field
        req_list = []
        search_res_dict_array = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(dim)] for _ in range(1)]
            search_res_dict = {}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the baseline of hybrid_search
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp, offset=0,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": default_limit})[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1/(j + k +1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search baseline for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, RRFRanker(k), default_limit,
                                                offset=offset,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            assert score_answer[i] - hybrid_res[0].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [0, 1, 5])
    @pytest.mark.parametrize("rerank", [RRFRanker(), WeightedRanker(0.1, 0.9, 1)])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_offset_inside_outside_params(self, primary_field, offset, rerank):
        """
        target: test hybrid search with offset inside and outside params
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK), and the result should be the same
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        req_list = []
        vectors_list = []
        # 3. generate vectors
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            vectors_list.append(vectors)
        # 4. prepare search params for each vector field
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors_list[i],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": offset},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with offset inside the params
        hybrid_res_inside = collection_w.hybrid_search(req_list, rerank, default_limit,
                                                       check_task=CheckTasks.check_search_results,
                                                       check_items={"nq": 1,
                                                                    "ids": insert_ids,
                                                                    "limit": default_limit})[0]
        # 5. hybrid search with offset parameter
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors_list[i],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        hybrid_res = collection_w.hybrid_search(req_list, rerank, default_limit-offset,
                                                offset=offset,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit-offset})[0]

        assert hybrid_res_inside[0].distances[offset:] == hybrid_res[0].distances

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_RRFRanker_empty_reqs(self):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. hybrid search with empty reqs
        collection_w.hybrid_search([], RRFRanker(), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 0})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("k", [0, 16385])
    @pytest.mark.skip(reason="issue #29867")
    def test_hybrid_search_RRFRanker_k_out_of_range(self, k):
        """
        target: test hybrid search with default value to RRFRanker
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True,  dim=default_dim,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params for each vector field
        req_list = []
        search_res_dict_array = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_res_dict = {}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the base line of hybrid_search
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": default_limit})[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1/(j + k +1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search base line for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, RRFRanker(k), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            delta = math.fabs(score_answer[i] - hybrid_res[0].distances[i])
            assert delta < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [1, 100, 16384])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_different_limit_round_decimal(self, primary_field, limit):
        """
        target: test hybrid search with different valid limit and round decimal
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        search_res_dict_array = []
        if limit > default_nb:
            limit = default_limit
        metrics = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_res_dict = {}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")
            # search to get the base line of hybrid_search
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             default_search_params, limit,
                                             default_search_exp, round_decimal= 5,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": limit})[0]
            ids = search_res[0].ids
            distance_array = search_res[0].distances
            for j in range(len(ids)):
                search_res_dict[ids[j]] = distance_array[j]
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search base line
        ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array, weights, metrics, 5)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), limit,
                                                round_decimal=5,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": limit})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:limit])):
            delta = math.fabs(score_answer[i] - hybrid_res[0].distances[i])
            assert delta < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_limit_out_of_range_max(self):
        """
        target: test hybrid search with over maximum limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with over maximum limit
        limit = 16385
        error = {ct.err_code: 65535, ct.err_msg: "invalid max query result window, (offset+limit) "
                                                 "should be in range [1, 16384], but got %d" % limit}
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), limit,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_limit_out_of_range_min(self):
        """
        target: test hybrid search with over minimum limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with over maximum limit
        limit = 0
        error = {ct.err_code: 1, ct.err_msg: "`limit` value 0 is illegal"}
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), limit,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_output_fields(self, nq, dim, auto_id, is_flush, enable_dynamic_field,
                                  primary_field, vector_data_type):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        nq = 10
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                             single_search_param, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": default_limit})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        output_fields = [default_int64_field_name]
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                output_fields=output_fields,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_output_fields_all_fields(self, nq, dim, auto_id, is_flush, enable_dynamic_field,
                                  primary_field, vector_data_type):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        nq = 10
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                             single_search_param, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": default_limit})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        output_fields = [default_int64_field_name, default_float_field_name, default_string_field_name,
                         default_json_field_name]
        output_fields = output_fields + vector_name_list
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                output_fields=output_fields,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_output_fields_all_fields(self, nq, dim, auto_id, is_flush, enable_dynamic_field,
                                  primary_field, vector_data_type):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        nq = 10
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                             single_search_param, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": default_limit})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                output_fields= ["*"],
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", [[default_search_field], [default_search_field, default_int64_field_name]])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_output_fields_sync_async(self, nq, primary_field, output_fields, _async):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim,
                                         primary_field=primary_field,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors_based_on_vector_type(nq, default_dim, "FLOAT_VECTOR")

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                                 single_search_param, default_limit,
                                                 default_search_exp, _async=_async,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "ids": insert_ids,
                                                              "limit": default_limit,
                                                              "_async": _async})[0]
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                output_fields=output_fields, _async=_async,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "_async": _async})[0]
        if _async:
            hybrid_res.done()
            hybrid_res = hybrid_res.result()
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("rerank", [RRFRanker(), WeightedRanker(0.1, 0.9, 1)])
    def test_hybrid_search_offset_both_inside_outside_params(self, rerank):
        """
        target: test hybrid search with offset inside and outside params
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: Raise exception
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        req_list = []
        vectors_list = []
        # 3. generate vectors
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            vectors_list.append(vectors)
        # 4. prepare search params for each vector field
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors_list[i],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with offset inside the params
        error = {ct.err_code: 1, ct.err_msg: "Provide offset both in kwargs and param, expect just one"}
        collection_w.hybrid_search(req_list, rerank, default_limit, offset=2,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [1, 100, 16384])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_is_partition_key(self, nq, primary_field, limit, vector_data_type):
        """
        target: test hybrid search with different valid limit and round decimal
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim],
                                         vector_data_type = vector_data_type,
                                         is_partition_key=ct.default_float_field_name)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors_based_on_vector_type(nq, default_dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                                 single_search_param, default_limit,
                                                 default_search_exp,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "ids": insert_ids,
                                                              "limit": default_limit})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_result_L2_order(self, nq):
        """
        target: test hybrid search result having correct order for L2 distance
        method: create connection, collection, insert and search
        expected: hybrid search successfully and result order is correct
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, is_index=False, multiple_dim_array=[default_dim, default_dim])[0:5]

        # 2. create index
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for i  in range(len(vector_name_list)) :
            default_index = { "index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128},}
            collection_w.create_index(vector_name_list[i], default_index)
        collection_w.load()

        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(nq)]
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "L2", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), 10)[0]
        is_sorted_descend = lambda lst: all(lst[i] >= lst[i + 1] for i in range(len(lst) - 1))
        for i in range(nq):
            assert is_sorted_descend(res[i].distances)

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_result_order(self, nq):
        """
        target: test hybrid search result having correct order for cosine distance
        method: create connection, collection, insert and search
        expected: hybrid search successfully and result order is correct
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(nq)]
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), 10)[0]
        is_sorted_descend = lambda lst: all(lst[i] >= lst[i+1] for i in range(len(lst)-1))
        for i in range(nq):
            assert is_sorted_descend(res[i].distances)

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_sparse_normal(self):
        """
        target: test hybrid search after loading sparse vectors
        method: Test hybrid search after loading sparse vectors
        expected: hybrid search successfully with limit(topK)
        """
        nb, auto_id, dim, enable_dynamic_field = 20000, False, 768, False
        # 1. init collection
        collection_w, insert_vectors, _, insert_ids = self.init_collection_general(prefix, True, nb=nb,
                                                    multiple_dim_array=[dim, dim*2], with_json=False,
                                                    vector_data_type="SPARSE_FLOAT_VECTOR")[0:4]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        # 3. prepare search params
        req_list = []
        search_res_dict_array = []
        k = 60

        for i in range(len(vector_name_list)):
            # vector = cf.gen_sparse_vectors(1, dim)
            vector = insert_vectors[0][i+3][-1:]
            search_res_dict = {}
            search_param = {
                "data": vector,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "IP", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the base line of hybrid_search
            search_res = collection_w.search(vector, vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp,
                                             )[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1/(j + k +1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search base line for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, RRFRanker(k), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            delta = math.fabs(score_answer[i] - hybrid_res[0].distances[i])
            assert delta < hybrid_search_epsilon


class TestSparseSearch(TestcaseBase):
    """ Add some test cases for the sparse vector """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_sparse_index_search(self, index):
        """
        target: verify that sparse index for sparse vectors can be searched properly
        method: create connection, collection, insert and search
        expected: search successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema(auto_id=False)
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(nb=4000)
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)

        collection_w.load()
        collection_w.search(data[-1][-1:], ct.default_sparse_vec_field_name,
                            ct.default_sparse_search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})
        expr = "int64 < 100 "
        collection_w.search(data[-1][-1:], ct.default_sparse_vec_field_name,
                            ct.default_sparse_search_params, default_limit,
                            expr,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq})

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
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(dim=dim)
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)

        collection_w.load()
        collection_w.search(data[-1][-1:], ct.default_sparse_vec_field_name,
                            ct.default_sparse_search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #31485")
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_sparse_index_enable_mmap_search(self, index):
        """
        target: verify that the sparse indexes of sparse vectors can be searched properly after turning on mmap
        method: create connection, collection, enable mmap,  insert and search
        expected: search successfully , query result is correct
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema(auto_id=False)
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=schema)

        data = cf.gen_default_list_sparse_data()
        collection_w.insert(data)

        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)

        collection_w.set_properties({'mmap.enabled': True})
        pro = collection_w.describe().get("properties")
        assert pro["mmap.enabled"] == 'True'
        collection_w.alter_index(index, {'mmap.enabled': True})
        assert collection_w.index().params["mmap.enabled"] == 'True'
        collection_w.load()
        collection_w.search(data[-1][-1:], ct.default_sparse_vec_field_name,
                            ct.default_sparse_search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})
        term_expr = f'{ct.default_int64_field_name} in [0, 1, 10, 100]'
        res = collection_w.query(term_expr)
        assert len(res) == 4

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("ratio", [0.01, 0.1, 0.5, 0.9])
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_search_sparse_ratio(self, ratio, index):
        """
        target: create a sparse index by adjusting the ratio parameter.
        method: create a sparse index by adjusting the ratio parameter.
        expected: search successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema(auto_id=False)
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(nb=4000)
        collection_w.insert(data)
        params = {"index_type": index, "metric_type": "IP", "params": {"drop_ratio_build": ratio}}
        collection_w.create_index(ct.default_sparse_vec_field_name, params, index_name=index)
        collection_w.load()
        assert collection_w.has_index(index_name=index) == True
        search_params = {"metric_type": "IP", "params": {"drop_ratio_search": ratio}}
        collection_w.search(data[-1][-1:], ct.default_sparse_vec_field_name,
                            search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})

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
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(nb=4000)
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)

        collection_w.load()
        d = cf.gen_default_list_sparse_data(nb=1)
        collection_w.search(d[-1][-1:], ct.default_sparse_vec_field_name,
                            ct.default_sparse_search_params, 5,
                            output_fields=["float", "sparse_vector"],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "output_fields": ["float", "sparse_vector"]
                                          })

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_sparse_vector_search_iterator(self, index):
        """
        target: create sparse vectors and search iterator
        method: create sparse vectors and search iterator
        expected: normal search
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(nb=4000)
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)

        collection_w.load()
        batch_size = 10
        collection_w.search_iterator(data[-1][-1:], ct.default_sparse_vec_field_name,
                                     ct.default_sparse_search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"batch_size": batch_size})
