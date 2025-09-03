import pytest
from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
import pandas as pd
import numpy as np
import random
from pymilvus import Function, FunctionType

prefix = "milvus_client_api_query"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'
exp_res = "exp_res"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name


class TestMilvusClientQueryInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_not_all_required_params(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. query using ids
        error = {ct.err_code: 65535, ct.err_msg: f"empty expression should be used with limit"}
        self.query(client, collection_name,
                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_no_collection(self):
        """
        target: test the scenario which query the non-exist collection
        method: 1. create collection
                2. drop collection
                3. query the dropped collection
        expected: raise exception and report the error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        # 2. drop collection
        self.drop_collection(client, collection_name)
        # 3. query the dropped collection
        error = {"err_code": 1, "err_msg": "collection not found"}
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr", ["12-s", "中文", "a"])
    def test_milvus_client_query_expr_invalid_string(self, expr):
        """
        target: test query with invalid expr
        method: query with invalid string expr
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. query with invalid string expression
        if expr == "中文":
            error = {ct.err_code: 1100, ct.err_msg: "cannot parse expression"}
        elif expr == "a" or expr == "12-s":
            error = {ct.err_code: 1100, ct.err_msg: f"predicate is not a boolean expression: {expr}"}
        self.query(client, collection_name, filter=expr,
                   check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("filter", [None, ""])
    def test_milvus_client_query_expr_none(self, filter):
        """
        target: test query with none expr
        method: query with expr None
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. query with None filter with no limit and no offset
        error = {ct.err_code: 0, ct.err_msg: "empty expression should be used with limit: invalid parameter"}
        self.query(client, collection_name, filter=filter,
                   check_task=CheckTasks.err_res, check_items=error)
        # 4. query with offset but no limit
        self.query(client, collection_name, filter=filter, offset=1, 
                   check_task=CheckTasks.err_res, check_items=error)          
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_query_expr_not_existed_field(self, enable_dynamic_field):
        """
        target: test query with not existed field
        method: query by term expr with fake field
        expected: raise exception

        Please note that when the query request contains non-existent output fields:
        - If enable_dynamic_field=True, the query will succeed but only return empty results.
        - If enable_dynamic_field=False, an error will be thrown: field not exist.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", enable_dynamic_field=enable_dynamic_field)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. query with non-existent field
        term_expr = 'invalid_field in [1, 2]'
        if enable_dynamic_field :
            self.query(client, collection_name, filter=term_expr, 
                  check_task=CheckTasks.check_query_results,
                  check_items={exp_res: [], "pk_name": ct.default_int64_field_name})
        else :
            error = {ct.err_code: 65535,
                     ct.err_msg: f"cannot parse expression: {term_expr}, error: field invalid_field not exist"}
            self.query(client, collection_name, filter=term_expr,
                       check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("output_fields", [["int"], [default_primary_key_field_name, "int"]])
    def test_milvus_client_query_output_not_existed_field(self, enable_dynamic_field, output_fields):
        """
        target: test query output not existed field
        method: query with not existed output field
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", enable_dynamic_field=enable_dynamic_field)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. test query with non-existent fields
        if enable_dynamic_field :
            exp_res = [{default_primary_key_field_name: i} for i in range(default_nb)]
            self.query(client, collection_name, filter=default_search_exp, output_fields=output_fields,
              check_task=CheckTasks.check_query_results,
              check_items={"exp_res": exp_res, "pk_name": ct.default_int64_field_name})
        else :
            error = {ct.err_code: 65535, ct.err_msg: 'field int not exist'}
            self.query(client, collection_name, filter=default_search_exp, output_fields=output_fields,
                   check_task=CheckTasks.err_res, check_items=error)
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_wrong_term_keyword(self):
        """
        target: test query with wrong term expr keyword
        method: query with wrong keyword term expr
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 4. test wrong term expressions
        expr_1 = f'{ct.default_int64_field_name} inn [1, 2]'
        error_1 = {ct.err_code: 65535, ct.err_msg: "cannot parse expression: int64 inn [1, 2], "
                                                   "error: invalid expression: int64 inn [1, 2]"}
        self.query(client, collection_name, filter=expr_1, check_task=CheckTasks.err_res, check_items=error_1)

        expr_2 = f'{ct.default_int64_field_name} in not [1, 2]'
        error_2 = {ct.err_code: 65535, ct.err_msg: "cannot parse expression: int64 in not [1, 2], "
                                                   "error: not can only apply on boolean: invalid parameter"}
        self.query(client, collection_name, filter=expr_2, check_task=CheckTasks.err_res, check_items=error_2)
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_non_array_term(self):
        """
        target: test query with non-array term expr
        method: query with non-array term expr
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. test non-array term expressions
        exprs = [f'{ct.default_int64_field_name} in 1',
                 f'{ct.default_int64_field_name} in "in"']
        for expr in exprs:
            error = {ct.err_code: 1100, ct.err_msg: f"cannot parse expression: {expr}, "
                                                    "error: the right-hand side of 'in' must be a list"}
            self.query(client, collection_name, filter=expr, check_task=CheckTasks.err_res, check_items=error)
        
        expr = f'{ct.default_int64_field_name} in (mn)'
        error = {ct.err_code: 1100, ct.err_msg: f"cannot parse expression: {expr}, "
                                                "error: value '(mn)' in list cannot be a non-const expression"}
        self.query(client, collection_name, filter=expr, check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("constant", [[1], (), {}])
    def test_milvus_client_query_expr_non_constant_array_term(self, constant):
        """
        target: test query with non-constant array term expr
        method: query with non-constant array expr
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", enable_dynamic_field=False)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. test non-constant array term expressions
        error = {ct.err_code: 1100,
                 ct.err_msg: f"cannot parse expression: int64 in [{constant}]"}
        term_expr = f'{ct.default_int64_field_name} in [{constant}]'
        self.query(client, collection_name, filter=term_expr, check_task=CheckTasks.err_res, check_items=error)
        # 4. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_inconsistent_mix_term_array(self):
        """
        target: test query with term expr that field and array are inconsistent or mix type
        method: 1.query with int field and float values
                2.query with term expr that has int and float type value
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", enable_dynamic_field=False, auto_id=False)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. test inconsistent mix term array
        values = [1., 2.]
        term_expr = f'{default_primary_key_field_name} in {values}'
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: cannot parse expression: {term_expr}, "
                             "error: value 'float_val:1' in list cannot be casted to Int64"}
        self.query(client, collection_name, filter=term_expr, check_task=CheckTasks.err_res, check_items=error)

        values = [1, 2.]
        term_expr = f'{default_primary_key_field_name} in {values}'
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: cannot parse expression: {term_expr}, "
                             "error: value 'float_val:2' in list cannot be casted to Int64"}
        self.query(client, collection_name, filter=term_expr, check_task=CheckTasks.err_res, check_items=error)
        # 4. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_any", "JSON_CONTAINS_ANY",
                                             "json_contains_all", "JSON_CONTAINS_ALL"])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("not_list", ["str", {1, 2, 3}, (1, 2, 3), 10])
    def test_milvus_client_query_expr_json_contains_invalid_type(self, expr_prefix, enable_dynamic_field, not_list):
        """
        target: test query with expression using json_contains_any
        method: query with expression using json_contains_any
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        nb = 10
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        for i in range(nb):
            rows[i][ct.default_json_field_name] = {"number": i,
                                                   "list": [m for m in range(i, i + 10)]}
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with invalid type
        expression = f"{expr_prefix}({ct.default_json_field_name}['list'], {not_list})"
        error = {ct.err_code: 1100, ct.err_msg: f"failed to create query plan: cannot parse expression: {expression}"}
        self.query(client, collection_name, filter=expression, check_task=CheckTasks.err_res, check_items=error)
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_query_expr_with_limit_offset_out_of_range(self):
        """
        target: test query with empty expression
        method: query empty expression with limit and offset out of range
        expected: raise error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with limit > 16384
        error = {ct.err_code: 1,
                 ct.err_msg: "invalid max query result window, (offset+limit) should be in range [1, 16384]"}
        self.query(client, collection_name, filter="", limit=16385, check_task=CheckTasks.err_res, check_items=error)
        # 5. query with offset + limit > 16384
        self.query(client, collection_name, filter="", limit=1, offset=16384, check_task=CheckTasks.err_res, check_items=error)
        self.query(client, collection_name, filter="", limit=16384, offset=1, check_task=CheckTasks.err_res, check_items=error)
        # 6. query with offset < 0
        error = {ct.err_code: 1,
                 ct.err_msg: "invalid max query result window, offset [-1] is invalid, should be gte than 0"}
        self.query(client, collection_name, filter="", limit=2, offset=-1, check_task=CheckTasks.err_res, check_items=error)
        # 7. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", [["*%"], ["**"], ["*", "@"]])
    def test_milvus_client_query_invalid_wildcard(self, output_fields):
        """
        target: test query with invalid output wildcard
        method: output_fields is invalid output wildcard
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=100, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. query with invalid output_fields wildcard
        error = {ct.err_code: 65535, ct.err_msg: f"parse output field name failed"}
        self.query(client, collection_name, filter=default_term_expr, output_fields=output_fields,
                   check_task=CheckTasks.err_res, check_items=error)
        # 4. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_partition_without_loading_collection(self):
        """
        target: test query on partition without loading collection
        method: query on partition and no loading collection
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        self.create_partition(client, collection_name, partition_name)
        self.release_collection(client, collection_name)
        # 3. insert data to partition
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows, partition_name=partition_name)
        # 4. verify partition has data
        self.flush(client, collection_name)
        partition_info = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert partition_info['row_count'] == default_nb, f"Expected {default_nb} entities in partition, got {partition_info['row_count']}"
        # 5. query on partition without loading collection
        error = {ct.err_code: 65535, ct.err_msg: "collection not loaded"}
        self.query(client, collection_name, filter=default_term_expr, partition_names=[partition_name],
                   check_task=CheckTasks.err_res, check_items=error)
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_partition_without_loading_partiton(self):
        """
        target: Verify that querying an unloaded partition raises an exception.
        method: 1. Create a collection and two partitions.
                2. Insert data into both partitions.
                3. Load only one partition.
                4. Attempt to query the other partition that is not loaded.
        expected: The query should fail with an error indicating the partition is not loaded.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name1 = cf.gen_unique_str("partition1")
        partition_name2 = cf.gen_unique_str("partition2")
        # 1. create collection and partition
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        self.create_partition(client, collection_name, partition_name1)
        self.create_partition(client, collection_name, partition_name2)
        self.release_collection(client, collection_name)
        # 2. insert data to partition
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows, partition_names=[partition_name1, partition_name2])
        self.flush(client, collection_name)
        self.load_partitions(client, collection_name, partition_name1)
        # 3. query on partition without loading
        error = {ct.err_code: 65535, ct.err_msg: f"partition name {partition_name2} not found"}
        self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name2],
                   check_task=CheckTasks.err_res, check_items=error)
        # 4. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_empty_partition_names(self):
        """
        target: test query with empty partition_names
        method: query with partition_names=[]
        expected: query from all partitions
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # 1. create collection and partition
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        self.create_partition(client, collection_name, partition_name)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 2. insert [0, half) into partition_w, [half, nb) into _default
        half = default_nb // 2
        schema_info = self.describe_collection(client, collection_name)[0]
        # Insert first half into custom partition
        rows_partition = cf.gen_row_data_by_schema(nb=half, schema=schema_info, start=0)
        self.insert(client, collection_name, rows_partition, partition_name=partition_name)
        # Insert second half into default partition
        rows_default = cf.gen_row_data_by_schema(nb=half, schema=schema_info, start=half)
        self.insert(client, collection_name, rows_default)
        # 3. create index and load both partitions
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_partitions(client, collection_name, [partition_name, ct.default_partition_name])
        # 4. query from empty partition_names (should query all partitions)
        term_expr = f'{default_primary_key_field_name} in [0, {half}, {default_nb-1}]'
        # Prepare expected results by combining data from both partitions
        all_rows = rows_partition + rows_default
        exp_res = [all_rows[0], all_rows[half], all_rows[default_nb-1]]
        self.query(client, collection_name, filter=term_expr, partition_names=[],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": exp_res, "with_vec": True, "pk_name": default_primary_key_field_name})
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_on_specific_partition(self):
        """
        Target: Verify that querying a specific partition only returns data from that partition.
        Method:
        1. Create a collection and two partitions (partition1 and partition2).
        2. Insert the first half of the data into partition1 and the second half into partition2.
        3. Create an index and load both partitions.
        4. Query only partition1 and check that the returned results only contain data from partition1.
        5. Clean up the collection.
        Expected: Only data from partition1 is returned and matches the inserted data.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name1 = cf.gen_unique_str("partition1")
        partition_name2 = cf.gen_unique_str("partition2")
        # 1. create collection and partition
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        self.create_partition(client, collection_name, partition_name1)
        self.create_partition(client, collection_name, partition_name2)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 2. insert data to partition
        half = default_nb // 2
        schema_info = self.describe_collection(client, collection_name)[0]
        rows_partition1 = cf.gen_row_data_by_schema(nb=half, schema=schema_info, start=0)
        rows_partition2 = cf.gen_row_data_by_schema(nb=half, schema=schema_info, start=half)
        self.insert(client, collection_name, rows_partition1, partition_name=partition_name1)
        self.insert(client, collection_name, rows_partition2, partition_name=partition_name2)
        self.flush(client, collection_name)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_partitions(client, collection_name, [partition_name1, partition_name2])
        # 4. query on partition
        self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name1],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": rows_partition1, "with_vec": True, "pk_name": default_primary_key_field_name})
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_empty_partition(self):
        """
        target: test query on empty partition
        method: query on an empty partition
        expected: empty query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # 1. create collection and partition
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        self.create_partition(client, collection_name, partition_name)
        # 2. verify partition is empty
        partition_info = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert partition_info['row_count'] == 0
        # 3. create index and load partition
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_partitions(client, collection_name, [partition_name])
        # 4. query on empty partition
        res = self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name])[0]
        assert len(res) == 0
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_not_existed_partition(self):
        """
        target: test query on a not existed partition
        method: query on not existed partition
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create index and load
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 3. query on non-existent partition
        partition_name = cf.gen_unique_str()
        error = {ct.err_code: 65535, ct.err_msg: f'partition name {partition_name} not found'}
        self.query(client, collection_name, filter=default_term_expr, partition_names=[partition_name],
                   check_task=CheckTasks.err_res, check_items=error)
        # 4. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ignore_growing", [2.3, "str"])
    def test_milvus_client_query_invalid_ignore_growing_param(self, ignore_growing):
        """
        target: test query ignoring growing segment param invalid
        method: 1. create a collection, insert data and load
                2. insert data again
                3. query with ignore_growing type invalid
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong", auto_id=False)
        # 2. insert initial data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. insert data again
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=100)
        self.insert(client, collection_name, new_rows)
        self.flush(client, collection_name)
        # 5. query with param ignore_growing invalid
        error = {ct.err_code: 999, ct.err_msg: "parse ignore growing field failed"}
        self.query(client, collection_name, filter=default_search_exp, ignore_growing=ignore_growing,
                   check_task=CheckTasks.err_res, check_items=error)
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", ["12 s", " ", [0, 1], {2}])
    def test_milvus_client_query_pagination_with_invalid_limit_type(self, limit):
        """
        target: test query pagination with invalid limit type
        method: query with invalid limit type
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load collection
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with invalid limit type
        error = {ct.err_code: 1, ct.err_msg: f"limit [{limit}] is invalid"}
        self.query(client, collection_name, filter=default_search_exp, 
                  offset=10, limit=limit,
                  check_task=CheckTasks.err_res, check_items=error)
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [-1, 67890])
    def test_milvus_client_query_pagination_with_invalid_limit_value(self, limit):
        """
        target: test query pagination with invalid limit value
        method: query with invalid limit value
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load collection
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with invalid limit value
        error = {ct.err_code: 65535,
                 ct.err_msg: f"invalid max query result window, (offset+limit) should be in range [1, 16384], but got 67900"}
        if limit == -1:
            error = {ct.err_code: 65535,
                     ct.err_msg: f"invalid max query result window, limit [{limit}] is invalid, should be greater than 0"}
        self.query(client, collection_name, filter=default_search_exp, 
                  offset=10, limit=limit,
                  check_task=CheckTasks.err_res, check_items=error)
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", ["12 s", " ", [0, 1], {2}])
    def test_milvus_client_query_pagination_with_invalid_offset_type(self, offset):
        """
        target: test query pagination with invalid offset type
        method: query with invalid offset type
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load collection
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with invalid offset type
        error = {ct.err_code: 1, ct.err_msg: f"offset [{offset}] is invalid"}
        self.query(client, collection_name, filter=default_search_exp, 
                  offset=offset, limit=10,
                  check_task=CheckTasks.err_res, check_items=error)
        # 5. clean up
        self.drop_collection(client, collection_name)


class TestMilvusClientQueryValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[0, 10, 100])
    def offset(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_default(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. query using ids
        self.query(client, collection_name, ids=[i for i in range(default_nb)],
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 4. query using filter
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_output_fields(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. query using ids
        self.query(client, collection_name, ids=[i for i in range(default_nb)],
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 4. query using filter
        res = self.query(client, collection_name, filter=default_search_exp,
                         output_fields=[default_primary_key_field_name, default_float_field_name,
                                        default_string_field_name, default_vector_field_name],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: rows,
                                      "with_vec": True,
                                      "pk_name": default_primary_key_field_name})[0]
        assert set(res[0].keys()) == {default_primary_key_field_name, default_vector_field_name,
                                      default_float_field_name, default_string_field_name}
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_output_fields_dynamic_name(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert, add field name(same as dynamic name) and query
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, max_length=64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_float_field_name, DataType.FLOAT, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.add_collection_field(client, collection_name, field_name=default_string_field_name,
                                  data_type=DataType.VARCHAR, nullable=True, default_value="default", max_length=64)
        for row in rows:
            row[default_string_field_name] = "default"
        # 3. query using ids
        self.query(client, collection_name, ids=[i for i in range(default_nb)],
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 4. query using filter
        res = self.query(client, collection_name, filter=default_search_exp,
                         output_fields=[f'$meta["{default_string_field_name}"]'],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: [{"id": item["id"]} for item in rows],
                                      "with_vec": True,
                                      "pk_name": default_primary_key_field_name})[0]
        assert set(res[0].keys()) == {default_primary_key_field_name}
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("vector_field, vector_type", [
    (ct.default_float_vec_field_name, DataType.FLOAT_VECTOR),
    (ct.default_float16_vec_field_name, DataType.FLOAT16_VECTOR),
    (ct.default_bfloat16_vec_field_name, DataType.BFLOAT16_VECTOR)])
    def test_milvus_client_query_output_fields_all(self, enable_dynamic_field, vector_field, vector_type):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=10)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(vector_field, vector_type, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
        # 2. insert
        rows = cf.gen_row_data_by_schema(nb=10, schema=schema, skip_field_names=[ct.default_json_field_name])
        for i in range(10):
            rows[i][ct.default_json_field_name] = {"key": i}
        self.insert(client, collection_name, rows)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        all_fields = [default_primary_key_field_name, ct.default_int32_field_name, ct.default_int16_field_name,
                      ct.default_int8_field_name, ct.default_bool_field_name, ct.default_float_field_name,
                      ct.default_double_field_name, ct.default_string_field_name, ct.default_json_field_name,
                      vector_field]
        # 3. query using ids
        self.query(client, collection_name, ids=[i for i in range(default_nb)],
                   output_fields=["*"],
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name,
                                "vector_field": vector_field,
                                "vector_type": vector_type})
        # 4. query using filter
        res = self.query(client, collection_name, filter=default_search_exp,
                         output_fields=["*"],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: rows,
                                      "with_vec": True,
                                      "pk_name": default_primary_key_field_name,
                                      "vector_field": vector_field,
                                      "vector_type": vector_type})[0]
        assert set(res[0].keys()) == set(all_fields)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("fields", [None, [], ""])
    def test_milvus_client_query_output_field_none_or_empty(self, enable_dynamic_field, fields):
        """
        target: test query with none and empty output field
        method: query with output field=None, field=[]
        expected: return primary field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", enable_dynamic_field=enable_dynamic_field)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with None and empty output fields
        res = self.query(client, collection_name, filter=default_search_exp, output_fields=fields)[0]
        assert res[0].keys() == {default_primary_key_field_name, default_vector_field_name}
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_query_output_one_field(self, enable_dynamic_field):
        """
        target: test query with output one field
        method: query with output one field
        expected: return one field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema = schema, consistency_level="Strong", enable_dynamic_field=enable_dynamic_field)
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with one output field
        res = self.query(client, collection_name, filter=default_search_exp, output_fields=[default_float_field_name])[0]
        # verify primary field and specified field are returned
        assert set(res[0].keys()) == {default_primary_key_field_name, default_float_field_name}
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_limit(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. query using ids
        limit = 5
        self.query(client, collection_name, ids=[i for i in range(default_nb)],
                   limit=limit,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[:limit],
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name[:limit]})
        # 4. query using filter
        self.query(client, collection_name, filter=default_search_exp,
                   limit=limit,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[:limit],
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name[:limit]})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("sample_rate", [0.7, 0.5, 0.01])
    def test_milvus_client_query_random_sample(self, sample_rate):
        """
        target: test query random sample
        method: create connection, collection, insert and query
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: vectors[i],
                default_float_field_name: i * 1.0,
                default_string_field_name: cf.generate_random_sentence("English")
            } for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        expr = f"{default_string_field_name} like '%red%'"

        # 3. query without sample rate
        all_res = self.query(client, collection_name, filter=expr)[0]
        exp_num = max(1, int(len(all_res) * sample_rate))

        # 4. query using sample rate
        expr = expr + f" && random_sample({sample_rate})"
        sample_res = self.query(client, collection_name, filter=expr)[0]
        log.info(exp_num)
        assert len(sample_res) == exp_num

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("sample_rate", [1, 0, -9])
    def test_milvus_client_query_invalid_sample_rate(self, sample_rate):
        """
        target: test query random sample
        method: create connection, collection, insert and query
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        vectors = cf.gen_vectors(1, default_dim)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: vectors[i],
                default_float_field_name: i * 1.0,
                default_string_field_name: cf.generate_random_sentence("English")
            } for i in range(1)
        ]
        self.insert(client, collection_name, rows)
        expr = f"{default_string_field_name} like '%red%' && random_sample({sample_rate})"

        # 3. query
        error = {ct.err_code: 999,
                 ct.err_msg: "the sample factor should be between 0 and 1 and not too close to 0 or 1"}
        self.query(client, collection_name, filter=expr,
                   check_task=CheckTasks.err_res, check_items=error)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_json_modulo_operator(self):
        """
        target: test query with modulo operator on JSON field values
        method: create collection with JSON field, insert data with numeric values, test modulo filters
        expected: modulo operations should work correctly and return all matching results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with JSON field
        schema, _ = self.create_schema(client, enable_dynamic_field=False)
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("json_field", DataType.JSON, nullable=True)
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params, consistency_level="Strong")
        
        # 2. insert 3000 rows with various numeric values
        nb = 3000
        rng = np.random.default_rng(seed=19530)
        
        # Store original data for verification
        rows = []
        all_numbers = []
        all_nested_numbers = []
        
        for i in range(nb):
            # Generate diverse numeric values
            if i % 5 == 0:
                # Some large numbers
                numeric_value = random.randint(100000, 999999)
            elif i % 3 == 0:
                # Some medium numbers
                numeric_value = random.randint(1000, 9999)
            else:
                # Regular sequential numbers
                numeric_value = i
            
            nested_value = i * 7 + (i % 13)  # Different pattern for nested
            
            all_numbers.append(numeric_value)
            all_nested_numbers.append(nested_value)
            
            rows.append({
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, default_dim))[0]),
                "json_field": {
                    "number": numeric_value,
                    "index": i,
                    "data": {"nested_number": nested_value}
                }
            })
        
        self.insert(client, collection_name, rows)
        
        # 3. Test modulo operations with different modulo values and remainders
        test_cases = [
            (10, list(range(10))),  # mod 10 - all remainders 0-9
            (2, [0, 1]),  # mod 2 - even/odd
            (3, [0, 1, 2]),  # mod 3
            (7, list(range(7))),  # mod 7 - all remainders 0-6
            (13, [0, 5, 12]),  # mod 13 - selected remainders
        ]
        
        for modulo, remainders in test_cases:
            log.info(f"\nTesting modulo {modulo}")
            
            for remainder in remainders:
                # Calculate expected results from original data
                expected_ids = [i for i, num in enumerate(all_numbers) if num % modulo == remainder]
                expected_count = len(expected_ids)
                
                # Query and get results
                filter_expr = f'json_field["number"] % {modulo} == {remainder}'
                res, _ = self.query(client, collection_name, filter=filter_expr, 
                                    output_fields=["json_field", default_primary_key_field_name])
                
                # Extract actual IDs from results
                actual_ids = sorted([item[default_primary_key_field_name] for item in res])
                expected_ids_sorted = sorted(expected_ids)
                
                log.info(f"Modulo {modulo} remainder {remainder}: expected {expected_count}, got {len(res)} results")
                
                # Verify we got exactly the expected results
                assert len(res) == expected_count, \
                    f"Expected {expected_count} results for % {modulo} == {remainder}, got {len(res)}"
                assert actual_ids == expected_ids_sorted, \
                    f"Result IDs don't match expected IDs for % {modulo} == {remainder}"
                
                # Also verify each result is correct
                for item in res:
                    actual_remainder = item["json_field"]["number"] % modulo
                    assert actual_remainder == remainder, \
                        f"Number {item['json_field']['number']} % {modulo} = {actual_remainder}, expected {remainder}"
                
                # Test nested field
                expected_nested_ids = [i for i, num in enumerate(all_nested_numbers) if num % modulo == remainder]
                nested_filter = f'json_field["data"]["nested_number"] % {modulo} == {remainder}'
                nested_res, _ = self.query(client, collection_name, filter=nested_filter,
                                          output_fields=["json_field", default_primary_key_field_name])
                
                actual_nested_ids = sorted([item[default_primary_key_field_name] for item in nested_res])
                expected_nested_ids_sorted = sorted(expected_nested_ids)
                
                assert len(nested_res) == len(expected_nested_ids), \
                    f"Expected {len(expected_nested_ids)} nested results for % {modulo} == {remainder}, got {len(nested_res)}"
                assert actual_nested_ids == expected_nested_ids_sorted, \
                    f"Nested result IDs don't match expected IDs for % {modulo} == {remainder}"
        
        
        # Test combining modulo with other conditions
        combined_expr = 'json_field["number"] % 2 == 0 && json_field["index"] < 100'
        expected_combined = [i for i in range(min(100, nb)) if all_numbers[i] % 2 == 0]
        res_combined, _ = self.query(client, collection_name, filter=combined_expr,
                                    output_fields=["json_field", default_primary_key_field_name])
        actual_combined_ids = sorted([item[default_primary_key_field_name] for item in res_combined])
        
        assert len(res_combined) == len(expected_combined), \
            f"Expected {len(expected_combined)} results for combined filter, got {len(res_combined)}"
        assert actual_combined_ids == sorted(expected_combined), \
            "Results for combined filter don't match expected"
        
        log.info(f"All modulo tests passed! Combined filter returned {len(res_combined)} results")
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_empty_collection(self):
        """
        target: test query empty collection
        method: query on an empty collection
        expected: empty result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        res = self.query(client, collection_name, filter=default_search_exp)[0]
        assert len(res) == 0
        
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_empty_term_array(self):
        """
        target: test query with empty array term expr
        method: query with empty term expr
        expected: empty result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. query with empty array
        term_expr = f'{ct.default_int64_field_name} in []'
        res = self.query(client, collection_name, filter=term_expr)[0]
        assert len(res) == 0
        # 4. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dup_times", [1, 2, 3])
    @pytest.mark.parametrize("dim", [8, 128])
    def test_milvus_client_query_with_dup_primary_key(self, dim, dup_times):
        """
        target: test query with duplicate primary key
        method: 1.insert same data twice
                2.search
        expected: query results are de-duplicated
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, dim, consistency_level="Strong")
        # 2. insert duplicate data multiple times
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        for _ in range(dup_times):
            self.insert(client, collection_name, rows)
        # 3. query
        res = self.query(client, collection_name, filter=default_search_exp)[0]
        # assert that query results are de-duplicated
        res_ids = [m[default_primary_key_field_name] for m in res]
        assert sorted(list(set(res_ids))) == sorted(res_ids)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_auto_id_not_existed_primary_values(self):
        """
        target: test query on auto_id true collection
        method: 1.create auto_id true collection
                2.query with not existed primary keys
        expected: query result is empty
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with auto_id=True
        self.create_collection(client, collection_name, default_dim, auto_id=True, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. query with non-existent primary keys
        term_expr = f'{default_primary_key_field_name} in [0, 1, 2]'
        res = self.query(client, collection_name, filter=term_expr)[0]
        assert len(res) == 0
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_non_primary_fields(self):
        """
        target: test query on non-primary non-vector fields
        method: query on non-primary non-vector fields
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with all field types
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query by non-primary non-vector scalar fields
        non_primary_fields = [ct.default_int32_field_name, ct.default_int16_field_name,
                             ct.default_float_field_name, ct.default_double_field_name, ct.default_string_field_name]
        # exp res: first two rows and all fields except vector field
        exp_res = rows[:2]
        for field in non_primary_fields:
            if field in schema.fields:
                filter_values = [rows[0][field], rows[1][field]]
                if field != ct.default_string_field_name:
                    term_expr = f'{field} in {filter_values}'
                else:
                    term_expr = f'{field} in {filter_values}'
                    term_expr = term_expr.replace("'", "\"")
                self.query(client, collection_name, filter=term_expr, output_fields=["*"],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": exp_res, "with_vec": True,
                                        "pk_name": ct.default_int64_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_by_bool_field(self):
        """
        target: test query by bool field and output bool field
        method: 1.create and insert with [int64, float, bool, float_vec] fields
                2.query by bool field, and output all int64, bool fields
        expected: verify query result and output fields
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with bool field
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
        # 2. insert data with bool values
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        # Update bool field values to alternate between True and False
        for i, row in enumerate(rows):
            row[ct.default_bool_field_name] = True if i % 2 == 0 else False
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. output bool field
        res = self.query(client, collection_name, filter=default_term_expr, 
                        output_fields=[ct.default_bool_field_name])[0]
        assert set(res[0].keys()) == {ct.default_int64_field_name, ct.default_bool_field_name}
        # 5. not support filter bool field with expr 'bool in [0/ 1]'
        not_support_expr = f'{ct.default_bool_field_name} in [0]'
        error = {ct.err_code: 65535,
                 ct.err_msg: "cannot parse expression: bool in [0], error: "
                             "value 'int64_val:0' in list cannot be casted to Bool"}
        self.query(client, collection_name, filter=not_support_expr, 
                  output_fields=[ct.default_bool_field_name],
                  check_task=CheckTasks.err_res, check_items=error)
        # 6. filter bool field by bool term expr
        for bool_value in [True, False]:
            exprs = [f'{ct.default_bool_field_name} in [{bool_value}]',
                     f'{ct.default_bool_field_name} == {bool_value}']
            for expr in exprs:
                res = self.query(client, collection_name, filter=expr, 
                               output_fields=[ct.default_bool_field_name])[0]
                assert len(res) == default_nb // 2
                for _r in res:
                    assert _r[ct.default_bool_field_name] == bool_value
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_by_int64(self):
        """
        target: test query through int64 field and output int64 field
        method: use int64 as query expr parameter
        expected: verify query output number
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection and index
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb * 10, schema=schema_info)
        self.insert(client, collection_name, rows)
        assert len(rows) == default_nb * 10
        a = rows[:10]
        # 3. filter on int64 fields
        expr_list = [f'{default_primary_key_field_name} > 8192 && {default_primary_key_field_name} < 8194',
                     f'{default_primary_key_field_name} > 16384 && {default_primary_key_field_name} < 16386']
        for expr in expr_list:
            res = self.query(client, collection_name, filter=expr, output_fields=[ct.default_int64_field_name])[0]
            assert len(res) == 1
        # 4. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_by_int8(self):
        """
        target: test query through int8 field and output all scalar fields
        method:
            1. create collection with fields [int64, float, int8, float_vec]
            2. insert data
            3. query by int8 field
        expected: query result matches expected rows
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection and index
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=100)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        # overwrite int8 field with explicit np.int8 values
        for i, row in enumerate(rows):
            row[ct.default_int8_field_name] = np.int8(i)

        self.insert(client, collection_name, rows)
        assert len(rows) == default_nb
        # 3. query expression
        term_expr = f"{ct.default_int8_field_name} in [0]"
        # expected query result:
        # int8 range [-128, 127], so repeated values every 256
        expected_rows = []
        for i in range(0, default_nb, 256):
            expected_rows.append(rows[i])

        res = self.query(client, collection_name, filter=term_expr, output_fields=["float", "int64", "int8", "varchar"])[0]
    
        assert len(res) == len(expected_rows)
        returned_ids = {r[ct.default_int64_field_name] for r in res}
        expected_ids = {r[ct.default_int64_field_name] for r in expected_rows}
        assert returned_ids == expected_ids
        # 4. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field", [ct.default_int64_field_name, ct.default_float_field_name])
    def test_milvus_client_query_expr_not_in_term(self, field):
        """
        target: test query with `not in` expr
        method: query with not in expr
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # query with not in expression
        values = [row[field] for row in rows]
        pos = 100
        term_expr = f'{field} not in {values[pos:]}'
        df = pd.DataFrame(rows)
        res = df.iloc[:pos, :3].to_dict('records')
        self.query(client, collection_name, filter=term_expr, 
                  output_fields=[ct.default_float_field_name, ct.default_int64_field_name, ct.default_string_field_name],
                  check_task=CheckTasks.check_query_results,
                  check_items={exp_res: res, "pk_name": ct.default_int64_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("pos", [0, default_nb])
    def test_milvus_client_query_expr_not_in_empty_and_all(self, pos):
        """
        target: test query with `not in` expr
        method: query with `not in` expr for (non)empty collection
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with not in expression
        int64_values = [row[ct.default_int64_field_name] for row in rows]
        term_expr = f'{ct.default_int64_field_name} not in {int64_values[pos:]}'
        df = pd.DataFrame(rows)
        res = df.iloc[:pos, :1].to_dict('records')
        self.query(client, collection_name, filter=term_expr,
                  output_fields=[ct.default_int64_field_name],
                  check_task=CheckTasks.check_query_results,
                  check_items={exp_res: res, "pk_name": ct.default_int64_field_name})
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_expr_random_values(self):
        """
        target: test query with random filter values
        method: query with random filter values, like [0, 2, 4, 3]
        expected: correct query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=100, schema=schema_info)
        self.insert(client, collection_name, rows)
        assert len(rows) == 100
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with random values
        random_values = [0, 2, 4, 3]
        term_expr = f'{ct.default_int64_field_name} in {random_values}'
        df = pd.DataFrame(rows)
        res = df.iloc[random_values, :1].to_dict('records')
        self.query(client, collection_name, filter=term_expr,
                  output_fields=[ct.default_int64_field_name],
                  check_task=CheckTasks.check_query_results,
                  check_items={exp_res: res, "pk_name": ct.default_int64_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_not_in_random(self):
        """
        target: test query with fixed filter values
        method: query with fixed filter values
        expected: correct query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=50, schema=schema_info)
        self.insert(client, collection_name, rows)
        assert len(rows) == 50
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with not in random values
        random_values = [i for i in range(10, 50)]
        log.debug(f'random values: {random_values}')
        random.shuffle(random_values)
        term_expr = f'{ct.default_int64_field_name} not in {random_values}'
        df = pd.DataFrame(rows)
        res = df.iloc[:10, :1].to_dict('records')
        self.query(client, collection_name, filter=term_expr,
                  output_fields=[ct.default_int64_field_name],
                  check_task=CheckTasks.check_query_results,
                  check_items={exp_res: res, "pk_name": ct.default_int64_field_name})
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("expr_prefix", ["json_contains", "JSON_CONTAINS",
                                             "array_contains", "ARRAY_CONTAINS"])
    def test_milvus_client_query_expr_json_contains(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        limit = 99
        for i in range(default_nb):
            rows[i][ct.default_json_field_name] = {"number": i,
                                                   "list": [m for m in range(i, i + limit)]}
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query
        expression = f"{expr_prefix}({ct.default_json_field_name}['list'], 1000)"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == limit
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["json_contains", "JSON_CONTAINS"])
    def test_milvus_client_query_expr_list_json_contains(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=True, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        limit = default_nb // 4
        rows = []
        for i in range(default_nb):
            data = {
                ct.default_int64_field_name: i,
                ct.default_json_field_name: [str(m) for m in range(i, i + limit)],
                ct.default_vector_field_name: cf.gen_vectors(1, default_dim)[0]
            }
            rows.append(data)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query
        expression = f"{expr_prefix}({ct.default_json_field_name}, '1000')"
        res = self.query(client, collection_name, filter=expression, output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == limit
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("expr_prefix", ["json_contains", "JSON_CONTAINS"])
    def test_milvus_client_query_expr_json_contains_combined_with_normal(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        limit = default_nb // 3
        tar = 1000
        for i in range(default_nb):
            # Set float field values to ensure some records satisfy the query condition
            rows[i][ct.default_json_field_name] = {"number": i, "list": [m for m in range(i, i + limit)]}
            if i % 2 == 0:
                rows[i][ct.default_float_field_name] = tar - limit
            else:
                rows[i][ct.default_float_field_name] = tar
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with combined expression
        expression = f"{expr_prefix}({ct.default_json_field_name}['list'], {tar}) && {ct.default_float_field_name} > {tar - limit // 2}"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == limit // 2
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("expr_prefix", ["json_contains_all", "JSON_CONTAINS_ALL",
                                             "array_contains_all", "ARRAY_CONTAINS_ALL"])
    def test_milvus_client_query_expr_all_datatype_json_contains_all(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        limit = 10
        for i in range(default_nb):
            content = {
                "listInt": [m for m in range(i, i + limit)],
                "listStr": [str(m) for m in range(i, i + limit)],
                "listFlt": [m * 1.0 for m in range(i, i + limit)],
                "listBool": [bool(i % 2)],
                "listList": [[i, str(i + 1)], [i * 1.0, i + 1]],
                "listMix": [i, i * 1.1, str(i), bool(i % 2), [i, str(i)]]
            }
            rows[i][ct.default_json_field_name] = content
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with different data types
        _id = random.randint(limit, default_nb - limit)
        # test for int
        ids = [i for i in range(_id, _id + limit)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listInt'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == 1
        # test for string
        ids = [str(_id), str(_id + 1), str(_id + 2)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listStr'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == limit - len(ids) + 1
        # test for float
        ids = [_id * 1.0]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listFlt'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == limit
        # test for bool
        ids = [True]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listBool'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == default_nb // 2
        # test for list
        ids = [[_id, str(_id + 1)]]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listList'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == 1
        # test for mixed data
        ids = [[_id, str(_id)], bool(_id % 2)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listMix'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == 1
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_all", "JSON_CONTAINS_ALL"])
    def test_milvus_client_query_expr_list_all_datatype_json_contains_all(self, expr_prefix):
        """
        target: test query with expression using json_contains_all
        method: query with expression using json_contains_all
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=True, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[ct.default_json_field_name])
        limit = 10
        for i in range(default_nb):
            rows[i][ct.default_json_field_name] = {
                "listInt": [m for m in range(i, i + limit)],
                "listStr": [str(m) for m in range(i, i + limit)],
                "listFlt": [m * 1.0 for m in range(i, i + limit)],
                "listBool": [bool(i % 2)],
                "listList": [[i, str(i + 1)], [i * 1.0, i + 1]],
                "listMix": [i, i * 1.1, str(i), bool(i % 2), [i, str(i)]],
            }
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with different data types
        _id = random.randint(limit, default_nb - limit)
        # test for int
        ids = [i for i in range(_id, _id + limit)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listInt'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == 1
        # test for string
        ids = [str(_id), str(_id + 1), str(_id + 2)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listStr'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == limit - len(ids) + 1
        # test for float
        ids = [_id * 1.0]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listFlt'], {ids})"
        res = self.query(client, collection_name, filter=expression, output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == limit
        # test for bool
        ids = [True]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listBool'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == default_nb // 2
        # test for list
        ids = [[_id, str(_id + 1)]]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listList'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == 1
        # test for mixed data
        ids = [[_id, str(_id)], bool(_id % 2)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listMix'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == 1
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("expr_prefix", ["json_contains_any", "JSON_CONTAINS_ANY"])
    def test_milvus_client_query_expr_all_datatype_json_contains_any(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        limit = 10
        for i in range(default_nb):
            rows[i][ct.default_json_field_name] = {
                "listInt": [m for m in range(i, i + limit)],
                "listStr": [str(m) for m in range(i, i + limit)],
                "listFlt": [m * 1.0 for m in range(i, i + limit)],
                "listBool": [bool(i % 2)],
                "listList": [[i, str(i + 1)], [i * 1.0, i + 1]],
                "listMix": [i, i * 1.1, str(i), bool(i % 2), [i, str(i)]]
            }
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with different data types
        _id = random.randint(limit, default_nb - limit)
        # test for int
        ids = [i for i in range(_id, _id + limit)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listInt'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == 2 * limit - 1
        # test for string
        ids = [str(_id), str(_id + 1), str(_id + 2)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listStr'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == limit + len(ids) - 1
        # test for float
        ids = [_id * 1.0]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listFlt'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == limit
        # test for bool
        ids = [True]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listBool'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == default_nb // 2
        # test for list
        ids = [[_id, str(_id + 1)]]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listList'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == 1
        # test for mixed data
        ids = [_id, bool(_id % 2)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listMix'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == default_nb // 2
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("limit", [10, 100, 1000])
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_milvus_client_query_expr_empty(self, auto_id, limit):
        """
        target: test query with empty expression
        method: query empty expression with a limit
        expected: return topK results by order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=auto_id)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with empty expression and limit
        res = self.query(client, collection_name, filter="", limit=limit)[0]
        # 5. verify results are ordered by primary key
        if not auto_id:
            primary_keys = [entity[default_primary_key_field_name] for entity in res]
            assert primary_keys == sorted(primary_keys)
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_expr_empty_pk_string(self):
        """
        target: test query with empty expression
        method: query empty expression with a limit
        expected: return topK results by order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with string primary key
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=10, is_primary=True)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with limit - string field is sorted by lexicographical order
        exp_ids = ['0', '1', '10', '100', '1000', '1001', '1002', '1003', '1004', '1005']
        res = self.query(client, collection_name, filter="", limit=ct.default_limit)[0]
        # verify results are in lexicographical order
        primary_keys = [entity[ct.default_string_field_name] for entity in res]
        assert primary_keys == exp_ids
        # 5. query with limit + offset
        res = self.query(client, collection_name, filter="", limit=5, offset=5)[0]
        primary_keys = [entity[ct.default_string_field_name] for entity in res]
        assert primary_keys == exp_ids[5:]
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("offset", [100, 1000])
    @pytest.mark.parametrize("limit", [100, 1000])
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_milvus_client_query_expr_empty_with_pagination(self, auto_id, limit, offset):
        """
        target: test query with empty expression
        method: query empty expression with a limit
        expected: return topK results by order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=auto_id)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with limit and offset
        res = self.query(client, collection_name, filter="", limit=limit, offset=offset)[0]
        # 5. verify results are ordered by primary key
        if not auto_id:
            primary_keys = [entity[default_primary_key_field_name] for entity in res]
            expected_ids = list(range(offset, offset + limit))
            assert primary_keys == expected_ids
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [100, 1000])
    @pytest.mark.parametrize("limit", [100, 1000])
    def test_milvus_client_query_expr_empty_with_random_pk(self, limit, offset):
        """
        target: test query with empty expression
        method: create a collection using random pk, query empty expression with a limit
        expected: return topK results by order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=10)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. generate unordered pk array and insert
        unordered_ids = [i for i in range(default_nb)]
        random.shuffle(unordered_ids)
        rows = []
        for i in range(default_nb):
            row = {
                ct.default_int64_field_name: unordered_ids[i],
                ct.default_float_field_name: np.float32(unordered_ids[i]),
                ct.default_string_field_name: str(unordered_ids[i]),
                ct.default_vector_field_name: cf.gen_vectors(nb=1, dim=default_dim)[0]
            }
            rows.append(row)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with empty expr and check the result
        exp_ids, res = sorted(unordered_ids)[:limit], []
        for ids in exp_ids:
            res.append({ct.default_int64_field_name: ids, ct.default_string_field_name: str(ids)})
        res = self.query(client, collection_name, filter="", limit=limit, output_fields=[ct.default_string_field_name],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: res, "pk_name": ct.default_int64_field_name})[0]
        # 5. query with pagination
        exp_ids, res = sorted(unordered_ids)[:limit + offset][offset:], []
        for ids in exp_ids:
            res.append({ct.default_int64_field_name: ids, ct.default_string_field_name: str(ids)})
        res = self.query(client, collection_name, filter="", limit=limit, offset=offset, output_fields=[ct.default_string_field_name],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: res, "pk_name": ct.default_int64_field_name})[0]
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_integer_overflow_expressions())
    def test_milvus_client_query_expr_out_of_range(self, expression):
        """
        target: test query with integer overflow in boolean expressions
        method: create a collection with various integer fields, insert data, 
                execute query using expressions with out-of-range integer constants,
                and compare the number of results with local Python evaluation.
        expected: query executes successfully and the result count matches local evaluation
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with all data types
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data with overflow values
        start = default_nb // 2
        rows = []
        for i in range(default_nb):
            row = {
                ct.default_int64_field_name: start + i,
                ct.default_int32_field_name: np.int32((start + i) * 2200000),
                ct.default_int16_field_name: np.int16((start + i) * 40),
                ct.default_int8_field_name: np.int8((start + i) % 128),
                ct.default_float_vec_field_name: cf.gen_vectors(nb=1, dim=default_dim)[0],
            }
            rows.append(row)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. filter result with expression in collection
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i in range(default_nb):
            int8 = np.int8((start + i) % 128)
            int16 = np.int16((start + i) * 40)
            int32 = np.int32((start + i) * 2200000)
            if not expression or eval(expression):
                filter_ids.append(start + i)
        # 5. query and verify result
        res = self.query(client, collection_name, filter=expression, output_fields=["int8"])[0]
        assert len(res) == len(filter_ids)
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_any", "JSON_CONTAINS_ANY",
                                             "array_contains_any", "ARRAY_CONTAINS_ANY"])
    def test_milvus_client_query_expr_list_all_datatype_json_contains_any(self, expr_prefix):
        """
        target: test query with expression using json_contains_any
        method: query with expression using json_contains_any
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=True, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[ct.default_json_field_name])
        limit = random.randint(10, 20)
        int_data = [[m for m in range(i, i + limit)] for i in range(default_nb)]
        str_data = [[str(m) for m in range(i, i + limit)] for i in range(default_nb)]
        flt_data = [[m * 1.0 for m in range(i, i + limit)] for i in range(default_nb)]
        bool_data = [[bool(i % 2)] for i in range(default_nb)]
        list_data = [[[i, str(i + 1)], [i * 1.0, i + 1]] for i in range(default_nb)]
        mix_data = [[i, i * 1.1, str(i), bool(i % 2), [i, str(i)]] for i in range(default_nb)]

        for i in range(default_nb):
            rows[i][ct.default_json_field_name] = {
                "listInt": int_data[i],
                "listStr": str_data[i],
                "listFlt": flt_data[i],
                "listBool": bool_data[i],
                "listList": list_data[i],
                "listMix": mix_data[i]
            }
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with different data types
        _id = random.randint(limit, default_nb - limit)
        # test for int
        ids = [i for i in range(_id, _id + limit)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listInt'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert [entity[ct.default_int64_field_name] for entity in res] == cf.assert_json_contains(expression, int_data)
        # test for string
        ids = [str(_id), str(_id + 1), str(_id + 2)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listStr'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert [entity[ct.default_int64_field_name] for entity in res] == cf.assert_json_contains(expression, str_data)
        # test for float
        ids = [_id * 1.0]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listFlt'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert [entity[ct.default_int64_field_name] for entity in res] == cf.assert_json_contains(expression, flt_data)
        # test for bool
        ids = [True]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listBool'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert [entity[ct.default_int64_field_name] for entity in res] == cf.assert_json_contains(expression, bool_data)
        # test for list
        ids = [[_id, str(_id + 1)]]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listList'], {ids})"
        res = self.query(client, collection_name, filter=expression, output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == 1
        # test for mixed data
        ids = [str(_id)]
        expression = f"{expr_prefix}({ct.default_json_field_name}['listMix'], {ids})"
        res = self.query(client, collection_name, filter=expression, output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == 1
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_any", "json_contains_all"])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_query_expr_json_contains_list_in_list(self, expr_prefix, enable_dynamic_field):
        """
        target: test query with expression using json_contains_any
        method: query with expression using json_contains_any
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[ct.default_json_field_name])
        for i in range(default_nb):
            rows[i][ct.default_json_field_name] = {"list": [[i, i + 1], [i, i + 2], [i, i + 3]]}
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with list in list
        _id = random.randint(3, default_nb - 3)
        ids = [[_id, _id + 1]]
        expression = f"{expr_prefix}({ct.default_json_field_name}['list'], {ids})"
        res = self.query(client, collection_name, filter=expression)[0]
        assert len(res) == 1

        ids = [[_id + 4, _id], [_id]]
        expression = f"{expr_prefix}({ct.default_json_field_name}['list'], {ids})"
        self.query(client, collection_name, filter=expression, check_task=CheckTasks.check_query_empty)
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("expr_prefix", ["json_contains", "JSON_CONTAINS"])
    def test_milvus_client_query_expr_json_contains_pagination(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        limit = default_nb // 3
        for i in range(default_nb):
            rows[i][ct.default_json_field_name] = {"number": i,
                                                   "list": [m for m in range(i, i + limit)]}
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with pagination
        expression = f"{expr_prefix}({ct.default_json_field_name}['list'], 1000)"
        offset = random.randint(1, limit)
        res = self.query(client, collection_name, filter=expression, limit=limit, offset=offset)[0]
        assert len(res) == limit - offset
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("array_length", ["ARRAY_LENGTH", "array_length"])
    @pytest.mark.parametrize("op", ["==", "!=", ">", "<="])
    def test_milvus_client_query_expr_array_length(self, array_length, op, enable_dynamic_field):
        """
        target: test query with expression using array_length
        method: query with expression using array_length
                array_length only support == , !=
        expected: succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_array_field_name, DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=1024)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[ct.default_float_array_field_name] )
        length = []
        for i in range(default_nb):
            ran_int = random.randint(50, 53)
            length.append(ran_int)
            rows[i][ct.default_float_array_field_name] = [np.float32(j) for j in range(ran_int)]
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query
        expression = f"{array_length}({ct.default_float_array_field_name}) {op} 51"
        res = self.query(client, collection_name, filter=expression)[0]
        # 5. check with local filter
        expression = expression.replace(f"{array_length}({ct.default_float_array_field_name})", "array_length")
        filter_ids = []
        for i in range(default_nb):
            array_length = length[i]
            if eval(expression):
                filter_ids.append(i)
        assert len(res) == len(filter_ids)
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("wildcard_output_fields", [["*"], ["*", default_float_field_name],
                                                        ["*", ct.default_int64_field_name]])
    def test_milvus_client_query_output_field_wildcard(self, wildcard_output_fields):
        """
        target: test query with output fields using wildcard
        method: query with one output_field (wildcard)
        expected: query success
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=True, auto_id=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_int64_field_name, DataType.INT64)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. get wildcard output field names
        schema_info = self.describe_collection(client, collection_name)[0]
        output_fields = cf.get_wildcard_output_field_names(schema_info, wildcard_output_fields)
        # 5. query with wildcard output fields
        actual_res = self.query(client, collection_name, filter=default_search_exp, output_fields=wildcard_output_fields)[0]
        # 6. verify output fields
        assert set(actual_res[0].keys()) == set(output_fields)
        # 7. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_output_binary_vec_field(self):
        """
        target: test query with binary vec output field
        method: specify binary vec field as output field
        expected: return primary field and binary vec field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with binary vector
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_binary_vec_field_name, index_type="BIN_FLAT", metric_type="JACCARD")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. test different output field combinations
        fields = [[ct.default_binary_vec_field_name],
                  [default_primary_key_field_name, ct.default_binary_vec_field_name]]
        for output_fields in fields:
            res = self.query(client, collection_name, filter=default_search_exp, output_fields=output_fields)[0]
            assert set(res[0].keys()) == set(fields[-1])
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_output_primary_field(self):
        """
        target: test query with output field only primary field
        method: specify int64 primary field as output field
        expected: return int64 field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with only primary field as output
        res = self.query(client, collection_name, filter=default_search_exp, output_fields=[default_primary_key_field_name])[0]
        # 5. verify only primary field is returned
        assert set(res[0].keys()) == {default_primary_key_field_name}
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_output_multi_float_vec_field(self):
        """
        target: test query and output multi float vec fields
        method: a.specify multi vec field as output
                b.specify output_fields with wildcard %
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with two float vector fields
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("float_vector1", DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        index_params.add_index(field_name="float_vector1", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with multi vec output_fields
        output_fields = [ct.default_int64_field_name, ct.default_float_vec_field_name, "float_vector1"]
        exp_res = []
        for i in range(2):
            result_item = {}
            for field_name in output_fields:
                result_item[field_name] = rows[i][field_name]
            exp_res.append(result_item)
        # Query and verify
        self.query(client, collection_name, filter=default_term_expr, output_fields=output_fields,
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": exp_res, "with_vec": True, "pk_name": ct.default_int64_field_name})
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_output_multi_sparse_vec_field(self):
        """
        target: test query and output multi sparse vec fields
        method: a.specify multi vec field as output
                b.specify output_fields with wildcard %
        expected: verify query result
        """
        from faker import Faker
        fake = Faker()

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with two float vector fields
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field("document", DataType.VARCHAR, max_length=10000, enable_analyzer=True)
        schema.add_field("sparse1", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field("sparse2", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field("bm25", DataType.SPARSE_FLOAT_VECTOR)
        # add bm25 function
        bm25_function = Function(
            name="bm25",
            input_field_names=["document"],
            output_field_names="bm25",
            function_type=FunctionType.BM25,
        )
        schema.add_function(bm25_function)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = []
        data_size = 3000
        for i in range(data_size):
            rows.append({
                ct.default_int64_field_name: i,
                "sparse1": {random.randint(1, 10000): random.random() for _ in range(100)},
                "sparse2": {random.randint(1, 10000): random.random() for _ in range(100)},
                "document": fake.text(),
            })
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="sparse1", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        index_params.add_index(field_name="sparse2", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        index_params.add_index(field_name="bm25", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25",
                               params={"bm25_k1": 1.2, "bm25_b": 0.75})
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with multi vec output_fields
        output_fields = [ct.default_int64_field_name, "sparse1", "sparse2"]
        exp_res = []
        for i in range(2):
            result_item = {}
            for field_name in output_fields:
                result_item[field_name] = rows[i][field_name]
            exp_res.append(result_item)
        self.query(client, collection_name, filter=default_term_expr, output_fields=output_fields,
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": exp_res, "with_vec": True, "pk_name": ct.default_int64_field_name})[0]
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vec_fields", [["binary_vector"], ["binary_vector", "binary_vec1"]])
    def test_milvus_client_query_output_mix_float_binary_field(self, vec_fields):
        """
        target:  test query and output mix float and binary vec fields
        method: a.specify mix vec field as output
                b.specify output_fields with wildcard %
        expected: output binary vector and float vec
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with float and binary vector fields
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        for vec_field_name in vec_fields:
            schema.add_field(vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        for vec_field_name in vec_fields:
            index_params.add_index(field_name=vec_field_name, index_type="BIN_FLAT", metric_type="JACCARD")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with mix vec output_fields
        output_fields = [ct.default_int64_field_name, ct.default_float_vec_field_name]
        for vec_field_name in vec_fields:
            output_fields.append(vec_field_name)
        
        res = self.query(client, collection_name, filter=default_term_expr, output_fields=output_fields)[0]
        assert len(res) == 2
        for result_item in res:
            assert set(result_item.keys()) == set(output_fields)
        # Query and verify with wildcard
        res_wildcard = self.query(client, collection_name, filter=default_term_expr, output_fields=["*"])[0]
        assert len(res_wildcard) == 2
        for result_item in res_wildcard:
            assert set(result_item.keys()) == set(output_fields)
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_fields", ["12-s", 1, [1, "2", 3], (1,), {1: 1}])
    def test_milvus_client_query_invalid_output_fields(self, invalid_fields):
        """
        target: test query with invalid output fields
        method: query with invalid field fields
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. test invalid output_fields type
        if invalid_fields == [1, "2", 3]:
            error = {ct.err_code: 1, ct.err_msg: "Unexpected error, message=<bad argument type for built-in operation>"}
        else:
            error = {ct.err_code: 1, ct.err_msg: "Invalid query format. 'output_fields' must be a list"}
        self.query(client, collection_name, filter=default_term_expr, output_fields=invalid_fields,
                  check_task=CheckTasks.err_res, check_items=error)
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_query_partition(self):
        """
        target: test query on partition
        method: create a partition and query
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 3. insert data to partition
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows, partition_name=partition_name)
        # 4. create index and load partition
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_partitions(client, collection_name, [partition_name])
        # 5. query on partition and verify results
        self.query(client, collection_name, filter=default_search_exp, 
                   partition_names=[partition_name],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": rows, "with_vec": True, "pk_name": default_primary_key_field_name})
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_default_partition(self):
        """
        target: test query on default partition
        method: query on default partition
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 2. insert data (will go to default partition by default)
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query on default partition and verify results
        self.query(client, collection_name, filter=default_search_exp, 
                   partition_names=[ct.default_partition_name],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": rows, "pk_name": default_primary_key_field_name})
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("ignore_growing", [True, False])
    def test_milvus_client_query_ignore_growing(self, ignore_growing):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. query with param ignore_growing
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong", auto_id=False)
        # 2. insert initial data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. insert data again
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=10000)
        self.insert(client, collection_name, new_rows)
        self.flush(client, collection_name)
        # 5. query with param ignore_growing
        if ignore_growing:
            res = self.query(client, collection_name, filter=default_search_exp, ignore_growing=ignore_growing)[0]
            assert len(res) == default_nb
            # verify that only original data (id < 10000) is returned
            for item in res:
                assert item[default_primary_key_field_name] < 10000
        else:
            res = self.query(client, collection_name, filter=default_search_exp, ignore_growing=ignore_growing)[0]
            assert len(res) == default_nb * 2
            for item in res:
                assert item[default_primary_key_field_name] < 10000 or item[default_primary_key_field_name] >= 10000
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ignore_growing", [True, False])
    def test_milvus_client_query_ignore_growing_after_upsert(self, ignore_growing):
        """
        target: test query ignoring growing segment after upsert
        method: 1. create a collection, insert data, create index and load
                2. upsert the inserted data
                3. query with param ignore_growing
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong", auto_id=False)
        # 2. insert initial data and flush
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. upsert data (which creates growing segment data)
        upsert_data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, upsert_data)
        self.flush(client, collection_name)
        # 5. query with param ignore_growing
        if ignore_growing:
            exp_len = 0
        else:
            exp_len = default_nb
        res = self.query(client, collection_name, filter=default_search_exp, ignore_growing=ignore_growing)[0]
        assert len(res) == exp_len
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_pagination(self, offset):
        """
        target: test query pagination
        method: create collection and query with pagination params,
                verify if the result is ordered by primary key
        expected: query successfully and verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 3. prepare pagination query
        int_values = [row[default_primary_key_field_name] for row in rows]
        pos = 10
        term_expr = f'{default_primary_key_field_name} in {int_values[offset: pos + offset]}'
        # Expected results: primary key values from offset to pos+offset
        res = []
        for i in range(offset, min(pos + offset, len(rows))):
            res.append({default_primary_key_field_name: rows[i][default_primary_key_field_name]})
        # 4. query with pagination params
        query_res = self.query(client, collection_name, filter=term_expr, 
                              output_fields=[default_primary_key_field_name],
                              limit=10,
                              check_task=CheckTasks.check_query_results,
                              check_items={exp_res: res, "pk_name": default_primary_key_field_name})[0]
        # 5. verify primary key order 
        key_res = [item[key] for item in query_res for key in item]
        assert key_res == int_values[offset: pos + offset]
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_binary_pagination(self, offset):
        """
        target: test query binary pagination
        method: create collection and query with pagination params,
                verify if the result is ordered by primary key
        expected: query successfully and verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with binary vector
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_binary_vec_field_name, index_type="BIN_FLAT", metric_type="JACCARD")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 3. prepare pagination query
        int_values = [row[default_primary_key_field_name] for row in rows]
        pos = 10
        term_expr = f'{default_primary_key_field_name} in {int_values[offset: pos + offset]}'
        # Expected results: primary key values from offset to pos+offset
        res = []
        for i in range(offset, min(pos + offset, len(rows))):
            res.append({default_primary_key_field_name: rows[i][default_primary_key_field_name]})
        # 4. query with pagination params
        query_res = self.query(client, collection_name, filter=term_expr, 
                              output_fields=[default_primary_key_field_name],
                              limit=10,
                              check_task=CheckTasks.check_query_results,
                              check_items={exp_res: res, "pk_name": default_primary_key_field_name})[0]
        # 5. verify primary key order
        key_res = [item[key] for item in query_res for key in item]
        assert key_res == int_values[offset: pos + offset]
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_pagination_with_partition(self, offset):
        """
        target: test query pagination on partition
        method: create a partition and query with different offset
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # 1. create collection and partition
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        self.create_partition(client, collection_name, partition_name)
        # 2. insert data to partition
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows, partition_name=partition_name)
        # 3. verify entity count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == default_nb
        # 4. create index and load partition
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_partitions(client, collection_name, [partition_name])
        # 5. prepare expected results (first 2 records)
        res = []
        for i in range(offset, min(10 + offset, len(rows))):
            res.append({default_primary_key_field_name: rows[i][default_primary_key_field_name]})
        # 6. query with pagination params on partition
        self.query(client, collection_name, filter=default_search_exp, 
                  output_fields=[default_primary_key_field_name],
                  partition_names=[partition_name],
                  offset=offset, limit=10,
                  check_task=CheckTasks.check_query_results,
                  check_items={exp_res: res, "pk_name": default_primary_key_field_name})
        # 7. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_query_pagination_with_insert_data(self, offset):
        """
        target: test query pagination on partition
        method: create a partition and query with pagination
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=True, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
        # 2. insert data
        df = cf.gen_default_dataframe_data()
        rows = df.to_dict('records')
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. prepare expected results (first 2 records)
        int_values = [row[ct.default_int64_field_name] for row in rows]
        pos = 10
        term_expr = f'{ct.default_int64_field_name} in {int_values[offset: pos + offset]}'
        # Expected results: primary key values from offset to pos+offset
        res = []
        for i in range(offset, min(pos + offset, len(rows))):
            res.append({ct.default_int64_field_name: rows[i][ct.default_int64_field_name]})
        # 5. query with pagination params
        query_res = self.query(client, collection_name, filter=term_expr, 
                  output_fields=[ct.default_int64_field_name],
                  limit=10,
                  check_task=CheckTasks.check_query_results,
                  check_items={exp_res: res, "pk_name": ct.default_int64_field_name})[0]
        key_res = [item[ct.default_int64_field_name] for item in query_res]
        assert key_res == int_values[offset: pos + offset]
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_pagination_without_limit(self, offset):
        """
        target: test query pagination without limit
        method: create collection and query with pagination params(only offset),
                compare the result with query without pagination params
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load collection
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with only offset parameter (no limit)
        query_res_with_offset = self.query(client, collection_name, filter="id in [0, 1]",
                                          offset=offset,
                                          check_task=CheckTasks.check_query_results,
                                          check_items={exp_res: rows[:2], "with_vec": True, "pk_name": default_primary_key_field_name})[0]
        # 5. query without pagination params
        query_res_without_pagination = self.query(client, collection_name, filter="id in [0, 1]",
                                                 check_task=CheckTasks.check_query_results,
                                                 check_items={exp_res: rows[:2], "with_vec": True, "pk_name": default_primary_key_field_name})[0]
        assert query_res_with_offset == query_res_without_pagination
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [3000, 5000])
    def test_milvus_client_query_pagination_with_offset_over_num_entities(self, offset):
        """
        target: test query pagination with offset over num_entities
        method: query with offset over num_entities
        expected: return an empty list
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", auto_id=False)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load collection
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. query with offset over num_entities
        # Use a broader query that could return results, but offset is too large
        res = self.query(client, collection_name, filter=default_search_exp, 
                        offset=offset, limit=10)[0]
        # 5. verify empty result
        assert len(res) == 0
        # 6. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_query_with_expression(self, enable_dynamic_field):
        """
        target: test query with different expr
        method: query with different boolean expr
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong",
                              enable_dynamic_field=enable_dynamic_field)
        # 2. insert data
        nb = 2000
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, rows)
        #assert self.num_entities(client, collection_name)[0] == nb
        self.flush(client, collection_name)
        # 3. create index and load collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        
        # 4. filter result with expression in collection
        for expressions in cf.gen_normal_expressions_and_templates():
            log.debug(f"test_milvus_client_query_with_expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, row in enumerate(rows):
                int64 = row[ct.default_int64_field_name]
                float = row[ct.default_float_field_name]
                if not expr or eval(expr):
                    filter_ids.append(row[ct.default_int64_field_name])

            # query and verify result
            res = self.query(client, collection_name, filter=expr, limit=nb)[0]
            query_ids = set(map(lambda x: x[ct.default_int64_field_name], res))
            assert query_ids == set(filter_ids)

            # query again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            res = self.query(client, collection_name, filter=expr, filter_params=expr_params, limit=nb)[0]
            query_ids = set(map(lambda x: x[ct.default_int64_field_name], res))
            assert query_ids == set(filter_ids)
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [0, 10, 100])
    def test_milvus_client_query_pagination_with_expression(self, offset):
        """
        target: test query pagination with different expression
        method: query with different expression and verify the result
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong",
                              enable_dynamic_field=False)
        # 2. insert data
        nb = 1000
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. create index and load collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 4. filter result with expression in collection
        for expressions in cf.gen_normal_expressions_and_templates()[1:]:
            log.debug(f"test_milvus_client_query_pagination_with_expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, row in enumerate(rows):
                int64 = row[ct.default_int64_field_name]
                float = row[ct.default_float_field_name]
                if not expr or eval(expr):
                    filter_ids.append(row[ct.default_int64_field_name])
                    
            # query and verify result
            query_params = {"offset": offset, "limit": 10}
            res = self.query(client, collection_name, filter=expr, **query_params)[0]
            query_ids = [item[ct.default_int64_field_name] for item in res]
            expected_ids = filter_ids[offset:offset+10]
            assert query_ids == expected_ids

            # query again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            res = self.query(client, collection_name, filter=expr, filter_params=expr_params, **query_params)[0]
            query_ids = [item[ct.default_int64_field_name] for item in res]
            assert query_ids == expected_ids
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_mmap_query_expr_empty_pk_string(self):
        """
        target: turn on mmap to test queries using empty expression
        method: enable mmap to query for empty expressions with restrictions.
        expected: return the first K results in order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with string primary key
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong",
                              enable_dynamic_field=False)
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)        
        # 3. create index and load collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)

        self.release_collection(client, collection_name)
        # 4. Set mmap enabled before loading
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        describe_res = self.describe_collection(client, collection_name)[0]
        properties = describe_res.get("properties")
        assert properties["mmap.enabled"] == 'True'
        self.load_collection(client, collection_name)

        # 5. prepare expected results
        exp_ids = ['0', '1', '10', '100', '1000', '1001', '1002', '1003', '1004', '1005']
        expected_res = []
        for ids in exp_ids:
            expected_res.append({ct.default_string_field_name: ids})
        
        # 6. query with empty expression and limit
        res = self.query(client, collection_name, filter="", limit=ct.default_limit)[0]
        query_res = [{ct.default_string_field_name: item[ct.default_string_field_name]} for item in res]
        assert query_res == expected_res
        
        # 7. query with empty expression, limit + offset
        expected_res_offset = expected_res[5:]
        res = self.query(client, collection_name, filter="", limit=5, offset=5)[0]
        query_res = [{ct.default_string_field_name: item[ct.default_string_field_name]} for item in res]
        assert query_res == expected_res_offset
        
        # 8. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_enable_mmap_query_with_expression(self, enable_dynamic_field):
        """
        target: turn on mmap use different expr queries
        method: turn on mmap and query with different expr
        expected: verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong",
                              enable_dynamic_field=enable_dynamic_field)
        # 2. insert data
        nb = 1000
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. enable mmap and create index
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.alter_index_properties(client, collection_name, ct.default_float_vec_field_name, properties={"mmap.enabled": True})
        self.load_collection(client, collection_name)
        # 4. filter result with expression in collection
        for expressions in cf.gen_normal_expressions_and_templates()[1:]:
            log.debug(f"expr: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, row in enumerate(rows):
                if enable_dynamic_field:
                    int64 = row[ct.default_int64_field_name]
                    float = row[ct.default_float_field_name]
                else:
                    int64 = row[ct.default_int64_field_name]
                    float = row[ct.default_float_field_name]
                if not expr or eval(expr):
                    filter_ids.append(row[ct.default_int64_field_name])

            # query and verify result
            res = self.query(client, collection_name, filter=expr, limit=nb)[0]
            query_ids = set(map(lambda x: x[ct.default_int64_field_name], res))
            assert query_ids == set(filter_ids)

            # query again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            res = self.query(client, collection_name, filter=expr, filter_params=expr_params, limit=nb)[0]
            query_ids = set(map(lambda x: x[ct.default_int64_field_name], res))
            assert query_ids == set(filter_ids)
        
        # 5. clean up
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_mmap_query_string_field_not_primary_is_empty(self):
        """
        target: enable mmap, use string expr to test query, string field is not the main field
        method: create collection , string field is primary
                enable mmap
                collection load and insert empty data with string field
                collection query uses string expr in string field
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
        # 2. insert data with empty string fields
        nb = 3000
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        # Set all string fields to empty
        for row in rows:
            row[ct.default_string_field_name] = ""
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        assert self.get_collection_stats(client, collection_name)[0]['row_count'] == nb
        # 3. enable mmap and create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        self.alter_index_properties(client, collection_name, ct.default_float_vec_field_name, properties={"mmap.enabled": True})
        self.load_collection(client, collection_name)
        # 4. query with empty string expression
        output_fields = [ct.default_int64_field_name, ct.default_float_field_name, ct.default_string_field_name]
        expr = 'varchar == ""'
        res = self.query(client, collection_name, filter=expr, output_fields=output_fields)[0]
        assert len(res) == nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_normal_string_expressions([ct.default_string_field_name]))
    def test_milvus_client_mmap_query_string_is_primary(self, expression):
        """
        target: test query with output field only primary field
        method: specify string primary field as output field
        expected: return string primary field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with string as primary key
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=False)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. enable mmap and create index
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.alter_index_properties(client, collection_name, ct.default_float_vec_field_name, properties={"mmap.enabled": True})
        self.load_collection(client, collection_name)
        # 4. query with string expression and only string field as output
        res = self.query(client, collection_name, filter=expression, output_fields=[ct.default_string_field_name])[0]
        assert set(res[0].keys()) == {ct.default_string_field_name}
        self.drop_collection(client, collection_name)


class TestMilvusClientGetInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("name",
                             ["12-s", "12 s", "(mn)", "中文", "%$#",
                              "".join("a" for i in range(ct.max_name_length + 1))])
    def test_milvus_client_get_invalid_collection_name(self, name):
        """
        target: test get interface invalid cases
        method: invalid collection name
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [i for i in range(default_nb)]
        # 3. get first primary key
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name"}
        self.get(client, name, ids=pks[0:1],
                 check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_get_not_exist_collection_name(self):
        """
        target: test get interface invalid cases
        method: invalid collection name
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [i for i in range(default_nb)]
        # 3. get first primary key
        name = "invalid"
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection[database=default][collection={name}]"}
        self.get(client, name, ids=pks[0:1],
                 check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_ids", ["中文", "%$#"])
    def test_milvus_client_get_invalid_ids(self, invalid_ids):
        """
        target: test get interface invalid cases
        method: invalid collection name
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. get first primary key
        error = {ct.err_code: 1100, ct.err_msg: f"cannot parse expression"}
        self.get(client, collection_name, ids=invalid_ids,
                 check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)


class TestMilvusClientGetValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_get_normal(self):
        """
        target: test get interface
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [i for i in range(default_nb)]
        # 3. get first primary key
        first_pk_data = self.get(client, collection_name, ids=pks[0:1])[0]
        assert len(first_pk_data) == len(pks[0:1])
        first_pk_data_1 = self.get(client, collection_name, ids=0)[0]
        assert first_pk_data == first_pk_data_1
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_get_output_fields(self):
        """
        target: test get interface with output fields
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [i for i in range(default_nb)]
        # 3. get first primary key
        output_fields_array = [default_primary_key_field_name, default_vector_field_name,
                               default_float_field_name, default_string_field_name]
        first_pk_data = self.get(client, collection_name, ids=pks[0:1], output_fields=output_fields_array)[0]
        assert len(first_pk_data) == len(pks[0:1])
        assert len(first_pk_data[0]) == len(output_fields_array)
        first_pk_data_1 = self.get(client, collection_name, ids=0, output_fields=output_fields_array)[0]
        assert first_pk_data == first_pk_data_1
        assert len(first_pk_data_1[0]) == len(output_fields_array)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2056")
    def test_milvus_client_get_normal_string(self):
        """
        target: test get interface for string field
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [str(i) for i in range(default_nb)]
        # 3. get first primary key
        first_pk_data = self.get(client, collection_name, ids=pks[0:1])[0]
        assert len(first_pk_data) == len(pks[0:1])
        first_pk_data_1 = self.get(client, collection_name, ids="0")[0]
        assert first_pk_data == first_pk_data_1

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="pymilvus issue 2056")
    def test_milvus_client_get_normal_string_output_fields(self):
        """
        target: test get interface for string field
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [str(i) for i in range(default_nb)]
        # 3. get first primary key
        output_fields_array = [default_primary_key_field_name, default_vector_field_name,
                               default_float_field_name, default_string_field_name]
        first_pk_data = self.get(client, collection_name, ids=pks[0:1], output_fields=output_fields_array)[0]
        assert len(first_pk_data) == len(pks[0:1])
        assert len(first_pk_data[0]) == len(output_fields_array)
        first_pk_data_1 = self.get(client, collection_name, ids="0", output_fields=output_fields_array)[0]
        assert first_pk_data == first_pk_data_1
        assert len(first_pk_data_1[0]) == len(output_fields_array)
        self.drop_collection(client, collection_name)


class TestMilvusClientQueryJsonPathIndex(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=["INVERTED"])
    def supported_varchar_scalar_index(self, request):
        yield request.param

    # @pytest.fixture(scope="function", params=["DOUBLE", "VARCHAR", "json"", "bool"])
    @pytest.fixture(scope="function", params=["DOUBLE"])
    def supported_json_cast_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    @pytest.mark.parametrize("single_data_num", [50])
    def test_milvus_client_search_json_path_index_all_expressions(self, enable_dynamic_field, supported_json_cast_type,
                                                                  supported_varchar_scalar_index, is_flush, is_release,
                                                                  single_data_num):
        """
        target: test query after json path index with all supported basic expressions
        method: Query after json path index with all supported basic expressions
        step: 1. create collection
              2. insert with different data distribution
              3. flush if specified
              4. query when there is no json path index under all expressions
              5. release if specified
              6. prepare index params with json path index
              7. create json path index
              8. create same json index twice
              9. reload collection if released before to make sure the new index load successfully
              10. sleep for 60s to make sure the new index load successfully without release and reload operations
              11. query after there is json path index under all expressions which should get the same result
                  with that without json path index
        expected: query successfully after there is json path index under all expressions which should get the same result
                  with that without json path index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "json_field"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb+60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = single_data_num
        for i in range(len(inserted_data_distribution)):
            rows = [{default_primary_key_field_name: j, default_vector_field_name: vectors[j],
                     default_string_field_name: f"{j}", json_field_name: inserted_data_distribution[i]} for j in
                    range(i * nb_single, (i + 1) * nb_single)]
            assert len(rows) == nb_single
            self.insert(client, collection_name=collection_name, data=rows)
            log.info(f"inserted {nb_single} {inserted_data_distribution[i]}")
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 4. query when there is no json path index under all expressions
        # skip negative expression for issue 40685
        # "my_json['a'] != 1", "my_json['a'] != 1.0", "my_json['a'] != '1'", "my_json['a'] != 1.1", "my_json['a'] not in [1]"
        express_list = cf.gen_json_field_expressions_all_single_operator()
        compare_dict = {}
        for i in range(len(express_list)):
            json_list = []
            id_list = []
            log.info(f"query with filter {express_list[i]} before json path index is:")
            res = self.query(client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"])[0]
            count = res[0]['count(*)']
            log.info(f"The count(*) after query with filter {express_list[i]} before json path index is: {count}")
            res = self.query(client, collection_name=collection_name, filter=express_list[i], output_fields=[f"{json_field_name}"])[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{json_field_name}"])
            assert count == len(id_list)
            assert count == len(json_list)
            compare_dict.setdefault(f'{i}', {})
            compare_dict[f'{i}']["id_list"] = id_list
            compare_dict[f'{i}']["json_list"] = json_list
        # 5. release if specified
        if is_release:
            self.release_collection(client, collection_name)
            self.drop_index(client, collection_name, default_vector_field_name)
        # 6. prepare index params with json path index
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        json_path_list = [f"{json_field_name}", f"{json_field_name}[0]", f"{json_field_name}[1]",
                          f"{json_field_name}[6]", f"{json_field_name}['a']", f"{json_field_name}['a']['b']",
                          f"{json_field_name}['a'][0]", f"{json_field_name}['a'][6]", f"{json_field_name}['a'][0]['b']",
                          f"{json_field_name}['a']['b']['c']", f"{json_field_name}['a']['b'][0]['d']",
                          f"{json_field_name}[10000]", f"{json_field_name}['a']['c'][0]['d']"]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        for i in range(len(json_path_list)):
            index_params.add_index(field_name=json_field_name, index_name=index_name + f'{i}',
                                   index_type=supported_varchar_scalar_index,
                                   params={"json_cast_type": supported_json_cast_type,
                                           "json_path": json_path_list[i]})
        # 7. create json path index
        self.create_index(client, collection_name, index_params)
        # 8. create same json index twice
        self.create_index(client, collection_name, index_params)
        # 9. reload collection if released before to make sure the new index load successfully
        if is_release:
            self.load_collection(client, collection_name)
        else:
            # 10. sleep for 60s to make sure the new index load successfully without release and reload operations
            time.sleep(60)
        # 11. query after there is json path index under all expressions which should get the same result
        # with that without json path index
        for i in range(len(express_list)):
            json_list = []
            id_list = []
            log.info(f"query with filter {express_list[i]} after json path index is:")
            count = self.query(client, collection_name=collection_name, filter=express_list[i],
                               output_fields=["count(*)"])[0]
            log.info(f"The count(*) after query with filter {express_list[i]} after json path index is: {count}")
            res = self.query(client, collection_name=collection_name, filter=express_list[i],
                             output_fields=[f"{json_field_name}"])[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{json_field_name}"])
            if len(json_list) != len(compare_dict[f'{i}']["json_list"]):
                log.debug(f"json field after json path index under expression {express_list[i]} is:")
                log.debug(json_list)
                log.debug(f"json field before json path index to be compared under expression {express_list[i]} is:")
                log.debug(compare_dict[f'{i}']["json_list"])
            assert json_list == compare_dict[f'{i}']["json_list"]
            if len(id_list) != len(compare_dict[f'{i}']["id_list"]):
                log.debug(f"primary key field after json path index under expression {express_list[i]} is:")
                log.debug(id_list)
                log.debug(f"primary key field before json path index to be compared under expression {express_list[i]} is:")
                log.debug(compare_dict[f'{i}']["id_list"])
            assert id_list == compare_dict[f'{i}']["id_list"]
            log.info(f"PASS with expression {express_list[i]}")
