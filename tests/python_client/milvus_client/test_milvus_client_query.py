import pytest
from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
import pandas as pd
import numpy as np

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
    def test_milvus_client_query_expr_not_existed_field(self):
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
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong", enable_dynamic_field=False)
        # 2. insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # 3. query with non-existent field
        term_expr = 'invalid_field in [1, 2]'
        error = {ct.err_code: 65535,
                 ct.err_msg: f"cannot parse expression: {term_expr}, error: field invalid_field not exist"}
        self.query(client, collection_name, filter=term_expr,
                   check_task=CheckTasks.err_res, check_items=error)
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
    def test_milvus_client_query_output_fields_all(self):
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
                         output_fields=["*"],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: rows,
                                      "with_vec": True,
                                      "pk_name": default_primary_key_field_name})[0]
        assert set(res[0].keys()) == {default_primary_key_field_name, default_vector_field_name,
                                      default_float_field_name, default_string_field_name}
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
