import numpy as np
import random
import pytest
from pymilvus import DataType
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base

prefix = "search_collection"
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name
default_float_vec_field_name = ct.default_float_vec_field_name
default_json_search_exp = "json_field[\"number\"] >= 0"
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]


@pytest.mark.xdist_group("TestSearchJSONShared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchJSONShared(TestMilvusClientV2Base):
    """Shared collection for JSON expression tests.
    Schema: int64(PK), float, varchar(65535), json, float_vector(128), dynamic=False
    Data: 3000 rows, json contains {"number": i, "list": [i, i+1, i+2]}
    Index: COSINE on float_vector
    """
    shared_alias = "TestSearchJSONShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchJSONShared" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        data = []
        for i in range(3000):
            row = {
                ct.default_int64_field_name: i,
                ct.default_float_field_name: i * 1.0,
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: {"number": i, "list": [i, i + 1, i + 2]},
                ct.default_float_vec_field_name: gen_vectors(1, default_dim)[0]
            }
            data.append(row)
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nq", [2, 500])
    def test_search_json_expression_default(self, nq):
        """
        target: test search case with default json expression (enable_dynamic=False)
        method: search with json filter on shared collection
        expected: 1. search successfully with limit(topK)
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = [[random.random() for _ in range(default_dim)] for _ in range(nq)]
        # search with json expression
        self.search(client, self.collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_json_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_expression_json_contains(self):
        """
        target: test search with expression using json_contains (enable_dynamic=False)
        method: search with expression (json_contains)
        expected: search successfully
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_expression_json_contains: Searching collection %s" %
                 self.collection_name)
        expressions = [
            "json_contains(json_field['list'], 100)", "JSON_CONTAINS(json_field['list'], 100)"]
        for expression in expressions:
            self.search(client, self.collection_name,
                        data=vectors[:default_nq],
                        anns_field=default_search_field,
                        search_params=default_search_params,
                        limit=default_limit,
                        filter=expression,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": default_nq,
                                     "limit": 3,
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_json_contains_combined_with_normal(self):
        """
        target: test search with expression using json_contains combined with normal expression (enable_dynamic=False)
        method: search with expression (json_contains && int64 >)
        expected: search successfully
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_expression_json_contains_combined_with_normal: Searching collection %s" %
                 self.collection_name)
        # With data {"number": i, "list": [i, i+1, i+2]}, value 1000 is in lists of rows 998, 999, 1000
        # Combined with int64 > 999, only row 1000 matches
        tar = 1000
        expressions = [f"json_contains(json_field['list'], {tar}) && int64 > {tar - 1}",
                       f"JSON_CONTAINS(json_field['list'], {tar}) && int64 > {tar - 1}"]
        for expression in expressions:
            self.search(client, self.collection_name,
                        data=vectors[:default_nq],
                        anns_field=default_search_field,
                        search_params=default_search_params,
                        limit=default_limit,
                        filter=expression,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": default_nq,
                                     "limit": 1,
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_json_contains_list(self):
        """
        target: test search with expression using json_contains on list field (auto_id=False)
        method: search with expression (json_contains)
        expected: search successfully
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_expression_json_contains_list: Searching collection %s" %
                 self.collection_name)
        # With data {"number": i, "list": [i, i+1, i+2]}, value 100 is in lists of rows 98, 99, 100
        expressions = [
            "json_contains(json_field['list'], 100)", "JSON_CONTAINS(json_field['list'], 100)"]
        for expression in expressions:
            self.search(client, self.collection_name,
                        data=vectors[:default_nq],
                        anns_field=default_search_field,
                        search_params=default_search_params,
                        limit=default_limit,
                        filter=expression,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": default_nq,
                                     "limit": 3,
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})


@pytest.mark.xdist_group("TestSearchArrayShared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchArrayShared(TestMilvusClientV2Base):
    """Shared collection for array expression tests.
    Schema: int64(PK), float_array(ARRAY<FLOAT>), string_array(ARRAY<VARCHAR>), float_vector(128)
    Data: default_nb rows
    Index: COSINE on float_vector
    """
    shared_alias = "TestSearchArrayShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchArrayShared" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = cf.gen_array_collection_schema()
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        # Insert data with custom string_field_value
        self.__class__.string_field_value = [[str(j) for j in range(i, i + 3)] for i in range(ct.default_nb)]
        data = cf.gen_array_dataframe_data()
        data[ct.default_string_array_field_name] = self.string_field_value
        self.insert(client, self.collection_name, data=data.to_dict(orient='records'))
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains", "ARRAY_CONTAINS"])
    def test_search_expr_array_contains(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client(alias=self.shared_alias)
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, '1000')"
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={},
                             limit=ct.default_nb,
                             filter=expression)
        exp_ids = cf.assert_json_contains(expression, self.string_field_value)
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains", "ARRAY_CONTAINS"])
    def test_search_expr_not_array_contains(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client(alias=self.shared_alias)
        expression = f"not {expr_prefix}({ct.default_string_array_field_name}, '1000')"
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={},
                             limit=ct.default_nb,
                             filter=expression)
        exp_ids = cf.assert_json_contains(expression, self.string_field_value)
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_all", "ARRAY_CONTAINS_ALL"])
    def test_search_expr_array_contains_all(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client(alias=self.shared_alias)
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, ['1000'])"
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={},
                             limit=ct.default_nb,
                             filter=expression)
        exp_ids = cf.assert_json_contains(expression, self.string_field_value)
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_any", "ARRAY_CONTAINS_ANY",
                                             "not array_contains_any", "not ARRAY_CONTAINS_ANY"])
    def test_search_expr_array_contains_any(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        client = self._client(alias=self.shared_alias)
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, ['1000'])"
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={},
                             limit=ct.default_nb,
                             filter=expression)
        exp_ids = cf.assert_json_contains(expression, self.string_field_value)
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_all", "ARRAY_CONTAINS_ALL",
                                             "array_contains_any", "ARRAY_CONTAINS_ANY"])
    def test_search_expr_array_contains_invalid(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains(a, b) b not list
        expected: report error
        """
        client = self._client(alias=self.shared_alias)
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, '1000')"
        error = {ct.err_code: 1100,
                 ct.err_msg: f"cannot parse expression: {expression}, "
                             f"error: ContainsAll operation element must be an array"}
        if expr_prefix in ["array_contains_any", "ARRAY_CONTAINS_ANY"]:
            error = {ct.err_code: 1100,
                     ct.err_msg: f"cannot parse expression: {expression}, "
                                 f"error: ContainsAny operation element must be an array"}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params={},
                    limit=ct.default_nb,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items=error)


class TestSearchJSONIndependent(TestMilvusClientV2Base):
    """ Test case of search interface with JSON expressions """

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
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        # Insert data
        data = cf.gen_default_rows_data(nb=default_nb, dim=dim, with_json=True)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        # Create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. search
        log.info("test_search_json_expression_object: searching collection %s" %
                 collection_name)
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 3. search after insert
        json_search_exp = "json_field > 0"
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=json_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1,
                                 ct.err_msg: "can not comparisons jsonField directly"})

    """
    ******************************************************************
    #  The followings are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nq", [2, 500])
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    def test_search_json_expression_default(self, nq, is_flush, enable_dynamic_field):
        """
        target: test search case with default json expression
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        # Insert data
        data = cf.gen_default_rows_data(nb=default_nb, dim=dim, auto_id=True, with_json=True)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        # Create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. search after insert
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_json_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "ids": insert_ids,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    @pytest.mark.parametrize("is_flush", [False, True])
    def test_search_json_nullable_load_before_insert(self, nq, is_flush):
        """
        target: test search case with default json expression
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize collection
        dim = 64
        enable_dynamic_field = False
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON, nullable=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        # Create index and load first (load_before_insert pattern)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # Insert data with null json
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nb)]
        rows = []
        for i in range(default_nb):
            rows.append({
                ct.default_float_field_name: np.float32(i),
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: None,
                ct.default_float_vec_field_name: search_vectors[i]
            })
        self.insert(client, collection_name, data=rows)
        if is_flush:
            self.flush(client, collection_name)
        # 2. search after insert
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nq", [2, 500])
    @pytest.mark.parametrize("is_flush", [False, True])
    def test_search_json_nullable_insert_before_load(self, nq, is_flush):
        """
        target: test search case with default json expression
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize collection
        dim = 64
        enable_dynamic_field = False
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON, nullable=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        # Create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        # Insert data with null json before load
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nb)]
        rows = []
        for i in range(default_nb):
            rows.append({
                ct.default_float_field_name: np.float32(i),
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: None,
                ct.default_float_vec_field_name: search_vectors[i]
            })
        self.insert(client, collection_name, data=rows)
        if is_flush:
            self.flush(client, collection_name)
        # Load after insert
        self.load_collection(client, collection_name)
        # 2. search after insert
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    def test_search_expression_json_contains(self, enable_dynamic_field):
        """
        target: test search with expression using json_contains (enable_dynamic=True)
        method: search with expression (json_contains)
        expected: search successfully
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

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
        self.insert(client, collection_name, data=array)

        # 3. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        log.info("test_search_with_output_field_json_contains: Searching collection %s" %
                 collection_name)
        expressions = [
            "json_contains(json_field['list'], 100)", "JSON_CONTAINS(json_field['list'], 100)"]
        for expression in expressions:
            self.search(client, collection_name,
                        data=vectors[:default_nq],
                        anns_field=default_search_field,
                        search_params=default_search_params,
                        limit=default_limit,
                        filter=expression,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": default_nq,
                                     "limit": 3,
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("auto_id", [True])
    def test_search_expression_json_contains_list(self, auto_id):
        """
        target: test search with expression using json_contains (auto_id=True)
        method: search with expression (json_contains)
        expected: search successfully
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

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
        self.insert(client, collection_name, data=array)

        # 3. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        log.info("test_search_with_output_field_json_contains: Searching collection %s" %
                 collection_name)
        expressions = [
            "json_contains(json_field, 100)", "JSON_CONTAINS(json_field, 100)"]
        for expression in expressions:
            self.search(client, collection_name,
                        data=vectors[:default_nq],
                        anns_field=default_search_field,
                        search_params=default_search_params,
                        limit=limit,
                        filter=expression,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": default_nq,
                                     "limit": limit,
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    def test_search_expression_json_contains_combined_with_normal(self, enable_dynamic_field):
        """
        target: test search with expression using json_contains (enable_dynamic=True)
        method: search with expression (json_contains)
        expected: search successfully
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

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
        self.insert(client, collection_name, data=array)

        # 3. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        log.info("test_search_with_output_field_json_contains: Searching collection %s" %
                 collection_name)
        tar = 1000
        expressions = [f"json_contains(json_field['list'], '{tar}') && int64 > {tar - limit // 2}",
                       f"JSON_CONTAINS(json_field['list'], '{tar}') && int64 > {tar - limit // 2}"]
        for expression in expressions:
            self.search(client, collection_name,
                        data=vectors[:default_nq],
                        anns_field=default_search_field,
                        search_params=default_search_params,
                        limit=limit,
                        filter=expression,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": default_nq,
                                     "limit": limit // 2,
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_any", "ARRAY_CONTAINS_ANY",
                                             "not array_contains_any", "not ARRAY_CONTAINS_ANY"])
    def test_search_expr_array_contains_any_with_float_field(self, expr_prefix):
        """
        target: test query with expression using array_contains with float field
        method: query with expression using array_contains with float field
        expected: succeed
        """
        # 1. create a collection
        client = self._client()
        schema = cf.gen_array_collection_schema()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data
        float_field_value = [[random.random() for _ in range(i, i + 3)] for i in range(ct.default_nb)]
        data = cf.gen_array_dataframe_data()
        data[ct.default_float_array_field_name] = float_field_value
        self.insert(client, collection_name, data=data.to_dict(orient='records'))
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)

        # 3. search with array_contains_any with float and int target
        self.load_collection(client, collection_name)
        expression = f"{expr_prefix}({ct.default_float_array_field_name}, [0.5, 0.6, 1, 2])"
        res, _ = self.search(client, collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={},
                             limit=ct.default_nb,
                             filter=expression)
        exp_ids = cf.assert_json_contains(expression, float_field_value)
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)
