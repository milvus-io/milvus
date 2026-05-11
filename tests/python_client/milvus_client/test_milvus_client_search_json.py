import numpy as np
import pytest
from pymilvus import DataType
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base

default_nb = ct.default_nb
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
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_json_search_exp = f"{default_json_field_name}[\"number\"] >= 1000"


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

        nb = 3000
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        # Override json with deterministic pattern for predictable filter expressions
        for i in range(nb):
            data[i][ct.default_json_field_name] = {"number": i, "list": [i, i + 1, i + 2]}
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
        target: verify search with JSON key comparison filter returns correct results
        method: 1. search with filter json_field["number"] >= 1500 on shared collection
                2. check nq, limit, distance order via check_task
                3. manually verify returned JSON structure and data consistency
        expected: all results have complete JSON with "number" and "list" keys,
                  "number" value is contained in "list" (data integrity, not enforced by filter)
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_vectors(nq, default_dim)
        # Use a non-trivial filter that excludes rows with number < 1500
        json_filter = f'{default_json_field_name}["number"] >= 1500'
        res, _ = self.search(client, self.collection_name,
                             data=search_vectors[:nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=json_filter,
                             output_fields=[ct.default_json_field_name],
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": nq,
                                          "limit": default_limit,
                                          "metric": "COSINE",
                                          "enable_milvus_client_api": True,
                                          "pk_name": ct.default_int64_field_name})
        # verify JSON structure and data consistency (not enforced by the search filter)
        for hits in res:
            for hit in hits:
                json_val = hit.entity.get(default_json_field_name)
                assert json_val is not None, "json_field should be in output"
                assert "number" in json_val and "list" in json_val, \
                    f"JSON should have 'number' and 'list' keys, got {json_val.keys()}"
                # data pattern: list = [number, number+1, number+2], so number is always in list
                assert json_val["number"] in json_val["list"], \
                    f"number {json_val['number']} should be in list {json_val['list']}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_expression_json_contains(self):
        """
        target: verify search with json_contains expression filters correctly (case-insensitive)
        method: 1. search with json_contains(json_field['list'], 100) on shared collection
                2. check nq, limit, distance order via check_task
                3. verify exactly 3 rows match (rows 98, 99, 100 each contain 100 in their list)
        expected: limit=3 results per query, distances in COSINE order
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_expression_json_contains: Searching collection %s" %
                 self.collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
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
                                     "metric": "COSINE",
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_json_contains_combined_with_normal(self):
        """
        target: verify search with json_contains combined with scalar filter narrows results correctly
        method: 1. search with filter "json_contains(list, 1000) && int64 > 999" on shared collection
                2. json_contains(list, 1000) matches rows 998,999,1000; int64 > 999 matches 1000+
                3. intersection = row 1000 only → expect limit=1
        expected: exactly 1 result per query, distances in COSINE order
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_expression_json_contains_combined_with_normal: Searching collection %s" %
                 self.collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
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
                                     "metric": "COSINE",
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})



@pytest.mark.xdist_group("TestSearchArrayShared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchArrayShared(TestMilvusClientV2Base):
    """Shared collection for array expression tests.
    Schema: int64(PK), float_array(ARRAY<FLOAT>), string_array(ARRAY<VARCHAR>), float_vector(128)
    Data: default_nb rows, string_array[i] = [str(i), str(i+1), str(i+2)]
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
        target: verify search with array_contains expression returns rows containing the target value
        method: 1. search with array_contains(string_array, '1000') on shared array collection
                2. compute expected matching IDs locally
                3. assert returned IDs match expected
        expected: returned IDs exactly match locally computed expected IDs
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, '1000')"
        exp_ids = cf.assert_json_contains(expression, self.string_field_value)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={"metric_type": "COSINE"},
                             limit=ct.default_nb,
                             filter=expression,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq, "limit": len(exp_ids),
                                          "metric": "COSINE", "enable_milvus_client_api": True,
                                          "pk_name": ct.default_int64_field_name})
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains", "ARRAY_CONTAINS"])
    def test_search_expr_not_array_contains(self, expr_prefix):
        """
        target: verify search with NOT array_contains returns rows NOT containing the target value
        method: 1. search with not array_contains(string_array, '1000') on shared collection
                2. compute expected matching IDs locally
                3. assert returned IDs match expected
        expected: returned IDs exactly match locally computed expected IDs (complement set)
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expression = f"not {expr_prefix}({ct.default_string_array_field_name}, '1000')"
        exp_ids = cf.assert_json_contains(expression, self.string_field_value)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={"metric_type": "COSINE"},
                             limit=ct.default_nb,
                             filter=expression,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq, "limit": len(exp_ids),
                                          "metric": "COSINE", "enable_milvus_client_api": True,
                                          "pk_name": ct.default_int64_field_name})
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_all", "ARRAY_CONTAINS_ALL"])
    def test_search_expr_array_contains_all(self, expr_prefix):
        """
        target: verify search with array_contains_all returns rows containing ALL target values
        method: 1. search with array_contains_all(string_array, ['1000']) on shared collection
                2. compute expected matching IDs locally
                3. assert returned IDs match expected
        expected: returned IDs exactly match locally computed expected IDs
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, ['1000'])"
        exp_ids = cf.assert_json_contains(expression, self.string_field_value)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={"metric_type": "COSINE"},
                             limit=ct.default_nb,
                             filter=expression,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq, "limit": len(exp_ids),
                                          "metric": "COSINE", "enable_milvus_client_api": True,
                                          "pk_name": ct.default_int64_field_name})
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_any", "ARRAY_CONTAINS_ANY",
                                             "not array_contains_any", "not ARRAY_CONTAINS_ANY"])
    def test_search_expr_array_contains_any(self, expr_prefix):
        """
        target: verify search with array_contains_any returns rows containing ANY of the target values
        method: 1. search with [not] array_contains_any(string_array, ['1000']) on shared collection
                2. compute expected matching IDs locally
                3. assert returned IDs match expected
        expected: returned IDs exactly match locally computed expected IDs
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expression = f"{expr_prefix}({ct.default_string_array_field_name}, ['1000'])"
        exp_ids = cf.assert_json_contains(expression, self.string_field_value)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={"metric_type": "COSINE"},
                             limit=ct.default_nb,
                             filter=expression,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq, "limit": len(exp_ids),
                                          "metric": "COSINE", "enable_milvus_client_api": True,
                                          "pk_name": ct.default_int64_field_name})
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_all", "ARRAY_CONTAINS_ALL",
                                             "array_contains_any", "ARRAY_CONTAINS_ANY"])
    def test_search_expr_array_contains_invalid(self, expr_prefix):
        """
        target: verify array_contains_all/any with non-list argument raises error
        method: 1. search with array_contains_all/any(string_array, '1000') (string, not list)
                2. check error response
        expected: error 1100 with "element must be an array" message
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
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
                    search_params={"metric_type": "COSINE"},
                    limit=ct.default_nb,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items=error)


class TestSearchJSONIndependent(TestMilvusClientV2Base):
    """Independent tests for JSON search scenarios requiring unique schemas
    (dynamic field, auto_id, nullable JSON, load ordering)
    """

    @pytest.mark.skip("Supported json like: 1, \"abc\", [1,2,3,4]")
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_json_expression_object(self):
        """
        target: verify search with direct JSON field comparison raises error
        method: 1. create collection with JSON field, insert data
                2. search with filter "json_field > 0" (comparing JSON object directly)
        expected: error indicating direct JSON comparison not supported
        """
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
        data = cf.gen_default_rows_data(nb=default_nb, dim=dim, with_json=True)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        search_vectors = cf.gen_vectors(nq, dim)
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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nq", [2, 500])
    @pytest.mark.parametrize("is_flush", [False, True])
    def test_search_json_expression_default(self, nq, is_flush):
        """
        target: verify search with JSON filter on dynamic-field-enabled collection (with/without flush)
        method: 1. create collection with enable_dynamic_field=True, insert data with JSON
                2. search with json_field["number"] >= 0 filter
                3. check nq, limit, IDs via check_task
        expected: search returns correct results with distances in COSINE order
        """
        dim = 64
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(nb=default_nb, dim=dim, auto_id=True, with_json=True)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        if is_flush:
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        search_vectors = cf.gen_vectors(nq, dim)
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
                                 "metric": "COSINE",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    @pytest.mark.parametrize("is_flush", [False, True])
    def test_search_json_nullable_load_before_insert(self, nq, is_flush):
        """
        target: verify search works when nullable JSON (all nulls) is loaded before insert
        method: 1. create collection with nullable JSON, create index, load
                2. insert data with json_field=None
                3. search without JSON filter (all nulls)
        expected: search returns results with distances in COSINE order
        """
        dim = 64
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON, nullable=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # Insert data with null json — reuse vectors for search-self verification
        insert_vectors = cf.gen_vectors(default_nb, dim)
        rows = []
        for i in range(default_nb):
            rows.append({
                ct.default_float_field_name: np.float32(i),
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: None,
                ct.default_float_vec_field_name: insert_vectors[i]
            })
        self.insert(client, collection_name, data=rows)
        if is_flush:
            self.flush(client, collection_name)
        self.search(client, collection_name,
                    data=insert_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": default_limit,
                                 "metric": "COSINE",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nq", [2, 500])
    @pytest.mark.parametrize("is_flush", [False, True])
    def test_search_json_nullable_insert_before_load(self, nq, is_flush):
        """
        target: verify search works when nullable JSON (all nulls) is inserted before load
        method: 1. create collection with nullable JSON, create index
                2. insert data with json_field=None
                3. load collection, then search
        expected: search returns results with distances in COSINE order
        """
        dim = 64
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON, nullable=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        # Insert data with null json before load — reuse vectors for search-self verification
        insert_vectors = cf.gen_vectors(default_nb, dim)
        rows = []
        for i in range(default_nb):
            rows.append({
                ct.default_float_field_name: np.float32(i),
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: None,
                ct.default_float_vec_field_name: insert_vectors[i]
            })
        self.insert(client, collection_name, data=rows)
        if is_flush:
            self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        self.search(client, collection_name,
                    data=insert_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": default_limit,
                                 "metric": "COSINE",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_expression_json_contains(self):
        """
        target: verify json_contains with dynamic field enabled returns correct results
        method: 1. create collection with enable_dynamic_field=True, insert JSON with list field
                2. search with json_contains(json_field['list'], 100)
                3. rows 98,99,100 contain 100 → expect limit=3
        expected: 3 results per query, distances in COSINE order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        nb = default_nb
        all_vectors = cf.gen_vectors(nb, default_dim)
        array = []
        for i in range(nb):
            array.append({
                default_int64_field_name: i,
                default_float_field_name: i * 1.0,
                default_string_field_name: str(i),
                default_json_field_name: {"number": i, "list": [i, i + 1, i + 2]},
                default_float_vec_field_name: all_vectors[i]
            })
        self.insert(client, collection_name, data=array)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        log.info("test_search_expression_json_contains: Searching collection %s" %
                 collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
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
                                     "metric": "COSINE",
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_json_contains_list(self):
        """
        target: verify json_contains on JSON-as-list (not nested key) with auto_id=True
        method: 1. create collection with auto_id=True, json_field is a plain list [i, i+1, ..., i+99]
                2. search with json_contains(json_field, 100) — rows 1..100 contain 100
                3. expect limit=100
        expected: 100 results per query, distances in COSINE order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        limit = 100
        nb = default_nb
        all_vectors = cf.gen_vectors(nb, default_dim)
        array = []
        for i in range(nb):
            array.append({
                default_json_field_name: [j for j in range(i, i + limit)],
                default_float_vec_field_name: all_vectors[i]
            })
        self.insert(client, collection_name, data=array)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        log.info("test_search_expression_json_contains_list: Searching collection %s" %
                 collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
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
                                     "metric": "COSINE",
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_json_contains_combined_with_normal(self):
        """
        target: verify json_contains + scalar filter with dynamic field and string-valued JSON list
        method: 1. create collection with dynamic field, JSON list contains string values
                2. search with json_contains(list, '1000') && int64 > 950
                3. json_contains matches rows 901..1000, int64 > 950 matches 951+
                4. intersection = rows 951..1000 → 50 results
        expected: 50 results per query, distances in COSINE order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        limit = 100
        nb = default_nb
        all_vectors = cf.gen_vectors(nb, default_dim)
        array = []
        for i in range(nb):
            array.append({
                default_int64_field_name: i,
                default_float_field_name: i * 1.0,
                default_string_field_name: str(i),
                default_json_field_name: {"number": i, "list": [str(j) for j in range(i, i + limit)]},
                default_float_vec_field_name: all_vectors[i]
            })
        self.insert(client, collection_name, data=array)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        log.info("test_search_expression_json_contains_combined_with_normal: Searching collection %s" %
                 collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
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
                                     "metric": "COSINE",
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["array_contains_any", "ARRAY_CONTAINS_ANY",
                                             "not array_contains_any", "not ARRAY_CONTAINS_ANY"])
    def test_search_expr_array_contains_any_with_float_field(self, expr_prefix):
        """
        target: verify array_contains_any with mixed float/int targets on float array field
        method: 1. create collection with float array field, insert deterministic float data
                2. search with array_contains_any(float_array, [0.5, 0.6, 1, 2])
                3. compute expected IDs locally and compare
        expected: returned IDs exactly match locally computed expected IDs
        """
        import random
        client = self._client()
        schema = cf.gen_array_collection_schema()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, schema=schema)

        float_field_value = [[random.random() for _ in range(i, i + 3)] for i in range(ct.default_nb)]
        data = cf.gen_array_dataframe_data()
        data[ct.default_float_array_field_name] = float_field_value
        self.insert(client, collection_name, data=data.to_dict(orient='records'))
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)

        self.load_collection(client, collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expression = f"{expr_prefix}({ct.default_float_array_field_name}, [0.5, 0.6, 1, 2])"
        res, _ = self.search(client, collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params={},
                             limit=ct.default_nb,
                             filter=expression)
        exp_ids = cf.assert_json_contains(expression, float_field_value)
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)


class TestMilvusClientSearchJsonPathIndex(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=["INVERTED"])
    def supported_varchar_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["JSON", "VARCHAR", "double", "bool"])
    def supported_json_cast_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_json_path_index_default(self, enable_dynamic_field, supported_json_cast_type,
                                                          supported_varchar_scalar_index, is_flush):
        """
        target: test search after the json path index created
        method: Search after creating json path index
        Step: 1. create schema
              2. prepare index_params with the required vector index params
              3. create collection with the above schema and index params
              4. insert
              5. flush if specified
              6. prepare json path index params
              7. create json path index using the above index params created in step 6
              8. create the same json path index again
              9. search with expressions related with the json paths
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 60, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i, "c": i}}} for i in
                range(default_nb)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: i} for i in
                range(default_nb, default_nb + 10)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {}} for i in
                range(default_nb + 10, default_nb + 20)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [1, 2, 3]}} for i in
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': 1}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 40, default_nb + 50)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': 1}} for i in
                range(default_nb + 50, default_nb + 60)]
        self.insert(client, collection_name, rows)
        if is_flush:
            self.flush(client, collection_name)
        # 2. prepare index params
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="FLAT", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_name=index_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '1',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '2',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '3',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '4',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        # 3. create index
        self.create_index(client, collection_name, index_params)
        # 4. create same json index twice
        self.create_index(client, collection_name, index_params)
        # 5. search without filter
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb + 60)]
        self.search(client, collection_name, vectors_to_search,
                    output_fields=[json_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 6. search with filter on json without output_fields
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        insert_ids = [default_nb / 2]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name} == {default_nb + 5}"
        insert_ids = [default_nb + 5]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name}['a'][0] == 1"
        insert_ids = [i for i in range(default_nb + 20, default_nb + 30)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'][0]['b'] == 1"
        insert_ids = [i for i in range(default_nb + 30, default_nb + 40)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'] == 1"
        insert_ids = [i for i in range(default_nb + 50, default_nb + 60)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_search_json_path_index_default_index_name(self, enable_dynamic_field,
                                                                     supported_json_cast_type,
                                                                     supported_varchar_scalar_index):
        """
        target: test json path index without specifying the index_name parameter
        method: create json path index without specifying the index_name parameter
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, is_primary=True, auto_id=False,
                         max_length=128)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        # 4. create index
        self.create_index(client, collection_name, index_params)
        # 5. search with filter on json with output_fields
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        vectors_to_search = [vectors[0]]
        insert_ids = [str(int(default_nb / 2))]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[json_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #40636")
    def test_milvus_client_search_json_path_index_on_non_json_field(self, supported_json_cast_type,
                                                                    supported_varchar_scalar_index):
        """
        target: test json path index on non-json field
        method: create json path index on int64 field
        expected: successfully with original inverted index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=default_primary_key_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{default_string_field_name}['a']['b']"})
        # 3. create index
        index_name = default_string_field_name
        self.create_index(client, collection_name, index_params)
        self.describe_index(client, collection_name, index_name,
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                # "json_cast_type": supported_json_cast_type, # issue 40426
                                "json_path": f"{default_string_field_name}['a']['b']",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": default_string_field_name,
                                "index_name": index_name})
        self.flush(client, collection_name)
        # 5. search with filter on json with output_fields
        expr = f"{default_primary_key_field_name} >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[default_string_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_search_diff_index_same_field_diff_index_name_diff_index_params(self, enable_dynamic_field,
                                                                                          supported_json_cast_type,
                                                                                          supported_varchar_scalar_index):
        """
        target: test search after different json path index with different default index name at the same time
        method: Search after different json path index with different default index name at the same index_params object
        expected: Search successfully
        """
        if enable_dynamic_field:
            pytest.skip('need to fix the field name when enabling dynamic field')
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        self.load_collection(client, collection_name)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        self.create_index(client, collection_name, index_params)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        self.create_index(client, collection_name, index_params)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        self.create_index(client, collection_name, index_params)
        # 4. release and load collection to make sure new index is loaded
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        # 5. search with filter on json with output_fields
        expr = f"{json_field_name}['a']['b'] >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[default_string_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    def test_milvus_client_json_search_index_same_json_path_diff_field(self, enable_dynamic_field,
                                                                       supported_json_cast_type,
                                                                       supported_varchar_scalar_index, is_flush,
                                                                       is_release):
        """
        target: test search after creating same json path for different field
        method: Search after creating same json path for different field
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
            schema.add_field(json_field_name + "1", DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {'b': i}},
                 json_field_name + "1": {'a': {'b': i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 3. release and drop index if specified
        if is_release:
            self.release_collection(client, collection_name)
            self.drop_index(client, collection_name, default_vector_field_name)
        # 4. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        self.create_index(client, collection_name, index_params)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name + "1",
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}1['a']['b']"})
        # 5. create index with json path index
        self.create_index(client, collection_name, index_params)
        if is_release:
            self.load_collection(client, collection_name)
        # 6. search with filter on json with output_fields on each json field
        expr = f"{json_field_name}['a']['b'] >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[json_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}1['a']['b'] >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[json_field_name + "1"],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_json_path_index_before_load(self, enable_dynamic_field, supported_json_cast_type,
                                                              supported_varchar_scalar_index, is_flush):
        """
        target: test search after creating json path index before load
        method: Search after creating json path index before load
        Step: 1. create schema
              2. prepare index_params with vector index params
              3. create collection with the above schema and index params
              4. release collection
              5. insert
              6. flush if specified
              7. prepare json path index params
              8. create index
              9. load collection
              10. search
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. release collection
        self.release_collection(client, collection_name)
        # 3. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 50, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in
                range(default_nb)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: i} for i in
                range(default_nb, default_nb + 10)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {}} for i in
                range(default_nb + 10, default_nb + 20)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [1, 2, 3]}} for i in
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': 1}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 40, default_nb + 50)]
        self.insert(client, collection_name, rows)
        # 4. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 5. prepare index params
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_name=index_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '1',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '2',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '3',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '4',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        # 5. create index
        self.create_index(client, collection_name, index_params)
        # 6. load collection
        self.load_collection(client, collection_name)
        # 7. search with filter on json without output_fields
        vectors_to_search = [vectors[0]]
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        insert_ids = [default_nb / 2]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name} == {default_nb + 5}"
        insert_ids = [default_nb + 5]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name}['a'][0] == 1"
        insert_ids = [i for i in range(default_nb + 20, default_nb + 30)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'][0]['b'] == 1"
        insert_ids = [i for i in range(default_nb + 30, default_nb + 40)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_json_path_index_after_release_load(self, enable_dynamic_field,
                                                                     supported_json_cast_type,
                                                                     supported_varchar_scalar_index, is_flush):
        """
        target: test search after creating json path index after release and load
        method: Search after creating json path index after release and load
        Step: 1. create schema
              2. prepare index_params with vector index params
              3. create collection with the above schema and index params
              4. insert
              5. flush if specified
              6. prepare json path index params
              7. create index
              8. release collection
              9. create index again
              10. load collection
              11. search with expressions related with the json paths
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 50, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in
                range(default_nb)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: i} for i in
                range(default_nb, default_nb + 10)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {}} for i in
                range(default_nb + 10, default_nb + 20)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [1, 2, 3]}} for i in
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': 1}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 40, default_nb + 50)]
        self.insert(client, collection_name, rows)
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 4. prepare index params
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_name=index_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '1',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '2',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '3',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '4',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        # 5. create json index
        self.create_index(client, collection_name, index_params)
        # 6. release collection
        self.release_collection(client, collection_name)
        # 7. create json index again
        self.create_index(client, collection_name, index_params)
        # 8. load collection
        self.load_collection(client, collection_name)
        # 9. search with filter on json without output_fields
        vectors_to_search = [vectors[0]]
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        insert_ids = [default_nb / 2]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name} == {default_nb + 5}"
        insert_ids = [default_nb + 5]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 1})
        expr = f"{json_field_name}['a'][0] == 1"
        insert_ids = [i for i in range(default_nb + 20, default_nb + 30)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'][0]['b'] == 1"
        insert_ids = [i for i in range(default_nb + 30, default_nb + 40)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)
