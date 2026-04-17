import numpy as np
import pytest
from pymilvus import DataType

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
from utils.util_pymilvus import *

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
default_json_search_exp = f'{default_json_field_name}["number"] >= 1000'


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
        res, _ = self.search(
            client,
            self.collection_name,
            data=search_vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=json_filter,
            output_fields=[ct.default_json_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "limit": default_limit,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )
        # verify JSON structure and data consistency (not enforced by the search filter)
        for hits in res:
            for hit in hits:
                json_val = hit.entity.get(default_json_field_name)
                assert json_val is not None, "json_field should be in output"
                assert "number" in json_val and "list" in json_val, (
                    f"JSON should have 'number' and 'list' keys, got {json_val.keys()}"
                )
                # data pattern: list = [number, number+1, number+2], so number is always in list
                assert json_val["number"] in json_val["list"], (
                    f"number {json_val['number']} should be in list {json_val['list']}"
                )

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
        log.info("test_search_expression_json_contains: Searching collection %s" % self.collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expressions = ["json_contains(json_field['list'], 100)", "JSON_CONTAINS(json_field['list'], 100)"]
        for expression in expressions:
            self.search(
                client,
                self.collection_name,
                data=vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=default_limit,
                filter=expression,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "limit": 3,
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )

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
        log.info(
            "test_search_expression_json_contains_combined_with_normal: Searching collection %s" % self.collection_name
        )
        vectors = cf.gen_vectors(default_nq, default_dim)
        # With data {"number": i, "list": [i, i+1, i+2]}, value 1000 is in lists of rows 998, 999, 1000
        # Combined with int64 > 999, only row 1000 matches
        tar = 1000
        expressions = [
            f"json_contains(json_field['list'], {tar}) && int64 > {tar - 1}",
            f"JSON_CONTAINS(json_field['list'], {tar}) && int64 > {tar - 1}",
        ]
        for expression in expressions:
            self.search(
                client,
                self.collection_name,
                data=vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=default_limit,
                filter=expression,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "limit": 1,
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )


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
        self.insert(client, self.collection_name, data=data.to_dict(orient="records"))
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
        res, _ = self.search(
            client,
            self.collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params={"metric_type": "COSINE"},
            limit=ct.default_nb,
            filter=expression,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": default_nq,
                "limit": len(exp_ids),
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )
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
        res, _ = self.search(
            client,
            self.collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params={"metric_type": "COSINE"},
            limit=ct.default_nb,
            filter=expression,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": default_nq,
                "limit": len(exp_ids),
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )
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
        res, _ = self.search(
            client,
            self.collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params={"metric_type": "COSINE"},
            limit=ct.default_nb,
            filter=expression,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": default_nq,
                "limit": len(exp_ids),
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr_prefix", ["array_contains_any", "ARRAY_CONTAINS_ANY", "not array_contains_any", "not ARRAY_CONTAINS_ANY"]
    )
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
        res, _ = self.search(
            client,
            self.collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params={"metric_type": "COSINE"},
            limit=ct.default_nb,
            filter=expression,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": default_nq,
                "limit": len(exp_ids),
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "expr_prefix", ["array_contains_all", "ARRAY_CONTAINS_ALL", "array_contains_any", "ARRAY_CONTAINS_ANY"]
    )
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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"cannot parse expression: {expression}, error: ContainsAll operation element must be an array",
        }
        if expr_prefix in ["array_contains_any", "ARRAY_CONTAINS_ANY"]:
            error = {
                ct.err_code: 1100,
                ct.err_msg: f"cannot parse expression: {expression}, "
                f"error: ContainsAny operation element must be an array",
            }
        self.search(
            client,
            self.collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params={"metric_type": "COSINE"},
            limit=ct.default_nb,
            filter=expression,
            check_task=CheckTasks.err_res,
            check_items=error,
        )


class TestSearchJSONIndependent(TestMilvusClientV2Base):
    """Independent tests for JSON search scenarios requiring unique schemas
    (dynamic field, auto_id, nullable JSON, load ordering)
    """

    @pytest.mark.skip('Supported json like: 1, "abc", [1,2,3,4]')
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
        self.search(
            client,
            collection_name,
            data=search_vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=json_search_exp,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "can not comparisons jsonField directly"},
        )

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
        self.search(
            client,
            collection_name,
            data=search_vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=default_json_search_exp,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "ids": insert_ids,
                "limit": default_limit,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )

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
            rows.append(
                {
                    ct.default_float_field_name: np.float32(i),
                    ct.default_string_field_name: str(i),
                    ct.default_json_field_name: None,
                    ct.default_float_vec_field_name: insert_vectors[i],
                }
            )
        self.insert(client, collection_name, data=rows)
        if is_flush:
            self.flush(client, collection_name)
        self.search(
            client,
            collection_name,
            data=insert_vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "limit": default_limit,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )

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
            rows.append(
                {
                    ct.default_float_field_name: np.float32(i),
                    ct.default_string_field_name: str(i),
                    ct.default_json_field_name: None,
                    ct.default_float_vec_field_name: insert_vectors[i],
                }
            )
        self.insert(client, collection_name, data=rows)
        if is_flush:
            self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            data=insert_vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "limit": default_limit,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )

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
            array.append(
                {
                    default_int64_field_name: i,
                    default_float_field_name: i * 1.0,
                    default_string_field_name: str(i),
                    default_json_field_name: {"number": i, "list": [i, i + 1, i + 2]},
                    default_float_vec_field_name: all_vectors[i],
                }
            )
        self.insert(client, collection_name, data=array)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        log.info("test_search_expression_json_contains: Searching collection %s" % collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expressions = ["json_contains(json_field['list'], 100)", "JSON_CONTAINS(json_field['list'], 100)"]
        for expression in expressions:
            self.search(
                client,
                collection_name,
                data=vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=default_limit,
                filter=expression,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "limit": 3,
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )

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
            array.append(
                {
                    default_json_field_name: [j for j in range(i, i + limit)],
                    default_float_vec_field_name: all_vectors[i],
                }
            )
        self.insert(client, collection_name, data=array)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        log.info("test_search_expression_json_contains_list: Searching collection %s" % collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expressions = ["json_contains(json_field, 100)", "JSON_CONTAINS(json_field, 100)"]
        for expression in expressions:
            self.search(
                client,
                collection_name,
                data=vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=limit,
                filter=expression,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "limit": limit,
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )

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
            array.append(
                {
                    default_int64_field_name: i,
                    default_float_field_name: i * 1.0,
                    default_string_field_name: str(i),
                    default_json_field_name: {"number": i, "list": [str(j) for j in range(i, i + limit)]},
                    default_float_vec_field_name: all_vectors[i],
                }
            )
        self.insert(client, collection_name, data=array)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        log.info("test_search_expression_json_contains_combined_with_normal: Searching collection %s" % collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
        tar = 1000
        expressions = [
            f"json_contains(json_field['list'], '{tar}') && int64 > {tar - limit // 2}",
            f"JSON_CONTAINS(json_field['list'], '{tar}') && int64 > {tar - limit // 2}",
        ]
        for expression in expressions:
            self.search(
                client,
                collection_name,
                data=vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=limit,
                filter=expression,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "limit": limit // 2,
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr_prefix", ["array_contains_any", "ARRAY_CONTAINS_ANY", "not array_contains_any", "not ARRAY_CONTAINS_ANY"]
    )
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
        self.insert(client, collection_name, data=data.to_dict(orient="records"))
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)

        self.load_collection(client, collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expression = f"{expr_prefix}({ct.default_float_array_field_name}, [0.5, 0.6, 1, 2])"
        res, _ = self.search(
            client,
            collection_name,
            data=vectors[:default_nq],
            anns_field=default_search_field,
            search_params={},
            limit=ct.default_nb,
            filter=expression,
        )
        exp_ids = cf.assert_json_contains(expression, float_field_value)
        assert set([r[ct.default_int64_field_name] for r in res[0]]) == set(exp_ids)
