import math
import random
import threading

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
default_search_exp = f"{ct.default_int64_field_name} >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name


@pytest.mark.xdist_group("TestSearchV2Shared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchV2Shared(TestMilvusClientV2Base):
    """Test search with shared enriched collection.
    Schema: int64(PK), int32, int16, int8, bool(nullable), float(nullable), double,
            varchar(65535), json, int32_array(100), float_array(100), string_array(100, nullable),
            float_vector(128), float16_vector(128), bfloat16_vector(128)
    Data: 10000 rows, gen_row_data_by_schema + 20% nulls for nullable fields + dynamic "new_added_field"
    Index: FLAT / COSINE on all 3 vector fields
    Dynamic: True
    """

    shared_alias = "TestSearchV2Shared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchV2Shared" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        # Scalar fields (float, bool, string_array are nullable for nullable coverage)
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL, nullable=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        # Array fields (string_array is nullable)
        schema.add_field(
            ct.default_int32_array_field_name,
            DataType.ARRAY,
            element_type=DataType.INT32,
            max_capacity=ct.default_max_capacity,
        )
        schema.add_field(
            ct.default_float_array_field_name,
            DataType.ARRAY,
            element_type=DataType.FLOAT,
            max_capacity=ct.default_max_capacity,
        )
        schema.add_field(
            ct.default_string_array_field_name,
            DataType.ARRAY,
            element_type=DataType.VARCHAR,
            max_capacity=ct.default_max_capacity,
            max_length=100,
            nullable=True,
        )
        # Vector fields
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_float16_vec_field_name, DataType.FLOAT16_VECTOR, dim=default_dim)
        schema.add_field(ct.default_bfloat16_vec_field_name, DataType.BFLOAT16_VECTOR, dim=default_dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        self.__class__.shared_nb = 10000
        self.__class__.shared_dim = default_dim
        data = cf.gen_row_data_by_schema(nb=self.shared_nb, schema=schema)
        # Inject ~20% null values for nullable fields (float, bool, string_array)
        null_ratio = 0.2
        null_count = int(self.shared_nb * null_ratio)
        for i in range(null_count):
            data[i][ct.default_float_field_name] = None
            data[i][ct.default_bool_field_name] = None
            data[i][ct.default_string_array_field_name] = None
        # Add dynamic field for exists test
        for i in range(self.shared_nb):
            data[i]["new_added_field"] = i
        self.__class__.shared_data = data
        self.__class__.shared_insert_ids = [i for i in range(self.shared_nb)]
        self.__class__.shared_vector_fields = [
            (ct.default_float_vec_field_name, DataType.FLOAT_VECTOR),
            (ct.default_float16_vec_field_name, DataType.FLOAT16_VECTOR),
            (ct.default_bfloat16_vec_field_name, DataType.BFLOAT16_VECTOR),
        ]
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE", index_type="FLAT", params={})
        idx.add_index(field_name=ct.default_float16_vec_field_name, metric_type="COSINE", index_type="FLAT", params={})
        idx.add_index(field_name=ct.default_bfloat16_vec_field_name, metric_type="COSINE", index_type="FLAT", params={})
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)

        request.addfinalizer(teardown)

    # ==================== Expression filter tests ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression(self):
        """
        target: verify filter expressions return only matching results and templates produce equivalent results
        method: 1. search with each expression from gen_normal_expressions_and_templates
                2. compute expected filter_ids locally using eval
                3. verify returned IDs match filter_ids exactly (FLAT index, 100% recall)
                4. repeat with expression template and with iterative_filter hint
        expected: exact ID match for FLAT index, distances in descending order (COSINE)
        """
        nb = self.shared_nb
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        data = self.shared_data
        insert_ids = self.shared_insert_ids

        # NULL sentinel: all comparisons return False (SQL NULL semantics)
        _null = type(
            "_Null",
            (),
            {
                "__eq__": lambda s, o: False,
                "__ne__": lambda s, o: False,
                "__lt__": lambda s, o: False,
                "__le__": lambda s, o: False,
                "__gt__": lambda s, o: False,
                "__ge__": lambda s, o: False,
                "__hash__": lambda s: hash(None),
            },
        )()

        for expressions in cf.gen_normal_expressions_and_templates():
            log.debug(f"test_search_with_expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                float_val = data[i][ct.default_float_field_name]
                local_vars = {
                    ct.default_int64_field_name: data[i][ct.default_int64_field_name],
                    ct.default_float_field_name: float_val if float_val is not None else _null,
                }
                if not expr or eval(expr, {}, local_vars):
                    filter_ids.append(_id)
            expected_limit = min(nb, len(filter_ids))

            # 3. search with expression
            search_vectors = cf.gen_vectors(default_nq, dim)
            search_res, _ = self.search(
                client,
                self.collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=nb,
                filter=expr,
            )
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == filter_ids_set
                assert len(hits) == expected_limit
                # verify distance ordering (COSINE: descending)
                distances = [hit["distance"] for hit in hits]
                assert all(distances[j] >= distances[j + 1] for j in range(len(distances) - 1)), (
                    "distances not in descending order for COSINE metric"
                )

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(
                client,
                self.collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=nb,
                filter=expr,
                filter_params=expr_params,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "limit": expected_limit,
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == filter_ids_set

            # 5. search again with expression template and search hints
            search_param = default_search_params.copy()
            search_param.update({"hints": "iterative_filter"})
            search_res, _ = self.search(
                client,
                self.collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=search_param,
                limit=nb,
                filter=expr,
                filter_params=expr_params,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "limit": expected_limit,
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == filter_ids_set

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false"])
    def test_search_with_expression_bool(self, bool_type):
        """
        target: verify search with bool filter returns only rows matching the bool condition
        method: 1. search shared collection with bool == True/False/"true"/"false"
                2. compute expected filter_ids locally
                3. verify returned IDs match exactly (FLAT, 100% recall)
        expected: exact match on filter_ids for bool filter (NULL bools excluded)
        """
        nb = self.shared_nb
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        data = self.shared_data
        insert_ids = self.shared_insert_ids

        filter_ids = []
        bool_type_cmp = bool_type
        if bool_type == "true":
            bool_type_cmp = True
        if bool_type == "false":
            bool_type_cmp = False
        for i, _id in enumerate(insert_ids):
            # NULL bool values are excluded from == comparison (SQL NULL semantics)
            val = data[i].get(ct.default_bool_field_name)
            if val is not None and val == bool_type_cmp:
                filter_ids.append(_id)

        expression = f"{default_bool_field_name} == {bool_type}"
        log.info("test_search_with_expression_bool: searching with expression: %s" % expression)
        search_vectors = cf.gen_vectors(default_nq, dim)

        expected_limit = min(nb, len(filter_ids))
        search_res, _ = self.search(
            client,
            self.collection_name,
            data=search_vectors[:default_nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=nb,
            filter=expression,
        )
        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = [hit[ct.default_int64_field_name] for hit in hits]
            assert set(ids) == filter_ids_set
            assert len(hits) == expected_limit

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field", [DataType.INT8, DataType.INT16, DataType.INT32])
    def test_search_expression_different_data_type(self, field):
        """
        target: verify search with out-of-bound integer filter returns empty and normal search returns correct output fields
        method: 1. search with expression where field value exceeds its type range (expect 0 results)
                2. search without filter, verify output_fields contains the requested scalar field
        expected: out-of-bound filter returns 0 results; normal search returns correct output fields
        """
        field_name_str = field.name.lower()
        num = int(field_name_str[3:])
        offset = 2 ** (num - 1)

        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_vectors(default_nq, self.shared_dim)

        expression = f"{field_name_str} >= {offset}"
        self.search(
            client,
            self.collection_name,
            data=search_vectors,
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=expression,
            output_fields=[field_name_str],
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": default_nq,
                "limit": 0,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )
        self.search(
            client,
            self.collection_name,
            data=search_vectors,
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            output_fields=[field_name_str],
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": default_nq,
                "limit": default_limit,
                "metric": "COSINE",
                "output_fields": [field_name_str],
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )

    # ==================== Round decimal / nq / output fields ====================

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_search_round_decimal(self, round_decimal):
        """
        target: verify search with round_decimal returns distances rounded to specified precision
        method: 1. search without round_decimal to get reference distances
                2. search with round_decimal and compare distances match expected rounding
        expected: rounded distances match Python's round() within abs_tol
        """
        tmp_nq = 1
        tmp_limit = 5
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_vectors(tmp_nq, self.shared_dim)

        res, _ = self.search(
            client,
            self.collection_name,
            data=search_vectors[:tmp_nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=tmp_limit,
        )

        res_round, _ = self.search(
            client,
            self.collection_name,
            data=search_vectors[:tmp_nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=tmp_limit,
            round_decimal=round_decimal,
        )

        abs_tol = pow(10, 1 - round_decimal)
        pk = ct.default_int64_field_name
        dist_map = {res[0][i][pk]: res[0][i]["distance"] for i in range(tmp_limit)}
        matched_count = 0
        for i in range(len(res_round[0])):
            _id = res_round[0][i][pk]
            if _id in dist_map:
                dis_expect = round(dist_map[_id], round_decimal)
                dis_actual = res_round[0][i]["distance"]
                assert math.isclose(dis_actual, dis_expect, rel_tol=0, abs_tol=abs_tol)
                matched_count += 1
        assert matched_count > 0, "no matching PKs found between rounded and unrounded results"

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [1, 10, 100])
    def test_search_with_different_nq(self, nq):
        """
        target: verify search returns correct results for various nq values
        method: 1. search shared collection with nq=1/10/100
                2. verify nq, limit, and distance ordering via check_task
        expected: each query returns correct number of results with proper distance ordering
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_vectors(nq, self.shared_dim)
        self.search(
            client,
            self.collection_name,
            data=search_vectors,
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields_all(self):
        """
        target: verify search with output_fields=["*"] returns all schema fields
        method: 1. search shared collection with output_fields=["*"]
                2. verify all schema fields are present in each result hit via check_task
        expected: every result contains all schema fields
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_vectors(default_nq, self.shared_dim)
        expected_output_fields = [
            ct.default_int64_field_name,
            ct.default_int32_field_name,
            ct.default_int16_field_name,
            ct.default_int8_field_name,
            ct.default_bool_field_name,
            ct.default_float_field_name,
            ct.default_double_field_name,
            ct.default_string_field_name,
            ct.default_json_field_name,
            ct.default_int32_array_field_name,
            ct.default_float_array_field_name,
            ct.default_string_array_field_name,
            ct.default_float_vec_field_name,
            ct.default_float16_vec_field_name,
            ct.default_bfloat16_vec_field_name,
            "new_added_field",
        ]
        self.search(
            client,
            self.collection_name,
            data=search_vectors,
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": default_nq,
                "limit": default_limit,
                "metric": "COSINE",
                "output_fields": expected_output_fields,
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )

    # ==================== Array expression tests ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_array(self):
        """
        target: verify search with array field expressions returns exact matching results
        method: 1. search with each array expression from gen_array_field_expressions_and_templates
                2. compute expected filter_ids locally using eval on array data
                3. verify exact ID match (FLAT index, 100% recall)
                4. repeat with expression template and iterative_filter hint
        expected: exact match between expected and returned IDs
        """
        nb = self.shared_nb
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        data = self.shared_data

        for expressions in cf.gen_array_field_expressions_and_templates():
            log.debug(f"search with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i in range(nb):
                int32_array = data[i][ct.default_int32_array_field_name]
                float_array = data[i][ct.default_float_array_field_name]
                string_array = data[i][ct.default_string_array_field_name]
                # Skip rows with null string_array when expression references it
                if ct.default_string_array_field_name in expr and string_array is None:
                    continue
                if not expr or eval(expr):
                    filter_ids.append(i)

            search_vectors = cf.gen_vectors(default_nq, dim)
            search_res, _ = self.search(
                client,
                self.collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=nb,
                filter=expr,
            )
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == set(filter_ids)

            # search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(
                client,
                self.collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=nb,
                filter=expr,
                filter_params=expr_params,
            )
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == set(filter_ids)

            # search again with expression template and hints
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(
                client,
                self.collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=search_params,
                limit=nb,
                filter=expr,
                filter_params=expr_params,
            )
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == set(filter_ids)

    # ==================== Exists expression tests ====================

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "json_field_name", ["json_field['name']", "json_field['number']", "not_exist_field", "new_added_field"]
    )
    def test_search_with_expression_exists(self, json_field_name):
        """
        target: verify 'exists' expression returns correct results for existing and non-existing fields
        method: 1. search with 'exists <field>' for json fields, json subfields, arrays, dynamic fields
                2. compute expected limit based on actual data field/subfield existence
        expected: existing fields/subfields return all rows, non-existing return empty
        """
        exists = "exists"
        nb = self.shared_nb
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        data = self.shared_data

        expression = exists + " " + json_field_name
        # Determine expected limit: check top-level field or JSON subfield existence
        if json_field_name in data[0]:
            limit = nb
        elif "'" in json_field_name:
            # JSON subfield: extract base field and key, e.g. "json_field['name']" → ("json_field", "name")
            base = json_field_name.split("[")[0]
            key = json_field_name.split("'")[1]
            if base in data[0] and isinstance(data[0][base], dict) and key in data[0][base]:
                limit = nb
            else:
                limit = 0
        else:
            limit = 0
        log.info("test_search_with_expression_exists: expression=%s, expected_limit=%d" % (expression, limit))
        search_vectors = cf.gen_vectors(default_nq, dim)
        self.search(
            client,
            self.collection_name,
            data=search_vectors[:default_nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=nb,
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

    # ==================== Multi-vector type tests ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_all_vector_types(self):
        """
        target: verify search works across all vector field types (float, float16, bfloat16) with scalar filter
        method: 1. search each vector field with scalar filter on shared enriched collection
                2. verify nq, limit, metric, and output fields via check_task
        expected: correct results for each vector type with proper output fields
        """
        client = self._client(alias=self.shared_alias)
        nq = 10
        search_exp = (
            f"{ct.default_int64_field_name} >= 0 && {ct.default_int32_field_name} >= 0 && "
            f"{ct.default_int16_field_name} >= 0 && {ct.default_int8_field_name} >= 0 && "
            f"{ct.default_float_field_name} >= 0 && {ct.default_double_field_name} >= 0"
        )

        for vec_field, vec_dtype in self.shared_vector_fields:
            search_vectors = cf.gen_vectors(nq, self.shared_dim, vec_dtype)
            res, _ = self.search(
                client,
                self.collection_name,
                data=search_vectors[:nq],
                anns_field=vec_field,
                search_params=default_search_params,
                limit=default_limit,
                filter=search_exp,
                output_fields=[default_int64_field_name, default_float_field_name, default_bool_field_name],
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": nq,
                    "limit": default_limit,
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )
            assert default_int64_field_name in res[0][0]["entity"]
            assert default_float_field_name in res[0][0]["entity"]
            assert default_bool_field_name in res[0][0]["entity"]

    # ==================== Large-scale expression tests ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large(self):
        """
        target: verify search with large nq (5000) and range filter works correctly
        method: 1. search shared collection with nq=5000 and range filter "0 < int64 < 5001"
                2. verify via check_task and manual filter assertion
        expected: all returned IDs satisfy the range filter
        """
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        insert_ids = self.shared_insert_ids

        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression_large: searching with expression: %s" % expression)

        nums = 5000
        search_vectors = cf.gen_vectors(nums, dim)
        search_res, _ = self.search(
            client,
            self.collection_name,
            data=search_vectors,
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=expression,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nums,
                "ids": insert_ids,
                "limit": default_limit,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )
        # Manual filter verification
        for hits in search_res:
            for hit in hits:
                pk = hit[ct.default_int64_field_name]
                assert 0 < pk < 5001, f"filter violation: id={pk} not in range (0, 5001)"

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large_two(self):
        """
        target: verify search with large 'in' expression (5000 random IDs) works correctly
        method: 1. search shared collection with nq=5000 and 'int64 in [...]' filter
                2. verify via check_task and manual filter assertion
        expected: all returned IDs are within the specified ID list
        """
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        insert_ids = self.shared_insert_ids

        nums = 5000
        search_vectors = cf.gen_vectors(nums, dim)
        vectors_id = [random.randint(0, nums) for _ in range(nums)]
        expression = f"{default_int64_field_name} in {vectors_id}"
        search_res, _ = self.search(
            client,
            self.collection_name,
            data=search_vectors,
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=expression,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nums,
                "ids": insert_ids,
                "limit": default_limit,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )
        # Manual filter verification
        vectors_id_set = set(vectors_id)
        for hits in search_res:
            for hit in hits:
                assert hit[ct.default_int64_field_name] in vectors_id_set, (
                    f"filter violation: id={hit[ct.default_int64_field_name]} not in filter list"
                )


class TestSearchV2LegacyIndependent(TestMilvusClientV2Base):
    """Test cases that require independent collections (auto_id, state modification, custom schema/data)"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_auto_id(self):
        """
        target: verify filter expressions work correctly with auto_id primary key
        method: 1. create collection with auto_id=True and dynamic fields
                2. search with each float-field expression, compute expected filter_ids
                3. verify returned IDs are subset of filter_ids (IVF_FLAT recall tolerance)
                4. repeat with expression template
        expected: all returned IDs satisfy the filter, recall >= 80% for IVF_FLAT
        """
        nb = ct.default_nb
        dim = 64
        search_limit = nb // 2
        enable_dynamic_field = True
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(
            field_name=ct.default_float_vec_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 100},
        )
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        search_vectors = cf.gen_vectors(default_nq, dim)
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 64}}
        for expressions in cf.gen_normal_expressions_and_templates_field(default_float_field_name):
            log.debug(f"search with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                local_vars = {default_float_field_name: data[i][default_float_field_name]}
                if not expr or eval(expr, {}, local_vars):
                    filter_ids.append(_id)
            expected_limit = min(search_limit, len(filter_ids))
            search_res, _ = self.search(
                client,
                collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=search_params,
                limit=search_limit,
                filter=expr,
            )
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, (
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"
                )

            # search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(
                client,
                collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=search_params,
                limit=search_limit,
                filter=expr,
                filter_params=expr_params,
            )
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, (
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"
                )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_expr_json_field(self):
        """
        target: verify search with JSON field expressions works before and after JSON path index creation
        method: 1. create collection with JSON field, insert data with number/float JSON keys
                2. search with JSON expressions, verify via check_task and manual subset assertion
                3. create JSON path indexes (INVERTED on number, AUTOINDEX on float)
                4. release/load and repeat searches to verify JSON index correctness
        expected: all returned IDs satisfy the JSON filter, results consistent before and after JSON indexing
        """
        nb = ct.default_nb
        dim = 64
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data - use gen_default_rows_data to get json with {"number": i, "float": i*1.0}
        data = cf.gen_default_rows_data(nb=nb, dim=dim, with_json=True)
        self.insert(client, collection_name, data=data)
        insert_ids = [i for i in range(nb)]
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE", index_type="FLAT")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        search_vectors = cf.gen_vectors(default_nq, dim)
        for expressions in cf.gen_json_field_expressions_and_templates():
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            json_field = {}
            for i, _id in enumerate(insert_ids):
                json_field["number"] = data[i][ct.default_json_field_name]["number"]
                json_field["float"] = data[i][ct.default_json_field_name]["float"]
                if not expr or eval(expr):
                    filter_ids.append(_id)

            # 3. search expressions
            search_res, _ = self.search(
                client,
                collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=nb,
                filter=expr,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "ids": insert_ids,
                    "limit": min(nb, len(filter_ids)),
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(
                client,
                collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=nb,
                filter=expr,
                filter_params=expr_params,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "ids": insert_ids,
                    "limit": min(nb, len(filter_ids)),
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 5. search again with expression template and hint
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(
                client,
                collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=search_params,
                limit=nb,
                filter=expr,
                filter_params=expr_params,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "ids": insert_ids,
                    "limit": min(nb, len(filter_ids)),
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 6. create json index
            idx2 = self.prepare_index_params(client)[0]
            idx2.add_index(
                field_name=ct.default_json_field_name,
                index_type="INVERTED",
                index_name=f"{ct.default_json_field_name}_0",
                params={"json_cast_type": "double", "json_path": f"{ct.default_json_field_name}['number']"},
            )
            self.create_index(client, collection_name, index_params=idx2)

            idx3 = self.prepare_index_params(client)[0]
            idx3.add_index(
                field_name=ct.default_json_field_name,
                index_type="AUTOINDEX",
                index_name=f"{ct.default_json_field_name}_1",
                params={"json_cast_type": "double", "json_path": f"{ct.default_json_field_name}['float']"},
            )
            self.create_index(client, collection_name, index_params=idx3)

            # 7. release and load to make sure the new index is loaded
            self.release_collection(client, collection_name)
            self.load_collection(client, collection_name)
            # 8. search expressions after json path index
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            search_res, _ = self.search(
                client,
                collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=nb,
                filter=expr,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "ids": insert_ids,
                    "limit": min(nb, len(filter_ids)),
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 9. search again with expression template after json path index
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(
                client,
                collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=nb,
                filter=expr,
                filter_params=expr_params,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "ids": insert_ids,
                    "limit": min(nb, len(filter_ids)),
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 10. search again with expression template and hint after json path index
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(
                client,
                collection_name,
                data=search_vectors[:default_nq],
                anns_field=default_search_field,
                search_params=search_params,
                limit=nb,
                filter=expr,
                filter_params=expr_params,
                check_task=CheckTasks.check_search_results,
                check_items={
                    "nq": default_nq,
                    "ids": insert_ids,
                    "limit": min(nb, len(filter_ids)),
                    "metric": "COSINE",
                    "enable_milvus_client_api": True,
                    "pk_name": ct.default_int64_field_name,
                },
            )
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_comparative_expression(self):
        """
        target: verify search with cross-field comparison expression (int64_1 <= int64_2)
        method: 1. create collection with two int64 fields, insert data where int64_1 == int64_2
                2. search with filter "int64_1 <= int64_2", verify all rows match
        expected: all rows returned since int64_1 always equals int64_2
        """
        nb = ct.default_nb
        dim = 2
        nq = 1
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client)[0]
        schema.add_field("int64_1", DataType.INT64, is_primary=True)
        schema.add_field("int64_2", DataType.INT64)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        all_vectors = cf.gen_vectors(nb, dim)
        data = [{"int64_1": i, "int64_2": i, ct.default_float_vec_field_name: all_vectors[i]} for i in range(nb)]
        insert_ids = list(range(nb))
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        filter_ids = []
        for i in range(nb):
            if data[i]["int64_1"] <= data[i]["int64_2"]:
                filter_ids.append(data[i]["int64_1"])

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        expression = "int64_1 <= int64_2"
        search_vectors = cf.gen_vectors(nq, dim)
        res, _ = self.search(
            client,
            collection_name,
            data=search_vectors[:nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=expression,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "ids": insert_ids,
                "limit": default_limit,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": "int64_1",
            },
        )
        filter_ids_set = set(filter_ids)
        for hits in res:
            ids = [hit["int64_1"] for hit in hits]
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_with_double_quotes(self):
        """
        target: verify search with varchar filter containing escaped double quotes returns exact match
        method: 1. insert data with varchar values containing single and double quotes
                2. search with escaped double-quote filter for a random row
                3. verify exactly 1 result returned with correct PK
        expected: exact match on the escaped string, returns the correct single row
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        string_value = [
            (f"'{cf.gen_str_by_length(3)}'{cf.gen_str_by_length(3)}\"{cf.gen_str_by_length(3)}\"")
            for _ in range(default_nb)
        ]
        for i in range(default_nb):
            data[i][default_string_field_name] = string_value[i]
        insert_ids = [i for i in range(default_nb)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        _id = random.randint(0, default_nb - 1)
        string_value[_id] = string_value[_id].replace('"', '\\"')
        expression = f'{default_string_field_name} == "{string_value[_id]}"'
        log.debug("test_search_expression_with_double_quotes: searching with expression: %s" % expression)
        search_vectors = cf.gen_vectors(default_nq, default_dim)
        search_res, _ = self.search(
            client,
            collection_name,
            data=search_vectors[:default_nq],
            anns_field=default_search_field,
            search_params=default_search_params,
            limit=default_limit,
            filter=expression,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": default_nq,
                "ids": insert_ids,
                "limit": 1,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )
        assert search_res[0][0][ct.default_int64_field_name] == _id

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_concurrent_two_collections_nullable(self):
        """
        target: verify concurrent search across two collections with all-null JSON field is thread-safe
        method: 1. create two collections with nullable JSON, insert data with all JSON=None
                2. launch 10 threads searching collection_1 concurrently
                3. verify each thread gets correct results via check_task
        expected: all concurrent searches succeed with correct nq/limit
        """
        nq = 200
        dim = 64
        enable_dynamic_field = False
        threads_num = 10
        threads = []

        client = self._client()
        collection_name_1 = cf.gen_collection_name_by_testcase_name() + "_1"
        collection_name_2 = cf.gen_collection_name_by_testcase_name() + "_2"

        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON, nullable=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name_1, schema=schema)
        self.create_collection(client, collection_name_2, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        for row in data:
            row[ct.default_json_field_name] = None
        insert_res_1, _ = self.insert(client, collection_name_1, data=data)
        insert_ids = insert_res_1["ids"]
        self.insert(client, collection_name_2, data=data)
        self.flush(client, collection_name_1)
        self.flush(client, collection_name_2)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name_1, index_params=idx)
        self.create_index(client, collection_name_2, index_params=idx)
        self.load_collection(client, collection_name_1)
        self.load_collection(client, collection_name_2)

        def search(coll_name):
            search_vectors = cf.gen_vectors(nq, dim)
            self.search(
                client,
                coll_name,
                data=search_vectors[:nq],
                anns_field=default_search_field,
                search_params=default_search_params,
                limit=default_limit,
                filter=default_search_exp,
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

        log.info("test_search_concurrent_two_collections_nullable: searching with %s threads" % threads_num)
        for _ in range(threads_num):
            t = threading.Thread(target=search, args=(collection_name_1,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", ["JACCARD", "HAMMING"])
    def test_search_with_invalid_metric_type(self, metric_type):
        """
        target: verify creating index with unsupported metric type for float vector raises error
        method: 1. create collection with float_vector field
                2. attempt to create index with JACCARD/HAMMING metric (unsupported for float vectors)
        expected: index creation raises an error with err_code 1100
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=100, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type=metric_type, index_type="FLAT", params={})
        self.create_index(
            client,
            collection_name,
            index_params=idx,
            check_task=CheckTasks.err_res,
            check_items={
                "err_code": 1100,
                "err_msg": f"float vector index does not support metric type: {metric_type}",
            },
        )
