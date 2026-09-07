import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from milvus_client.expressions.expression_test_utils import (
    add_minimal_query_vector_field,
    assert_query_ids,
    assert_same_query_result,
    create_minimal_vector_index,
    insert_by_segment_mode,
    prepare_loaded_empty_collection_for_segment,
    register_collection_cleanup,
    wait_for_segment_mode,
)
from milvus_client.expressions.filtering_case_matrix import (
    ARITHMETIC_EXTENDED_FILTER_CASES,
    ARRAY_FILTER_CASES,
    ARRAY_LENGTH_FILTER_CASES,
    ARRAY_NULL_EMPTY_FILTER_CASES,
    ARRAY_OTHER_TYPE_FILTER_CASES,
    BOOLEAN_COMBINATORIAL_STRESS_CASES,
    JSON_ARRAY_COMPOSITION_CASES,
    JSON_ARRAY_FILTER_CASES,
    JSON_KEY_NULL_FILTER_CASES,
    NEGATIVE_FILTER_ERROR_CASES,
    NULL_FILTER_CASES,
    NUMERIC_DISTINCT_FILTER_CASES,
    ORDER_ARRAY_FUNCTION_EXPRESSIONS,
    SCALAR_FILTER_CASES,
    SEGMENT_MODES_ACTIVE,
    UNARY_NOT_FILTER_CASES,
    UNKNOWN_BOOLEAN_COMPOSITION_CASES,
    build_filter_matrix_rows,
)
from pymilvus import DataType

default_pk = "id"
default_vec = "vector"
default_dim = 8


NUMERIC_SCALAR_FILTER_CASES = [
    case for case in SCALAR_FILTER_CASES if case[0].split("_", 1)[0] in {"i8", "i16", "i32", "i64", "f", "d"}
]
NON_NUMERIC_SCALAR_FILTER_CASES = [case for case in SCALAR_FILTER_CASES if case not in NUMERIC_SCALAR_FILTER_CASES]
SCALAR_NULL_FILTER_CASES = [
    case for case in NULL_FILTER_CASES if case[0].startswith(("nullable_i64_", "nullable_varchar_", "nullable_bool_"))
]
JSON_NULL_FILTER_CASES = [case for case in NULL_FILTER_CASES if case[0].startswith("meta_nullable_")]
L2_NEGATIVE_FILTER_ERROR_CASE_NAMES = {
    "array_contains_wrong_literal_type",
    "invalid_json_path_syntax",
    "geometry_func_on_non_geometry_field",
    "cross_field_arithmetic_rejected",
    "array_length_arithmetic_rejected",
    "field_power_arithmetic_rejected",
}


def case_params(cases):
    return [pytest.param(expr, expected_ids, id=case_name) for case_name, expr, expected_ids in cases]


def numeric_case_params(cases):
    params = []
    for case_name, expr, expected_ids in cases:
        field_name = case_name.split("_", 1)[0]
        level = CaseLabel.L1 if field_name == "i64" else CaseLabel.L2
        params.append(pytest.param(expr, expected_ids, marks=pytest.mark.tags(level), id=case_name))
    return params


def negative_filter_error_params():
    params = []
    for case in NEGATIVE_FILTER_ERROR_CASES:
        case_name = case[0]
        level = CaseLabel.L2 if case_name in L2_NEGATIVE_FILTER_ERROR_CASE_NAMES else CaseLabel.L1
        params.append(pytest.param(*case, marks=pytest.mark.tags(level), id=case_name))
    return params


@pytest.mark.xdist_group("TestFilteringExpressionMatrix")
class TestFilteringExpressionMatrix(TestMilvusClientV2Base):
    shared_alias = "TestFilteringExpressionMatrix"

    def build_schema(self, client):
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
        schema.add_field("i8", DataType.INT8)
        schema.add_field("i16", DataType.INT16)
        schema.add_field("i32", DataType.INT32)
        schema.add_field("i64", DataType.INT64)
        schema.add_field("f", DataType.FLOAT)
        schema.add_field("d", DataType.DOUBLE)
        schema.add_field("active", DataType.BOOL)
        schema.add_field("name", DataType.VARCHAR, max_length=64)
        schema.add_field("nullable_i64", DataType.INT64, nullable=True)
        schema.add_field("nullable_varchar", DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field("nullable_bool", DataType.BOOL, nullable=True)
        schema.add_field("arr_i64", DataType.ARRAY, element_type=DataType.INT64, max_capacity=8)
        schema.add_field("arr_float", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=8)
        schema.add_field("arr_double", DataType.ARRAY, element_type=DataType.DOUBLE, max_capacity=8)
        schema.add_field("arr_bool", DataType.ARRAY, element_type=DataType.BOOL, max_capacity=8)
        schema.add_field(
            "arr_varchar",
            DataType.ARRAY,
            element_type=DataType.VARCHAR,
            max_capacity=8,
            max_length=64,
        )
        schema.add_field(
            "nullable_arr_i64",
            DataType.ARRAY,
            element_type=DataType.INT64,
            max_capacity=8,
            nullable=True,
        )
        schema.add_field("meta", DataType.JSON, nullable=True)
        schema.add_field("meta_nullable", DataType.JSON, nullable=True)
        return schema

    @pytest.fixture(scope="class", autouse=True)
    def expression_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_expression_matrix" + cf.gen_unique_str("_")
        self.create_collection(
            client,
            collection_name,
            schema=self.build_schema(client),
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(client, collection_name, data=build_filter_matrix_rows(default_pk))
        self.flush(client, collection_name)
        create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
        self.load_collection(client, collection_name)
        self.__class__.collection_name = collection_name
        yield

    @pytest.mark.parametrize("expr, expected_ids", numeric_case_params(NUMERIC_SCALAR_FILTER_CASES))
    def test_numeric_scalar_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(NUMERIC_DISTINCT_FILTER_CASES))
    def test_numeric_type_distinct_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expected_ids", case_params(NON_NUMERIC_SCALAR_FILTER_CASES))
    def test_non_numeric_scalar_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(UNARY_NOT_FILTER_CASES))
    def test_unary_not_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expected_ids", case_params(SCALAR_NULL_FILTER_CASES))
    def test_scalar_null_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(ARRAY_NULL_EMPTY_FILTER_CASES))
    def test_array_null_empty_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(JSON_NULL_FILTER_CASES))
    def test_json_null_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(JSON_KEY_NULL_FILTER_CASES))
    def test_json_key_missing_and_null_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(UNKNOWN_BOOLEAN_COMPOSITION_CASES))
    def test_unknown_boolean_composition_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(ARITHMETIC_EXTENDED_FILTER_CASES))
    def test_extended_arithmetic_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(BOOLEAN_COMBINATORIAL_STRESS_CASES))
    def test_boolean_combinatorial_stress_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(ARRAY_FILTER_CASES))
    def test_array_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(ARRAY_LENGTH_FILTER_CASES))
    def test_array_length_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(ARRAY_OTHER_TYPE_FILTER_CASES))
    def test_array_other_type_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "left_expr, right_expr, expected_ids",
        [
            pytest.param(*case, id=f"array_order_{i}")
            for i, case in enumerate(ORDER_ARRAY_FUNCTION_EXPRESSIONS, start=1)
        ],
    )
    def test_array_function_order_permutation_cases(self, left_expr, right_expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_same_query_result(
            self,
            client,
            self.collection_name,
            left_expr,
            right_expr,
            expected_ids,
            pk_field=default_pk,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(JSON_ARRAY_FILTER_CASES))
    def test_json_filter_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expected_ids", case_params(JSON_ARRAY_COMPOSITION_CASES))
    def test_json_array_composition_cases(self, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, self.collection_name, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.parametrize(
        "case_name, expr, expected_message_substring",
        negative_filter_error_params(),
    )
    def test_filter_negative_meaningful_error_cases(self, case_name, expr, expected_message_substring):
        client = self._client(alias=self.shared_alias)
        self.query(
            client,
            self.collection_name,
            filter=expr,
            output_fields=[default_pk],
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: expected_message_substring},
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("segment_mode", SEGMENT_MODES_ACTIVE, ids=SEGMENT_MODES_ACTIVE)
    def test_segment_mode_expression_smoke(self, segment_mode):
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        prepare_loaded_empty_collection_for_segment(
            self,
            client,
            collection_name,
            self.build_schema(client),
            vector_field=default_vec,
        )
        rows = build_filter_matrix_rows(default_pk)
        insert_by_segment_mode(self, client, collection_name, rows[:5], rows[5:], segment_mode)
        wait_for_segment_mode(
            client,
            collection_name,
            segment_mode,
            expected_row_count=len(rows),
            expected_sealed_rows=len(rows[:5]) if segment_mode == "mixed" else None,
        )
        for expr, expected_ids in [
            ("i64 >= 3 and i64 <= 6", [3, 4, 5, 6]),
            ('meta["rank"] >= 7', [7, 8, 9, 10]),
            ("nullable_i64 is null or meta['rank'] >= 9", [3, 4, 7, 9, 10]),
        ]:
            assert_query_ids(self, client, collection_name, expr, expected_ids, pk_field=default_pk)
