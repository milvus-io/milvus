import ast
from collections import Counter

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from milvus_client.expressions.expression_test_utils import (
    add_minimal_query_vector_field,
    assert_query_ids,
    assert_same_query_result,
    assert_search_result_shape,
    create_minimal_vector_index,
    insert_by_segment_mode,
    prepare_loaded_empty_collection_for_segment,
    register_collection_cleanup,
    wait_for_materialized_index,
    wait_for_segment_mode,
)
from milvus_client.expressions.filtering_case_matrix import (
    BOOLEAN_FANOUT_EXPRESSIONS_GENERALIZED_L2,
    BOOLEAN_FANOUT_EXPRESSIONS_L2,
    EQUIVALENT_EXPRESSION_CASES,
    JSON_BOOL_MIXED_51567_CONTROL_CASES,
    JSON_BOOL_MIXED_IN_51567_CASES,
    JSON_BOOL_MIXED_OR_51567_CASES,
    JSON_MIXED_TYPE_IN_51489_CASES,
    JSON_MIXED_TYPE_OR_51568_CASES,
    ORDER_SENSITIVE_EXPRESSIONS,
    REAL_INDEX_ROW_COUNT,
    SAME_FIELD_OR_FANOUT_EXPRESSIONS_GENERALIZED_L2,
    SEGMENT_MODES_ACTIVE,
)
from pymilvus import DataType, MilvusException
from pymilvus.client.types import IndexState

default_pk = "id"
default_vec = "vector"
default_dim = 8
REGRESSION_VECTOR_INDEX_NAME = "idx_regression_vector_hnsw"

KNOWN_ISSUE_51568_LOSS_IDS = {
    "float_int_or_5": [1, 2, 5],
    "float_middle_int_or_5": [1, 3, 5],
}
KNOWN_ISSUE_51568_ERROR_VALUE_CASES = {
    "float_int_or_4": "kInt64Val",
    "float_int_or_alternating_6": "kInt64Val",
    "float_int_or_20": "kInt64Val",
    "str_int_or_3": "kStringVal",
}
KNOWN_ISSUE_51489_IN_ERROR_VALUE_CASES = {
    "int_string_in": "kStringVal",
    "string_int_in": "kInt64Val",
    "int_unrelated_string_in": "kStringVal",
    "json_array_subscript_mixed_in": "kStringVal",
}
KNOWN_ISSUE_51567_IN_ERROR_VALUE_CASES = {
    "bool_int_in_true_one": "kInt64Val",
    "bool_string_in": "kStringVal",
    "bool_int_string_in": "kInt64Val",
}
KNOWN_ISSUE_51567_IN_SUCCESS_IDS = {
    "bool_int_in_false_one": [3, 4],
}
KNOWN_ISSUE_51567_OR_LOSS_IDS = {
    "bool_int_or_bool_first_three": [5, 6],
    "bool_int_or_bool_last_three": [5, 6],
    "bool_int_or_false_first_three": [5, 6],
    "bool_int_or_false_last_three": [5, 6],
}
KNOWN_ISSUE_51567_OR_ERROR_VALUE_CASES = {
    "bool_string_or_three": "kBoolVal",
}
SEGMENT_51568_CASES = [case for case in JSON_MIXED_TYPE_OR_51568_CASES if case[0] == "float_int_or_4"]
SEARCH_51568_CASES = [
    case for case in JSON_MIXED_TYPE_OR_51568_CASES if case[0] in {"float_int_or_4", "float_last_int_or_5"}
]
SEGMENT_51568_MODES = ["growing", "mixed"]
L0_51568_CASE_NAMES = {"pure_int_in_5_control", "float_int_or_4"}
L0_51489_IN_CASE_NAMES = {"int_string_in"}
L0_51567_BOOL_IN_CASE_NAMES = set()
L0_51567_BOOL_OR_CASE_NAMES = {"bool_int_or_int_first_three", "bool_int_or_bool_first_three"}
L1_51568_CASE_NAMES = {"mixed_numeric_in_5_control", "str_int_or_3"}
L1_51567_BOOL_IN_CASE_NAMES = {"bool_int_in_true_one", "bool_int_in_false_one"}
MIXED_LITERAL_TYPE_TOKENS = {
    "int": ("int64_val", "int64", "integer"),
    "string": ("string_val", "varchar", "string"),
    "bool": ("bool_val", "bool"),
}
MIXED_IN_LITERAL_TYPES_51489 = {case_name: {"int", "string"} for case_name, _ in JSON_MIXED_TYPE_IN_51489_CASES}
MIXED_IN_LITERAL_TYPES_51567 = {
    "bool_int_in_true_one": {"bool", "int"},
    "bool_int_in_false_one": {"bool", "int"},
    "bool_string_in": {"bool", "string"},
    "bool_int_string_in": {"bool", "int", "string"},
}
JSON_HOMOGENEOUS_IN_CONTROL_CASES = [
    ("numeric_in", 'meta["p"] in [1, 2]', [1, 2]),
    ("bool_in", 'meta["b"] in [true, false]', [1, 2, 3, 4]),
    ("bool_path_int_in", 'meta["b"] in [0, 1]', [5, 6]),
    ("bool_path_string_in", 'meta["b"] in ["yes", "no"]', [7, 8]),
    ("string_in", 'meta["p"] in ["1"]', [REAL_INDEX_ROW_COUNT]),
    ("array_subscript_in", 'meta["arr"][0] in [1, 2]', [1, 2]),
]
JSON_HOMOGENEOUS_IN_QUERY_LEVELS = {
    "numeric_in": CaseLabel.L1,
    "bool_in": CaseLabel.L2,
    "bool_path_int_in": CaseLabel.L2,
    "bool_path_string_in": CaseLabel.L2,
    "string_in": CaseLabel.L0,
    "array_subscript_in": CaseLabel.L0,
}


def fanout_params(cases):
    return [pytest.param(*case, id=case[0]) for case in cases]


def order_params(cases):
    params = []
    for i, case in enumerate(cases, start=1):
        level = CaseLabel.L0 if i == 1 else CaseLabel.L2
        params.append(pytest.param(*case, marks=pytest.mark.tags(level), id=f"order_{i}"))
    return params


def equivalent_expression_params(cases):
    return [pytest.param(*case, id=case[0]) for case in cases]


def hit_id(hit):
    if default_pk in hit:
        return hit[default_pk]
    entity = hit.get("entity", {})
    if default_pk in entity:
        return entity[default_pk]
    return hit["id"]


def sorted_hit_ids(hits):
    return sorted(hit_id(hit) for hit in hits)


def vector_for_id(row_id):
    return [float(((row_id + 1) * (dimension + 3)) % 17 + 1) / 20.0 for dimension in range(default_dim)]


def assert_cosine_search_shape(result):
    return assert_search_result_shape(result, "COSINE")


def assert_expression_fanout(expr, expected_count):
    if " in [" in expr.lower() and " and " not in expr.lower() and " or " not in expr.lower():
        in_offset = expr.lower().index(" in ") + len(" in ")
        list_start = expr.index("[", in_offset)
        values = ast.literal_eval(expr[list_start : expr.rindex("]") + 1])
        actual_count = len(values)
    else:
        actual_count = expr.lower().count(" and ") + expr.lower().count(" or ") + 1
    assert actual_count == expected_count, f"{expr} has fanout {actual_count}, expected {expected_count}"


def id_delta(actual_ids, expected_ids):
    actual_counter = Counter(actual_ids)
    expected_counter = Counter(expected_ids)
    missing = sorted((expected_counter - actual_counter).elements())
    extra = sorted((actual_counter - expected_counter).elements())
    return missing, extra


def is_known_executor_type_assertion(exc, expected_value_case):
    message = str(exc)
    return exc.code == 2000 and all(
        token in message
        for token in (
            "Operator:PhyFilterBitsNode",
            "value_proto.val_case()",
            f"GenericValue::{expected_value_case}",
            "internal/core/src/exec/expression/Utils.h:",
            "segcoreCode=2001",
        )
    )


def assert_ids_or_xfail_known_loss(actual_ids, expected_ids, case_name, known_actual_ids, issue, context):
    actual_ids = sorted(actual_ids)
    expected_ids = sorted(expected_ids)
    if actual_ids == expected_ids:
        return

    missing, extra = id_delta(actual_ids, expected_ids)
    if case_name in known_actual_ids and actual_ids == sorted(known_actual_ids[case_name]):
        pytest.xfail(
            f"Known issue {issue}: {context} reproduced exact result {actual_ids}, missing={missing}, extra={extra}"
        )

    assert actual_ids == expected_ids, (
        f"{context} got {actual_ids}, expected {expected_ids}, missing={missing}, extra={extra}"
    )


def query_ids_or_xfail_known_loss(
    client,
    collection_name,
    expr,
    expected_ids,
    case_name,
    known_actual_ids,
    known_error_value_cases,
    issue,
    context,
):
    try:
        rows = client.query(collection_name, filter=expr, output_fields=[default_pk])
    except MilvusException as exc:
        expected_value_case = known_error_value_cases.get(case_name)
        if expected_value_case and is_known_executor_type_assertion(exc, expected_value_case):
            pytest.xfail(f"Known issue {issue}: {context} failed with {exc}")
        raise

    actual_ids = [row[default_pk] for row in rows]
    assert_ids_or_xfail_known_loss(actual_ids, expected_ids, case_name, known_actual_ids, issue, context)


def search_ids_or_xfail_known_loss(
    client,
    collection_name,
    expr,
    expected_ids,
    case_name,
    known_actual_ids,
    known_error_value_cases,
    issue,
    context,
):
    try:
        result = client.search(
            collection_name,
            data=[[0.1] * default_dim],
            anns_field=default_vec,
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            filter=expr,
            output_fields=[default_pk],
            limit=30,
        )
    except MilvusException as exc:
        expected_value_case = known_error_value_cases.get(case_name)
        if expected_value_case and is_known_executor_type_assertion(exc, expected_value_case):
            pytest.xfail(f"Known issue {issue}: {context} failed with {exc}")
        raise

    hits = assert_cosine_search_shape(result)
    actual_ids = sorted_hit_ids(hits)
    known_ids = known_actual_ids.get(case_name)
    if known_ids is not None and actual_ids == sorted(known_ids):
        try:
            query_rows = client.query(collection_name, filter=expr, output_fields=[default_pk])
        except MilvusException as exc:
            expected_value_case = known_error_value_cases.get(case_name)
            if expected_value_case and is_known_executor_type_assertion(exc, expected_value_case):
                pytest.xfail(f"Known issue {issue}: scalar baseline for {context} failed with {exc}")
            raise

        query_ids = sorted(row[default_pk] for row in query_rows)
        if query_ids == actual_ids:
            pytest.xfail(
                f"Known issue {issue}: scalar query and search reproduced the same filtering loss {actual_ids}"
            )
        assert query_ids == sorted(expected_ids), (
            f"{context}: scalar baseline returned {query_ids}, expected {sorted(expected_ids)}"
        )
        pytest.fail(
            f"{context}: search returned known-loss IDs {actual_ids}, but scalar filtering returned {query_ids}; "
            "do not classify ANN recall loss as a filtering xfail"
        )
    assert actual_ids == sorted(expected_ids), f"{context} got IDs {actual_ids}, expected {sorted(expected_ids)}"


def result_ids(result):
    if result and isinstance(result[0], list):
        rows = assert_cosine_search_shape(result)
    else:
        rows = result
    return sorted(hit_id(row) for row in rows)


def assert_meaningful_type_mismatch_rejection(
    call,
    expr,
    expected_literal_types,
    known_issue=None,
    known_executor_value_case=None,
    known_success_ids=None,
    known_success_baseline=None,
):
    try:
        result = call()
    except MilvusException as exc:
        message = str(exc).lower()
        if (
            known_issue
            and known_executor_value_case
            and is_known_executor_type_assertion(exc, known_executor_value_case)
        ):
            pytest.xfail(f"Known issue {known_issue}: expected planner rejection, got {exc}")

        assert exc.code == 1100, f"expected planner invalid-parameter code 1100, got code={exc.code}: {exc}"
        assert any(
            marker in message
            for marker in (
                "failed to create query plan",
                "cannot parse expression",
                "query plan failed",
            )
        ), f"expected planner-stage rejection, got: {exc}"
        assert "phyfilterbitsnode" not in message and "segcore" not in message, (
            f"mixed types reached executor instead of planner rejection: {exc}"
        )
        assert any(
            signature in message
            for signature in (
                "value type mismatch",
                "type mismatch",
                "cannot be casted",
                "mixed types",
            )
        ), f"expected a meaningful type-mismatch error, got: {exc}"
        assert " in " in message or "in list" in message, f"expected IN operator context in planner error: {exc}"

        field_expression = expr.lower().split(" in ", 1)[0].replace(" ", "")
        assert field_expression in message.replace(" ", ""), (
            f"expected field path {field_expression!r} in planner error: {exc}"
        )
        for literal_type in expected_literal_types:
            assert any(token in message for token in MIXED_LITERAL_TYPE_TOKENS[literal_type]), (
                f"expected literal type {literal_type!r} in planner error: {exc}"
            )
        return

    actual_ids = result_ids(result)
    if known_issue and known_success_ids is not None and actual_ids == sorted(known_success_ids):
        if known_success_baseline is None:
            pytest.xfail(f"Known issue {known_issue}: mixed-type JSON IN returned exact known IDs {actual_ids}")
        try:
            baseline_result = known_success_baseline()
        except MilvusException as exc:
            if known_executor_value_case and is_known_executor_type_assertion(exc, known_executor_value_case):
                pytest.xfail(f"Known issue {known_issue}: scalar baseline failed with {exc}")
            pytest.fail(f"search accepted mixed-type IN while scalar baseline rejected it: {exc}")
        baseline_ids = result_ids(baseline_result)
        if baseline_ids == actual_ids:
            pytest.xfail(
                f"Known issue {known_issue}: scalar query and search returned the same mixed-type IDs {actual_ids}"
            )
        pytest.fail(
            f"search returned known mixed-type IDs {actual_ids}, but scalar baseline returned {baseline_ids}; "
            "do not classify ANN recall loss as a filtering xfail"
        )
    pytest.fail(f"mixed-type JSON IN unexpectedly succeeded with IDs {actual_ids}; planner rejection is required")


def mark_51568_cases(cases):
    params = []
    for case in cases:
        case_name = case[0]
        if case_name in L0_51568_CASE_NAMES:
            level = CaseLabel.L0
        elif case_name in L1_51568_CASE_NAMES:
            level = CaseLabel.L1
        else:
            level = CaseLabel.L2
        marks = [pytest.mark.tags(level)]
        params.append(pytest.param(*case, marks=marks, id=case_name))
    return params


def mark_51568_segment_cases(cases, segment_modes):
    params = []
    for case in cases:
        case_name = case[0]
        for segment_mode in segment_modes:
            marks = [pytest.mark.tags(CaseLabel.L2)]
            params.append(
                pytest.param(
                    *case,
                    segment_mode,
                    marks=marks,
                    id=f"{case_name}-{segment_mode}",
                )
            )
    return params


def mark_json_mixed_type_in_cases(cases):
    params = []
    for case in cases:
        case_name = case[0]
        level = CaseLabel.L0 if case_name in L0_51489_IN_CASE_NAMES else CaseLabel.L2
        params.append(pytest.param(*case, marks=pytest.mark.tags(level), id=case_name))
    return params


def mark_json_bool_mixed_in_cases(cases):
    params = []
    for case in cases:
        case_name = case[0]
        if case_name in L0_51567_BOOL_IN_CASE_NAMES:
            level = CaseLabel.L0
        elif case_name in L1_51567_BOOL_IN_CASE_NAMES:
            level = CaseLabel.L1
        else:
            level = CaseLabel.L2
        params.append(pytest.param(*case, marks=pytest.mark.tags(level), id=case_name))
    return params


def mark_51567_or_cases(cases):
    params = []
    for case in cases:
        case_name = case[0]
        level = CaseLabel.L0 if case_name in L0_51567_BOOL_OR_CASE_NAMES else CaseLabel.L2
        marks = [pytest.mark.tags(level)]
        params.append(pytest.param(*case, marks=marks, id=case_name))
    return params


def build_order_rows():
    return [
        {
            default_pk: 1,
            "age": 8,
            "score": 85.0,
            "active": True,
            "tag": "qa",
            "meta": {"group": "qa", "rank": 1, "p": 1},
        },
        {
            default_pk: 2,
            "age": 12,
            "score": 91.0,
            "active": True,
            "tag": "qa",
            "meta": {"group": "qa", "rank": 1, "p": 2},
        },
        {
            default_pk: 3,
            "age": 13,
            "score": 89.0,
            "active": False,
            "tag": "dev",
            "meta": {"group": "dev", "rank": 3, "p": 3},
        },
        {
            default_pk: 4,
            "age": 14,
            "score": 80.0,
            "active": True,
            "tag": "qa",
            "meta": {"group": "qa", "rank": 2, "p": 4},
        },
        {
            default_pk: 5,
            "age": 15,
            "score": 91.0,
            "active": False,
            "tag": "ops",
            "meta": {"group": "ops", "rank": 5, "p": 5},
        },
        {
            default_pk: 6,
            "age": 16,
            "score": 70.0,
            "active": False,
            "tag": "ops",
            "meta": {"group": "ops", "rank": 6, "p": 6},
        },
        {
            default_pk: 7,
            "age": 17,
            "score": 75.0,
            "active": False,
            "tag": "dev",
            "meta": {"group": "dev", "rank": 7, "p": 7},
        },
        {
            default_pk: 8,
            "age": 18,
            "score": 76.0,
            "active": False,
            "tag": "dev",
            "meta": {"group": "dev", "rank": 8, "p": 8},
        },
        {
            default_pk: 9,
            "age": 19,
            "score": 77.0,
            "active": False,
            "tag": "dev",
            "meta": {"group": "dev", "rank": 9, "p": 9},
        },
        {
            default_pk: 10,
            "age": 20,
            "score": 78.0,
            "active": False,
            "tag": "dev",
            "meta": {"group": "dev", "rank": 10, "p": 10},
        },
        {
            default_pk: 11,
            "age": 12,
            "score": 80.0,
            "active": False,
            "tag": "dev",
            "meta": {"group": "control", "rank": 1, "p": 11},
        },
        {
            default_pk: 12,
            "age": 8,
            "score": 80.0,
            "active": False,
            "tag": "dev",
            "meta": {"group": "control", "rank": 3, "p": 12},
        },
        {
            default_pk: 13,
            "age": 12,
            "score": 95.0,
            "active": False,
            "tag": "dev",
            "meta": {"group": "control", "rank": 3, "p": 13},
        },
        {
            default_pk: 14,
            "age": 12,
            "score": 80.0,
            "active": True,
            "tag": "dev",
            "meta": {"group": "control", "rank": 3, "p": 14},
        },
    ]


class TestFilterRegressions(TestMilvusClientV2Base):
    shared_alias = "TestFilterRegressions"

    def build_order_schema(self, client):
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
        schema.add_field("age", DataType.INT64)
        schema.add_field("score", DataType.DOUBLE)
        schema.add_field("active", DataType.BOOL)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)
        schema.add_field("meta", DataType.JSON)
        return schema

    def build_51568_schema(self, client):
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
        schema.add_field("meta", DataType.JSON)
        return schema

    @pytest.fixture(scope="class")
    def regression_51568_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_regression_51568" + cf.gen_unique_str("_")
        self.create_collection(
            client,
            collection_name,
            schema=self.build_51568_schema(client),
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        bool_mixed_values = {
            1: True,
            2: True,
            3: False,
            4: False,
            5: 0,
            6: 1,
            7: "yes",
            8: "no",
        }
        rows = []
        for i in range(1, REAL_INDEX_ROW_COUNT + 1):
            p_value = "1" if i == REAL_INDEX_ROW_COUNT else i
            meta = {"p": p_value, "arr": [i, i + 10]}
            if i in bool_mixed_values:
                meta["b"] = bool_mixed_values[i]
            rows.append(
                {
                    default_pk: i,
                    default_vec: vector_for_id(i),
                    "meta": meta,
                }
            )
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            default_vec,
            index_name=REGRESSION_VECTOR_INDEX_NAME,
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 8, "efConstruction": 64},
        )
        self.create_index(client, collection_name, index_params=index_params)
        self.__class__.regression_vector_index_info = wait_for_materialized_index(
            self,
            client,
            collection_name,
            REGRESSION_VECTOR_INDEX_NAME,
            expected_total_rows=REAL_INDEX_ROW_COUNT,
            expected_field_name=default_vec,
            expected_index_type="HNSW",
        )
        assert self.regression_vector_index_info["index_name"] == REGRESSION_VECTOR_INDEX_NAME
        assert self.regression_vector_index_info["field_name"] == default_vec
        assert self.regression_vector_index_info["index_type"] == "HNSW"
        assert self.regression_vector_index_info["state"] == IndexState.Finished.name
        assert self.regression_vector_index_info["indexed_rows"] == self.regression_vector_index_info["total_rows"]
        assert self.regression_vector_index_info["total_rows"] == REAL_INDEX_ROW_COUNT
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.fixture(scope="class")
    def order_fanout_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_order" + cf.gen_unique_str("_")
        self.create_collection(
            client,
            collection_name,
            schema=self.build_order_schema(client),
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(client, collection_name, data=build_order_rows())
        self.flush(client, collection_name)
        create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.fixture(scope="class")
    def one_doc_51568_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_51568_one_doc" + cf.gen_unique_str("_")
        self.create_collection(
            client,
            collection_name,
            schema=self.build_51568_schema(client),
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(client, collection_name, data=[{default_pk: 1, "meta": {"p": 1}}])
        self.flush(client, collection_name)
        create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.mark.parametrize(
        "case_name, expr, fanout_count, expected_ids",
        mark_51568_cases(JSON_MIXED_TYPE_OR_51568_CASES),
    )
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_same_path_mixed_type_or_regression_51568(
        self,
        regression_51568_collection,
        case_name,
        expr,
        fanout_count,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        assert_expression_fanout(expr, fanout_count)
        query_ids_or_xfail_known_loss(
            client,
            regression_51568_collection,
            expr,
            expected_ids,
            case_name,
            KNOWN_ISSUE_51568_LOSS_IDS,
            KNOWN_ISSUE_51568_ERROR_VALUE_CASES,
            "https://github.com/milvus-io/milvus/issues/51568",
            f"{case_name} query",
        )

    @pytest.mark.parametrize(
        "case_name, expr",
        mark_json_mixed_type_in_cases(JSON_MIXED_TYPE_IN_51489_CASES),
    )
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_mixed_type_in_rejected_at_planner_51489(
        self,
        regression_51568_collection,
        case_name,
        expr,
    ):
        client = self._client(alias=self.shared_alias)
        assert_meaningful_type_mismatch_rejection(
            lambda: client.query(
                regression_51568_collection,
                filter=expr,
                output_fields=[default_pk],
            ),
            expr=expr,
            expected_literal_types=MIXED_IN_LITERAL_TYPES_51489[case_name],
            known_issue="https://github.com/milvus-io/milvus/issues/51489",
            known_executor_value_case=KNOWN_ISSUE_51489_IN_ERROR_VALUE_CASES[case_name],
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_mixed_type_in_search_rejected_at_planner_51489(self, regression_51568_collection):
        client = self._client(alias=self.shared_alias)
        assert_meaningful_type_mismatch_rejection(
            lambda: client.search(
                regression_51568_collection,
                data=[[0.1] * default_dim],
                anns_field=default_vec,
                search_params={"metric_type": "COSINE", "params": {}},
                filter='meta["p"] in [1, "2"]',
                output_fields=[default_pk],
                limit=20,
            ),
            expr='meta["p"] in [1, "2"]',
            expected_literal_types={"int", "string"},
            known_issue="https://github.com/milvus-io/milvus/issues/51489",
            known_executor_value_case=KNOWN_ISSUE_51489_IN_ERROR_VALUE_CASES["int_string_in"],
        )

    @pytest.mark.parametrize(
        "case_name, expr, expected_ids",
        [
            pytest.param(
                *case,
                marks=pytest.mark.tags(CaseLabel.L0 if case[0] == "bool_only_in" else CaseLabel.L1),
                id=case[0],
            )
            for case in JSON_BOOL_MIXED_51567_CONTROL_CASES
        ],
    )
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_bool_mixed_type_controls_51567(
        self,
        regression_51568_collection,
        case_name,
        expr,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, regression_51568_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.parametrize(
        "case_name, expr",
        mark_json_bool_mixed_in_cases(JSON_BOOL_MIXED_IN_51567_CASES),
    )
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_bool_mixed_type_in_rejected_at_planner_51567(
        self,
        regression_51568_collection,
        case_name,
        expr,
    ):
        client = self._client(alias=self.shared_alias)
        assert_meaningful_type_mismatch_rejection(
            lambda: client.query(
                regression_51568_collection,
                filter=expr,
                output_fields=[default_pk],
            ),
            expr=expr,
            expected_literal_types=MIXED_IN_LITERAL_TYPES_51567[case_name],
            known_issue="https://github.com/milvus-io/milvus/issues/51567",
            known_executor_value_case=KNOWN_ISSUE_51567_IN_ERROR_VALUE_CASES.get(case_name),
            known_success_ids=KNOWN_ISSUE_51567_IN_SUCCESS_IDS.get(case_name),
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_bool_mixed_type_in_search_rejected_at_planner_51567(self, regression_51568_collection):
        client = self._client(alias=self.shared_alias)
        case_name = "bool_int_in_false_one"
        assert_meaningful_type_mismatch_rejection(
            lambda: client.search(
                regression_51568_collection,
                data=[[0.1] * default_dim],
                anns_field=default_vec,
                search_params={"metric_type": "COSINE", "params": {}},
                filter='meta["b"] in [false, 1]',
                output_fields=[default_pk],
                limit=20,
            ),
            expr='meta["b"] in [false, 1]',
            expected_literal_types={"bool", "int"},
            known_issue="https://github.com/milvus-io/milvus/issues/51567",
            known_success_ids=KNOWN_ISSUE_51567_IN_SUCCESS_IDS[case_name],
            known_success_baseline=lambda: client.query(
                regression_51568_collection,
                filter='meta["b"] in [false, 1]',
                output_fields=[default_pk],
            ),
        )

    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    @pytest.mark.parametrize(
        "expr, expected_ids",
        [
            pytest.param(
                expr,
                expected_ids,
                marks=pytest.mark.tags(JSON_HOMOGENEOUS_IN_QUERY_LEVELS[case_name]),
                id=case_name,
            )
            for case_name, expr, expected_ids in JSON_HOMOGENEOUS_IN_CONTROL_CASES
        ],
    )
    def test_json_homogeneous_in_query_controls(self, regression_51568_collection, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, regression_51568_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    @pytest.mark.parametrize(
        "expr, expected_ids",
        [
            pytest.param(expr, expected_ids, id=case_name)
            for case_name, expr, expected_ids in JSON_HOMOGENEOUS_IN_CONTROL_CASES
        ],
    )
    def test_json_homogeneous_in_search_controls(self, regression_51568_collection, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        result = client.search(
            regression_51568_collection,
            data=[[0.1] * default_dim],
            anns_field=default_vec,
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            filter=expr,
            output_fields=[default_pk],
            limit=20,
        )
        hits = assert_cosine_search_shape(result)
        assert sorted_hit_ids(hits) == expected_ids

    @pytest.mark.parametrize(
        "case_name, expr, expected_ids",
        mark_51567_or_cases(JSON_BOOL_MIXED_OR_51567_CASES),
    )
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_bool_mixed_type_or_typed_union_51567(
        self,
        regression_51568_collection,
        case_name,
        expr,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        query_ids_or_xfail_known_loss(
            client,
            regression_51568_collection,
            expr,
            expected_ids,
            case_name,
            KNOWN_ISSUE_51567_OR_LOSS_IDS,
            KNOWN_ISSUE_51567_OR_ERROR_VALUE_CASES,
            "https://github.com/milvus-io/milvus/issues/51567",
            f"{case_name} query",
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_bool_mixed_type_or_search_51567(self, regression_51568_collection):
        client = self._client(alias=self.shared_alias)
        case_name = "bool_int_or_bool_first_three"
        search_ids_or_xfail_known_loss(
            client,
            regression_51568_collection,
            '(meta["b"] == true) or (meta["b"] == 1) or (meta["b"] == 0)',
            [1, 2, 5, 6],
            case_name,
            KNOWN_ISSUE_51567_OR_LOSS_IDS,
            KNOWN_ISSUE_51567_OR_ERROR_VALUE_CASES,
            "https://github.com/milvus-io/milvus/issues/51567",
            f"{case_name} search",
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_same_path_mixed_type_or_51568_empty_collection_control(self):
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            collection_name,
            schema=self.build_51568_schema(client),
            consistency_level="Strong",
        )
        create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
        self.load_collection(client, collection_name)
        expr = '(meta["p"] == 1.0) or (meta["p"] == 2) or (meta["p"] == 3) or (meta["p"] == 4)'
        assert_query_ids(self, client, collection_name, expr, [], pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_same_path_mixed_type_or_51568_one_doc_negative(self, one_doc_51568_collection):
        client = self._client(alias=self.shared_alias)
        expr = '(meta["p"] == 2.0) or (meta["p"] == 3) or (meta["p"] == 4) or (meta["p"] == 5)'
        query_ids_or_xfail_known_loss(
            client,
            one_doc_51568_collection,
            expr,
            [],
            "float_int_or_4",
            KNOWN_ISSUE_51568_LOSS_IDS,
            KNOWN_ISSUE_51568_ERROR_VALUE_CASES,
            "https://github.com/milvus-io/milvus/issues/51568",
            "one-doc 51568 negative query",
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    def test_json_same_path_mixed_type_or_51568_one_doc_later_int_positive(self, one_doc_51568_collection):
        client = self._client(alias=self.shared_alias)
        expr = '(meta["p"] == 2.0) or (meta["p"] == 1) or (meta["p"] == 3) or (meta["p"] == 4)'
        query_ids_or_xfail_known_loss(
            client,
            one_doc_51568_collection,
            expr,
            [1],
            "float_int_or_4",
            KNOWN_ISSUE_51568_LOSS_IDS,
            KNOWN_ISSUE_51568_ERROR_VALUE_CASES,
            "https://github.com/milvus-io/milvus/issues/51568",
            "one-doc 51568 later-int positive query",
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterOrderAndFanout")
    @pytest.mark.parametrize(
        "case_name, expr, fanout_count, expected_ids",
        fanout_params(BOOLEAN_FANOUT_EXPRESSIONS_GENERALIZED_L2 + SAME_FIELD_OR_FANOUT_EXPRESSIONS_GENERALIZED_L2),
    )
    def test_boolean_fanout_count_query_result_generalized_l2(
        self,
        order_fanout_collection,
        case_name,
        expr,
        fanout_count,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        assert_expression_fanout(expr, fanout_count)
        assert_query_ids(self, client, order_fanout_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterOrderAndFanout")
    @pytest.mark.parametrize(
        "case_name, expr, fanout_count, expected_ids",
        fanout_params(BOOLEAN_FANOUT_EXPRESSIONS_L2),
    )
    def test_boolean_fanout_count_query_result_l2(
        self,
        order_fanout_collection,
        case_name,
        expr,
        fanout_count,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        assert_expression_fanout(expr, fanout_count)
        assert_query_ids(self, client, order_fanout_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.parametrize(
        "left_expr, right_expr, expected_ids",
        order_params(ORDER_SENSITIVE_EXPRESSIONS),
    )
    @pytest.mark.xdist_group("TestFilterOrderAndFanout")
    def test_expression_order_permutation_same_result(
        self,
        order_fanout_collection,
        left_expr,
        right_expr,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        assert_same_query_result(
            self,
            client,
            order_fanout_collection,
            left_expr,
            right_expr,
            expected_ids,
            pk_field=default_pk,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterOrderAndFanout")
    @pytest.mark.parametrize(
        "case_name, left_expr, right_expr, expected_ids",
        equivalent_expression_params(EQUIVALENT_EXPRESSION_CASES),
    )
    def test_equivalent_expression_same_result(
        self,
        order_fanout_collection,
        case_name,
        left_expr,
        right_expr,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        assert_same_query_result(
            self,
            client,
            order_fanout_collection,
            left_expr,
            right_expr,
            expected_ids,
            pk_field=default_pk,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterIssueRegressions")
    @pytest.mark.parametrize(
        "case_name, expr, fanout_count, expected_ids",
        [pytest.param(*case, id=case[0]) for case in SEARCH_51568_CASES],
    )
    def test_json_same_path_mixed_type_or_search_51568(
        self,
        regression_51568_collection,
        case_name,
        expr,
        fanout_count,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        assert_expression_fanout(expr, fanout_count)
        search_ids_or_xfail_known_loss(
            client,
            regression_51568_collection,
            expr,
            expected_ids,
            case_name,
            KNOWN_ISSUE_51568_LOSS_IDS,
            KNOWN_ISSUE_51568_ERROR_VALUE_CASES,
            "https://github.com/milvus-io/milvus/issues/51568",
            f"{case_name} search",
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xdist_group("TestFilterSegmentRegressions")
    @pytest.mark.parametrize("segment_mode", SEGMENT_MODES_ACTIVE, ids=SEGMENT_MODES_ACTIVE)
    def test_segment_mode_order_permutation(self, segment_mode):
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        prepare_loaded_empty_collection_for_segment(
            self,
            client,
            collection_name,
            self.build_order_schema(client),
            vector_field=default_vec,
        )
        rows = build_order_rows()
        insert_by_segment_mode(self, client, collection_name, rows[:5], rows[5:], segment_mode)
        wait_for_segment_mode(
            client,
            collection_name,
            segment_mode,
            expected_row_count=len(rows),
            expected_sealed_rows=len(rows[:5]) if segment_mode == "mixed" else None,
        )
        left_expr, right_expr, expected_ids = ORDER_SENSITIVE_EXPRESSIONS[0]
        assert_same_query_result(
            self,
            client,
            collection_name,
            left_expr,
            right_expr,
            expected_ids,
            pk_field=default_pk,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, expr, fanout_count, expected_ids, segment_mode",
        mark_51568_segment_cases(SEGMENT_51568_CASES, SEGMENT_51568_MODES),
    )
    @pytest.mark.xdist_group("TestFilterSegmentRegressions")
    def test_json_same_path_mixed_type_or_regression_51568_by_segment(
        self,
        case_name,
        expr,
        fanout_count,
        expected_ids,
        segment_mode,
    ):
        client = self._client()
        assert_expression_fanout(expr, fanout_count)
        collection_name = cf.gen_collection_name_by_testcase_name()
        prepare_loaded_empty_collection_for_segment(
            self,
            client,
            collection_name,
            self.build_51568_schema(client),
            vector_field=default_vec,
        )
        rows = [{default_pk: i, "meta": {"p": i}} for i in range(1, 21)]
        sealed_rows = rows[::2]
        growing_cross_type_witness = {default_pk: 21, "meta": {"p": 1}}
        growing_rows = rows[1::2] + [growing_cross_type_witness]
        insert_by_segment_mode(self, client, collection_name, sealed_rows, growing_rows, segment_mode)
        wait_for_segment_mode(
            client,
            collection_name,
            segment_mode,
            expected_row_count=len(rows) + 1,
            expected_sealed_rows=len(sealed_rows) if segment_mode == "mixed" else None,
        )
        segment_expected_ids = sorted(expected_ids + [growing_cross_type_witness[default_pk]])
        query_ids_or_xfail_known_loss(
            client,
            collection_name,
            expr,
            segment_expected_ids,
            case_name,
            KNOWN_ISSUE_51568_LOSS_IDS,
            KNOWN_ISSUE_51568_ERROR_VALUE_CASES,
            "https://github.com/milvus-io/milvus/issues/51568",
            f"{case_name} {segment_mode} query",
        )
