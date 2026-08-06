from collections import Counter

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from milvus_client.expressions.expression_test_utils import (
    add_minimal_query_vector_field,
    assert_query_ids,
    assert_search_result_shape,
    create_minimal_vector_index,
    insert_by_segment_mode,
    prepare_loaded_empty_collection_for_segment,
    register_collection_cleanup,
    wait_for_materialized_index,
    wait_for_segment_mode,
)
from milvus_client.expressions.filtering_case_matrix import (
    BITWISE_CONTROL_CASES,
    EMPTY_LIST_TEMPLATE_51617_CASES,
    NON_EMPTY_TEMPLATE_CONTROL_CASES,
    REAL_INDEX_ROW_COUNT,
)
from pymilvus import AnnSearchRequest, DataType, MilvusException, WeightedRanker
from pymilvus.client.types import IndexState

default_pk = "id"
default_vec = "vector"
default_dim = 8

VECTOR_SEARCH_PARAMS = {"metric_type": "L2", "params": {"ef": 64}}
INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)
INT64_BOUNDARY_VALUES = [
    INT64_MAX - 1,
    100,
    INT64_MIN,
    -1,
    0,
    1,
    INT64_MAX,
    INT64_MIN + 1,
    20000,
    19950,
]
INT64_OVERFLOW_CASES = [
    (
        "addition",
        "i64_plain + 33 <= 19974",
        "i64_indexed + 33 <= 19974",
        lambda value: value + 33 <= 19974,
    ),
    (
        "subtraction",
        "i64_plain - 1 >= 0",
        "i64_indexed - 1 >= 0",
        lambda value: value - 1 >= 0,
    ),
    (
        "multiplication",
        "i64_plain * 2 > 1",
        "i64_indexed * 2 > 1",
        lambda value: value * 2 > 1,
    ),
]
INT8_BOUNDARY_VALUES = [126, 127, -128, -127, 0, 1, 2, 3]
INT16_BOUNDARY_VALUES = [32766, 32767, -32768, -32767, 0, 1, 2, 3]
INT32_BOUNDARY_VALUES = [2147483646, 2147483647, -2147483648, -2147483647, 0, 1, 2, 3, 19950]
NARROW_INTEGER_ARITHMETIC_CASES = [
    ("int32_add_exact_math", "i32_plain + 33 <= 19974", lambda values: values["i32"] + 33 <= 19974),
    ("int32_multiply_exact_math", "i32_plain * 2 > 1", lambda values: values["i32"] * 2 > 1),
    ("int16_add_exact_math", "i16_plain + 2 > 0", lambda values: values["i16"] + 2 > 0),
    ("int8_add_exact_math", "i8_plain + 2 > 0", lambda values: values["i8"] + 2 > 0),
]
INT64_OVERFLOW_KNOWN_DELTAS = {
    "addition": ([], [0, 6]),
    "subtraction": ([], [2]),
    "multiplication": ([0, 6], [7]),
}


def vector_for_id(row_id):
    denominator = float(REAL_INDEX_ROW_COUNT + 101)
    return [
        float(((row_id + 1) * (dimension + 3)) % (REAL_INDEX_ROW_COUNT + 97) + 1) / denominator
        for dimension in range(default_dim)
    ]


def hit_id(hit):
    if default_pk in hit:
        return hit[default_pk]
    entity = hit.get("entity", {})
    if default_pk in entity:
        return entity[default_pk]
    return hit["id"]


def sorted_hit_ids(hits):
    return sorted(hit_id(hit) for hit in hits)


def assert_exact_ids(actual_ids, expected_ids, context):
    actual_ids = sorted(actual_ids)
    expected_ids = sorted(expected_ids)
    missing, extra = ids_delta(actual_ids, expected_ids)
    assert actual_ids == expected_ids, (
        f"{context}: missing={missing[:10]} (count={len(missing)}), "
        f"extra={extra[:10]} (count={len(extra)}), actual_count={len(actual_ids)}, expected_count={len(expected_ids)}"
    )


def ids_delta(actual_ids, expected_ids):
    actual_counter = Counter(actual_ids)
    expected_counter = Counter(expected_ids)
    missing = sorted((expected_counter - actual_counter).elements())
    extra = sorted((actual_counter - expected_counter).elements())
    return missing, extra


def int64_overflow_params():
    params = []
    for case_name, plain_expr, indexed_expr, predicate in INT64_OVERFLOW_CASES:
        params.append(pytest.param(case_name, "plain", plain_expr, predicate, id=f"{case_name}-plain"))
        params.append(pytest.param(case_name, "indexed", indexed_expr, predicate, id=f"{case_name}-indexed"))
    return params


def is_empty_template_error(exc):
    message = str(exc)
    return exc.code == 1100 and all(
        token in message
        for token in (
            "unknown template variable value type: <nil>",
            "query plan failed: invalid parameter",
        )
    )


def query_template_ids_or_xfail(client, collection_name, expr, filter_params, expected_ids, context):
    try:
        rows = client.query(collection_name, filter=expr, filter_params=filter_params, output_fields=[default_pk])
    except MilvusException as exc:
        if is_empty_template_error(exc):
            pytest.xfail(f"Known issue https://github.com/milvus-io/milvus/issues/51617: {context} failed with {exc}")
        raise

    actual_ids = sorted(row[default_pk] for row in rows)
    assert_exact_ids(actual_ids, expected_ids, context)
    return actual_ids


def search_template_ids_or_xfail(client, collection_name, expr, filter_params, expected_ids, context):
    try:
        result = client.search(
            collection_name,
            data=[vector_for_id(2)],
            anns_field=default_vec,
            search_params=VECTOR_SEARCH_PARAMS,
            filter=expr,
            filter_params=filter_params,
            output_fields=[default_pk],
            limit=10,
        )
    except MilvusException as exc:
        if is_empty_template_error(exc):
            pytest.xfail(f"Known issue https://github.com/milvus-io/milvus/issues/51617: {context} failed with {exc}")
        raise

    hits = assert_search_result_shape(result, "L2")
    actual_ids = sorted_hit_ids(hits)
    assert_exact_ids(actual_ids, expected_ids, context)
    return actual_ids


def hybrid_template_ids_or_xfail(client, collection_name, request, expected_ids, context):
    try:
        result = client.hybrid_search(
            collection_name,
            [request],
            ranker=WeightedRanker(1.0),
            limit=10,
            output_fields=[default_pk],
        )
    except MilvusException as exc:
        if is_empty_template_error(exc):
            pytest.xfail(f"Known issue https://github.com/milvus-io/milvus/issues/51617: {context} failed with {exc}")
        raise

    hits = assert_search_result_shape(result, "RANKER")
    actual_ids = sorted_hit_ids(hits)
    assert_exact_ids(actual_ids, expected_ids, context)
    return actual_ids


def delete_template_or_xfail(client, collection_name, expr, filter_params, expected_delete_count, context):
    try:
        result = client.delete(collection_name, filter=expr, filter_params=filter_params)
    except MilvusException as exc:
        if is_empty_template_error(exc):
            pytest.xfail(f"Known issue https://github.com/milvus-io/milvus/issues/51617: {context} failed with {exc}")
        raise

    assert result["delete_count"] == expected_delete_count, (
        f"{context} delete_count={result['delete_count']}, expected {expected_delete_count}"
    )
    return result


def query_int64_ids_or_xfail(client, collection_name, expr, expected_ids, case_name, context):
    rows = client.query(collection_name, filter=expr, output_fields=[default_pk], limit=REAL_INDEX_ROW_COUNT)
    actual_ids = sorted(row[default_pk] for row in rows)
    missing, extra = ids_delta(actual_ids, expected_ids)
    known_missing, known_extra = INT64_OVERFLOW_KNOWN_DELTAS[case_name]
    if missing == known_missing and extra == known_extra:
        pytest.xfail(
            "Known issue https://github.com/milvus-io/milvus/issues/48440: "
            f"{context} reproduced exact overflow delta, missing={missing}, extra={extra}"
        )
    assert_exact_ids(actual_ids, expected_ids, context)


def search_int64_ids_or_xfail(client, collection_name, expr, expected_ids, case_name, context):
    result = client.search(
        collection_name,
        data=[vector_for_id(0)],
        anns_field=default_vec,
        search_params={
            "metric_type": "L2",
            "params": {"ef": REAL_INDEX_ROW_COUNT},
            "hints": "iterative_filter",
        },
        filter=expr,
        output_fields=[default_pk],
        limit=REAL_INDEX_ROW_COUNT,
    )
    hits = assert_search_result_shape(result, "L2")
    actual_ids = sorted_hit_ids(hits)
    missing, extra = ids_delta(actual_ids, expected_ids)
    known_missing, known_extra = INT64_OVERFLOW_KNOWN_DELTAS[case_name]
    if missing == known_missing and extra == known_extra:
        pytest.xfail(
            "Known issue https://github.com/milvus-io/milvus/issues/48440: "
            f"{context} reproduced exact overflow delta, missing={missing}, extra={extra}"
        )
    assert_exact_ids(actual_ids, expected_ids, context)


@pytest.mark.xdist_group("TestEmptyListTemplateIssueRegressions")
class TestEmptyListTemplateIssueRegressions(TestMilvusClientV2Base):
    shared_alias = "TestEmptyListTemplateIssueRegressions"
    vector_index_name = "idx_empty_template_vector_hnsw"

    def create_template_collection(
        self,
        client,
        collection_name,
        force_teardown=True,
        cleanup_request=None,
        cleanup_alias=None,
    ):
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=8, max_length=64)
        schema.add_field("meta", DataType.JSON)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            force_teardown=force_teardown,
            consistency_level="Strong",
        )
        if cleanup_request is not None:
            register_collection_cleanup(self, cleanup_request, cleanup_alias or self.shared_alias, collection_name)
        rows = [
            {default_pk: 1, "tags": [], "meta": {"tags": []}, default_vec: vector_for_id(1)},
            {default_pk: 2, "tags": ["blue"], "meta": {"tags": ["blue"]}, default_vec: vector_for_id(2)},
        ]
        rows.extend(
            {
                default_pk: row_id,
                "tags": ["filler"],
                "meta": {"tags": ["filler"]},
                default_vec: vector_for_id(row_id),
            }
            for row_id in range(3, REAL_INDEX_ROW_COUNT + 1)
        )
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            default_vec,
            index_name=self.vector_index_name,
            index_type="HNSW",
            metric_type="L2",
            params={"M": 8, "efConstruction": 64},
        )
        self.create_index(client, collection_name, index_params=index_params)
        vector_index_info = wait_for_materialized_index(
            self,
            client,
            collection_name,
            self.vector_index_name,
            expected_total_rows=REAL_INDEX_ROW_COUNT,
            expected_field_name=default_vec,
            expected_index_type="HNSW",
        )
        assert vector_index_info["index_name"] == self.vector_index_name
        assert vector_index_info["field_name"] == default_vec
        assert vector_index_info["index_type"] == "HNSW"
        assert vector_index_info["state"] == IndexState.Finished.name
        assert vector_index_info["indexed_rows"] == vector_index_info["total_rows"]
        assert vector_index_info["total_rows"] == REAL_INDEX_ROW_COUNT
        self.load_collection(client, collection_name)
        return rows

    @pytest.fixture(scope="class")
    def template_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_empty_template" + cf.gen_unique_str("_")
        self.create_template_collection(
            client,
            collection_name,
            force_teardown=False,
            cleanup_request=request,
            cleanup_alias=self.shared_alias,
        )
        yield collection_name

    @pytest.fixture
    def template_delete_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_template_delete" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(
            client,
            collection_name,
            data=[{default_pk: row_id, default_vec: vector_for_id(row_id)} for row_id in range(1, 4)],
        )
        self.flush(client, collection_name)
        create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.mark.parametrize(
        "case_name, template_expr, inline_expr, expected_ids",
        [
            pytest.param(
                *case, marks=pytest.mark.tags(CaseLabel.L0 if case[0] == "scalar_in" else CaseLabel.L2), id=case[0]
            )
            for case in EMPTY_LIST_TEMPLATE_51617_CASES
        ],
    )
    def test_empty_list_template_query_matches_inline_51617(
        self,
        template_collection,
        case_name,
        template_expr,
        inline_expr,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        inline_rows = client.query(template_collection, filter=inline_expr, output_fields=[default_pk])
        assert sorted(row[default_pk] for row in inline_rows) == expected_ids
        template_ids = query_template_ids_or_xfail(
            client,
            template_collection,
            template_expr,
            {"values": []},
            expected_ids,
            f"{case_name} query",
        )
        assert template_ids == expected_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_empty_list_template_search_matches_inline_51617(self, template_collection):
        client = self._client(alias=self.shared_alias)
        inline_expr = "id <= 2 and array_contains_all(tags, [])"
        template_expr = "id <= 2 and array_contains_all(tags, {values})"
        inline_result = client.search(
            template_collection,
            data=[vector_for_id(2)],
            anns_field=default_vec,
            search_params=VECTOR_SEARCH_PARAMS,
            filter=inline_expr,
            output_fields=[default_pk],
            limit=10,
        )
        inline_hits = assert_search_result_shape(inline_result, "L2")
        assert sorted_hit_ids(inline_hits) == [1, 2]
        template_ids = search_template_ids_or_xfail(
            client,
            template_collection,
            template_expr,
            {"values": []},
            [1, 2],
            "array_contains_all search",
        )
        assert template_ids == [1, 2]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "function_name, expected_ids",
        [
            pytest.param("json_contains_any", [], id="contains_any_empty"),
            pytest.param("json_contains_all", [1, 2], id="contains_all_empty"),
        ],
    )
    def test_empty_list_template_hybrid_search_matches_inline_51617(
        self,
        template_collection,
        function_name,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        inline_expr = f'id <= 2 and {function_name}(meta["tags"], [])'
        template_expr = f'id <= 2 and {function_name}(meta["tags"], {{values}})'
        inline_request = AnnSearchRequest(
            data=[vector_for_id(2)],
            anns_field=default_vec,
            param=VECTOR_SEARCH_PARAMS,
            limit=10,
            expr=inline_expr,
        )
        template_request = AnnSearchRequest(
            data=[vector_for_id(2)],
            anns_field=default_vec,
            param=VECTOR_SEARCH_PARAMS,
            limit=10,
            expr=template_expr,
            expr_params={"values": []},
        )
        inline_result = client.hybrid_search(
            template_collection,
            [inline_request],
            ranker=WeightedRanker(1.0),
            limit=10,
            output_fields=[default_pk],
        )
        inline_hits = assert_search_result_shape(inline_result, "RANKER")
        assert sorted_hit_ids(inline_hits) == expected_ids
        template_ids = hybrid_template_ids_or_xfail(
            client,
            template_collection,
            template_request,
            expected_ids,
            f"{function_name} hybrid search",
        )
        assert template_ids == expected_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_empty_list_template_delete_is_noop_51617(self, template_delete_collection):
        client = self._client(alias=self.shared_alias)
        collection_name = template_delete_collection
        inline_result = client.delete(collection_name, filter="id <= 2 and id in []")
        assert inline_result["delete_count"] == 0
        try:
            result = client.delete(
                collection_name,
                filter="id <= 2 and id in {values}",
                filter_params={"values": []},
            )
        except MilvusException as exc:
            remaining = client.query(collection_name, filter="id <= 2", output_fields=[default_pk])
            assert sorted(row[default_pk] for row in remaining) == [1, 2]
            count = client.query(collection_name, filter="", output_fields=["count(*)"])
            assert count[0]["count(*)"] == 3
            if is_empty_template_error(exc):
                pytest.xfail(
                    "Known issue https://github.com/milvus-io/milvus/issues/51617: "
                    f"scalar empty-list delete failed with {exc}"
                )
            raise

        assert result["delete_count"] == 0
        remaining = client.query(collection_name, filter="id <= 2", output_fields=[default_pk])
        assert sorted(row[default_pk] for row in remaining) == [1, 2]
        count = client.query(collection_name, filter="", output_fields=["count(*)"])
        assert count[0]["count(*)"] == 3

    @pytest.mark.tags(CaseLabel.L2)
    def test_empty_list_template_delete_positive_51617(self, template_delete_collection):
        client = self._client(alias=self.shared_alias)
        collection_name = template_delete_collection
        result = delete_template_or_xfail(
            client,
            collection_name,
            "id <= 2 and id not in {values}",
            {"values": []},
            2,
            "scalar empty-list NOT IN delete",
        )
        assert result["delete_count"] == 2
        remaining = client.query(collection_name, filter="id <= 2", output_fields=[default_pk])
        assert remaining == []
        count = client.query(collection_name, filter="", output_fields=["count(*)"])
        assert count[0]["count(*)"] == 1

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, template_expr, filter_params, expected_ids",
        [pytest.param(*case, id=case[0]) for case in NON_EMPTY_TEMPLATE_CONTROL_CASES],
    )
    def test_non_empty_template_query_controls(
        self,
        template_collection,
        case_name,
        template_expr,
        filter_params,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        rows = client.query(
            template_collection,
            filter=template_expr,
            filter_params=filter_params,
            output_fields=[default_pk],
        )
        assert sorted(row[default_pk] for row in rows) == expected_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_non_empty_template_search_control(self, template_collection):
        client = self._client(alias=self.shared_alias)
        result = client.search(
            template_collection,
            data=[vector_for_id(2)],
            anns_field=default_vec,
            search_params=VECTOR_SEARCH_PARAMS,
            filter="id <= 2 and array_contains_any(tags, {values})",
            filter_params={"values": ["blue"]},
            output_fields=[default_pk],
            limit=2,
        )
        hits = assert_search_result_shape(result, "L2")
        assert sorted_hit_ids(hits) == [2]

    @pytest.mark.tags(CaseLabel.L2)
    def test_non_empty_template_hybrid_search_control(self, template_collection):
        client = self._client(alias=self.shared_alias)
        request = AnnSearchRequest(
            data=[vector_for_id(2)],
            anns_field=default_vec,
            param=VECTOR_SEARCH_PARAMS,
            limit=2,
            expr='id <= 2 and json_contains_any(meta["tags"], {values})',
            expr_params={"values": ["blue"]},
        )
        result = client.hybrid_search(
            template_collection,
            [request],
            ranker=WeightedRanker(1.0),
            limit=2,
            output_fields=[default_pk],
        )
        hits = assert_search_result_shape(result, "RANKER")
        assert sorted_hit_ids(hits) == [2]

    @pytest.mark.tags(CaseLabel.L2)
    def test_non_empty_template_delete_control(self, template_delete_collection):
        client = self._client(alias=self.shared_alias)
        result = client.delete(
            template_delete_collection,
            filter="id in {values}",
            filter_params={"values": [1]},
        )
        assert result["delete_count"] == 1
        remaining = client.query(template_delete_collection, filter="id >= 1", output_fields=[default_pk])
        assert sorted(row[default_pk] for row in remaining) == [2, 3]


@pytest.mark.xdist_group("TestParserAndBitwiseIssueCoverage")
class TestParserAndBitwiseIssueCoverage(TestMilvusClientV2Base):
    shared_alias = "TestParserAndBitwiseIssueCoverage"

    @pytest.fixture(scope="class")
    def bitwise_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_bitwise" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
        schema.add_field("flags", DataType.INT64)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(client, collection_name, [{default_pk: i, "flags": i} for i in range(8)])
        self.flush(client, collection_name)
        create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.fixture(scope="class")
    def null_literal_collections(self, request):
        client = self._client(alias=self.shared_alias)
        collection_names = {}
        for enable_dynamic_field in (False, True):
            mode = "dynamic" if enable_dynamic_field else "static"
            collection_name = f"filter_null_literal_{mode}" + cf.gen_unique_str("_")
            schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
            schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
            add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
            self.create_collection(
                client,
                collection_name,
                schema=schema,
                force_teardown=False,
                consistency_level="Strong",
            )
            register_collection_cleanup(self, request, self.shared_alias, collection_name)
            create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
            self.load_collection(client, collection_name)
            collection_names[enable_dynamic_field] = collection_name
        yield collection_names

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case_name, expr, expected_ids",
        [pytest.param(*case, id=case[0]) for case in BITWISE_CONTROL_CASES],
    )
    def test_supported_bitwise_operator_controls_50964(
        self,
        bitwise_collection,
        case_name,
        expr,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, bitwise_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [False, True], ids=["static", "dynamic"])
    @pytest.mark.parametrize("operation", ["query", "delete"])
    def test_null_literal_in_has_meaningful_rejection_50882(
        self,
        null_literal_collections,
        enable_dynamic_field,
        operation,
    ):
        client = self._client(alias=self.shared_alias)
        collection_name = null_literal_collections[enable_dynamic_field]
        with pytest.raises(MilvusException) as exc_info:
            if operation == "query":
                client.query(collection_name, filter="id in [1, NULL, 2]", output_fields=[default_pk])
            else:
                client.delete(collection_name, filter="id in [1, NULL, 2]")
        assert "NULL literal is not supported in expressions" in str(exc_info.value)
        assert "field NULL not exist" not in str(exc_info.value)


@pytest.mark.xdist_group("TestInt64OverflowIssueMining")
class TestInt64OverflowIssueMining(TestMilvusClientV2Base):
    shared_alias = "TestInt64OverflowIssueMining"
    index_name = "idx_i64_overflow_inverted"
    vector_index_name = "idx_i64_overflow_vector_hnsw"

    @pytest.fixture(scope="class")
    def overflow_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_i64_overflow" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("i64_plain", DataType.INT64)
        schema.add_field("i64_indexed", DataType.INT64)
        schema.add_field("i8_plain", DataType.INT8)
        schema.add_field("i16_plain", DataType.INT16)
        schema.add_field("i32_plain", DataType.INT32)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)

        values_by_id = {}
        narrow_values_by_id = {}
        rows = []
        for row_id in range(REAL_INDEX_ROW_COUNT):
            value = INT64_BOUNDARY_VALUES[row_id] if row_id < len(INT64_BOUNDARY_VALUES) else row_id - 1024
            values_by_id[row_id] = value
            if row_id < len(INT8_BOUNDARY_VALUES):
                narrow_values = {
                    "i8": INT8_BOUNDARY_VALUES[row_id],
                    "i16": INT16_BOUNDARY_VALUES[row_id],
                    "i32": INT32_BOUNDARY_VALUES[row_id],
                }
            else:
                narrow_values = {
                    "i8": (row_id % 101) - 50,
                    "i16": row_id - 1024,
                    "i32": row_id - 1024,
                }
            narrow_values_by_id[row_id] = narrow_values
            rows.append(
                {
                    default_pk: row_id,
                    default_vec: vector_for_id(row_id),
                    "i64_plain": value,
                    "i64_indexed": value,
                    "i8_plain": narrow_values["i8"],
                    "i16_plain": narrow_values["i16"],
                    "i32_plain": narrow_values["i32"],
                }
            )
        self.__class__.overflow_values_by_id = values_by_id
        self.__class__.narrow_values_by_id = narrow_values_by_id
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            default_vec,
            index_type="HNSW",
            index_name=self.vector_index_name,
            metric_type="L2",
            params={"M": 8, "efConstruction": 64},
        )
        index_params.add_index("i64_indexed", index_type="INVERTED", index_name=self.index_name)
        self.create_index(client, collection_name, index_params=index_params)
        index_specs = {
            self.vector_index_name: (default_vec, "HNSW"),
            self.index_name: ("i64_indexed", "INVERTED"),
        }
        self.__class__.overflow_index_infos = {
            index_name: wait_for_materialized_index(
                self,
                client,
                collection_name,
                index_name,
                expected_total_rows=REAL_INDEX_ROW_COUNT,
                expected_field_name=field_name,
                expected_index_type=index_type,
            )
            for index_name, (field_name, index_type) in index_specs.items()
        }
        self.load_collection(client, collection_name)
        yield collection_name

    def expected_ids(self, predicate):
        return sorted(row_id for row_id, value in self.overflow_values_by_id.items() if predicate(value))

    def narrow_expected_ids(self, predicate):
        return sorted(row_id for row_id, values in self.narrow_values_by_id.items() if predicate(values))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, path_name, expr, predicate",
        int64_overflow_params(),
    )
    def test_int64_overflow_plain_indexed_exact_math_48440(
        self,
        overflow_collection,
        case_name,
        path_name,
        expr,
        predicate,
    ):
        client = self._client(alias=self.shared_alias)
        expected_ids = self.expected_ids(predicate)
        query_int64_ids_or_xfail(
            client,
            overflow_collection,
            expr,
            expected_ids,
            case_name,
            f"{case_name} {path_name}",
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_int64_overflow_iterative_search_exact_math_48440(self, overflow_collection):
        client = self._client(alias=self.shared_alias)
        expected_ids = self.expected_ids(lambda value: value + 33 <= 19974)
        search_int64_ids_or_xfail(
            client,
            overflow_collection,
            "i64_plain + 33 <= 19974",
            expected_ids,
            "addition",
            "iterative search addition",
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_int64_overflow_index_is_materialized(self, overflow_collection):
        expected_fields = {
            self.vector_index_name: default_vec,
            self.index_name: "i64_indexed",
        }
        assert set(self.overflow_index_infos) == set(expected_fields)
        for index_name, index_info in self.overflow_index_infos.items():
            assert index_info["index_name"] == index_name
            assert index_info["field_name"] == expected_fields[index_name]
            assert index_info["index_type"] == ("HNSW" if index_name == self.vector_index_name else "INVERTED")
            assert index_info["state"] == IndexState.Finished.name
            assert index_info["indexed_rows"] == index_info["total_rows"] == REAL_INDEX_ROW_COUNT
            assert index_info["pending_index_rows"] == 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, expr, predicate",
        [pytest.param(*case, id=case[0]) for case in NARROW_INTEGER_ARITHMETIC_CASES],
    )
    def test_narrow_integer_arithmetic_exact_math_controls(
        self,
        overflow_collection,
        case_name,
        expr,
        predicate,
    ):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(
            self,
            client,
            overflow_collection,
            expr,
            self.narrow_expected_ids(predicate),
            pk_field=default_pk,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("segment_mode", ["growing", "mixed"])
    def test_int64_overflow_segment_exact_math_48440(self, segment_mode):
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
        schema.add_field("i64_plain", DataType.INT64)
        prepare_loaded_empty_collection_for_segment(
            self,
            client,
            collection_name,
            schema,
            vector_field=default_vec,
        )
        rows = [{default_pk: row_id, "i64_plain": value} for row_id, value in enumerate(INT64_BOUNDARY_VALUES)]
        insert_by_segment_mode(self, client, collection_name, rows[:4], rows[4:], segment_mode)
        wait_for_segment_mode(
            client,
            collection_name,
            segment_mode,
            expected_row_count=len(rows),
            expected_sealed_rows=4 if segment_mode == "mixed" else None,
        )
        expected_ids = [row_id for row_id, value in enumerate(INT64_BOUNDARY_VALUES) if value + 33 <= 19974]
        query_int64_ids_or_xfail(
            client,
            collection_name,
            "i64_plain + 33 <= 19974",
            expected_ids,
            "addition",
            f"{segment_mode} segment addition",
        )
