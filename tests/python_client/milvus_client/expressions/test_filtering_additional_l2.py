import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from milvus_client.expressions.expression_test_utils import (
    add_minimal_query_vector_field,
    assert_query_id_offsets,
    assert_query_ids,
    create_minimal_vector_index,
    query_ids,
    register_collection_cleanup,
    wait_for_materialized_index,
)
from pymilvus import DataType
from pymilvus.client.types import IndexState

default_pk = "id"
default_vec = "vector"
default_dim = 8
STRUCT_INDEX_ROW_COUNT = 3000


def vector_for_id(row_id):
    return [float(((row_id + 1) * (dimension + 3)) % 17 + 1) / 20.0 for dimension in range(default_dim)]


STRUCT_ARRAY_FILTER_CASES = [
    (
        "element_filter_same_element_compound",
        'element_filter(events, $[rank] >= 10 && $[tag] == "qa")',
        [1, 2, 2, 9],
    ),
    (
        "match_all_scoped",
        "id in [1, 2, 3, 5] && MATCH_ALL(events, $[rank] >= 10)",
        [2],
    ),
    (
        "match_most_one_scoped",
        "id in [1, 2, 3, 5] && MATCH_MOST(events, $[rank] >= 10, threshold=1)",
        [1, 3, 5],
    ),
    (
        "match_exact_one",
        "MATCH_EXACT(events, $[rank] >= 10, threshold=1)",
        [1, 5, 8, 9],
    ),
]

STRUCT_ARRAY_SUBFIELD_FILTER_CASES = [
    ("array_length_empty", "array_length(events[rank]) == 0", [4]),
    ("array_contains_int8", "array_contains(events[level_i8], -5)", [5]),
    ("array_contains_int16", "array_contains(events[shard_i16], 30)", [2]),
    ("array_contains_int32", "array_contains(events[count_i32], 300)", [3]),
    ("array_contains_int", "array_contains(events[rank], 10)", [1]),
    ("array_contains_float", "array_contains(events[ratio_f32], 2.25)", [2]),
    ("array_contains_all_varchar", 'array_contains_all(events[tag], ["qa", "dev"])', [1]),
    ("array_contains_any_varchar", 'array_contains_any(events[tag], ["ops", "missing"])', [5]),
    ("array_contains_bool", "array_contains(events[active], true)", [1, 2, 5, 8]),
    ("fixed_index_int", "events[0][rank] >= 10", [1, 2, 5, 8, 9]),
    ("fixed_index_varchar", 'events[1][tag] == "qa"', [2, 5]),
    ("fixed_index_double", "events[0][score] > 2.0", [2, 5]),
    ("parent_is_null", "events is null", [6, 7]),
    ("parent_is_not_null", "events is not null", [1, 2, 3, 4, 5, 8, 9]),
]

STRUCT_ARRAY_OPERATOR_MATRIX_CASES = [
    ("element_eq_int8", "element_filter(events, $[level_i8] == 2)", [2]),
    ("element_ne_bool", "id <= 5 && element_filter(events, $[active] != true)", [1, 3, 5]),
    ("fixed_lt", "events[0][rank] < 10", [3]),
    ("fixed_le", "events[0][rank] <= 10", [1, 3]),
    ("fixed_gt", "events[0][rank] > 10", [2, 5, 8, 9]),
    ("fixed_ge", "events[0][rank] >= 10", [1, 2, 5, 8, 9]),
    ("fixed_ne", "events[0][rank] != 10", [2, 3, 5, 8, 9]),
    ("fixed_in", 'events[0][tag] in ["qa", "ops"]', [1, 2, 5, 9]),
    ("fixed_not_in", 'events[0][tag] not in ["qa", "ops"]', [3, 8]),
    ("fixed_chained_range", "10 <= events[0][rank] <= 12", [1, 2]),
    ("fixed_reverse_chained_range", "12 >= events[0][rank] >= 10", [1, 2]),
    ("element_like_prefix", 'element_filter(events, $[tag] like "q%")', [1, 2, 2, 5, 9]),
    ("element_not", 'element_filter(events, not ($[tag] == "qa"))', [1, 3, 5, 8]),
    ("element_or", 'element_filter(events, $[tag] == "ops" || $[score] < 0.2)', [1, 5]),
    ("element_add", "element_filter(events, $[rank] + 2 == 12)", [1]),
    ("element_sub", "element_filter(events, $[rank] - 1 == 12)", [5]),
    ("element_mul", "element_filter(events, $[rank] * 2 == 24)", [2]),
    ("element_div", "element_filter(events, $[score] / 2.0 > 1.0)", [2, 2, 5]),
    ("element_mod", "element_filter(events, $[rank] % 5 == 0)", [1, 5]),
    ("element_constant_power", "element_filter(events, $[rank] == 2 ** 3 + 2)", [1]),
    ("match_any_like", 'MATCH_ANY(events, $[tag] like "q%")', [1, 2, 5, 9]),
    ("match_any_in", "MATCH_ANY(events, $[rank] in [10, 13])", [1, 5]),
    ("match_any_not", "MATCH_ANY(events, not ($[active] == true))", [1, 3, 5, 9]),
    ("match_exact_or", 'MATCH_EXACT(events, $[tag] == "qa" || $[rank] > 12, threshold=2)', [2, 5]),
]

STRUCT_ARRAY_ELEMENT_FILTER_OFFSET_CASES = {
    "element_filter_same_element_compound": [(1, 0), (2, 0), (2, 1), (9, 0)],
    "element_eq_int8": [(2, 0)],
    "element_ne_bool": [(1, 1), (3, 0), (5, 0)],
    "element_like_prefix": [(1, 0), (2, 0), (2, 1), (5, 1), (9, 0)],
    "element_not": [(1, 1), (3, 0), (5, 0), (8, 0)],
    "element_or": [(1, 1), (5, 0)],
    "element_add": [(1, 0)],
    "element_sub": [(5, 0)],
    "element_mul": [(2, 1)],
    "element_div": [(2, 0), (2, 1), (5, 0)],
    "element_mod": [(1, 0), (5, 1)],
    "element_constant_power": [(1, 0)],
}

STRUCT_ARRAY_MATCH_NULL_EMPTY_CASES = [
    ("match_any", 'MATCH_ANY(events, $[active] == true && $[tag] == "qa")', [1, 2, 5]),
    ("match_all_vacuous_empty", "MATCH_ALL(events, $[rank] >= 10)", [2, 4, 8, 9]),
    ("match_least_two", "MATCH_LEAST(events, $[rank] >= 10, threshold=2)", [2]),
    ("match_most_one", "MATCH_MOST(events, $[rank] >= 10, threshold=1)", [1, 3, 4, 5, 8, 9]),
    ("match_exact_zero", "MATCH_EXACT(events, $[rank] >= 10, threshold=0)", [3, 4]),
    ("not_match_any", "not MATCH_ANY(events, $[rank] >= 10)", [3, 4]),
]

STRUCT_ARRAY_NEGATIVE_FILTER_CASES = [
    ("unknown_subfield", "MATCH_ANY(events, $[missing] == 1)", "array field not found: events[missing]"),
    (
        "wrong_literal_type",
        'element_filter(events, $[rank] == "bad")',
        "comparisons between Int64 and VarChar are not supported",
    ),
    (
        "zero_threshold",
        "MATCH_LEAST(events, $[rank] >= 10, threshold=0)",
        "count in MATCH_LEAST must be positive",
    ),
    (
        "negative_threshold",
        "MATCH_LEAST(events, $[rank] >= 10, threshold=-1)",
        "expecting IntegerConstant",
    ),
    ("field_power_rejected", "element_filter(events, $[rank] ** 2 == 100)", "power can only apply on constants"),
]

STRUCT_ARRAY_INDEX_CONSISTENCY_CASES = [
    (
        "stl_sort_rank_match_any",
        "MATCH_ANY(events_plain, $[rank] >= 10)",
        "MATCH_ANY(events_indexed, $[rank] >= 10)",
        [1, 2, 5, 1023, 1024],
    ),
    (
        "inverted_tag_contains_any",
        'array_contains_any(events_plain[tag], ["ops", "missing"])',
        'array_contains_any(events_indexed[tag], ["ops", "missing"])',
        [5],
    ),
    (
        "bitmap_active_contains",
        "array_contains(events_plain[active], true)",
        "array_contains(events_indexed[active], true)",
        [1, 2, 5, 1023, 2047],
    ),
    (
        "mixed_indexed_subfields",
        'MATCH_ANY(events_plain, $[rank] >= 10 && $[tag] == "qa" && $[active] == true)',
        'MATCH_ANY(events_indexed, $[rank] >= 10 && $[tag] == "qa" && $[active] == true)',
        [1, 2],
    ),
    (
        "stl_sort_score_range",
        "MATCH_ANY(events_plain, $[score] >= 2.0 && $[score] < 3.0)",
        "MATCH_ANY(events_indexed, $[score] >= 2.0 && $[score] < 3.0)",
        [2],
    ),
    (
        "indexed_fixed_rank_in",
        "events_plain[0][rank] in [10, 11, 13]",
        "events_indexed[0][rank] in [10, 11, 13]",
        [1, 2, 5],
    ),
]


def build_struct_array_rows():
    return [
        {
            default_pk: 1,
            default_vec: vector_for_id(1),
            "events": [
                {
                    "rank": 10,
                    "tag": "qa",
                    "active": True,
                    "score": 1.5,
                    "level_i8": 1,
                    "shard_i16": 10,
                    "count_i32": 100,
                    "ratio_f32": 1.25,
                },
                {
                    "rank": 1,
                    "tag": "dev",
                    "active": False,
                    "score": 0.1,
                    "level_i8": -1,
                    "shard_i16": -10,
                    "count_i32": 101,
                    "ratio_f32": 0.5,
                },
            ],
        },
        {
            default_pk: 2,
            default_vec: vector_for_id(2),
            "events": [
                {
                    "rank": 11,
                    "tag": "qa",
                    "active": True,
                    "score": 2.1,
                    "level_i8": 2,
                    "shard_i16": 20,
                    "count_i32": 200,
                    "ratio_f32": 2.25,
                },
                {
                    "rank": 12,
                    "tag": "qa",
                    "active": True,
                    "score": 2.2,
                    "level_i8": 3,
                    "shard_i16": 30,
                    "count_i32": 201,
                    "ratio_f32": 2.5,
                },
            ],
        },
        {
            default_pk: 3,
            default_vec: vector_for_id(3),
            "events": [
                {
                    "rank": 2,
                    "tag": "dev",
                    "active": False,
                    "score": 0.2,
                    "level_i8": 4,
                    "shard_i16": 40,
                    "count_i32": 300,
                    "ratio_f32": 0.75,
                }
            ],
        },
        {
            default_pk: 4,
            default_vec: vector_for_id(4),
            "events": [],
        },
        {
            default_pk: 5,
            default_vec: vector_for_id(5),
            "events": [
                {
                    "rank": 13,
                    "tag": "ops",
                    "active": False,
                    "score": 3.0,
                    "level_i8": 5,
                    "shard_i16": 50,
                    "count_i32": 400,
                    "ratio_f32": 3.5,
                },
                {
                    "rank": 5,
                    "tag": "qa",
                    "active": True,
                    "score": 0.5,
                    "level_i8": -5,
                    "shard_i16": -50,
                    "count_i32": 401,
                    "ratio_f32": 1.0,
                },
            ],
        },
        {
            default_pk: 6,
            default_vec: vector_for_id(6),
            "events": None,
        },
        {
            default_pk: 7,
            default_vec: vector_for_id(7),
        },
        {
            default_pk: 8,
            default_vec: vector_for_id(8),
            "events": [
                {
                    "rank": 14,
                    "tag": "dev",
                    "active": True,
                    "score": 0.3,
                    "level_i8": 8,
                    "shard_i16": 80,
                    "count_i32": 800,
                    "ratio_f32": 0.8,
                }
            ],
        },
        {
            default_pk: 9,
            default_vec: vector_for_id(9),
            "events": [
                {
                    "rank": 16,
                    "tag": "qa",
                    "active": False,
                    "score": 0.4,
                    "level_i8": 9,
                    "shard_i16": 90,
                    "count_i32": 900,
                    "ratio_f32": 0.9,
                }
            ],
        },
    ]


def build_struct_array_index_rows(row_count=STRUCT_INDEX_ROW_COUNT):
    rows = []
    controls = {
        1023: {"rank": 14, "tag": "dev", "active": True},
        1024: {"rank": 15, "tag": "qa", "active": False},
        2047: {"rank": 5, "tag": "qa", "active": True},
    }
    for row in build_struct_array_rows()[:5]:
        events = row["events"]
        rows.append(
            {
                default_pk: row[default_pk],
                default_vec: row[default_vec],
                "events_plain": events,
                "events_indexed": events,
            }
        )
    for row_id in range(6, row_count + 1):
        control = controls.get(row_id, {"rank": 0, "tag": "filler", "active": False})
        events = [
            {
                **control,
                "score": 0.0,
                "level_i8": 0,
                "shard_i16": 0,
                "count_i32": 0,
                "ratio_f32": 0.0,
            }
        ]
        rows.append(
            {
                default_pk: row_id,
                default_vec: vector_for_id(row_id),
                "events_plain": events,
                "events_indexed": events,
            }
        )
    return rows


def add_struct_events_field(client, schema, field_name, nullable=False):
    event_schema = client.create_struct_field_schema()
    event_schema.add_field("rank", DataType.INT64)
    event_schema.add_field("tag", DataType.VARCHAR, max_length=64)
    event_schema.add_field("active", DataType.BOOL)
    event_schema.add_field("score", DataType.DOUBLE)
    event_schema.add_field("level_i8", DataType.INT8)
    event_schema.add_field("shard_i16", DataType.INT16)
    event_schema.add_field("count_i32", DataType.INT32)
    event_schema.add_field("ratio_f32", DataType.FLOAT)
    schema.add_field(
        field_name,
        datatype=DataType.ARRAY,
        element_type=DataType.STRUCT,
        struct_schema=event_schema,
        max_capacity=4,
        nullable=nullable,
    )


def assert_struct_array_filter_result(testcase, client, collection_name, case_name, expr, expected_ids):
    if "element_filter(" in expr:
        expected_id_offsets = STRUCT_ARRAY_ELEMENT_FILTER_OFFSET_CASES[case_name]
        assert sorted(row_id for row_id, _ in expected_id_offsets) == sorted(expected_ids)
        assert_query_id_offsets(testcase, client, collection_name, expr, expected_id_offsets, pk_field=default_pk)
    else:
        assert_query_ids(testcase, client, collection_name, expr, expected_ids, pk_field=default_pk)


@pytest.mark.xdist_group("TestFilteringStructArrayL2")
class TestFilteringStructArrayL2(TestMilvusClientV2Base):
    """
    StructArray filtering tests that need a dedicated schema.

    When to add tests here:
    - Test uses ARRAY<STRUCT> expression syntax
    - Test verifies element-level predicates or MATCH-family row predicates

    Fixture: struct_array_collection
    """

    shared_alias = "TestFilteringStructArrayL2"

    @pytest.fixture(scope="class")
    def struct_array_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_struct_array_l2" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        add_struct_events_field(client, schema, "events", nullable=True)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(client, collection_name, data=build_struct_array_rows())
        self.flush(client, collection_name)
        create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, expr, expected_ids",
        [pytest.param(*case, id=case[0]) for case in STRUCT_ARRAY_FILTER_CASES],
    )
    def test_struct_array_filter_family_cases(self, struct_array_collection, case_name, expr, expected_ids):
        """
        target: Verify StructArray element predicates and MATCH-family predicates.
        method: Query a dedicated ARRAY<STRUCT> collection with deterministic rows.
        expected: Returned IDs match the oracle; element_filter keeps one result per matching element.
        """
        client = self._client(alias=self.shared_alias)
        assert_struct_array_filter_result(self, client, struct_array_collection, case_name, expr, expected_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, expr, expected_ids",
        [pytest.param(*case, id=case[0]) for case in STRUCT_ARRAY_SUBFIELD_FILTER_CASES],
    )
    def test_struct_array_subfield_projection_filter_cases(
        self,
        struct_array_collection,
        case_name,
        expr,
        expected_ids,
    ):
        """
        target: Verify StructArray sub-field projection filters.
        method: Query array_length/array_contains/fixed-index/null predicates on StructArray sub-fields.
        expected: Returned row IDs match the nullable StructArray oracle.
        """
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, struct_array_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, expr, expected_ids",
        [pytest.param(*case, id=case[0]) for case in STRUCT_ARRAY_OPERATOR_MATRIX_CASES],
    )
    def test_struct_array_operator_matrix_cases(
        self,
        struct_array_collection,
        case_name,
        expr,
        expected_ids,
    ):
        """
        target: Verify StructArray filtering across scalar sub-field types and operator families.
        method: Query comparison, IN/NOT IN, LIKE, NOT, OR, range, and arithmetic expressions.
        expected: Returned IDs match the deterministic StructArray oracle.
        """
        client = self._client(alias=self.shared_alias)
        assert_struct_array_filter_result(self, client, struct_array_collection, case_name, expr, expected_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, expr, expected_ids",
        [pytest.param(*case, id=case[0]) for case in STRUCT_ARRAY_MATCH_NULL_EMPTY_CASES],
    )
    def test_struct_array_match_null_empty_semantics(
        self,
        struct_array_collection,
        case_name,
        expr,
        expected_ids,
    ):
        """
        target: Verify MATCH-family NULL and empty-array semantics.
        method: Query every MATCH-family row predicate against null, omitted, empty, matching, and non-matching rows.
        expected: NULL/omitted rows are excluded while empty arrays follow zero-element MATCH semantics.
        """
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, struct_array_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_returns_matching_element_offsets_but_match_any_is_row_level(self, struct_array_collection):
        """
        target: Verify element_filter expands matching elements while MATCH_ANY returns matched rows once.
        method: Query the same multi-match StructArray predicate with element_filter and MATCH_ANY.
        expected: element_filter includes duplicate row IDs with offsets; MATCH_ANY has unique row IDs.
        """
        client = self._client(alias=self.shared_alias)
        element_rows = client.query(
            struct_array_collection,
            filter='element_filter(events, $[rank] >= 10 && $[tag] == "qa")',
            output_fields=[default_pk],
            limit=10,
        )
        match_any_rows = client.query(
            struct_array_collection,
            filter='MATCH_ANY(events, $[rank] >= 10 && $[tag] == "qa")',
            output_fields=[default_pk],
            limit=10,
        )

        assert sorted((row[default_pk], row["offset"]) for row in element_rows) == [
            (1, 0),
            (2, 0),
            (2, 1),
            (9, 0),
        ]
        match_any_ids = sorted(row[default_pk] for row in match_any_rows)
        assert match_any_ids == [1, 2, 9]
        assert len(match_any_ids) == len(set(match_any_ids))
        assert all("offset" not in row for row in match_any_rows)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, expr, expected_message",
        [pytest.param(*case, id=case[0]) for case in STRUCT_ARRAY_NEGATIVE_FILTER_CASES],
    )
    def test_struct_array_negative_filter_cases(
        self,
        struct_array_collection,
        case_name,
        expr,
        expected_message,
    ):
        """
        target: Verify invalid StructArray filters fail with meaningful messages.
        method: Query unsupported sub-fields, type mismatches, and invalid MATCH parameters.
        expected: Error message contains a stable diagnostic substring.
        """
        client = self._client(alias=self.shared_alias)
        self.query(
            client,
            struct_array_collection,
            filter=expr,
            output_fields=[default_pk],
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: expected_message},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_array_delete_by_match_any_filter(self):
        """
        target: Verify delete accepts row-level StructArray MATCH_ANY filters.
        method: Delete the row containing an `ops` event from an isolated collection.
        expected: Exactly one row is deleted and the remaining rows no longer match the deleted predicate.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        add_struct_events_field(client, schema, "events", nullable=True)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
        try:
            self.insert(client, collection_name, data=build_struct_array_rows())
            self.flush(client, collection_name)
            create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
            self.load_collection(client, collection_name)
            result = client.delete(
                collection_name,
                filter='MATCH_ANY(events, $[tag] == "ops")',
            )
            assert result["delete_count"] == 1
            assert_query_ids(
                self,
                client,
                collection_name,
                'MATCH_ANY(events, $[tag] == "ops")',
                [],
                pk_field=default_pk,
            )
            remaining_ids = query_ids(self, client, collection_name, "id >= 1", pk_field=default_pk)
            assert remaining_ids == [1, 2, 3, 4, 6, 7, 8, 9]
        finally:
            if client.has_collection(collection_name):
                client.drop_collection(collection_name)


@pytest.mark.xdist_group("TestFilteringStructArrayIndexConsistencyL2")
class TestFilteringStructArrayIndexConsistencyL2(TestMilvusClientV2Base):
    """
    StructArray sub-field index consistency tests.

    When to add tests here:
    - Test compares indexed StructArray sub-field filtering with an unindexed twin field
    - Test needs enough sealed rows to prove scalar sub-field index materialization

    Fixture: struct_array_index_collection
    """

    shared_alias = "TestFilteringStructArrayIndexConsistencyL2"
    index_names = {
        "events_indexed[rank]": "idx_events_rank_stl_sort",
        "events_indexed[tag]": "idx_events_tag_inverted",
        "events_indexed[active]": "idx_events_active_bitmap",
        "events_indexed[score]": "idx_events_score_stl_sort",
    }
    index_types = {
        "events_indexed[rank]": "STL_SORT",
        "events_indexed[tag]": "INVERTED",
        "events_indexed[active]": "BITMAP",
        "events_indexed[score]": "STL_SORT",
    }

    @pytest.fixture(scope="class")
    def struct_array_index_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_struct_array_idx_l2" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        add_struct_events_field(client, schema, "events_plain")
        add_struct_events_field(client, schema, "events_indexed")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(client, collection_name, data=build_struct_array_index_rows())
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            default_vec,
            index_type="FLAT",
            metric_type="COSINE",
        )
        index_params.add_index(
            field_name="events_indexed[rank]",
            index_type="STL_SORT",
            index_name=self.index_names["events_indexed[rank]"],
        )
        index_params.add_index(
            field_name="events_indexed[tag]",
            index_type="INVERTED",
            index_name=self.index_names["events_indexed[tag]"],
        )
        index_params.add_index(
            field_name="events_indexed[active]",
            index_type="BITMAP",
            index_name=self.index_names["events_indexed[active]"],
        )
        index_params.add_index(
            field_name="events_indexed[score]",
            index_type="STL_SORT",
            index_name=self.index_names["events_indexed[score]"],
        )
        self.create_index(client, collection_name, index_params=index_params)
        self.__class__.index_infos = {
            index_name: wait_for_materialized_index(
                self,
                client,
                collection_name,
                index_name,
                expected_total_rows=STRUCT_INDEX_ROW_COUNT,
                expected_field_name=field_name,
                expected_index_type=self.index_types[field_name],
            )
            for field_name, index_name in self.index_names.items()
        }
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_array_subfield_indexes_are_materialized(self, struct_array_index_collection):
        assert set(self.index_infos) == set(self.index_names.values())
        for field_name, index_name in self.index_names.items():
            index_info = self.index_infos[index_name]
            assert index_info["index_name"] == index_name
            assert index_info["field_name"] == field_name
            assert index_info["index_type"] == self.index_types[field_name]
            assert index_info["state"] == IndexState.Finished.name
            assert index_info["indexed_rows"] == index_info["total_rows"] == STRUCT_INDEX_ROW_COUNT
            assert index_info["pending_index_rows"] == 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, plain_expr, indexed_expr, expected_ids",
        [pytest.param(*case, id=case[0]) for case in STRUCT_ARRAY_INDEX_CONSISTENCY_CASES],
    )
    def test_struct_array_subfield_index_consistency_cases(
        self,
        struct_array_index_collection,
        case_name,
        plain_expr,
        indexed_expr,
        expected_ids,
    ):
        """
        target: Verify StructArray sub-field scalar indexes preserve filtering semantics.
        method: Compare unindexed and indexed twin StructArray fields with identical values.
        expected: Plain and indexed filters return the same expected row IDs.
        """
        client = self._client(alias=self.shared_alias)
        plain_ids = query_ids(self, client, struct_array_index_collection, plain_expr, pk_field=default_pk)
        indexed_ids = query_ids(self, client, struct_array_index_collection, indexed_expr, pk_field=default_pk)
        assert plain_ids == expected_ids, f"{case_name} plain got {plain_ids}, expected {expected_ids}"
        assert indexed_ids == expected_ids, f"{case_name} indexed got {indexed_ids}, expected {expected_ids}"
        assert plain_ids == indexed_ids, f"{case_name} plain={plain_ids}, indexed={indexed_ids}"


@pytest.mark.xdist_group("TestFilteringRandomSampleL2")
class TestFilteringRandomSampleL2(TestMilvusClientV2Base):
    """
    Random sampling filter tests that need enough rows for statistical assertions.

    When to add tests here:
    - Test verifies RANDOM_SAMPLE behavior
    - Test uses bounded subset assertions instead of exact random row IDs

    Fixture: random_sample_collection
    """

    shared_alias = "TestFilteringRandomSampleL2"

    @pytest.fixture(scope="class")
    def random_sample_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_random_sample_l2" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
        schema.add_field("bucket", DataType.INT64)
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
            data=[
                {default_pk: i, "bucket": i % 10, default_vec: vector_for_id(i)} for i in range(STRUCT_INDEX_ROW_COUNT)
            ],
        )
        self.flush(client, collection_name)
        create_minimal_vector_index(self, client, collection_name, vector_field=default_vec)
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.mark.tags(CaseLabel.L2)
    def test_random_sample_and_scalar_filter_returns_subset(self, random_sample_collection):
        """
        target: Verify RANDOM_SAMPLE composes with scalar filters without leaking rows.
        method: Compare the base and sampled IDs with the PK-derived bucket oracle.
        expected: Every call returns exactly 150 unique IDs satisfying id % 10 == 3.
        """
        client = self._client(alias=self.shared_alias)
        expected_base_ids = list(range(3, STRUCT_INDEX_ROW_COUNT, 10))
        base_ids = query_ids(self, client, random_sample_collection, "bucket == 3", pk_field=default_pk)
        assert base_ids == expected_base_ids
        sample_sizes = []
        sample_id_sets = []
        expected_base_id_set = set(expected_base_ids)
        for _ in range(5):
            sampled_rows = client.query(
                random_sample_collection,
                filter="bucket == 3 and RANDOM_SAMPLE(0.5)",
                output_fields=[default_pk],
                limit=len(base_ids),
            )
            sampled_ids = sorted(row[default_pk] for row in sampled_rows)
            sample_sizes.append(len(sampled_ids))
            sample_id_sets.append(tuple(sampled_ids))
            assert len(sampled_ids) == len(set(sampled_ids)), f"sample returned duplicate IDs: {sampled_ids}"
            assert all(sampled_id % 10 == 3 for sampled_id in sampled_ids), (
                f"sample leaked IDs outside bucket 3: {sampled_ids}"
            )
            assert set(sampled_ids).issubset(expected_base_id_set)

        assert all(sample_size == 150 for sample_size in sample_sizes), (
            f"RANDOM_SAMPLE(0.5) should return exactly 150 of 300 eligible rows, got {sample_sizes}"
        )
        assert len(set(sample_id_sets)) > 1, "repeated RANDOM_SAMPLE calls returned the same ID set"
