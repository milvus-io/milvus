"""
JSON filtering regression tests for SQL-style UNKNOWN semantics.

These cases use fixed expected IDs instead of only comparing raw and indexed
results, because raw and index paths can otherwise be wrong in the same way.
"""

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType

default_dim = 8
default_pk = "id"
default_vec = "vector"
json_field = "json_field"
array_field = "array_field"
indexed_total_rows = 3000


def _vector(i):
    value = float((i % 7) + 1) / 10.0
    return [value] * default_dim


def _base_rows():
    payloads = [
        None,
        {},
        {"a": None},
        {"a": "bad"},
        {"a": 2},
        {"a": 3},
        {"arr": []},
        {"arr": [1, 2]},
        {"arr": "bad"},
        {"nested": {"age": 30}},
        {"nested": {}},
        {"s": "abc"},
        {"s": "def"},
        {"s": 123},
        {"s": None},
        {"a": [2]},
        {"nested": {"age": 40}},
        {"nested": {"age": "bad"}},
        {"nested": None},
        {"b": True},
        {"b": False},
        {"b": None},
        {"b": "true"},
        {"f": 1.5},
        {"f": -2.25},
        {"f": 0.0},
        {"f": "bad"},
        {"path_arr": [3, "bad", None]},
        {"op": 10},
        {"op": 20},
        {"op": None},
        {"op": "bad"},
        {"mixed": [1, "bad", None]},
        {"path_arr": [1, 2]},
        {"path_arr": []},
        {"path_arr": "bad"},
    ]
    arrays = [
        None,
        [],
        None,
        None,
        [2],
        [3],
        [],
        None,
        None,
        None,
        [],
        None,
        None,
        None,
        [],
        None,
        None,
        None,
        None,
    ]
    arrays.extend([None] * (len(payloads) - len(arrays)))
    return [
        {default_pk: i, default_vec: _vector(i), json_field: payload, array_field: arrays[i]}
        for i, payload in enumerate(payloads)
    ]


def _rows(total=20):
    rows = _base_rows()
    for i in range(len(rows), total):
        rows.append({default_pk: i, default_vec: _vector(i), json_field: {"pad": i}, array_field: None})
    return rows


def _hit_id(hit):
    if default_pk in hit:
        return hit[default_pk]
    entity = hit.get("entity", {})
    if default_pk in entity:
        return entity[default_pk]
    return hit["id"]


class JsonFilteringUnknownMixin:
    def _collection_name(self, *parts):
        name = "_".join([self.__class__.__name__[:40], *parts])
        return cf.gen_unique_str(name)

    def _create_schema(self, client):
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(json_field, DataType.JSON, nullable=True)
        schema.add_field(array_field, DataType.ARRAY, element_type=DataType.INT64, max_capacity=8, nullable=True)
        return schema

    def _create_collection(self, client, collection_name, segment_mode, total_rows=20):
        schema = self._create_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        if segment_mode == "growing":
            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
            self.create_index(client, collection_name, index_params=index_params)
            self.load_collection(client, collection_name)
            self.insert(client, collection_name, _rows(total_rows))
            return

        self.insert(client, collection_name, _rows(total_rows))
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

    def _query_ids(self, client, collection_name, expr):
        res = self.query(
            client,
            collection_name,
            filter=expr,
            output_fields=[default_pk],
            consistency_level="Strong",
            check_task=CheckTasks.check_nothing,
        )[0]
        assert not hasattr(res, "message"), f"query failed for {expr}: {getattr(res, 'message', res)}"
        return sorted(row[default_pk] for row in res)

    def _assert_query_cases(self, client, collection_name, cases):
        for expr, expected in cases:
            assert self._query_ids(client, collection_name, expr) == expected, expr


class TestJsonFilteringUnknownSemantics(JsonFilteringUnknownMixin, TestMilvusClientV2Base):
    """
    JSON UNKNOWN semantics on raw filtering paths.

    When to add tests here:
    - The test has exact expected IDs.
    - The test does not require a JSON scalar/path index.
    - The test should run against both growing and sealed segment states.
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_numeric_missing_path_unknown_query(self, segment_mode):
        """
        target: missing/null/cast-fail JSON scalar values are UNKNOWN
        method: run positive and negative numeric JSON path predicates
        expected: only comparable known values can match, even for !=/not in/not
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "numeric")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'{json_field}["a"] == 2', [4]),
                (f'{json_field}["a"] > 2', [5]),
                (f'{json_field}["a"] in [2, 3]', [4, 5]),
                (f'{json_field}["a"] != 2', [5]),
                (f'{json_field}["a"] not in [2]', [5]),
                (f'not ({json_field}["a"] > 2)', [4]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_json_arithmetic_missing_operand_unknown_query(self, segment_mode):
        """
        target: JSON arithmetic path extraction failures are UNKNOWN
        method: run arithmetic predicates over missing/null/cast-fail JSON paths
        expected: UNKNOWN rows do not match != or outer NOT
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "arith")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'{json_field}["a"] + 1 != 3', [5]),
                (f'not ({json_field}["a"] + 1 > 3)', [4]),
                (f'{json_field}["a"] * 2 == 4', [4]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_json_array_missing_path_unknown_query(self, segment_mode):
        """
        target: missing/non-array JSON array paths are UNKNOWN
        method: run array_length and json_contains predicates
        expected: only real arrays participate in array predicates
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "array")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'array_length({json_field}["arr"]) == 0', [6]),
                (f'array_length({json_field}["arr"]) != 0', [7]),
                (f'not (array_length({json_field}["arr"]) > 0)', [6]),
                (f'json_contains({json_field}["arr"], 1)', [7]),
                (f'json_contains_any({json_field}["arr"], [1, 9])', [7]),
                (f'json_contains_all({json_field}["arr"], [1, 2])', [7]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_varchar_missing_path_unknown_query(self, segment_mode):
        """
        target: VARCHAR JSON path missing/null/cast-fail values are UNKNOWN
        method: run equality and negative term predicates over string path
        expected: only comparable known string values can match
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "varchar")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'{json_field}["s"] == "abc"', [11]),
                (f'{json_field}["s"] != "abc"', [12]),
                (f'{json_field}["s"] in ["abc", "def"]', [11, 12]),
                (f'{json_field}["s"] not in ["abc"]', [12]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_nested_missing_leaf_unknown_query(self, segment_mode):
        """
        target: missing nested JSON leaf values are UNKNOWN
        method: run comparison and NOT predicates over a nested JSON path
        expected: only comparable known nested values can match
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "nested")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'{json_field}["nested"]["age"] == 30', [9]),
                (f'{json_field}["nested"]["age"] > 30', [16]),
                (f'{json_field}["nested"]["age"] != 30', [16]),
                (f'not ({json_field}["nested"]["age"] > 30)', [9]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_array_subscript_unknown_query(self, segment_mode):
        """
        target: ARRAY subscript missing/out-of-range values are UNKNOWN
        method: run direct array subscript predicates and outer NOT over empty/null arrays
        expected: only rows with existing comparable array elements match
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "array_subscript")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f"{array_field}[0] == 2", [4]),
                (f"{array_field}[0] > 2", [5]),
                (f"{array_field}[0] != 2", [5]),
                (f"not ({array_field}[0] > 2)", [4]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_contradiction_rewrite_keeps_unknown_query(self, segment_mode):
        """
        target: contradiction rewrites do not turn UNKNOWN JSON/array extraction into TRUE under outer NOT
        method: run impossible ranges and their outer NOT over JSON paths and array subscripts
        expected: missing/null/type-mismatch rows remain UNKNOWN and do not match
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "rewrite")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'{json_field}["a"] > 100 and {json_field}["a"] < 50', []),
                (f'not ({json_field}["a"] > 100 and {json_field}["a"] < 50)', [4, 5]),
                (f"{array_field}[0] > 100 and {array_field}[0] < 50", []),
                (f"not ({array_field}[0] > 100 and {array_field}[0] < 50)", [4, 5]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_l2_boolean_and_fractional_values_query(self, segment_mode):
        """
        target: JSON boolean and fractional double values keep UNKNOWN semantics
        method: run bool and fractional numeric predicates over missing/null/type-mismatch JSON paths
        expected: only rows with comparable bool/double values match
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "l2_bool_float")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'{json_field}["b"] == true', [19]),
                (f'{json_field}["b"] == false', [20]),
                (f'{json_field}["b"] != true', [20]),
                (f'not ({json_field}["b"] == true)', [20]),
                (f'{json_field}["f"] > 1.0', [23]),
                (f'{json_field}["f"] < 0', [24]),
                (f'{json_field}["f"] >= 0', [23, 25]),
                (f'not ({json_field}["f"] <= 0)', [23]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_l2_operator_matrix_query(self, segment_mode):
        """
        target: additional comparison and logical operators preserve UNKNOWN semantics
        method: run <, <=, >=, or, and, and negated equality over a numeric JSON path
        expected: null/missing/type-mismatch rows do not match positive or outer NOT predicates
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "l2_ops")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'{json_field}["op"] < 20', [28]),
                (f'{json_field}["op"] <= 10', [28]),
                (f'{json_field}["op"] >= 20', [29]),
                (f'not ({json_field}["op"] == 10)', [29]),
                (f'{json_field}["op"] == 10 or {json_field}["op"] == 20', [28, 29]),
                (f'{json_field}["op"] > 5 and {json_field}["op"] < 15', [28]),
                (f'not ({json_field}["op"] < 20 or {json_field}["op"] > 30)', [29]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_l2_json_path_array_subscript_query(self, segment_mode):
        """
        target: JSON path array subscripts keep UNKNOWN semantics
        method: run numeric and string comparisons over JSON array elements
        expected: empty arrays, non-arrays, missing paths, and type mismatches do not match
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "l2_json_array_subscript")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'{json_field}["path_arr"][0] == 1', [33]),
                (f'{json_field}["path_arr"][0] > 1', [27]),
                (f'{json_field}["path_arr"][1] == 2', [33]),
                (f'{json_field}["path_arr"][1] == "bad"', [27]),
                (f'not ({json_field}["path_arr"][0] > 1)', [33]),
            ],
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("segment_mode", ["growing", "sealed"])
    def test_l2_mixed_array_contains_query(self, segment_mode):
        """
        target: JSON array predicates handle mixed-type arrays without matching missing paths
        method: run contains predicates over an array with numeric, string, and null elements
        expected: only the row containing the requested numeric element matches
        """
        client = self._client()
        collection_name = self._collection_name(segment_mode, "l2_mixed_array")
        self._create_collection(client, collection_name, segment_mode)

        self._assert_query_cases(
            client,
            collection_name,
            [
                (f'json_contains({json_field}["mixed"], 1)', [32]),
                (f'json_contains_any({json_field}["mixed"], [1, 9])', [32]),
                (f'json_contains_all({json_field}["mixed"], [1])', [32]),
            ],
        )


class TestJsonFilteringIndexedUnknownSemantics(JsonFilteringUnknownMixin, TestMilvusClientV2Base):
    """
    JSON UNKNOWN semantics across no-index, JSON path index, query, and search.

    When to add tests here:
    - The test requires JSON path index or JSON flat index coverage.
    - The first phase must cover DOUBLE and VARCHAR path-index cast types.
    - ARRAY_* cast types are L2 and should be added with dedicated array-path
      UNKNOWN assertions.
    """

    def _create_sealed_collection(self, client, collection_name, json_indexes=None, total_rows=indexed_total_rows):
        schema = self._create_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.insert(client, collection_name, _rows(total_rows))
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        for index in json_indexes or []:
            index_params.add_index(field_name=json_field, **index)
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

    def _search_ids(self, client, collection_name, expr, limit=indexed_total_rows):
        res = self.search(
            client,
            collection_name,
            data=[_vector(0)],
            anns_field=default_vec,
            filter=expr,
            limit=limit,
            output_fields=[default_pk],
            search_params={"metric_type": "COSINE", "params": {}},
            consistency_level="Strong",
            check_task=CheckTasks.check_nothing,
        )[0]
        assert not hasattr(res, "message"), f"search failed for {expr}: {getattr(res, 'message', res)}"
        return sorted(_hit_id(hit) for hit in res[0])

    def _assert_query_and_search_cases(self, client, collection_name, cases):
        for expr, expected in cases:
            assert self._query_ids(client, collection_name, expr) == expected, expr
            assert self._search_ids(client, collection_name, expr) == expected, expr

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "index_name,json_indexes,cases",
        [
            (
                "inverted_double",
                [
                    {
                        "index_name": "idx_json_a_inverted_double",
                        "index_type": "INVERTED",
                        "params": {"json_cast_type": "DOUBLE", "json_path": f"{json_field}['a']"},
                    }
                ],
                [
                    (f'{json_field}["a"] == 2', [4]),
                    (f'{json_field}["a"] > 2', [5]),
                    (f'{json_field}["a"] != 2', [5]),
                    (f'{json_field}["a"] not in [2]', [5]),
                    (f'not ({json_field}["a"] > 2)', [4]),
                ],
            ),
            (
                "stl_sort_double",
                [
                    {
                        "index_name": "idx_json_a_stl_sort_double",
                        "index_type": "STL_SORT",
                        "params": {"json_cast_type": "DOUBLE", "json_path": f"{json_field}['a']"},
                    }
                ],
                [
                    (f'{json_field}["a"] == 2', [4]),
                    (f'{json_field}["a"] > 2', [5]),
                    (f'{json_field}["a"] != 2', [5]),
                    (f'{json_field}["a"] not in [2]', [5]),
                    (f'not ({json_field}["a"] > 2)', [4]),
                ],
            ),
            (
                "inverted_varchar",
                [
                    {
                        "index_name": "idx_json_s_inverted_varchar",
                        "index_type": "INVERTED",
                        "params": {"json_cast_type": "VARCHAR", "json_path": f"{json_field}['s']"},
                    }
                ],
                [
                    (f'{json_field}["s"] == "abc"', [11]),
                    (f'{json_field}["s"] != "abc"', [12]),
                    (f'{json_field}["s"] in ["abc", "def"]', [11, 12]),
                    (f'{json_field}["s"] not in ["abc"]', [12]),
                ],
            ),
        ],
    )
    def test_json_path_index_unknown_semantics_query(self, index_name, json_indexes, cases):
        """
        target: JSON path indexes preserve UNKNOWN semantics
        method: compare no-index and indexed sealed collections with 3000 rows against fixed expected IDs
        expected: indexed and no-index results both match expected IDs
        """
        client = self._client()
        raw_collection = self._collection_name(index_name, "raw")
        indexed_collection = self._collection_name(index_name, "idx")
        self._create_sealed_collection(client, raw_collection)
        self._create_sealed_collection(client, indexed_collection, json_indexes=json_indexes)

        self._assert_query_and_search_cases(client, raw_collection, cases)
        self._assert_query_and_search_cases(client, indexed_collection, cases)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_flat_index_unknown_semantics_query(self):
        """
        target: JSON flat index preserves UNKNOWN semantics for comparable values
        method: compare no-index and json_cast_type=json flat-index collections with 3000 rows
        expected: flat-index and no-index results both match fixed expected IDs
        """
        client = self._client()
        raw_collection = self._collection_name("flat_raw")
        indexed_collection = self._collection_name("flat_idx")
        json_indexes = [
            {
                "index_name": "idx_json_flat",
                "index_type": "INVERTED",
                "params": {"json_cast_type": "json"},
            }
        ]
        cases = [
            (f'{json_field}["a"] != 2', [5]),
            (f'{json_field}["a"] not in [2]', [5]),
            (f'{json_field}["s"] != "abc"', [12]),
            (f'{json_field}["s"] not in ["abc"]', [12]),
            (f'{json_field}["nested"]["age"] != 30', [16]),
        ]
        self._create_sealed_collection(client, raw_collection)
        self._create_sealed_collection(client, indexed_collection, json_indexes=json_indexes)

        self._assert_query_and_search_cases(client, raw_collection, cases)
        self._assert_query_and_search_cases(client, indexed_collection, cases)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="Known JSON flat index UNKNOWN mismatch: https://github.com/milvus-io/milvus/issues/51193")
    def test_json_flat_index_array_scalar_comparison_unknown_semantics_query(self):
        """
        target: JSON flat index should preserve UNKNOWN for array-vs-scalar comparisons
        method: compare no-index and json_cast_type=json flat-index collections with 3000 rows
        expected: {"a": [2]} is UNKNOWN for scalar comparison and does not match outer NOT
        """
        client = self._client()
        raw_collection = self._collection_name("flat_array_raw")
        indexed_collection = self._collection_name("flat_array_idx")
        json_indexes = [
            {
                "index_name": "idx_json_flat",
                "index_type": "INVERTED",
                "params": {"json_cast_type": "json"},
            }
        ]
        cases = [(f'not ({json_field}["a"] > 2)', [4])]
        self._create_sealed_collection(client, raw_collection)
        self._create_sealed_collection(client, indexed_collection, json_indexes=json_indexes)

        self._assert_query_cases(client, raw_collection, cases)
        self._assert_query_cases(client, indexed_collection, cases)

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_and_search_filter_consistency(self):
        """
        target: query and search filter consumers exclude UNKNOWN rows
        method: run representative JSON filters through both APIs on a 3000-row sealed collection
        expected: both APIs return the same fixed expected IDs
        """
        client = self._client()
        collection_name = self._collection_name("search_query")
        self._create_sealed_collection(client, collection_name)

        cases = [
            (f'{json_field}["a"] != 2', [5]),
            (f'{json_field}["a"] not in [2]', [5]),
            (f'not ({json_field}["a"] > 2)', [4]),
            (f'array_length({json_field}["arr"]) == 0', [6]),
            (f'json_contains({json_field}["arr"], 1)', [7]),
        ]
        for expr, expected in cases:
            assert self._query_ids(client, collection_name, expr) == expected, expr
            assert self._search_ids(client, collection_name, expr) == expected, expr

    @pytest.mark.tags(CaseLabel.L2)
    def test_l2_json_array_path_index_unknown_semantics_query(self):
        """
        target: ARRAY_DOUBLE JSON path indexes preserve UNKNOWN semantics
        method: compare no-index and ARRAY_DOUBLE indexed sealed collections with fixed expected IDs
        expected: query and search results match expected IDs on both raw and indexed collections
        """
        client = self._client()
        raw_collection = self._collection_name("l2_array_path_raw")
        indexed_collection = self._collection_name("l2_array_path_idx")
        json_indexes = [
            {
                "index_name": "idx_json_arr_array_double",
                "index_type": "INVERTED",
                "params": {"json_cast_type": "ARRAY_DOUBLE", "json_path": f"{json_field}['arr']"},
            }
        ]
        cases = [
            (f'json_contains({json_field}["arr"], 1)', [7]),
            (f'json_contains_any({json_field}["arr"], [1, 9])', [7]),
            (f'json_contains_all({json_field}["arr"], [1, 2])', [7]),
        ]
        self._create_sealed_collection(client, raw_collection)
        self._create_sealed_collection(client, indexed_collection, json_indexes=json_indexes)

        self._assert_query_and_search_cases(client, raw_collection, cases)
        self._assert_query_and_search_cases(client, indexed_collection, cases)
