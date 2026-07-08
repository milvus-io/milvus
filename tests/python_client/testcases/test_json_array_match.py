import pytest
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log

prefix = "json_array_match"
exp_res = "exp_res"

# Field names used by the test collection.
pk_field = ct.default_int64_field_name  # "int64"
vec_field = ct.default_float_vec_field_name  # "float_vector"
json_field = "meta"  # JSON, nullable

# Known dataset. Row index == primary key (non-autoID Int64 pk with explicit values).
# Designed to exercise every three-valued-NULL shape of MATCH_* over JSON paths:
#   - a real numeric array, a real string array, a real nested array (meta["nested"]["b"])
#   - a real empty array []            -> vacuous TRUE for ALL / MOST / EXACT(0)
#   - null value at the path           -> row EXCLUDED (also under `not`)
#   - missing key                      -> row EXCLUDED (also under `not`)
#   - non-array value at the path      -> row EXCLUDED (also under `not`)
#   - whole JSON field NULL            -> row EXCLUDED (also under `not`)
#
#   pk 0 -> {"a": [95, 80],        "s": ["a", "x"],      "nested": {"b": [1, 2]}}
#   pk 1 -> {"a": [40],            "s": ["b"],           "nested": {"b": []}}
#   pk 2 -> {"a": [100, 100, 100], "s": ["x", "x", "y"]}                (no "nested")
#   pk 3 -> {"a": [],              "s": [],              "nested": {"b": []}}
#   pk 4 -> {"a": None,            "s": None}                           (null at path)
#   pk 5 -> {}                                                          (missing keys)
#   pk 6 -> {"a": 42, "s": "scalar", "nested": {"b": 7}}                (non-array at path)
#   pk 7 -> None                                                        (JSON field null)
#   pk 8 -> {"a": [60, 60],        "s": ["c", "x"]}
KNOWN_META = [
    {"a": [95, 80], "s": ["a", "x"], "nested": {"b": [1, 2]}},
    {"a": [40], "s": ["b"], "nested": {"b": []}},
    {"a": [100, 100, 100], "s": ["x", "x", "y"]},
    {"a": [], "s": [], "nested": {"b": []}},
    {"a": None, "s": None},
    {},
    {"a": 42, "s": "scalar", "nested": {"b": 7}},
    None,
    {"a": [60, 60], "s": ["c", "x"]},
]


def _path_array(meta, path):
    """Walk `path` (a tuple of keys) inside one row's JSON value.

    Returns the list found at the path, or None when the row must be EXCLUDED
    by three-valued MATCH semantics: NULL JSON field, missing key, null value
    at the path, or a non-array value at the path. An empty list [] is a real
    array and is returned as-is (it is NOT an excluded shape).
    """
    cur = meta
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur if isinstance(cur, list) else None


def _count_pks(path, pred):
    """Oracle helper: [(pk, match_count, total_count)] over rows with a real array at path."""
    out = []
    for pk, meta in enumerate(KNOWN_META):
        arr = _path_array(meta, path)
        if arr is None:
            continue
        out.append((pk, sum(1 for e in arr if pred(e)), len(arr)))
    return out


def match_pks(match_type, path, pred, threshold=None, negated=False):
    """Python oracle for MATCH_* over a JSON path, honoring three-valued NULL:
    rows without a real array at the path are excluded from the result whether
    or not the expression is negated."""
    pks = []
    for pk, cnt, total in _count_pks(path, pred):
        if match_type == "ANY":
            matched = cnt >= 1
        elif match_type == "ALL":
            matched = cnt == total
        elif match_type == "LEAST":
            matched = cnt >= threshold
        elif match_type == "MOST":
            matched = cnt <= threshold
        elif match_type == "EXACT":
            matched = cnt == threshold
        else:
            raise ValueError(match_type)
        if matched != negated:
            pks.append(pk)
    return pks


class TestJsonArrayMatch(TestcaseBase):
    """
    Test quantified element filtering on JSON path arrays:
        MATCH_ANY / MATCH_ALL / MATCH_LEAST / MATCH_MOST / MATCH_EXACT
    with "$" referring to the JSON array element itself.

    JSON MATCH is brute-force only (no scalar index accelerates it), so unlike
    test_scalar_array_match.py there is no with/without-index parametrization.
    element_filter over JSON is intentionally rejected by the parser (JSON
    arrays have no element offsets) and is covered as an error case.
    """

    def _build_collection(self):
        """Create a collection with int64 pk, float_vector and a nullable JSON
        field, insert the known dataset, flush, index and load."""
        fields = [
            cf.gen_int64_field(name=pk_field, is_primary=True),
            cf.gen_float_vec_field(name=vec_field, dim=ct.default_dim),
            cf.gen_json_field(name=json_field, nullable=True),
        ]
        schema = cf.gen_collection_schema(fields=fields, primary_field=pk_field, auto_id=False)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

        vectors = cf.gen_vectors(len(KNOWN_META), ct.default_dim)
        rows = []
        for i in range(len(KNOWN_META)):
            rows.append(
                {
                    pk_field: i,
                    vec_field: list(vectors[i]),
                    json_field: KNOWN_META[i],
                }
            )
        collection_w.insert(data=rows)
        collection_w.flush()

        collection_w.create_index(vec_field, index_params=ct.default_flat_index)
        collection_w.load()
        return collection_w

    @staticmethod
    def _exp_pks(pks):
        """Build the expected query result list (only the pk field per row)."""
        return [{pk_field: pk} for pk in pks]

    def _check_match(self, collection_w, expr, expected_pks, **query_kwargs):
        """Query `expr` and assert the returned pk set equals `expected_pks`."""
        collection_w.query(
            expr=expr,
            output_fields=[pk_field],
            check_task=CheckTasks.check_query_results,
            check_items={exp_res: self._exp_pks(expected_pks), "pk_name": pk_field},
            **query_kwargs,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_array_match_int_operators(self):
        """
        target: test all five MATCH_* operators over a numeric JSON path array
        method: insert known JSON rows, query each operator, compare with the python oracle
        expected: returned pks equal the oracle sets; null/missing/non-array rows excluded
        """
        collection_w = self._build_collection()
        path = ("a",)

        # MATCH_ANY(meta["a"], $ > 90) -> pk 0 [95,80], pk 2 [100,100,100]
        expected = match_pks("ANY", path, lambda e: isinstance(e, (int, float)) and e > 90)
        assert expected == [0, 2]
        self._check_match(collection_w, f'MATCH_ANY({json_field}["a"], $ > 90)', expected)

        # MATCH_ALL(meta["a"], $ >= 60) -> pk 0, pk 2, pk 3 ([] vacuous), pk 8
        expected = match_pks("ALL", path, lambda e: isinstance(e, (int, float)) and e >= 60)
        assert expected == [0, 2, 3, 8]
        self._check_match(collection_w, f'MATCH_ALL({json_field}["a"], $ >= 60)', expected)

        # MATCH_LEAST(meta["a"], $ == 100, threshold=2) -> pk 2 only
        expected = match_pks("LEAST", path, lambda e: e == 100, threshold=2)
        assert expected == [2]
        self._check_match(collection_w, f'MATCH_LEAST({json_field}["a"], $ == 100, threshold=2)', expected)

        # MATCH_MOST(meta["a"], $ < 50, threshold=1) -> every real-array row
        # (counts: pk0=0, pk1=1, pk2=0, pk3=0, pk8=0); excluded rows 4,5,6,7 stay out
        expected = match_pks("MOST", path, lambda e: isinstance(e, (int, float)) and e < 50, threshold=1)
        assert expected == [0, 1, 2, 3, 8]
        self._check_match(collection_w, f'MATCH_MOST({json_field}["a"], $ < 50, threshold=1)', expected)

        # MATCH_EXACT(meta["a"], $ == 100, threshold=3) -> pk 2 only
        expected = match_pks("EXACT", path, lambda e: e == 100, threshold=3)
        assert expected == [2]
        self._check_match(collection_w, f'MATCH_EXACT({json_field}["a"], $ == 100, threshold=3)', expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_array_match_in_and_compound(self):
        """
        target: test `$ in [...]` and compound `$ > x && $ < y` element predicates
        method: query MATCH_ANY with an in-list and with a compound range predicate
        expected: returned pks equal the oracle sets
        """
        collection_w = self._build_collection()
        path = ("a",)

        # MATCH_ANY(meta["a"], $ in [40, 60]) -> pk 1 (40), pk 8 (60)
        expected = match_pks("ANY", path, lambda e: e in [40, 60])
        assert expected == [1, 8]
        self._check_match(collection_w, f'MATCH_ANY({json_field}["a"], $ in [40, 60])', expected)

        # MATCH_ANY(meta["a"], $ > 50 && $ < 90) -> pk 0 (80), pk 8 (60)
        expected = match_pks("ANY", path, lambda e: isinstance(e, (int, float)) and 50 < e < 90)
        assert expected == [0, 8]
        self._check_match(collection_w, f'MATCH_ANY({json_field}["a"], $ > 50 && $ < 90)', expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_array_match_not_any_3vl(self):
        """
        target: test three-valued NULL semantics of `not MATCH_ANY` over a JSON path
        method: negate a MATCH_ANY query and check null/missing/non-array rows stay excluded
        expected: only real-array rows whose match count is 0 are returned
        """
        collection_w = self._build_collection()
        path = ("a",)

        # not MATCH_ANY(meta["a"], $ > 90): real-array rows with no element > 90
        # -> pk 1 [40], pk 3 [], pk 8 [60,60].
        # pks 4 (null value), 5 (missing key), 6 (non-array), 7 (NULL field) must stay
        # excluded even under negation (UNKNOWN, not FALSE).
        expected = match_pks("ANY", path, lambda e: isinstance(e, (int, float)) and e > 90, negated=True)
        assert expected == [1, 3, 8]
        for excluded_pk in (4, 5, 6, 7):
            assert excluded_pk not in expected
        self._check_match(collection_w, f'not MATCH_ANY({json_field}["a"], $ > 90)', expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_array_match_threshold_edges(self):
        """
        target: test MATCH_LEAST/MATCH_MOST/MATCH_EXACT threshold edges over JSON arrays
        method: query thresholds at 0 (MOST/EXACT only: LEAST requires threshold >= 1),
                len and len+1 around pk 2's [100, 100, 100]
        expected: returned pks equal the oracle sets; [] satisfies MOST(0)/EXACT(0)
        """
        collection_w = self._build_collection()
        path = ("a",)
        pred = lambda e: e == 100  # noqa: E731

        # LEAST threshold=1 (minimum legal threshold; ==0 is rejected by the parser)
        expected = match_pks("LEAST", path, pred, threshold=1)
        assert expected == [2]
        self._check_match(collection_w, f'MATCH_LEAST({json_field}["a"], $ == 100, threshold=1)', expected)

        # LEAST threshold=len(3) still matches, len+1(4) matches nothing
        expected = match_pks("LEAST", path, pred, threshold=3)
        assert expected == [2]
        self._check_match(collection_w, f'MATCH_LEAST({json_field}["a"], $ == 100, threshold=3)', expected)
        expected = match_pks("LEAST", path, pred, threshold=4)
        assert expected == []
        res, _ = collection_w.query(
            expr=f'MATCH_LEAST({json_field}["a"], $ == 100, threshold=4)',
            output_fields=[pk_field],
        )
        assert res == [], f"expected empty result, got: {res}"

        # EXACT threshold=0: real-array rows with zero matches, including [] (pk 3)
        expected = match_pks("EXACT", path, pred, threshold=0)
        assert expected == [0, 1, 3, 8]
        self._check_match(collection_w, f'MATCH_EXACT({json_field}["a"], $ == 100, threshold=0)', expected)

        # EXACT threshold=len(3) -> pk 2; threshold=len+1(4) -> nothing
        expected = match_pks("EXACT", path, pred, threshold=3)
        assert expected == [2]
        self._check_match(collection_w, f'MATCH_EXACT({json_field}["a"], $ == 100, threshold=3)', expected)
        res, _ = collection_w.query(
            expr=f'MATCH_EXACT({json_field}["a"], $ == 100, threshold=4)',
            output_fields=[pk_field],
        )
        assert res == [], f"expected empty result, got: {res}"

        # MOST threshold=0: real-array rows with zero matches (same set as EXACT 0)
        expected = match_pks("MOST", path, pred, threshold=0)
        assert expected == [0, 1, 3, 8]
        self._check_match(collection_w, f'MATCH_MOST({json_field}["a"], $ == 100, threshold=0)', expected)

        # MOST threshold=len(3): every real-array row qualifies (count <= 3 always here)
        expected = match_pks("MOST", path, pred, threshold=3)
        assert expected == [0, 1, 2, 3, 8]
        self._check_match(collection_w, f'MATCH_MOST({json_field}["a"], $ == 100, threshold=3)', expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_array_match_nested_path(self):
        """
        target: test MATCH_* over a nested JSON path meta["nested"]["b"]
        method: query the two-level path; rows without a real array there are excluded
        expected: returned pks equal the oracle sets
        """
        collection_w = self._build_collection()
        path = ("nested", "b")

        # MATCH_ANY(meta["nested"]["b"], $ > 1) -> pk 0 ([1,2]); pk 6 has non-array 7,
        # pks 2/4/5/7/8 miss the path entirely
        expected = match_pks("ANY", path, lambda e: isinstance(e, (int, float)) and e > 1)
        assert expected == [0]
        self._check_match(collection_w, f'MATCH_ANY({json_field}["nested"]["b"], $ > 1)', expected)

        # MATCH_ALL(meta["nested"]["b"], $ > 0) -> pk 0 ([1,2]), pk 1 ([] vacuous),
        # pk 3 ([] vacuous)
        expected = match_pks("ALL", path, lambda e: isinstance(e, (int, float)) and e > 0)
        assert expected == [0, 1, 3]
        self._check_match(collection_w, f'MATCH_ALL({json_field}["nested"]["b"], $ > 0)', expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_array_match_string_elements(self):
        """
        target: test string element predicates (== and like-prefix) over a JSON path array
        method: query MATCH_ANY(meta["s"], $ == "x") and MATCH_ANY(meta["s"], $ like "x%")
        expected: returned pks equal the oracle sets; pk 6's non-array "scalar" is excluded
        """
        collection_w = self._build_collection()
        path = ("s",)

        # MATCH_ANY(meta["s"], $ == "x") -> pk 0, pk 2, pk 8
        expected = match_pks("ANY", path, lambda e: e == "x")
        assert expected == [0, 2, 8]
        self._check_match(collection_w, f'MATCH_ANY({json_field}["s"], $ == "x")', expected)

        # MATCH_ANY(meta["s"], $ like "x%") -> same rows for this dataset
        expected = match_pks("ANY", path, lambda e: isinstance(e, str) and e.startswith("x"))
        assert expected == [0, 2, 8]
        self._check_match(collection_w, f'MATCH_ANY({json_field}["s"], $ like "x%")', expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_element_filter_rejected(self):
        """
        target: verify element_filter over JSON paths is rejected (documented asymmetry)
        method: query element_filter(meta["a"], $ > 1); JSON arrays have no element
                offsets, users must go through MATCH_ANY instead
        expected: query fails to parse the expression
        """
        collection_w = self._build_collection()

        error = {ct.err_code: 1100, ct.err_msg: "cannot parse expression"}
        collection_w.query(
            expr=f'element_filter({json_field}["a"], $ > 1)',
            output_fields=[pk_field],
            check_task=CheckTasks.err_res,
            check_items=error,
        )
        # the MATCH_ANY equivalent stays supported
        expected = match_pks("ANY", ("a",), lambda e: isinstance(e, (int, float)) and e > 1)
        assert expected == [0, 1, 2, 8]
        self._check_match(collection_w, f'MATCH_ANY({json_field}["a"], $ > 1)', expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_array_match_template(self):
        """
        target: test template placeholders inside MATCH_* element predicates
        method: query MATCH_ANY with {v} / {vals} placeholders filled via expr_params
        expected: results identical to the literal expressions
        """
        collection_w = self._build_collection()
        path = ("a",)

        # MATCH_ANY(meta["a"], $ > {v}) with v=90 -> pk 0, pk 2
        expected = match_pks("ANY", path, lambda e: isinstance(e, (int, float)) and e > 90)
        assert expected == [0, 2]
        self._check_match(
            collection_w,
            f'MATCH_ANY({json_field}["a"], $ > {{v}})',
            expected,
            expr_params={"v": 90},
        )

        # MATCH_ANY(meta["a"], $ in {vals}) with vals=[40, 60] -> pk 1, pk 8
        expected = match_pks("ANY", path, lambda e: e in [40, 60])
        assert expected == [1, 8]
        self._check_match(
            collection_w,
            f'MATCH_ANY({json_field}["a"], $ in {{vals}})',
            expected,
            expr_params={"vals": [40, 60]},
        )

        log.info("JSON MATCH template placeholder assertions passed")
