"""
E2E tests for quantified element filtering (MATCH_* family) on struct array
fields, complementing test_milvus_client_struct_array_element_query.py:

- all five MATCH_* operators on an int sub-field of a small, fully-known dataset
  with nullable rows and empty struct arrays (three-valued NULL semantics)
- string predicates on a varchar sub-field: == and like-prefix
- compound predicates across two sub-fields inside one element
- element_filter(sa, p) row-set equivalence with MATCH_ANY(sa, p)
- array_contains / array_contains_any / array_contains_all desugar behavior,
  including contains_all([]) IS-NOT-NULL semantics (vacuous true for real
  arrays, NULL row excluded) and the documented rejection of contains_all with
  a template placeholder list
- `not MATCH_*` with nullable rows (NULL row stays excluded under negation)
- scalar/struct ARRAY NULL parity: the same logical data in both array shapes
  must answer the same MATCH query identically
"""

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType

prefix = "struct_array_match"

PK_FIELD = "id"
VECTOR_FIELD = "normal_vector"
VECTOR_DIM = 32
STRUCT_FIELD = "profile"
INT_SUBFIELD = "p_int"
STR_SUBFIELD = "p_tag"
STRUCT_MAX_CAPACITY = 8
STR_MAX_LENGTH = 64

FLAT_INDEX = {"index_type": "FLAT", "metric_type": "L2"}


def _elem(p_int, p_tag):
    return {INT_SUBFIELD: p_int, STR_SUBFIELD: p_tag}


# Known dataset. Row index == primary key (non-autoID Int64 pk with explicit values).
#   pk 0 -> [(1, "x"), (2, "y")]
#   pk 1 -> [(2, "x"), (2, "x"), (5, "pre_a")]
#   pk 2 -> [(0, "pre_b")]
#   pk 3 -> []            (real empty struct array: vacuous TRUE for ALL/MOST/EXACT(0))
#   pk 4 -> None          (NULL struct array: EXCLUDED by every MATCH_*, also under `not`)
#   pk 5 -> [(9, "z")]
KNOWN_PROFILES = [
    [_elem(1, "x"), _elem(2, "y")],
    [_elem(2, "x"), _elem(2, "x"), _elem(5, "pre_a")],
    [_elem(0, "pre_b")],
    [],
    None,
    [_elem(9, "z")],
]
ALL_PKS = list(range(len(KNOWN_PROFILES)))


def match_pks(match_type, pred, threshold=None, negated=False, profiles=KNOWN_PROFILES):
    """Python oracle for MATCH_* over a struct array, honoring three-valued NULL:
    a NULL struct array row is excluded whether or not the expression is negated;
    an empty [] row is a real zero-element array and is evaluated normally."""
    pks = []
    for pk, profile in enumerate(profiles):
        if profile is None:
            continue
        cnt = sum(1 for e in profile if pred(e))
        total = len(profile)
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


class TestMilvusClientStructArrayMatch(TestMilvusClientV2Base):
    """Quantified MATCH_* / element_filter / array_contains* on a struct array
    field with an int and a varchar sub-field, over nullable rows and empty
    struct arrays."""

    def _build_collection(self, client):
        collection_name = cf.gen_unique_str(prefix)

        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=DataType.FLOAT_VECTOR, dim=VECTOR_DIM)

        struct_schema, _ = self.create_struct_field_schema(client)
        struct_schema.add_field(INT_SUBFIELD, DataType.INT64)
        struct_schema.add_field(STR_SUBFIELD, DataType.VARCHAR, max_length=STR_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        self.create_collection(client, collection_name, schema=schema)

        vectors = cf.gen_vectors(len(KNOWN_PROFILES), VECTOR_DIM)
        rows = [
            {
                PK_FIELD: pk,
                VECTOR_FIELD: list(vectors[pk]),
                STRUCT_FIELD: KNOWN_PROFILES[pk],
            }
            for pk in ALL_PKS
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name=VECTOR_FIELD, **FLAT_INDEX)
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        return collection_name

    def _query_pks(self, client, collection_name, expr, **kwargs):
        res, _ = self.query(client, collection_name, filter=expr, output_fields=[PK_FIELD], **kwargs)
        return sorted(row[PK_FIELD] for row in res)

    def _check_match(self, client, collection_name, expr, expected_pks, **kwargs):
        actual = self._query_pks(client, collection_name, expr, **kwargs)
        assert actual == expected_pks, f"{expr}: expected {expected_pks}, got {actual}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_match_int_operators(self):
        """
        target: test all five MATCH_* operators on $[p_int] with nullable and empty rows
        method: insert the known dataset, run each operator, compare with the python oracle
        expected: returned pks equal the oracle sets; the NULL row (pk 4) never appears
        """
        client = self._client()
        collection_name = self._build_collection(client)

        # MATCH_ANY(profile, $[p_int] > 1) -> pk 0 (2), pk 1 (2,2,5), pk 5 (9)
        expected = match_pks("ANY", lambda e: e[INT_SUBFIELD] > 1)
        assert expected == [0, 1, 5]
        self._check_match(client, collection_name, f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] > 1)", expected)

        # MATCH_ALL(profile, $[p_int] >= 1) -> pk 0, pk 1, pk 3 ([] vacuous), pk 5
        expected = match_pks("ALL", lambda e: e[INT_SUBFIELD] >= 1)
        assert expected == [0, 1, 3, 5]
        self._check_match(client, collection_name, f"MATCH_ALL({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 1)", expected)

        # MATCH_LEAST(profile, $[p_int] == 2, threshold=2) -> pk 1 (two 2s)
        expected = match_pks("LEAST", lambda e: e[INT_SUBFIELD] == 2, threshold=2)
        assert expected == [1]
        self._check_match(
            client, collection_name, f"MATCH_LEAST({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 2, threshold=2)", expected
        )

        # MATCH_MOST(profile, $[p_int] > 1, threshold=1) -> counts 1,3,0,0,-,1 -> pk 0,2,3,5
        expected = match_pks("MOST", lambda e: e[INT_SUBFIELD] > 1, threshold=1)
        assert expected == [0, 2, 3, 5]
        self._check_match(
            client, collection_name, f"MATCH_MOST({STRUCT_FIELD}, $[{INT_SUBFIELD}] > 1, threshold=1)", expected
        )

        # MATCH_EXACT(profile, $[p_int] == 2, threshold=2) -> pk 1
        expected = match_pks("EXACT", lambda e: e[INT_SUBFIELD] == 2, threshold=2)
        assert expected == [1]
        self._check_match(
            client, collection_name, f"MATCH_EXACT({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 2, threshold=2)", expected
        )

        # MATCH_EXACT threshold=0: zero-match rows including the empty array (pk 3),
        # but never the NULL row (pk 4)
        expected = match_pks("EXACT", lambda e: e[INT_SUBFIELD] == 2, threshold=0)
        assert expected == [2, 3, 5]
        self._check_match(
            client, collection_name, f"MATCH_EXACT({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 2, threshold=0)", expected
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_match_varchar_predicates(self):
        """
        target: test string element predicates on $[p_tag]: equality and like-prefix
        method: query MATCH_ANY with $[p_tag] == "x" and $[p_tag] like "pre%"
        expected: returned pks equal the oracle sets
        """
        client = self._client()
        collection_name = self._build_collection(client)

        # MATCH_ANY(profile, $[p_tag] == "x") -> pk 0, pk 1
        expected = match_pks("ANY", lambda e: e[STR_SUBFIELD] == "x")
        assert expected == [0, 1]
        self._check_match(client, collection_name, f'MATCH_ANY({STRUCT_FIELD}, $[{STR_SUBFIELD}] == "x")', expected)

        # MATCH_ANY(profile, $[p_tag] like "pre%") -> pk 1 ("pre_a"), pk 2 ("pre_b")
        expected = match_pks("ANY", lambda e: e[STR_SUBFIELD].startswith("pre"))
        assert expected == [1, 2]
        self._check_match(
            client, collection_name, f'MATCH_ANY({STRUCT_FIELD}, $[{STR_SUBFIELD}] like "pre%")', expected
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_match_compound_subfields(self):
        """
        target: test a compound element predicate across two sub-fields
        method: query MATCH_ANY(profile, $[p_int] > 1 && $[p_tag] == "x"); both conditions
                must hold on the SAME element
        expected: only pk 1 matches (element (2, "x")); pk 0 has 1/"x" and 2/"y" split
                  across two elements and must not match
        """
        client = self._client()
        collection_name = self._build_collection(client)

        expected = match_pks("ANY", lambda e: e[INT_SUBFIELD] > 1 and e[STR_SUBFIELD] == "x")
        assert expected == [1]
        self._check_match(
            client,
            collection_name,
            f'MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] > 1 && $[{STR_SUBFIELD}] == "x")',
            expected,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_element_filter_matches_match_any(self):
        """
        target: verify element_filter(sa, p) selects exactly the MATCH_ANY(sa, p) row set
        method: run both forms for an int and a string predicate; element_filter returns
                one row per matching element, so dedup its pks before comparing
        expected: distinct element_filter pks == MATCH_ANY pks; element row count equals
                  the oracle's total match count
        """
        client = self._client()
        collection_name = self._build_collection(client)

        cases = [
            (f"$[{INT_SUBFIELD}] > 1", lambda e: e[INT_SUBFIELD] > 1),
            (f'$[{STR_SUBFIELD}] == "x"', lambda e: e[STR_SUBFIELD] == "x"),
        ]
        for condition, pred in cases:
            match_any_pks = self._query_pks(client, collection_name, f"MATCH_ANY({STRUCT_FIELD}, {condition})")
            res, _ = self.query(
                client,
                collection_name,
                filter=f"element_filter({STRUCT_FIELD}, {condition})",
                output_fields=[PK_FIELD],
            )
            distinct_pks = sorted({row[PK_FIELD] for row in res})
            assert distinct_pks == match_any_pks, (
                f"element_filter({condition}) pks {distinct_pks} != MATCH_ANY pks {match_any_pks}"
            )
            expected_element_rows = sum(
                sum(1 for e in profile if pred(e)) for profile in KNOWN_PROFILES if profile is not None
            )
            assert len(res) == expected_element_rows, (
                f"element_filter({condition}): expected {expected_element_rows} element rows, got {len(res)}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_contains_desugar(self):
        """
        target: test array_contains/any/all on struct sub-fields (desugared to MATCH_ANY)
        method: compare each contains form with its MATCH_ANY equivalent; check the
                contains_all([]) vacuous-true fast path and the documented rejection of
                contains_all with a template placeholder list
        expected: contains == MATCH_ANY equality-row set; contains_all([]) has
                  IS NOT NULL semantics (every real array incl. empty [], NULL
                  row excluded); template list rejected
        """
        client = self._client()
        collection_name = self._build_collection(client)

        # array_contains(profile[p_int], 2) desugars to MATCH_ANY(profile, $[p_int] == 2)
        expected = match_pks("ANY", lambda e: e[INT_SUBFIELD] == 2)
        assert expected == [0, 1]
        contains_pks = self._query_pks(client, collection_name, f"array_contains({STRUCT_FIELD}[{INT_SUBFIELD}], 2)")
        match_any_pks = self._query_pks(client, collection_name, f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 2)")
        assert contains_pks == match_any_pks == expected

        # array_contains_any(profile[p_tag], ["x", "z"]) -> pk 0, pk 1, pk 5
        expected = match_pks("ANY", lambda e: e[STR_SUBFIELD] in ["x", "z"])
        assert expected == [0, 1, 5]
        self._check_match(
            client, collection_name, f'array_contains_any({STRUCT_FIELD}[{STR_SUBFIELD}], ["x", "z"])', expected
        )

        # array_contains_all(profile[p_int], [1, 2]) -> rows holding both values -> pk 0
        expected = [
            pk
            for pk, profile in enumerate(KNOWN_PROFILES)
            if profile is not None and all(v in [e[INT_SUBFIELD] for e in profile] for v in [1, 2])
        ]
        assert expected == [0]
        self._check_match(
            client, collection_name, f"array_contains_all({STRUCT_FIELD}[{INT_SUBFIELD}], [1, 2])", expected
        )

        # array_contains_all(profile[p_int], []) has IS NOT NULL semantics:
        # vacuously true for every REAL array — including the empty-array row
        # (pk 3) — but UNKNOWN for the NULL row (pk 4), which is excluded
        # (pg: `arr @> '{}'` is strict, NULL @> '{}' yields NULL, not true).
        expected = [pk for pk, profile in enumerate(KNOWN_PROFILES) if profile is not None]
        assert expected == [0, 1, 2, 3, 5]
        self._check_match(client, collection_name, f"array_contains_all({STRUCT_FIELD}[{INT_SUBFIELD}], [])", expected)

        # Documented divergence: contains_all with a template placeholder list is
        # rejected on struct sub-fields (the desugar expands values at parse time).
        error = {ct.err_code: 1100, ct.err_msg: "template placeholder list"}
        self.query(
            client,
            collection_name,
            filter=f"array_contains_all({STRUCT_FIELD}[{INT_SUBFIELD}], {{vals}})",
            filter_params={"vals": [1, 2]},
            output_fields=[PK_FIELD],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_match_not_with_nullable(self):
        """
        target: test `not MATCH_*` three-valued semantics with nullable struct rows
        method: negate MATCH_ANY and MATCH_ALL; the NULL row must stay excluded and the
                empty-array row must follow vacuous evaluation
        expected: returned pks equal the oracle sets computed with negated=True
        """
        client = self._client()
        collection_name = self._build_collection(client)

        # not MATCH_ANY(profile, $[p_int] > 1): real rows with zero matches -> pk 2, pk 3.
        # pk 4 (NULL) is UNKNOWN and excluded even under negation.
        expected = match_pks("ANY", lambda e: e[INT_SUBFIELD] > 1, negated=True)
        assert expected == [2, 3]
        self._check_match(client, collection_name, f"not MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] > 1)", expected)

        # not MATCH_ALL(profile, $[p_int] >= 1): pk 2 only ([] is vacuously ALL-true,
        # so pk 3 is NOT returned; pk 4 stays excluded)
        expected = match_pks("ALL", lambda e: e[INT_SUBFIELD] >= 1, negated=True)
        assert expected == [2]
        self._check_match(client, collection_name, f"not MATCH_ALL({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 1)", expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_match_template(self):
        """
        target: test a template placeholder inside a struct MATCH_* element predicate
        method: query MATCH_ANY(profile, $[p_int] == {v}) with filter_params
        expected: results identical to the literal expression
        """
        client = self._client()
        collection_name = self._build_collection(client)

        expected = match_pks("ANY", lambda e: e[INT_SUBFIELD] == 2)
        assert expected == [0, 1]
        actual = self._query_pks(
            client,
            collection_name,
            f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {{v}})",
            filter_params={"v": 2},
        )
        assert actual == expected, f"template MATCH_ANY: expected {expected}, got {actual}"
        log.info("struct MATCH template placeholder assertion passed")


class TestMilvusClientArrayStructNullMatch(TestMilvusClientV2Base):
    """The same logical data stored in scalar and struct arrays must answer the
    same MATCH query with identical row sets, including NULL and empty []."""

    ARRAY_FIELD = "arr_int"

    # Logical rows (pk = index): a real [1, 2] array, a real empty array, a NULL.
    LOGICAL_ARRAYS = [[1, 2], [], None]

    def _build_collection(self, client):
        collection_name = cf.gen_unique_str(f"{prefix}_cross")

        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=DataType.FLOAT_VECTOR, dim=VECTOR_DIM)
        schema.add_field(
            field_name=self.ARRAY_FIELD,
            datatype=DataType.ARRAY,
            element_type=DataType.INT64,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )
        struct_schema, _ = self.create_struct_field_schema(client)
        struct_schema.add_field(INT_SUBFIELD, DataType.INT64)
        schema.add_field(
            STRUCT_FIELD,
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        self.create_collection(client, collection_name, schema=schema)

        vectors = cf.gen_vectors(len(self.LOGICAL_ARRAYS), VECTOR_DIM)
        rows = []
        for pk, values in enumerate(self.LOGICAL_ARRAYS):
            rows.append(
                {
                    PK_FIELD: pk,
                    VECTOR_FIELD: list(vectors[pk]),
                    self.ARRAY_FIELD: values,
                    STRUCT_FIELD: None if values is None else [{INT_SUBFIELD: v} for v in values],
                }
            )
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name=VECTOR_FIELD, **FLAT_INDEX)
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        return collection_name

    def _query_pks(self, client, collection_name, expr):
        res, _ = self.query(client, collection_name, filter=expr, output_fields=[PK_FIELD])
        return sorted(row[PK_FIELD] for row in res)

    @pytest.mark.tags(CaseLabel.L1)
    def test_array_struct_null_and_empty_parity(self):
        """
        target: verify NULL / empty-[] MATCH semantics are identical across scalar
                and struct ARRAY containers
        method: store the same logical arrays in both containers, run the same
                logical MATCH query against each, compare the two row sets
        expected: both containers return the same pks; the NULL row (pk 2) never
                  appears (even under `not`), the [] row (pk 1) follows vacuous rules
        """
        client = self._client()
        collection_name = self._build_collection(client)

        # (description, scalar-array expr, struct-array expr, expected pks)
        cases = [
            (
                "MATCH_ANY $ > 0: only the real non-empty array",
                f"MATCH_ANY({self.ARRAY_FIELD}, $ > 0)",
                f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] > 0)",
                [0],
            ),
            (
                "MATCH_ALL $ >= 1: [] is vacuously true, NULL is excluded",
                f"MATCH_ALL({self.ARRAY_FIELD}, $ >= 1)",
                f"MATCH_ALL({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 1)",
                [0, 1],
            ),
            (
                "not MATCH_ANY $ > 5: NULL stays excluded under negation",
                f"not MATCH_ANY({self.ARRAY_FIELD}, $ > 5)",
                f"not MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] > 5)",
                [0, 1],
            ),
            (
                "MATCH_MOST threshold=0 $ > 0: only the empty array has zero matches",
                f"MATCH_MOST({self.ARRAY_FIELD}, $ > 0, threshold=0)",
                f"MATCH_MOST({STRUCT_FIELD}, $[{INT_SUBFIELD}] > 0, threshold=0)",
                [1],
            ),
        ]

        for description, array_expr, struct_expr, expected in cases:
            array_pks = self._query_pks(client, collection_name, array_expr)
            struct_pks = self._query_pks(client, collection_name, struct_expr)
            assert array_pks == struct_pks == expected, (
                f"{description}: scalar={array_pks} struct={struct_pks} expected={expected}"
            )
            log.info(f"scalar/struct array parity ok: {description} -> {expected}")
