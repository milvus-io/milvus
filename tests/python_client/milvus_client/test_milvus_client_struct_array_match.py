"""
E2E tests for quantified element filtering (MATCH_* family) on struct array
fields, complementing test_milvus_client_struct_array_element_query.py:

- all five MATCH_* operators on an int sub-field of a small, fully-known dataset
  with nullable rows and empty struct arrays (three-valued NULL semantics)
- string predicates on a varchar sub-field: == and like-prefix
- compound predicates across two sub-fields inside one element
- element_filter(sa, p) row-set equivalence with MATCH_ANY(sa, p)
- `not MATCH_*` with nullable rows (NULL row stays excluded under negation)
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
    """Quantified MATCH_* / element_filter on a struct array field with an int
    and a varchar sub-field, over nullable rows and empty struct arrays."""

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
