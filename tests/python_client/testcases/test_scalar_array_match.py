import pytest
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType
from utils.util_log import test_log as log

prefix = "scalar_array_match"
exp_res = "exp_res"

# Field names used by the test collection.
pk_field = ct.default_int64_field_name  # "int64"
vec_field = ct.default_float_vec_field_name  # "float_vector"
scores_field = "scores"  # Array<Int64>
tags_field = "tags"  # Array<VarChar>

# Known dataset. Row index == primary key (non-autoID Int64 pk with explicit values).
#   pk 0 -> scores [95, 80]          tags ["a", "x"]
#   pk 1 -> scores [40]              tags ["b"]
#   pk 2 -> scores [100, 100, 100]   tags ["x", "x", "y"]
#   pk 3 -> scores []                tags []
#   pk 4 -> scores [60, 60]          tags ["c", "x"]
KNOWN_SCORES = [
    [95, 80],
    [40],
    [100, 100, 100],
    [],
    [60, 60],
]
KNOWN_TAGS = [
    ["a", "x"],
    ["b"],
    ["x", "x", "y"],
    [],
    ["c", "x"],
]


class TestScalarArrayMatch(TestcaseBase):
    """
    Test quantified element filtering on scalar ARRAY fields:
        MATCH_ANY / MATCH_ALL / MATCH_LEAST / MATCH_MOST / MATCH_EXACT / element_filter
    with "$" referring to the array element itself.
    """

    def _build_collection(self, with_scalar_index=False):
        """Create a collection with int64 pk, float_vector, Array<Int64> and Array<VarChar>,
        insert the known dataset, flush, index and load. Returns the collection wrapper."""
        fields = [
            cf.gen_int64_field(name=pk_field, is_primary=True),
            cf.gen_float_vec_field(name=vec_field, dim=ct.default_dim),
            cf.gen_array_field(name=scores_field, element_type=DataType.INT64, max_capacity=16),
            cf.gen_array_field(
                name=tags_field,
                element_type=DataType.VARCHAR,
                max_capacity=16,
                max_length=64,
            ),
        ]
        schema = cf.gen_collection_schema(fields=fields, primary_field=pk_field, auto_id=False)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

        # insert known rows as a list of row dicts
        rng = cf.gen_vectors(len(KNOWN_SCORES), ct.default_dim)
        rows = []
        for i in range(len(KNOWN_SCORES)):
            rows.append(
                {
                    pk_field: i,
                    vec_field: list(rng[i]),
                    scores_field: KNOWN_SCORES[i],
                    tags_field: KNOWN_TAGS[i],
                }
            )
        collection_w.insert(data=rows)
        collection_w.flush()

        # vector index
        collection_w.create_index(vec_field, index_params=ct.default_flat_index)
        # optional scalar INVERTED index on the int64 array field
        if with_scalar_index:
            collection_w.create_index(scores_field, index_params={"index_type": "INVERTED"})
        collection_w.load()
        return collection_w

    @staticmethod
    def _exp_pks(pks):
        """Build the expected query result list (only the pk field per row)."""
        return [{pk_field: pk} for pk in pks]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("with_scalar_index", [False, True])
    def test_scalar_array_quantified_match_int64(self, with_scalar_index):
        """
        target: test MATCH_ANY/MATCH_ALL/MATCH_LEAST/MATCH_EXACT/element_filter on Array<Int64>
        method: insert known arrays, query with quantified element filters, assert matched pks
        expected: returned primary keys equal the expected sets
        """
        collection_w = self._build_collection(with_scalar_index=with_scalar_index)

        # MATCH_ANY(scores, $ > 90) -> some element > 90 -> pk 0 [95,80], pk 2 [100,100,100]
        collection_w.query(
            expr=f"MATCH_ANY({scores_field}, $ > 90)",
            output_fields=[pk_field],
            check_task=CheckTasks.check_query_results,
            check_items={exp_res: self._exp_pks([0, 2]), "pk_name": pk_field},
        )

        # MATCH_ALL(scores, $ >= 60) -> all elements >= 60; empty array vacuously true.
        #   pk 0 [95,80], pk 2 [100,100,100], pk 3 [] (vacuous), pk 4 [60,60]
        collection_w.query(
            expr=f"MATCH_ALL({scores_field}, $ >= 60)",
            output_fields=[pk_field],
            check_task=CheckTasks.check_query_results,
            check_items={exp_res: self._exp_pks([0, 2, 3, 4]), "pk_name": pk_field},
        )

        # MATCH_LEAST(scores, $ == 100, threshold=2) -> at least 2 elements == 100 -> pk 2
        collection_w.query(
            expr=f"MATCH_LEAST({scores_field}, $ == 100, threshold=2)",
            output_fields=[pk_field],
            check_task=CheckTasks.check_query_results,
            check_items={exp_res: self._exp_pks([2]), "pk_name": pk_field},
        )

        # MATCH_EXACT(scores, $ == 100, threshold=3) -> exactly 3 elements == 100 -> pk 2
        collection_w.query(
            expr=f"MATCH_EXACT({scores_field}, $ == 100, threshold=3)",
            output_fields=[pk_field],
            check_task=CheckTasks.check_query_results,
            check_items={exp_res: self._exp_pks([2]), "pk_name": pk_field},
        )

        # MATCH_MOST(scores, $ < 50, threshold=1) -> at most 1 element < 50.
        #   pk 0 [95,80] -> 0 (<=1) true
        #   pk 1 [40]    -> 1 (<=1) true
        #   pk 2 [100..] -> 0 true
        #   pk 3 []      -> 0 true
        #   pk 4 [60,60] -> 0 true
        collection_w.query(
            expr=f"MATCH_MOST({scores_field}, $ < 50, threshold=1)",
            output_fields=[pk_field],
            check_task=CheckTasks.check_query_results,
            check_items={exp_res: self._exp_pks([0, 1, 2, 3, 4]), "pk_name": pk_field},
        )

        # element_filter(scores, $ > 90) is element-granularity, NOT row-level like MATCH_ANY:
        # it returns one row per matching array element, with an extra "offset" column and the
        # pk repeated once per matching element (parity with struct-array element_filter, which
        # MATCH_* deliberately does not do). Dedup client-side and assert the distinct pks, the
        # same way the struct-array element_filter query tests do.
        #   pk 0 [95, 80]        -> 95 matches            -> 1 element row
        #   pk 2 [100, 100, 100] -> all three match       -> 3 element rows
        res, _ = collection_w.query(
            expr=f"element_filter({scores_field}, $ > 90)",
            output_fields=[pk_field],
        )
        distinct_pks = sorted({row[pk_field] for row in res})
        assert distinct_pks == [0, 2], f"unexpected element_filter pks: {res}"
        assert len(res) == 4, f"expected 4 element-granularity rows, got: {res}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_scalar_array_quantified_match_varchar(self):
        """
        target: test MATCH_ANY on a VarChar-element Array field
        method: insert known string arrays, query MATCH_ANY(tags, $ == "x"), assert matched pks
        expected: returned primary keys equal {0, 2, 4}
        """
        collection_w = self._build_collection(with_scalar_index=False)

        # MATCH_ANY(tags, $ == "x") -> rows containing "x" -> pk 0, pk 2, pk 4
        collection_w.query(
            expr=f'MATCH_ANY({tags_field}, $ == "x")',
            output_fields=[pk_field],
            check_task=CheckTasks.check_query_results,
            check_items={exp_res: self._exp_pks([0, 2, 4]), "pk_name": pk_field},
        )

        log.info("VarChar array MATCH_ANY assertion passed")
