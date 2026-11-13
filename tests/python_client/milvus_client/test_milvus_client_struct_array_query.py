# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Struct Array query E2E coverage."""

import copy
import json

import pytest
from base.struct_array import MetadataStructArrayTestBase, StructArrayTestBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_data import (
    DIM,
    SCALAR_IDS,
    SCALARS,
    assert_values_equal,
    child_schema,
    make_metadata_row,
    make_nullable_elements,
    make_nullable_row,
    make_predicate_rows,
    normalized_vector,
    nullable_children,
    scalar_boundary_value,
)
from common.struct_array_wire import iter_wire_fields
from pymilvus import DataType
from pymilvus.client.prepare import Prepare
from utils.util_log import test_log as log


def _not(value):
    return None if value is None else not value


def _and(left, right):
    return False if left is False or right is False else None if left is None or right is None else True


def _or(left, right):
    return True if left is True or right is True else None if left is None or right is None else False


def _positive(element):
    return None if element["score"] is None else element["score"] > 0


class TestMilvusClientStructArrayNullablePredicates(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "quantifier,threshold",
        [("ANY", None), ("ALL", None)]
        + [(op, n) for op in ("EXACT", "MOST", "LEAST") for n in (0, 1, 2, 8, 9) if op != "LEAST" or n > 0],
    )
    @pytest.mark.parametrize("combination", ["plain", "and", "or", "not"])
    def test_match_predicates_preserve_null_truth_values(self, quantifier, threshold, combination):
        """
        target: preserve UNKNOWN inside predicates and on null parent rows
        method: evaluate independent three-valued predicates and outer NOT before/after reload
        expected: exact PK sets, including vacuous ALL and null-parent exclusion under NOT
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        rows = make_predicate_rows(schema)
        for scores in ([1] * 8, [None, 1, -1, None, 1, 0, None, 1]):
            clips = [
                {"anchor": offset, "score": score, "label": None, "emb": None, "tags": [None]}
                for offset, score in enumerate(scores)
            ]
            rows.append(make_nullable_row(schema, len(rows), clips))
        model = self.insert_rows_with_model(client, name, rows)
        expressions = {
            "plain": "$[score] > 0",
            "and": "$[score] > 0 and $[tags] IS NULL",
            "or": "$[score] > 0 or $[tags] IS NULL",
            "not": "not ($[score] > 0)",
        }

        def predicate(element):
            value = _positive(element)
            if combination == "and":
                return _and(value, element["tags"] is None)
            if combination == "or":
                return _or(value, element["tags"] is None)
            return _not(value) if combination == "not" else value

        expected = {}
        for pk, row in model.items():
            if row["clips"] is None:
                expected[pk] = None
                continue
            matches = [predicate(e) is True for e in row["clips"]]
            if quantifier == "ANY":
                expected[pk] = any(matches)
            elif quantifier == "ALL":
                expected[pk] = all(matches)
            else:
                count = sum(matches)
                expected[pk] = {"EXACT": count == threshold, "MOST": count <= threshold, "LEAST": count >= threshold}[
                    quantifier
                ]
        suffix = f", threshold={threshold}" if threshold is not None else ""
        expression = f"MATCH_{quantifier}(clips, {expressions[combination]}{suffix})"
        for phase in range(2):
            for negate in (False, True):
                actual = client.query(
                    name,
                    filter=f"not ({expression})" if negate else expression,
                    output_fields=["id"],
                    limit=100,
                    consistency_level="Strong",
                )
                wanted = {pk for pk, value in expected.items() if (_not(value) if negate else value) is True}
                assert len(actual) == len(wanted) and {row["id"] for row in actual} == wanted, (
                    phase,
                    expression,
                    negate,
                    actual,
                    wanted,
                )
                hits, _ = self.search(
                    client,
                    name,
                    [next(iter(model.values()))["normal_vector"]],
                    anns_field="normal_vector",
                    filter=f"not ({expression})" if negate else expression,
                    limit=len(model) + 1,
                    search_params={"metric_type": "L2"},
                    consistency_level="Strong",
                )
                assert len(hits[0]) == len(wanted) and {hit["id"] for hit in hits[0]} == wanted, (
                    phase,
                    expression,
                    negate,
                    hits,
                    wanted,
                )
            if phase == 0:
                self.reload_collection(client, name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "operation", ["contains", "any", "all", "any_empty", "all_empty", "length", "equal", "unequal"]
    )
    def test_nested_row_expressions_and_out_of_bounds(self, operation):
        """
        target: distinguish null/OOB arrays, null leaves and empty arrays
        method: compare row expressions and negation with an independent array-value oracle
        expected: exact PK sets at offsets 0/1/7 in growing and sealed data
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(client, name, make_predicate_rows(schema))
        templates = {
            "contains": "array_contains({path}, 1)",
            "any": "array_contains_any({path}, [1, 3])",
            "all": "array_contains_all({path}, [1, 2])",
            "any_empty": "array_contains_any({path}, [])",
            "all_empty": "array_contains_all({path}, [])",
            "length": "array_length({path}) == 2",
            "equal": "{path} == [1, 2]",
            "unequal": "{path} != [1, 2]",
        }

        def evaluate(tags):
            if tags is None:
                return None
            return {
                "contains": 1 in tags,
                "any": any(v in tags for v in [1, 3]),
                "all": all(v in tags for v in [1, 2]),
                "any_empty": False,
                "all_empty": True,
                "length": len(tags) == 2,
                "equal": tags == [1, 2],
                "unequal": tags != [1, 2],
            }[operation]

        for phase in range(2):
            for offset in (0, 1, 7):
                expression = templates[operation].format(path=f"clips[{offset}][tags]")
                values = {
                    pk: evaluate(row["clips"][offset]["tags"])
                    if row["clips"] is not None and offset < len(row["clips"])
                    else None
                    for pk, row in model.items()
                }
                for negate in (False, True):
                    actual = client.query(
                        name,
                        filter=f"not ({expression})" if negate else expression,
                        output_fields=["id"],
                        limit=100,
                        consistency_level="Strong",
                    )
                    wanted = {pk for pk, value in values.items() if (_not(value) if negate else value) is True}
                    assert {row["id"] for row in actual} == wanted, (phase, expression, negate, actual, wanted)
            if phase == 0:
                self.reload_collection(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expression",
        [
            "clips[score] IS NULL",
            "array_contains(clips[tags], 1)",
            "array_contains_any(clips[tags], [1])",
            "array_contains_all(clips[tags], [1])",
            "clips[tags] == [1]",
            "clips[tags] != [1]",
            "MATCH_ANY(clips, $[tags] > 1)",
            "MATCH_ANY(clips, $[tags] in [1])",
            'MATCH_ANY(clips, $[tags] like "a%")',
            "MATCH_ANY(clips, $[tags][0] == 1)",
            "clips[0][tags] < [1]",
            "clips[0][tags] <= [1]",
            "clips[0][tags] > [1]",
            "clips[0][tags] >= [1]",
            "clips[0][tags] in [1]",
            "clips[0][tags] not in [1]",
            'clips[0][tags] like "a%"',
            "clips[0][tags] + 1 == 2",
            "clips[0][tags] == 1",
            "clips[0][tags] != 1",
            "clips[0][tags] == {needle}",
            "clips[0][tags] != {needle}",
            "clips[tags] IS NULL",
            "clips[tags] IS NOT NULL",
            "clips[0][tags] IS NULL",
            "clips[0][tags] IS NOT NULL",
            "MATCH_ANY(clips, $[tags] == [1])",
            "MATCH_ANY(clips, $[tags] != [1])",
            "MATCH_ANY(clips, $[tags] + 1 == 2)",
            "element_filter(clips, $[tags] == [1])",
            "element_filter(clips, $[tags] in [1])",
            'element_filter(clips, $[tags] like "a%")',
            "element_filter(clips, $[tags][0] == 1)",
        ],
    )
    def test_unsupported_nested_expressions(self, expression):
        """
        target: reject unsupported nested expression syntax without poisoning subsequent queries
        method: reject each expression through query/search, then check a supported array predicate and full rows
        expected: code 1100 for unsupported syntax, valid array filters still work and stored rows are unchanged
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(client, name, make_predicate_rows(schema))
        params = {"needle": [1]} if "{needle}" in expression else {}
        result, ok = self.query(
            client,
            name,
            filter=expression,
            filter_params=params,
            output_fields=["id"],
            check_task=CheckTasks.check_nothing,
        )
        assert not ok and getattr(result, "code", None) == 1100, result
        result, ok = self.search(
            client,
            name,
            [next(iter(model.values()))["normal_vector"]],
            anns_field="normal_vector",
            filter=expression,
            filter_params=params,
            limit=10,
            search_params={"metric_type": "L2"},
            check_task=CheckTasks.check_nothing,
        )
        assert not ok and getattr(result, "code", None) == 1100, result
        actual = client.query(
            name, filter="MATCH_ANY(clips, array_contains($[tags], 1))", output_fields=["id"], limit=100
        )
        assert {row["id"] for row in actual} == {
            pk for pk, row in model.items() if any(e["tags"] is not None and 1 in e["tags"] for e in row["clips"] or [])
        }
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayNestedArrayPredicates(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", SCALARS, ids=SCALAR_IDS)
    def test_nested_leaf_predicates_preserve_null_and_empty(self, dtype):
        """
        target: evaluate nested predicates for all supported scalar leaf representations
        method: shared scalar generator with null, empty and mixed arrays, before/after reload
        expected: contains and length results match independent input-derived identities
        """
        client = self._client()
        options = {"element_type": dtype, "max_capacity": 4, "element_nullable": True}
        if dtype == DataType.VARCHAR:
            options["max_length"] = 32
        name, schema = self.create_struct_collection(
            client, {"anchor": child_schema(DataType.INT64, False), "tags": child_schema(DataType.ARRAY, **options)}
        )
        needle = scalar_boundary_value(dtype, 2)
        shapes = [None, [], [None], [needle, None], [scalar_boundary_value(dtype, 3)], [needle, needle]]
        rows = [
            make_nullable_row(
                schema,
                pk,
                None
                if pk == 0
                else []
                if pk == 1
                else [{"anchor": j, "tags": copy.deepcopy(shapes[(pk + j) % len(shapes)])} for j in range(3)],
            )
            for pk in range(20)
        ]
        model = self.insert_rows_with_model(client, name, rows)
        for phase in range(2):
            for operation in ("array_contains", "array_contains_any", "array_contains_all"):
                argument = json.dumps(needle if operation == "array_contains" else [needle], ensure_ascii=False)
                result, _ = self.query(
                    client,
                    name,
                    filter=f"element_filter(clips, {operation}($[tags], {argument}))",
                    output_fields=["id", "clips"],
                    limit=100,
                )
                expected = {
                    (pk, j)
                    for pk, row in model.items()
                    for j, e in enumerate(row["clips"] or [])
                    if e["tags"] is not None and needle in e["tags"]
                }
                assert {(r["id"], r["offset"]) for r in result} == expected
                assert len(result) == len(expected)
            result, _ = self.query(
                client,
                name,
                filter="element_filter(clips, array_length($[tags]) == 2)",
                output_fields=["id"],
                limit=100,
            )
            assert {(r["id"], r["offset"]) for r in result} == {
                (pk, j)
                for pk, row in model.items()
                for j, e in enumerate(row["clips"] or [])
                if e["tags"] is not None and len(e["tags"]) == 2
            }
            self.assert_rows(client, name, model)
            if phase == 0:
                self.reload_collection(client, name)


class TestMilvusClientStructArrayIndexedQuery(MetadataStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "value_type,index_type",
        [
            pytest.param(DataType.INT8, None, id="int8_scan"),
            pytest.param(DataType.INT8, "BITMAP", id="int8_bitmap"),
            pytest.param(DataType.INT8, "STL_SORT", id="int8_sort"),
            pytest.param(DataType.INT64, "INVERTED", id="int64_inverted"),
            pytest.param(DataType.INT64, "STL_SORT", id="int64_sort"),
            pytest.param(DataType.INT64, "AUTOINDEX", id="int64_auto"),
            pytest.param(DataType.DOUBLE, "INVERTED", id="double_inverted"),
            pytest.param(DataType.DOUBLE, "STL_SORT", id="double_sort"),
            pytest.param(DataType.VARCHAR, "BITMAP", id="varchar_bitmap"),
            pytest.param(DataType.VARCHAR, "INVERTED", id="varchar_inverted"),
            pytest.param(DataType.VARCHAR, "STL_SORT", id="varchar_sort"),
            pytest.param(DataType.BOOL, "BITMAP", id="bool_bitmap"),
        ],
    )
    def test_struct_array_scalar_index_three_valued_truth(self, value_type, index_type):
        """
        target: preserve NULL/empty and same-element truth across scalar index paths
        method: enumerate 18 predicates over 1100 rows, mutate shapes, then reload and compare
        expected: complete PK sets match a local three-valued semantic model in every phase
        """
        client = self._client()
        name, schema = self.create_metadata_collection(
            client, value_type=value_type, vectors=False, scalar_index=index_type
        )
        rows = [make_metadata_row(schema, i) for i in range(1100)]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        self.assert_required_predicates(client, name, model, value_type)
        self.reload_collection(client, name)
        if index_type:
            assert self.wait_for_index_ready(client, name, index_name="value_idx", timeout=300)
        self.assert_required_predicates(client, name, model, value_type)
        changes = [make_metadata_row(schema, i, revision=3) for i in range(0, 1100, 11)]
        self.upsert(client, name, changes)
        model.update({row["id"]: copy.deepcopy(row) for row in changes})
        self.reload_collection(client, name)
        self.assert_metadata_rows(client, name, model)
        self.assert_required_predicates(client, name, model, value_type)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("index_type", ["FLAT", "HNSW"])
    def test_struct_array_element_coordinates_after_mutation(self, metric, index_type):
        """
        target: preserve logical offsets, filtering and scores through mutable nullable arrays
        method: search independent flattened vectors before flush, after indexing, and after delete/upsert/reload
        expected: valid unique coordinates and distances; exact FLAT top-k or bounded HNSW recall
        """
        client = self._client()
        # Keep a custom name so field-to-index resolution is exercised in regular CI.
        name, schema = self.create_metadata_collection(
            client, vector_index=index_type, metric=metric, clips_index_name="clips_idx"
        )
        rows = [make_metadata_row(schema, i) for i in range(1100)]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        self.assert_element_search(client, name, model, metric, index_type)
        self.reload_collection(client, name)
        self.assert_element_search(client, name, model, metric, index_type)
        deleted = list(range(0, 1100, 9))
        self.delete(client, name, ids=deleted)
        for pk in deleted:
            del model[pk]
        changes = [make_metadata_row(schema, i, revision=5) for i in range(1, 1100, 7)]
        self.upsert(client, name, changes)
        model.update({row["id"]: copy.deepcopy(row) for row in changes})
        self.reload_collection(client, name)
        self.assert_metadata_rows(client, name, model)
        self.assert_element_search(client, name, model, metric, index_type)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("value_type", [DataType.INT64, DataType.VARCHAR], ids=["int64", "varchar"])
    def test_rebuild_index_and_toggle_mmap_preserve_mutated_rows(self, value_type):
        """
        target: preserve mutation history through repeated scalar-index and mmap transitions
        method: replace arrays, change mmap and rebuild BITMAP/INVERTED/STL_SORT indexes
        expected: projection and all three-valued predicates remain equal to retained inputs
        """
        client = self._client()
        name, schema = self.create_metadata_collection(
            client, vectors=False, value_type=value_type, scalar_index="BITMAP"
        )
        rows = [make_metadata_row(schema, i) for i in range(1100)]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        for revision, index_type in enumerate(["INVERTED", "STL_SORT", "BITMAP"], 1):
            changes = [make_metadata_row(schema, i, revision) for i in range(revision, 1100, 13)]
            self.upsert(client, name, changes)
            model.update({row["id"]: row for row in changes})
            self.flush(client, name)
            self.release_collection(client, name)
            self.drop_index(client, name, index_name="value_idx")
            self.alter_collection_properties(client, name, properties={"mmap.enabled": revision % 2 == 1})
            indexes, _ = self.prepare_index_params(client)
            indexes.add_index("clips[value]", index_name="value_idx", index_type=index_type)
            self.create_index(client, name, indexes)
            assert self.wait_for_index_ready(client, name, index_name="value_idx", timeout=300)
            self.load_collection(client, name)
            self.assert_metadata_rows(client, name, model)
            self.assert_required_predicates(client, name, model, value_type)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expression",
        [
            "MATCH_LEAST(clips, $[value] > 0, threshold=0)",
            "MATCH_EXACT(clips, $[value] > 0, threshold=-1)",
            "MATCH_MOST(clips, $[value] > 0, threshold=-1)",
        ],
    )
    def test_struct_array_invalid_match_threshold_is_rejected(self, expression):
        """
        target: reject invalid MATCH thresholds without affecting subsequent valid queries
        method: issue malformed query, verify full retained data and valid predicate results
        expected: parameter error followed by correct complete query results
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client, vectors=False)
        rows = [make_metadata_row(schema, i) for i in range(14)]
        model = {row["id"]: row for row in rows}
        self.insert(client, name, rows)
        error, _ = self.query(
            client, name, filter=expression, check_task=CheckTasks.err_res, check_items={ct.err_msg: ""}
        )
        assert error.code == 1100
        self.assert_metadata_rows(client, name, model)
        self.assert_required_predicates(client, name, model)


class TestMilvusClientStructArrayIndexedSubfields(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index_type", ["FLAT", "HNSW", "DISKANN"])
    def test_all_null_vector_index_rebuild_and_visibility(self, index_type):
        """
        target: support an index whose nullable child has no searchable elements
        method: seal all-null vectors, rebuild the index, insert a vector and null it again
        expected: zero hits, then the exact logical coordinate, then zero hits without data loss
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children(), child_index=index_type)
        rows = [
            make_nullable_row(
                schema,
                n,
                [{"anchor": j, "score": None, "label": None, "emb": None, "tags": [None]} for j in range(n % 5)],
            )
            for n in range(1200)
        ]
        model = self.insert_rows_with_model(client, name, rows, split=False)
        self.reload_collection(client, name)
        for phase in range(3):
            result, _ = self.search(
                client,
                name,
                [normalized_vector(8)],
                anns_field="clips[emb]",
                limit=10,
                search_params={"metric_type": "L2"},
                output_fields=["clips"],
            )
            if phase == 1:
                assert len(result[0]) == 1, result
                hit = result[0][0]
                assert (hit["id"], hit["offset"]) == (7, 1)
                assert hit["distance"] == pytest.approx(0, abs=1e-6)
            else:
                assert result == [[]], result
            self.assert_rows(client, name, model)
            if phase == 0:
                self.release_collection(client, name)
                self.drop_index(client, name, index_name="clips[emb]")
                indexes, _ = self.prepare_index_params(client)
                indexes.add_index(
                    "clips[emb]",
                    index_type=index_type,
                    metric_type="L2",
                    params={"M": 16, "efConstruction": 200} if index_type == "HNSW" else {},
                )
                self.create_index(client, name, indexes)
                self.load_collection(client, name)
            if phase < 2:
                model[7]["clips"][1]["emb"] = normalized_vector(8) if phase == 0 else None
                self.upsert(client, name, [model[7]])
                self.reload_collection(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_max_sim_rejection_and_original_provider_reassignment(self):
        """
        target: reject unsupported MAX_SIM and preserve offsets after dropping the first original
        method: reject an index change, drop the original anchor and compare every remaining value
        expected: the parameter error is non-mutating and the remaining originals supply correct offsets
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(100)],
        )
        self.release_collection(client, name)
        self.drop_index(client, name, index_name="clips[emb]")
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index(
            "clips[emb]", index_type="HNSW", metric_type="MAX_SIM_COSINE", params={"M": 16, "efConstruction": 200}
        )
        self.create_index(
            client, name, indexes, check_task=CheckTasks.err_res, check_items={"err_code": 1100, "err_msg": ""}
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("clips[emb]", index_type="FLAT", metric_type="L2")
        self.create_index(client, name, indexes)
        self.load_collection(client, name)
        self.assert_rows(client, name, model)
        self.drop_collection_field(client, name, field_name="clips[anchor]")
        for row in model.values():
            for element in row["clips"] or []:
                del element["anchor"]
        self.assert_rows(client, name, model)
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)
        self.assert_nullable_predicates(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_keeps_original_provider_and_rejects_scalar_indexes(self):
        """
        target: enforce original-provider and unsupported-index boundaries without data loss
        method: add nullable child, reject dropping all originals and reject scalar indexes
        expected: schema and complete rows survive every rejection and remain queryable
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, {"original": child_schema(DataType.INT64, False)})
        rows = [make_nullable_row(schema, n, None if n % 3 == 0 else [{"original": n}]) for n in range(12)]
        model = self.insert_rows_with_model(client, name, rows)
        self.add_collection_field(client, name, "clips[added]", DataType.INT64, nullable=True)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self.drop_collection_field(
            client,
            name,
            field_name="clips[original]",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "original"},
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("clips[added]", index_type="INVERTED")
        self.create_index(
            client, name, indexes, check_task=CheckTasks.err_res, check_items={"err_code": 1100, "err_msg": ""}
        )
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayQueryNullReduction(MetadataStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    @pytest.mark.parametrize("tail", ["all_null", "mixed"])
    def test_struct_array_query_reduction_preserves_all_null_validity(self, pk_type, tail):
        """
        target: retain row masks when reducing all-null and mixed vector/struct query outputs
        method: create four write populations, query cross-population subsets through SDK and raw RPC
        expected: exact rows and legacy/current validity masks agree before and after reload
        """
        client = self._client()
        name = cf.gen_unique_str("struct_reduction")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(
            "id",
            DataType.INT64 if pk_type == "int" else DataType.VARCHAR,
            is_primary=True,
            **({"max_length": 64} if pk_type == "str" else {}),
        )
        schema.add_field("revision", DataType.INT64)
        schema.add_field("bucket", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=DIM, nullable=True)
        schema.add_field("note", DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field(
            "clips",
            DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=self.required_child_schema(client),
            max_capacity=8,
            nullable=True,
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("normal_vector", index_type="FLAT", metric_type="L2")
        indexes.add_index("clips[emb]", index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=2,
            properties={"collection.autocompaction.enabled": "false"},
        )
        rows = [make_metadata_row(schema, number) for number in range(44)]
        for number, item in enumerate(rows):
            if number // 11 in (0, 2) or (number // 11 == 3 and tail == "all_null"):
                item.update(normal_vector=None, clips=None, note=None)
        model = {item["id"]: copy.deepcopy(item) for item in rows}
        for batch in range(4):
            self.insert(client, name, rows[batch * 11 : (batch + 1) * 11])
            if batch < 3:
                self.flush(client, name)
        groups = [
            list(range(11)) + list(range(22, 33)) + list(range(33, 44)),
            list(range(44)),
            [0, 11, 22, 33],
            [1, 12, 23, 34],
        ]
        for phase in ("before_reload", "after_reload"):
            if phase == "after_reload":
                self.reload_collection(client, name)
            for numbers in groups:
                keys = [rows[number]["id"] for number in numbers]
                expression = f"id in {json.dumps(keys)}"
                expected = {key: model[key] for key in keys}
                decoded, _ = self.query(
                    client, name, filter=expression, output_fields=["*"], limit=100, consistency_level="Strong"
                )
                assert len(decoded) == len(expected) and {item["id"] for item in decoded} == set(expected)
                for item in decoded:
                    assert_values_equal(item, expected[item["id"]], exact_fields=False)
                request = Prepare.query_request(
                    name,
                    expression,
                    ["id", "normal_vector", "note", "clips"],
                    [],
                    use_default_consistency=False,
                    consistency_level=0,
                    limit=100,
                )
                response = client._get_connection()._stub.Query(request, timeout=30)
                assert response.status.code == 0, response.status
                fields = dict(iter_wire_fields(response))
                ids = fields["id"].scalars.long_data.data if pk_type == "int" else fields["id"].scalars.string_data.data
                assert len(ids) == len(expected) and set(ids) == set(expected)
                for path in ("normal_vector", "note", "clips[value]", "clips[tag]", "clips[flag]", "clips[emb]"):
                    field = fields[path]
                    root = path.split("[")[0]
                    validity = [expected[key][root] is not None for key in ids]
                    assert list(field.valid_data) == validity, (phase, path, list(field.valid_data), validity)
                    wrapper = field.WhichOneof("field")
                    if wrapper:
                        assert list(getattr(field, wrapper).valid_data) == validity, (phase, path, field)
                    else:
                        assert not any(validity), (phase, path)
                log.info(
                    "STRUCT_REDUCTION_VALIDITY phase=%s pk=%s tail=%s rows=%s all_null=%s",
                    phase,
                    pk_type,
                    tail,
                    len(ids),
                    all(expected[key]["normal_vector"] is None for key in ids),
                )


class TestMilvusClientStructArrayArrayJsonSemantics(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", [DataType.INT64, DataType.VARCHAR], ids=["int64", "varchar"])
    def test_array_json_struct_filters_share_value_and_unknown_semantics(self, dtype):
        """
        target: align common array predicates across ARRAY, JSON and Struct Array
        method: share boundary inputs and independent oracles through query/search, updates and reload
        expected: equal PK sets for common predicates; explicit NULL/path capability differences
        """
        client = self._client()
        options = {"element_type": dtype, "max_capacity": 8}
        if dtype == DataType.VARCHAR:
            options["max_length"] = 16
        name, schema = self.create_struct_collection(
            client,
            {
                "anchor": child_schema(DataType.INT64, False),
                "tags": child_schema(DataType.ARRAY, element_nullable=True, **options),
            },
            dim=24,
            extra_fields=[
                cf.gen_scalar_field(DataType.ARRAY, name="values", nullable=True, **options),
                cf.gen_scalar_field(DataType.JSON, name="doc", nullable=True),
            ],
        )
        first, second, other = (1, 2, 3) if dtype == DataType.INT64 else ("雪", 'a"b', "λ")
        boundary = [-(2**63), 2**63 - 1] if dtype == DataType.INT64 else ["", "雪" * 5 + "x"]
        values = [
            None,
            None,
            None,
            [],
            [first],
            [first, second],
            [second, first, second],
            [other],
            [first] * 8,
            boundary,
            None,
        ]
        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=len(values),
            schema=schema,
            default_values={
                "values": copy.deepcopy(values),
                "doc": [{"values": copy.deepcopy(value)} for value in values],
                "clips": [[{"anchor": 0, "tags": copy.deepcopy(value)}] for value in values],
            },
        )
        # Omitted parents, omitted nested members and explicit parent NULL are distinct inputs.
        for field in ("values", "doc", "clips"):
            del rows[0][field]
        rows[1].pop("values")
        rows[1]["doc"] = {}
        rows[1]["clips"] = [{"anchor": 0}]
        rows[10].update(values=None, doc=None, clips=None)
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        model[0].update(values=None, doc=None, clips=None)
        model[1]["values"] = None
        model[1]["clips"][0]["tags"] = None
        self.insert(client, name, rows)
        query_vector = rows[3]["normal_vector"]
        predicates = [
            (f"array_contains({{path}}, {json.dumps(first)})", lambda value: first in value),
            (
                f"array_contains_any({{path}}, {json.dumps([first, other])})",
                lambda value: first in value or other in value,
            ),
            (
                f"array_contains_all({{path}}, {json.dumps([first, second])})",
                lambda value: first in value and second in value,
            ),
            ("array_contains_any({path}, [])", lambda value: False),
            ("array_contains_all({path}, [])", lambda value: True),
            ("array_length({path}) == 0", lambda value: len(value) == 0),
            ("array_length({path}) == 2", lambda value: len(value) == 2),
            (f"{{path}} == {json.dumps([first, second])}", lambda value: value == [first, second]),
            (f"{{path}} != {json.dumps([first, second])}", lambda value: value != [first, second]),
        ]
        failures = []

        def check_filter(expression, expected, phase):
            actual, _ = self.query(client, name, filter=expression, output_fields=["id"], limit=len(model) + 1)
            hits, _ = self.search(
                client,
                name,
                [query_vector],
                anns_field="normal_vector",
                filter=expression,
                limit=len(model) + 1,
                search_params={"metric_type": "L2"},
                output_fields=["id"],
            )
            for surface, result in (("query", actual), ("search", hits[0])):
                ids = [row["id"] for row in result]
                if len(ids) != len(set(ids)) or set(ids) != expected:
                    failures.append(
                        {
                            "phase": phase,
                            "surface": surface,
                            "expression": expression,
                            "expected": sorted(expected),
                            "actual": ids,
                        }
                    )

        for phase in ("growing", "sealed", "updated_sealed"):
            for template, predicate in predicates:
                for negate in (False, True):
                    expected = {
                        pk
                        for pk, value in enumerate(values)
                        if value is not None and (not predicate(value) if negate else predicate(value))
                    }
                    for path in ("values", 'doc["values"]', "clips[0][tags]"):
                        expression = template.format(path=path)
                        wanted = expected
                        # JSON empty candidate lists are constants; typed nullable arrays propagate UNKNOWN.
                        if path == 'doc["values"]' and template in (
                            "array_contains_any({path}, [])",
                            "array_contains_all({path}, [])",
                        ):
                            matches = template.startswith("array_contains_all") != negate
                            wanted = set(model) if matches else set()
                        check_filter(f"not ({expression})" if negate else expression, wanted, phase)
            # JSON path existence excludes missing, null and recursively empty containers.
            # The same stored JSON must retain this result across growing/sealed execution.
            check_filter("values IS NULL", {pk for pk, value in enumerate(values) if value is None}, phase)
            json_null = {pk for pk, row in model.items() if row["doc"] is None or not row["doc"].get("values")}
            check_filter('doc["values"] IS NULL', json_null, phase)
            check_filter('doc["values"] IS NOT NULL', set(model) - json_null, phase)
            child_nulls = {pk for pk, row in model.items() if row["clips"] and row["clips"][0]["tags"] is None}
            check_filter("MATCH_ANY(clips, $[tags] IS NULL)", child_nulls, phase)
            # Out-of-range Struct extraction remains UNKNOWN even under NOT.
            check_filter("array_length(clips[7][tags]) == 0", set(), phase)
            check_filter("not (array_length(clips[7][tags]) == 0)", set(), phase)
            self.assert_rows(client, name, model)
            if phase == "sealed":
                patches = []
                for pk in (0, 1, 2):
                    value = [second, first]
                    patch = {
                        "id": pk,
                        "values": value,
                        "doc": {"values": value},
                        "clips": [{"anchor": 0, "tags": value}],
                    }
                    patches.append(patch)
                    values[pk] = value
                    model[pk].update(copy.deepcopy(patch))
                self.upsert(client, name, patches, partial_update=True)
                # SDK batches require a uniform field set; omitted fields remain unchanged.
                self.upsert(client, name, [{"id": 3, "revision": 17}], partial_update=True)
                model[3]["revision"] = 17
            if phase != "updated_sealed":
                self.reload_collection(client, name)
        for expression in ("values[0] IS NULL", "clips[0][tags] IS NULL"):
            self.query(
                client,
                name,
                filter=expression,
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1100, "err_msg": ""},
            )
        assert not failures, json.dumps(failures, ensure_ascii=False)

    @pytest.mark.tags(CaseLabel.L2)
    def test_json_struct_null_leaves_preserve_slots_and_array_rejects_them(self):
        """
        target: distinguish supported NULL leaves from ordinary typed Array restrictions
        method: share JSON/Struct NULL-leaf inputs, reject the same ordinary Array input, then reload
        expected: logical lengths include NULL slots and rejected writes leave the model unchanged
        """
        client = self._client()
        options = {"element_type": DataType.INT64, "max_capacity": 4}
        name, schema = self.create_struct_collection(
            client,
            {
                "anchor": child_schema(DataType.INT64, False),
                "tags": child_schema(DataType.ARRAY, element_nullable=True, **options),
            },
            extra_fields=[
                cf.gen_scalar_field(DataType.ARRAY, name="values", nullable=True, **options),
                cf.gen_scalar_field(DataType.JSON, name="doc", nullable=True),
            ],
        )
        values = [None, [], [None], [1, None, 2], [None, None, None, None]]
        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=len(values),
            schema=schema,
            default_values={
                "values": [None] * len(values),
                "doc": [{"values": v} for v in values],
                "clips": [[{"anchor": 0, "tags": v}] for v in values],
            },
        )
        model = self.insert_rows_with_model(client, name, rows, split=False)
        for phase in range(2):
            for path in ('doc["values"]', "clips[0][tags]"):
                for length in (0, 1, 3, 4):
                    actual, _ = self.query(
                        client, name, filter=f"array_length({path}) == {length}", output_fields=["id"], limit=10
                    )
                    assert {r["id"] for r in actual} == {
                        i for i, v in enumerate(values) if v is not None and len(v) == length
                    }
                for negate in (False, True):
                    expression = f"array_contains({path}, 1)"
                    actual, _ = self.query(
                        client,
                        name,
                        filter=f"not ({expression})" if negate else expression,
                        output_fields=["id"],
                        limit=10,
                    )
                    expected = {
                        i for i, v in enumerate(values) if v is not None and ((1 not in v) if negate else (1 in v))
                    }
                    assert {r["id"] for r in actual} == expected
            rejected = copy.deepcopy(rows[3])
            rejected.update(id=100, values=[1, None, 2])
            self.assert_insert_rejected(client, name, [rejected], model)
            self.assert_rows(client, name, model)
            if phase == 0:
                self.reload_collection(client, name)
