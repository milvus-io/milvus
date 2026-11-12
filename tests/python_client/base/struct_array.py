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

"""Shared collection lifecycle and model assertions for Struct Array E2E tests."""

import copy

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CheckTasks
from common.struct_array_data import (
    CAPACITY,
    DIM,
    assert_values_equal,
    child_schema,
    decode_vector,
    make_nullable_row,
    make_typed_vector,
    normalize_vector_row,
    normalized_vector,
    nullable_children,
    vector_score,
    vector_score_tolerance,
)
from pymilvus import DataType


class StructArrayTestBase(TestMilvusClientV2Base):
    def create_struct_collection(
        self,
        client,
        children,
        parent_nullable=True,
        pk_type="int",
        child_index=None,
        metric="L2",
        dim=DIM,
        capacity=CAPACITY,
        extra_fields=(),
        shards_num=None,
    ):
        name = cf.gen_unique_str("struct_enhance")
        schema = cf.gen_collection_schema(
            fields=[
                cf.gen_scalar_field(
                    DataType.INT64 if pk_type == "int" else DataType.VARCHAR,
                    name="id",
                    is_primary=True,
                    **({"max_length": 64} if pk_type == "str" else {}),
                ),
                cf.gen_int64_field(name="revision"),
                cf.gen_float_vec_field(name="normal_vector", dim=dim),
                *extra_fields,
            ],
            enable_dynamic_field=False,
        )
        struct, _ = self.create_struct_field_schema(client)
        for field, (dtype, options) in children.items():
            struct.add_field(field, dtype, **options)
        schema.add_field(
            "clips",
            DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct,
            max_capacity=capacity,
            nullable=parent_nullable,
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("normal_vector", index_type="FLAT", metric_type="L2")
        for field, (dtype, _) in children.items():
            if dtype == DataType.FLOAT_VECTOR:
                kind = child_index or "FLAT"
                indexes.add_index(
                    f"clips[{field}]",
                    index_type=kind,
                    metric_type=metric,
                    params={"M": 16, "efConstruction": 200} if kind == "HNSW" else {},
                )
        options = {"shards_num": shards_num} if shards_num is not None else {}
        self.create_collection(client, name, schema=schema, index_params=indexes, consistency_level="Strong", **options)
        return name, schema

    def assert_rows(self, client, name, model):
        actual, _ = self.query(
            client, name, filter="", output_fields=["*"], limit=max(1, len(model) + 10), consistency_level="Strong"
        )
        assert len(actual) == len(model), (name, len(actual), len(model))
        assert {row["id"] for row in actual} == set(model)
        for row in actual:
            assert_values_equal(row, model[row["id"]])

    def reload_collection(self, client, name):
        self.flush(client, name)
        self.release_collection(client, name)
        self.load_collection(client, name)

    def assert_nullable_element_search(self, client, name, model, index_type="FLAT"):
        """Check every live vector coordinate and score, including gaps left by NULL slots."""
        vectors = {
            (pk, offset): element["emb"]
            for pk, row in model.items()
            for offset, element in enumerate(row["clips"] or [])
            if element["emb"] is not None
        }
        limit = max(1, len(vectors) + 1)
        assert limit <= 16384, "Use a bounded corpus for exhaustive element search"
        queries = [normalized_vector(701), normalized_vector(907)]
        results, _ = self.search(
            client,
            name,
            queries,
            anns_field="clips[emb]",
            limit=limit,
            search_params={"metric_type": "L2", "params": {"ef": max(512, limit)} if index_type == "HNSW" else {}},
            output_fields=["clips"],
            consistency_level="Strong",
        )
        assert len(results) == len(queries)
        for query, hits in zip(queries, results):
            coordinates = [(hit["id"], hit["offset"]) for hit in hits]
            assert len(coordinates) == len(vectors) and set(coordinates) == set(vectors), {
                "missing": set(vectors) - set(coordinates),
                "unexpected": set(coordinates) - set(vectors),
                "actual_count": len(coordinates),
                "expected_count": len(vectors),
            }
            distances = [hit["distance"] for hit in hits]
            assert distances == sorted(distances)
            for hit, coordinate in zip(hits, coordinates):
                assert hit["distance"] == pytest.approx(
                    vector_score(query, vectors[coordinate], "L2"), abs=2e-4, rel=2e-4
                )
                assert_values_equal(hit["entity"]["clips"], model[hit["id"]]["clips"])

    def assert_insert_rejected(self, client, name, rows, model):
        response, succeeded = self.insert(client, name, rows, check_task=CheckTasks.check_nothing)
        assert not succeeded, ("Invalid batch was accepted", rows)
        assert getattr(response, "code", None) in (1, 1100), response
        self.assert_rows(client, name, model)

    def get_child_id(self, client, name, child):
        desc, _ = self.describe_collection(client, name)
        struct = next(field for field in desc["fields"] if field["name"] == "clips")
        return next(field["field_id"] for field in struct["struct_fields"] if field["name"] == child)

    def insert_rows_with_model(self, client, name, rows, split=True):
        if split:
            cut = len(rows) // 2
            self.insert(client, name, rows[:cut])
            self.flush(client, name)
            self.insert(client, name, rows[cut:])
        else:
            self.insert(client, name, rows)
        return {row["id"]: copy.deepcopy(row) for row in rows}

    def assert_nullable_predicates(self, client, name, model):
        predicates = [
            ("$[score] IS NULL", lambda e: e["score"] is None),
            ("$[score] IS NOT NULL", lambda e: e["score"] is not None),
            ("$[emb] IS NULL", lambda e: e["emb"] is None),
            ("$[tags] IS NULL", lambda e: e["tags"] is None),
            ("$[score] > 0", lambda e: e["score"] is not None and e["score"] > 0),
            ("not ($[score] > 0)", lambda e: e["score"] is not None and e["score"] <= 0),
            ("$[score] IS NULL and $[emb] IS NOT NULL", lambda e: e["score"] is None and e["emb"] is not None),
            ("array_length($[tags]) == 0", lambda e: e["tags"] == []),
            ("array_length($[tags]) == 1", lambda e: e["tags"] is not None and len(e["tags"]) == 1),
            ("array_contains($[tags], 1)", lambda e: e["tags"] is not None and 1 in e["tags"]),
            ("not ($[score] in [])", lambda e: True),
        ]
        for expression, predicate in predicates:
            for quantifier in ("ANY", "ALL", "EXACT"):
                expected = set()
                for pk, row in model.items():
                    if row["clips"] is None:
                        continue
                    values = [predicate(e) for e in row["clips"]]
                    keep = (
                        any(values) if quantifier == "ANY" else all(values) if quantifier == "ALL" else sum(values) == 1
                    )
                    if keep:
                        expected.add(pk)
                suffix = ", threshold=1" if quantifier == "EXACT" else ""
                expr = f"MATCH_{quantifier}(clips, {expression}{suffix})"
                actual, _ = self.query(client, name, filter=expr, output_fields=["id"], limit=len(model) + 10)
                assert {r["id"] for r in actual} == expected, expr
        expected = {(pk, j) for pk, r in model.items() for j, e in enumerate(r["clips"] or []) if e["score"] is None}
        assert len(expected) < 16384, "Use a bounded corpus for exhaustive element filtering"
        filtered, _ = self.query(
            client,
            name,
            filter="element_filter(clips, $[score] IS NULL)",
            output_fields=["id", "clips"],
            limit=max(1, len(expected) + 1),
        )
        assert {(r["id"], r["offset"]) for r in filtered} == expected
        assert len(filtered) == len(expected)
        for row in filtered:
            assert_values_equal(row["clips"], model[row["id"]]["clips"])


class MetadataStructArrayTestBase(StructArrayTestBase):
    def required_child_schema(self, client, value_type=DataType.INT64, vectors=True):
        schema, _ = self.create_struct_field_schema(client)
        schema.add_field("value", value_type, **({"max_length": 64} if value_type == DataType.VARCHAR else {}))
        schema.add_field("tag", DataType.VARCHAR, max_length=64)
        schema.add_field("flag", DataType.BOOL)
        if vectors:
            schema.add_field("emb", DataType.FLOAT_VECTOR, dim=DIM)
        return schema

    def create_metadata_collection(
        self,
        client,
        pk_type="int",
        capacity=CAPACITY,
        value_type=DataType.INT64,
        vectors=True,
        vector_index="FLAT",
        metric="L2",
        scalar_index=None,
        second=False,
        dim=DIM,
        extra_fields=(),
    ):
        name = cf.gen_unique_str("struct_contract")
        schema = cf.gen_collection_schema(
            fields=[
                cf.gen_scalar_field(
                    DataType.INT64 if pk_type == "int" else DataType.VARCHAR,
                    name="id",
                    is_primary=True,
                    **({"max_length": 64} if pk_type == "str" else {}),
                ),
                cf.gen_int64_field(name="revision"),
                cf.gen_int64_field(name="bucket"),
                cf.gen_float_vec_field(name="normal_vector", dim=dim),
                cf.gen_string_field(name="note", max_length=64, nullable=True),
                *extra_fields,
            ],
            enable_dynamic_field=False,
        )
        for field in ("clips", "other") if second else ("clips",):
            schema.add_field(
                field,
                DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=self.required_child_schema(client, value_type, vectors),
                max_capacity=capacity,
                nullable=True,
            )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("normal_vector", index_type="FLAT", metric_type="L2")
        if vectors:
            for field in ("clips", "other") if second else ("clips",):
                indexes.add_index(
                    f"{field}[emb]",
                    index_type=vector_index,
                    metric_type=metric,
                    params={"M": 16, "efConstruction": 200} if vector_index == "HNSW" else {},
                )
        if scalar_index:
            indexes.add_index("clips[value]", index_name="value_idx", index_type=scalar_index)
        self.create_collection(client, name, schema=schema, index_params=indexes)
        return name, schema

    def assert_metadata_rows(self, client, name, model, pk_type="int", partition_names=None):
        fields = list(next(iter(model.values()))) if model else ["id", "revision", "clips"]
        rows, _ = self.query(
            client,
            name,
            filter="id >= 0" if pk_type == "int" else 'id != ""',
            output_fields=fields,
            limit=max(len(model) + 16, 64),
            partition_names=partition_names,
            consistency_level="Strong",
        )
        assert len(rows) == len(model), (name, len(rows), len(model))
        actual = {row["id"]: row for row in rows}
        assert len(actual) == len(rows) and set(actual) == set(model), (set(actual), set(model))
        for pk, expected in model.items():
            assert_values_equal(actual[pk], expected, str(pk), exact_fields=False)
        return actual

    def assert_required_predicates(self, client, name, model, value_type=DataType.INT64):
        if value_type == DataType.VARCHAR:
            predicate, accept = '$[value] == "v1" && $[flag] == true', lambda e: e["value"] == "v1" and e["flag"]
        elif value_type == DataType.BOOL:
            predicate, accept = "$[value] == true && $[flag] == true", lambda e: e["value"] and e["flag"]
        else:
            predicate, accept = "$[value] >= 0 && $[flag] == true", lambda e: e["value"] >= 0 and e["flag"]

        def evaluate(row, operation, threshold=0):
            elements = row["clips"]
            if elements is None:
                return None
            count = sum(bool(accept(element)) for element in elements)
            return {
                "ANY": count > 0,
                "ALL": count == len(elements),
                "EXACT": count == threshold,
                "MOST": count <= threshold,
                "LEAST": count >= threshold,
            }[operation]

        cases = [(f"MATCH_{op}(clips, {predicate})", lambda r, op=op: evaluate(r, op)) for op in ("ANY", "ALL")]
        for op in ("EXACT", "MOST", "LEAST"):
            for threshold in (1, 2, CAPACITY, CAPACITY + 1) if op == "LEAST" else (0, 1, 2, CAPACITY + 1):
                cases.append(
                    (
                        f"MATCH_{op}(clips, {predicate}, threshold={threshold})",
                        lambda r, op=op, n=threshold: evaluate(r, op, n),
                    )
                )
        cases.extend(
            [
                (
                    f"not MATCH_ANY(clips, {predicate})",
                    lambda r: None if r["clips"] is None else not evaluate(r, "ANY"),
                ),
                (
                    f"bucket == 1 or MATCH_ANY(clips, {predicate})",
                    lambda r: r["bucket"] == 1 or evaluate(r, "ANY") is True,
                ),
                (
                    f"bucket == 2 and MATCH_ANY(clips, {predicate})",
                    lambda r: r["bucket"] == 2 and evaluate(r, "ANY") is True,
                ),
                (
                    'array_contains(clips[tag], "tag_1")',
                    lambda r: r["clips"] is not None and any(e["tag"] == "tag_1" for e in r["clips"]),
                ),
            ]
        )
        for expression, oracle in cases:
            rows, _ = self.query(
                client, name, filter=expression, output_fields=["id"], limit=len(model) + 16, consistency_level="Strong"
            )
            expected = {pk for pk, row in model.items() if oracle(row) is True}
            assert len(rows) == len(expected) and {row["id"] for row in rows} == expected, expression

    def assert_element_search(self, client, name, model, metric, index_type, field="clips"):
        index, _ = self.describe_index(client, name, f"{field}[emb]")
        tolerance = vector_score_tolerance(index)
        available = [
            (pk, offset, element) for pk, row in model.items() for offset, element in enumerate(row[field] or [])
        ]
        assert len(available) > 30
        queries = [normalized_vector(701), normalized_vector(907), available[len(available) // 2][2]["emb"]]
        filters = [
            ("", lambda row, e: True),
            ("bucket == 1", lambda row, e: row["bucket"] == 1),
            (
                f"bucket != 2 and element_filter({field}, $[value] >= 0 && $[flag] == true)",
                lambda row, e: row["bucket"] != 2 and e["value"] >= 0 and e["flag"],
            ),
        ]
        for expression, accept in filters:
            hits, _ = self.search(
                client,
                name,
                data=queries,
                anns_field=f"{field}[emb]",
                filter=expression,
                limit=20,
                output_fields=["revision"],
                consistency_level="Strong",
                search_params={"metric_type": metric, "params": {"ef": 512} if index_type == "HNSW" else {}},
            )
            assert len(hits) == len(queries)
            for query, result in zip(queries, hits):
                candidates = {
                    (pk, offset): vector_score(query, element["emb"], metric)
                    for pk, offset, element in available
                    if accept(model[pk], element)
                }
                ordered = sorted(candidates, key=candidates.get, reverse=metric != "L2")
                expected = set(ordered[:20])
                keys = [(hit["id"], hit["offset"]) for hit in result]
                assert len(keys) == len(set(keys)) and len(keys) == len(expected)
                for hit, key in zip(result, keys):
                    assert key in candidates, (expression, key)
                    assert hit["distance"] == pytest.approx(candidates[key], **tolerance), {
                        "coordinate": key,
                        "metric": metric,
                        "tolerance": tolerance,
                        "actual": hit["distance"],
                        "expected": candidates[key],
                    }
                distances = [hit["distance"] for hit in result]
                assert distances == sorted(distances, reverse=metric != "L2")
                if index_type == "FLAT":
                    assert set(keys) == expected, (expression, keys, ordered[:20])
                else:
                    # A quality threshold for this small corpus, not a universal ANN guarantee.
                    assert len(set(keys) & expected) / len(expected) >= 0.95, (expression, keys, ordered[:20])


class TypedStructArrayTestBase(StructArrayTestBase):
    def create_typed_collection(self, client, dtype, kind, metric):
        # Reuse the enhancement schema family; only the vector representation varies.
        name = cf.gen_unique_str("struct_vector_matrix")
        schema = cf.gen_collection_schema(
            fields=[
                cf.gen_int64_field(name="id", is_primary=True),
                cf.gen_int64_field(name="revision"),
                cf.gen_float_vec_field(name="normal_vector", dim=DIM),
            ],
            enable_dynamic_field=False,
        )
        children = nullable_children()
        children["emb"] = child_schema(dtype, dim=128 if dtype == DataType.BINARY_VECTOR else 32)
        struct, _ = self.create_struct_field_schema(client)
        for field, (field_type, options) in children.items():
            struct.add_field(field, field_type, **options)
        schema.add_field(
            "clips",
            DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct,
            max_capacity=8,
            nullable=True,
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("normal_vector", index_type="AUTOINDEX" if kind == "AUTOINDEX" else "FLAT", metric_type="L2")
        params = {"M": 16, "efConstruction": 200} if kind == "HNSW" else {"nlist": 16} if "IVF" in kind else {}
        indexes.add_index("clips[emb]", index_type=kind, metric_type=metric, params=params)
        self.create_collection(client, name, schema=schema, index_params=indexes, consistency_level="Strong")
        return name, schema

    def make_typed_rows(self, schema):
        struct = schema.struct_fields[0]
        vector = next(field for field in struct.fields if field.name == "emb")
        rows = []
        for pk in range(1200):
            clips = [
                {
                    "anchor": j,
                    "score": None if j < 2 else pk,
                    "label": None,
                    "emb": None
                    if j % 2 == 0 or pk < 16
                    else make_typed_vector(vector.dtype, pk * 17 + j, int(vector.params["dim"])),
                    "tags": [None, pk] if j % 2 else [],
                }
                for j in range(min(4, struct.max_capacity))
            ]
            rows.append(make_nullable_row(schema, pk, None if pk % 19 == 0 else [] if pk % 19 == 1 else clips))
        return rows

    def assert_typed_rows(self, client, name, model, dtype):
        rows, _ = self.query(client, name, filter="", output_fields=["*"], limit=len(model) + 10)
        assert len(rows) == len(model) and {r["id"] for r in rows} == set(model)
        for row in rows:
            assert_values_equal(normalize_vector_row(row, dtype), normalize_vector_row(model[row["id"]], dtype))

    def assert_typed_search(self, client, name, model, dtype, kind, metric, score_deviations=None):
        index, _ = self.describe_index(client, name, "clips[emb]")
        tolerance = vector_score_tolerance(index, dtype)
        query = make_typed_vector(dtype, 900001)
        a = decode_vector(query, dtype)
        for filtered in (False, True):
            scores = {}
            for pk, row in model.items():
                for offset, element in enumerate(row["clips"] or []):
                    if element["emb"] is None or (filtered and element["score"] is not None):
                        continue
                    b = decode_vector(element["emb"], dtype)
                    if metric == "L2":
                        score = np.sum((a - b) ** 2)
                    elif metric == "HAMMING":
                        score = np.count_nonzero(a != b)
                    elif metric == "JACCARD":
                        score = 1 - np.count_nonzero(a * b) / np.count_nonzero(a + b)
                    else:
                        score = np.dot(a, b)
                        if metric == "COSINE":
                            score /= np.linalg.norm(a) * np.linalg.norm(b)
                    scores[(pk, offset)] = float(score)
            result, _ = self.search(
                client,
                name,
                [query],
                anns_field="clips[emb]",
                limit=20,
                filter="element_filter(clips, $[score] IS NULL)" if filtered else "",
                search_params={
                    "metric_type": metric,
                    "params": {
                        "ef": 200,
                        "search_list": 200,
                        "nprobe": 1024 if kind == "AUTOINDEX" and dtype == DataType.BINARY_VECTOR else 16,
                    },
                },
                output_fields=["clips"],
            )
            hits = result[0]
            coordinates = [(hit["id"], hit["offset"]) for hit in hits]
            assert len(hits) == 20 and len(set(coordinates)) == 20
            invalid = [key for key in coordinates if key not in scores]
            assert not invalid, {
                "unexpected_coordinates": invalid,
                "filtered": filtered,
                "kind": kind,
                "metric": metric,
                "collection": name,
            }
            reverse = metric in ("IP", "COSINE")
            ordered = sorted(scores.values(), reverse=reverse)
            cutoff = ordered[19]
            eligible = (
                {key for key, score in scores.items() if score >= cutoff}
                if reverse
                else {key for key, score in scores.items() if score <= cutoff}
            )
            # Tie-aware top-k quality: binary distances legitimately tie at the boundary.
            recall = len(set(coordinates) & eligible) / 20
            if kind in ("FLAT", "BIN_FLAT", "IVF_FLAT", "BIN_IVF_FLAT"):
                assert recall == 1.0
                required = (
                    {key for key, score in scores.items() if score > cutoff}
                    if reverse
                    else {key for key, score in scores.items() if score < cutoff}
                )
                assert required <= set(coordinates), {
                    "missing_better_neighbors": required - set(coordinates),
                    "filtered": filtered,
                    "kind": kind,
                    "metric": metric,
                }
            else:
                assert recall >= 0.95
            distances = [hit["distance"] for hit in hits]
            assert distances == sorted(distances, reverse=reverse)
            for hit, coordinate in zip(hits, coordinates):
                # Retain the strict oracle for the existing typed DISKANN issue.
                budget = tolerance if score_deviations is None else {"abs": 2e-4, "rel": 2e-4}
                expected_score = pytest.approx(scores[coordinate], **budget)
                if score_deviations is None:
                    assert hit["distance"] == expected_score, {
                        "coordinate": coordinate,
                        "metric": metric,
                        "tolerance": budget,
                        "actual": hit["distance"],
                        "expected": scores[coordinate],
                    }
                elif hit["distance"] != expected_score:
                    # Defer only the score assertion so mutation/reload and all
                    # structural checks still run before the issue is classified.
                    score_deviations.append(
                        {"coordinate": coordinate, "actual": hit["distance"], "expected": scores[coordinate]}
                    )
                actual = normalize_vector_row({"clips": hit["entity"]["clips"]}, dtype)
                assert_values_equal(actual, normalize_vector_row({"clips": model[hit["id"]]["clips"]}, dtype))
