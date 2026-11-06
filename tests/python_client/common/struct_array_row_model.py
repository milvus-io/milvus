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

"""Seeded Struct rows, mutation models and independent result assertions."""

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from pymilvus import DataType

DIM = 8

CAPACITY = 8


def _vector(seed):
    rng = np.random.RandomState(seed % (2**32))
    vector = rng.normal(size=DIM).astype(np.float32)
    return (vector / np.linalg.norm(vector)).tolist()


def _pk(number, pk_type):
    return number if pk_type == "int" else f"key_雪_{number:06d}"


def _value(number, value_type):
    number = number % 7 - 3
    if value_type == DataType.VARCHAR:
        return f"v{number}"
    if value_type == DataType.BOOL:
        return number >= 0
    if value_type == DataType.DOUBLE:
        return number + 0.25
    return number


def _element(number, offset, revision=0, value_type=DataType.INT64, vectors=True):
    result = {
        "value": _value(number + offset + revision, value_type),
        "tag": "雪" * 21 if offset % 3 == 0 else f"tag_{(number + offset) % 5}",
        "flag": (number + offset) % 2 == 0,
    }
    if vectors:
        result["emb"] = _vector(number * 1009 + offset * 31 + revision * 100003 + 7)
    return result


def _profile(number, revision=0, capacity=CAPACITY, value_type=DataType.INT64, vectors=True):
    shape = (number + revision) % 7
    if shape in (0, 6):
        return None
    if shape == 1:
        return []
    length = {2: 1, 3: 2, 4: capacity, 5: 3}[shape]
    return [_element(number, offset, revision, value_type, vectors) for offset in range(min(length, capacity))]


def _row(number, revision=0, pk_type="int", capacity=CAPACITY, value_type=DataType.INT64, vectors=True):
    return {
        "id": _pk(number, pk_type),
        "revision": revision,
        "bucket": number % 3,
        "normal_vector": _vector(number + revision * 100003 + 999),
        "note": None if number % 3 == 0 else f"note_{revision}",
        "clips": _profile(number, revision, capacity, value_type, vectors),
    }


def _assert_value(actual, expected, path="row"):
    # Compare integers exactly: converting INT64 values to float loses boundary bits.
    if isinstance(expected, dict):
        assert isinstance(actual, dict), (path, actual, expected)
        for key, value in expected.items():
            assert key in actual, (path, key, actual)
            _assert_value(actual[key], value, f"{path}.{key}")
    elif isinstance(expected, list):
        assert isinstance(actual, list) and len(actual) == len(expected), (path, actual, expected)
        for offset, (left, right) in enumerate(zip(actual, expected)):
            _assert_value(left, right, f"{path}[{offset}]")
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, abs=2e-6, rel=2e-6), path
    else:
        assert actual == expected, (path, actual, expected)


def _score(query, vector, metric):
    query = np.asarray(query, dtype=np.float64)
    vector = np.asarray(vector, dtype=np.float64)
    if metric == "L2":
        return float(np.sum((query - vector) ** 2))
    dot = float(np.dot(query, vector))
    return dot if metric == "IP" else dot / float(np.linalg.norm(query) * np.linalg.norm(vector))


class StructArrayRowModelBase(TestMilvusClientV2Base):
    def _struct_schema(self, client, value_type=DataType.INT64, vectors=True):
        schema, _ = self.create_struct_field_schema(client)
        schema.add_field("value", value_type, **({"max_length": 64} if value_type == DataType.VARCHAR else {}))
        schema.add_field("tag", DataType.VARCHAR, max_length=64)
        schema.add_field("flag", DataType.BOOL)
        if vectors:
            schema.add_field("emb", DataType.FLOAT_VECTOR, dim=DIM)
        return schema

    def _create_contract_collection(
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
    ):
        name = cf.gen_unique_str("struct_contract")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(
            "id",
            DataType.INT64 if pk_type == "int" else DataType.VARCHAR,
            is_primary=True,
            **({"max_length": 64} if pk_type == "str" else {}),
        )
        schema.add_field("revision", DataType.INT64)
        schema.add_field("bucket", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=DIM)
        schema.add_field("note", DataType.VARCHAR, max_length=64, nullable=True)
        for field in ("clips", "other") if second else ("clips",):
            schema.add_field(
                field,
                DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=self._struct_schema(client, value_type, vectors),
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
        return name

    def _assert_model(self, client, name, model, pk_type="int", partition_names=None):
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
            _assert_value(actual[pk], expected, str(pk))
        return actual

    def _reload(self, client, name):
        self.flush(client, name)
        self.release_collection(client, name)
        self.load_collection(client, name)

    def _assert_predicates(self, client, name, model, value_type=DataType.INT64):
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

    def _assert_element_search(self, client, name, model, metric, index_type, field="clips"):
        available = [
            (pk, offset, element) for pk, row in model.items() for offset, element in enumerate(row[field] or [])
        ]
        assert len(available) > 30
        queries = [_vector(701), _vector(907), available[len(available) // 2][2]["emb"]]
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
                    (pk, offset): _score(query, element["emb"], metric)
                    for pk, offset, element in available
                    if accept(model[pk], element)
                }
                ordered = sorted(candidates, key=candidates.get, reverse=metric != "L2")
                expected = set(ordered[:20])
                keys = [(hit["id"], hit["offset"]) for hit in result]
                assert len(keys) == len(set(keys)) and len(keys) == len(expected)
                for hit, key in zip(result, keys):
                    assert key in candidates, (expression, key)
                    assert hit["distance"] == pytest.approx(candidates[key], abs=2e-4, rel=2e-4)
                distances = [hit["distance"] for hit in result]
                assert distances == sorted(distances, reverse=metric != "L2")
                if index_type == "FLAT":
                    assert set(keys) == expected, (expression, keys, ordered[:20])
                else:
                    # A quality threshold for this fixed synthetic corpus, not a universal ANN guarantee.
                    assert len(set(keys) & expected) / len(expected) >= 0.95, (expression, keys, ordered[:20])
