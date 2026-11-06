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

"""Nullable Struct children, nested arrays and schema-evolution helpers."""

import copy

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CheckTasks
from pymilvus import DataType

SCALARS = [
    DataType.BOOL,
    DataType.INT8,
    DataType.INT16,
    DataType.INT32,
    DataType.INT64,
    DataType.FLOAT,
    DataType.DOUBLE,
    DataType.VARCHAR,
]

SCALAR_IDS = ["bool", "int8", "int16", "int32", "int64", "float", "double", "varchar"]

DIM = 8


def _value(dtype, n):
    if dtype == DataType.BOOL:
        return bool(n % 2)
    if dtype == DataType.VARCHAR:
        return ["", '雪"a', "abcd", "λ"][n % 4]
    if dtype in (DataType.FLOAT, DataType.DOUBLE):
        return (n % 7 - 3) * 0.25
    bits = {DataType.INT8: 8, DataType.INT16: 16, DataType.INT32: 32, DataType.INT64: 64}[dtype]
    return [-(2 ** (bits - 1)), 2 ** (bits - 1) - 1, 0, -1, 1][n % 5]


def _vector(n):
    value = np.random.RandomState(n % (2**32)).normal(size=DIM).astype(np.float32)
    return (value / np.linalg.norm(value)).tolist()


def _record(n, clips, pk_type="int", revision=0):
    return {
        "id": n if pk_type == "int" else f"key_雪_{n:06d}",
        "revision": revision,
        "normal_vector": _vector(n + 100003 * revision + 9),
        "clips": copy.deepcopy(clips),
    }


def _equal(actual, expected, path="row"):
    if isinstance(expected, dict):
        assert isinstance(actual, dict), (path, actual)
        assert set(actual) == set(expected), (path, set(actual), set(expected))
        for key in expected:
            assert key in actual, (path, key, actual)
            _equal(actual[key], expected[key], f"{path}.{key}")
    elif isinstance(expected, list):
        assert isinstance(actual, list) and len(actual) == len(expected), (path, actual, expected)
        for n, (a, e) in enumerate(zip(actual, expected)):
            _equal(a, e, f"{path}[{n}]")
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, abs=2e-6, rel=2e-6), (path, actual, expected)
    else:
        assert actual == expected, (path, actual, expected)


def _child(dtype, nullable=True, **kwargs):
    if dtype == DataType.VARCHAR:
        kwargs.setdefault("max_length", 32)
    if dtype == DataType.FLOAT_VECTOR:
        kwargs.setdefault("dim", DIM)
    return dtype, {"nullable": nullable, **kwargs}


def _core_children():
    return {
        "anchor": _child(DataType.INT64, False),
        "score": _child(DataType.INT64),
        "label": _child(DataType.VARCHAR),
        "emb": _child(DataType.FLOAT_VECTOR),
        "tags": _child(DataType.ARRAY, element_type=DataType.INT64, max_capacity=4, element_nullable=True),
    }


def _clips(n, revision=0, parent_nullable=True):
    shape = (n + revision) % 8
    if shape == 0:
        return None if parent_nullable else []
    if shape == 1:
        return []
    return [
        {
            "anchor": j,
            "score": None if (n + j + revision) % 3 == 0 else (n + j + revision) % 7 - 3,
            "label": None if (n + j) % 4 == 0 else f"tag_{(n + j) % 3}",
            "emb": None if (n + j + revision) % 3 == 1 else _vector(n * 31 + j + revision * 100003),
            "tags": [None, [], [None], [n, None], [1, 2, 3, 4]][(n + j + revision) % 5],
        }
        for j in range(1 + shape % 5)
    ]


class StructArrayNullableBase(TestMilvusClientV2Base):
    def _create(self, client, children, parent_nullable=True, pk_type="int", child_index=None, metric="L2"):
        name = cf.gen_unique_str("struct_enhance")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(
            "id",
            DataType.INT64 if pk_type == "int" else DataType.VARCHAR,
            is_primary=True,
            **({"max_length": 64} if pk_type == "str" else {}),
        )
        schema.add_field("revision", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=DIM)
        struct, _ = self.create_struct_field_schema(client)
        for field, (dtype, options) in children.items():
            struct.add_field(field, dtype, **options)
        schema.add_field(
            "clips",
            DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct,
            max_capacity=8,
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
        self.create_collection(client, name, schema=schema, index_params=indexes, consistency_level="Strong")
        return name

    def _check(self, client, name, model):
        actual, _ = self.query(
            client, name, filter="", output_fields=["*"], limit=max(1, len(model) + 10), consistency_level="Strong"
        )
        assert len(actual) == len(model), (name, len(actual), len(model))
        assert {row["id"] for row in actual} == set(model)
        for row in actual:
            _equal(row, model[row["id"]])

    def _reload(self, client, name):
        self.flush(client, name)
        self.release_collection(client, name)
        self.load_collection(client, name)

    def _reject_insert(self, client, name, rows, model):
        response, succeeded = self.insert(client, name, rows, check_task=CheckTasks.check_nothing)
        assert not succeeded, ("Invalid batch was accepted", rows)
        assert getattr(response, "code", None) in (1, 1100), response
        self._check(client, name, model)

    def _child_id(self, client, name, child):
        desc, _ = self.describe_collection(client, name)
        struct = next(field for field in desc["fields"] if field["name"] == "clips")
        return next(field["field_id"] for field in struct["struct_fields"] if field["name"] == child)

    def _seed(self, client, name, rows, split=True):
        if split:
            cut = len(rows) // 2
            self.insert(client, name, rows[:cut])
            self.flush(client, name)
            self.insert(client, name, rows[cut:])
        else:
            self.insert(client, name, rows)
        return {row["id"]: copy.deepcopy(row) for row in rows}

    def _predicates(self, client, name, model):
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
        filtered, _ = self.query(
            client,
            name,
            filter="element_filter(clips, $[score] IS NULL)",
            output_fields=["id", "clips"],
            limit=len(model) * 8 + 10,
        )
        expected = {(pk, j) for pk, r in model.items() for j, e in enumerate(r["clips"] or []) if e["score"] is None}
        assert {(r["id"], r["offset"]) for r in filtered} == expected
        assert len(filtered) == len(expected)
        for row in filtered:
            _equal(row["clips"], model[row["id"]]["clips"])
