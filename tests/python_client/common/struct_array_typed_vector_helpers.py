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

"""Shared setup, data models and assertions for struct vector matrix."""

import copy

import numpy as np
import pytest
from common import common_func as cf
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _child,
    _core_children,
    _equal,
    _record,
)
from ml_dtypes import bfloat16
from pymilvus import DataType

VECTOR_CASES = [
    (dtype, kind, metric)
    for dtype in (DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR)
    for kind in ("FLAT", "HNSW", "DISKANN")
    for metric in ("L2", "IP", "COSINE")
] + [(DataType.INT8_VECTOR, "HNSW", metric) for metric in ("L2", "IP", "COSINE")]

VECTOR_CASES += [
    (DataType.BINARY_VECTOR, kind, metric) for kind in ("BIN_FLAT", "BIN_IVF_FLAT") for metric in ("HAMMING", "JACCARD")
]

VECTOR_CASES += [(DataType.FLOAT_VECTOR, "IVF_FLAT", metric) for metric in ("L2", "IP", "COSINE")]


class DiskannScoreAccuracyMismatch(AssertionError):
    """Strict score-budget deviation tracked by zilliztech/knowhere#1956."""


def _typed_vector(dtype, seed):
    rng = np.random.RandomState(seed)
    if dtype == DataType.BINARY_VECTOR:
        return rng.bytes(16)
    if dtype == DataType.INT8_VECTOR:
        return rng.randint(-16, 17, 32).astype(np.int8)
    values = rng.uniform(-1, 1, 32)
    if dtype == DataType.FLOAT16_VECTOR:
        return values.astype(np.float16)
    if dtype == DataType.BFLOAT16_VECTOR:
        return values.astype(bfloat16)
    return values.astype(np.float32).tolist()


def _decode(value, dtype):
    if dtype == DataType.BINARY_VECTOR:
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], bytes):
            value = value[0]
        return np.unpackbits(np.frombuffer(value, dtype=np.uint8)).astype(np.float64)
    values = np.asarray(value)
    if dtype == DataType.BFLOAT16_VECTOR and values.dtype == np.uint16:
        values = (values.astype(np.uint32) << 16).view(np.float32)
    return values.astype(np.float64)


def _normalized(row, dtype):
    row = copy.deepcopy(row)
    for element in row["clips"] or []:
        for field in ("emb", "newvec"):
            if element.get(field) is not None:
                element[field] = _decode(element[field], dtype).tolist()
    return row


class StructArrayTypedVectorHelpersBase(StructArrayNullableBase):
    def _typed_collection(self, client, dtype, kind, metric):
        # Reuse the enhancement schema family; only the vector representation varies.
        name = cf.gen_unique_str("struct_vector_matrix")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("revision", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=8)
        children = _core_children()
        children["emb"] = _child(dtype, dim=128 if dtype == DataType.BINARY_VECTOR else 32)
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
        return name

    def _typed_rows(self, dtype):
        rows = []
        for pk in range(1200):
            clips = [
                {
                    "anchor": j,
                    "score": None if j < 2 else pk,
                    "label": None,
                    "emb": None if j % 2 == 0 or pk < 16 else _typed_vector(dtype, pk * 17 + j),
                    "tags": [None, pk] if j % 2 else [],
                }
                for j in range(4)
            ]
            rows.append(_record(pk, None if pk % 19 == 0 else [] if pk % 19 == 1 else clips))
        return rows

    def _typed_check(self, client, name, model, dtype):
        rows, _ = self.query(client, name, filter="", output_fields=["*"], limit=len(model) + 10)
        assert len(rows) == len(model) and {r["id"] for r in rows} == set(model)
        for row in rows:
            _equal(_normalized(row, dtype), _normalized(model[row["id"]], dtype))

    def _typed_search(self, client, name, model, dtype, kind, metric, score_deviations=None):
        query = _typed_vector(dtype, 900001)
        a = _decode(query, dtype)
        for filtered in (False, True):
            scores = {}
            for pk, row in model.items():
                for offset, element in enumerate(row["clips"] or []):
                    if element["emb"] is None or (filtered and element["score"] is not None):
                        continue
                    b = _decode(element["emb"], dtype)
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
            assert recall >= (1.0 if kind in ("FLAT", "BIN_FLAT", "IVF_FLAT", "BIN_IVF_FLAT") else 0.95)
            distances = [hit["distance"] for hit in hits]
            assert distances == sorted(distances, reverse=reverse)
            for hit, coordinate in zip(hits, coordinates):
                expected_score = pytest.approx(scores[coordinate], abs=2e-4, rel=2e-4)
                if score_deviations is None:
                    assert hit["distance"] == expected_score
                elif hit["distance"] != expected_score:
                    # Defer only the score assertion so mutation/reload and all
                    # structural checks still run before the issue is classified.
                    score_deviations.append(
                        {"coordinate": coordinate, "actual": hit["distance"], "expected": scores[coordinate]}
                    )
                actual = _normalized({"clips": hit["entity"]["clips"]}, dtype)
                _equal(actual, _normalized({"clips": model[hit["id"]]["clips"]}, dtype))
